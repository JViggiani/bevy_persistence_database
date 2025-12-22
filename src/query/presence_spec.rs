use crate::Persist;
use crate::query::filter_expression::{BinaryOperator, FilterExpression};
use bevy::prelude::{Or, With, Without};

#[derive(Default, Clone)]
pub struct PresenceSpec {
    withs: Vec<&'static str>,
    withouts: Vec<&'static str>,
    expr: Option<FilterExpression>,
}

impl PresenceSpec {
    pub fn withs(&self) -> &[&'static str] {
        &self.withs
    }
    pub fn withouts(&self) -> &[&'static str] {
        &self.withouts
    }
    pub fn expr(&self) -> Option<&FilterExpression> {
        self.expr.as_ref()
    }

    pub(crate) fn merge_and(mut self, other: PresenceSpec) -> PresenceSpec {
        self.withs.extend(other.withs);
        self.withouts.extend(other.withouts);
        self.expr = match (self.expr.take(), other.expr) {
            (Some(a), Some(b)) => Some(a.and(b)),
            (Some(a), None) => Some(a),
            (None, Some(b)) => Some(b),
            (None, None) => None,
        };
        self
    }

    pub(crate) fn to_expr(&self) -> Option<FilterExpression> {
        let mut acc: Option<FilterExpression> = None;
        for n in &self.withs {
            let field = FilterExpression::field(*n, "");
            let e = FilterExpression::BinaryOperator {
                op: BinaryOperator::Ne,
                lhs: Box::new(field),
                rhs: Box::new(FilterExpression::Literal(serde_json::Value::Null)),
            };
            acc = Some(match acc {
                Some(cur) => cur.and(e),
                None => e,
            });
        }
        for n in &self.withouts {
            let field = FilterExpression::field(*n, "");
            let e = FilterExpression::BinaryOperator {
                op: BinaryOperator::Eq,
                lhs: Box::new(field),
                rhs: Box::new(FilterExpression::Literal(serde_json::Value::Null)),
            };
            acc = Some(match acc {
                Some(cur) => cur.and(e),
                None => e,
            });
        }
        match (&acc, &self.expr) {
            (Some(a), Some(b)) => Some(a.clone().and(b.clone())),
            (Some(a), None) => Some(a.clone()),
            (None, Some(b)) => Some(b.clone()),
            (None, None) => None,
        }
    }
}

// Guard trait for supported filter forms.
pub trait FilterSupported {}
impl FilterSupported for () {}
impl<T: bevy::prelude::Component + Persist> FilterSupported for With<T> {}
impl<T: bevy::prelude::Component + Persist> FilterSupported for Without<T> {}
impl<T: FilterSupportedTuple> FilterSupported for Or<T> {}
impl<T: FilterSupportedTuple> FilterSupported for T {}

// Helper trait to mark tuples as supported
pub trait FilterSupportedTuple {}
macro_rules! impl_filter_supported_tuple {
    ( $( $name:ident ),+ ) => {
        impl<$( $name: FilterSupported ),+> FilterSupportedTuple for ( $( $name, )+ ) {}
    };
}
impl_filter_supported_tuple!(A);
impl_filter_supported_tuple!(A, B);
impl_filter_supported_tuple!(A, B, C);
impl_filter_supported_tuple!(A, B, C, D);
impl_filter_supported_tuple!(A, B, C, D, E);
impl_filter_supported_tuple!(A, B, C, D, E, F);
impl_filter_supported_tuple!(A, B, C, D, E, F, G);
impl_filter_supported_tuple!(A, B, C, D, E, F, G, H);

// Core trait: extract presence spec from F
pub trait ToPresenceSpec {
    fn to_presence_spec() -> PresenceSpec;
}
impl ToPresenceSpec for () {
    fn to_presence_spec() -> PresenceSpec {
        PresenceSpec::default()
    }
}
impl<T: bevy::prelude::Component + Persist> ToPresenceSpec for With<T> {
    fn to_presence_spec() -> PresenceSpec {
        PresenceSpec {
            withs: vec![T::name()],
            withouts: vec![],
            expr: None,
        }
    }
}
impl<T: bevy::prelude::Component + Persist> ToPresenceSpec for Without<T> {
    fn to_presence_spec() -> PresenceSpec {
        PresenceSpec {
            withs: vec![],
            withouts: vec![T::name()],
            expr: None,
        }
    }
}

// Tuple AND: merge specs and AND any expression branches
macro_rules! impl_to_presence_for_tuple {
    ( $( $name:ident ),+ ) => {
        impl<$( $name: ToPresenceSpec ),+> ToPresenceSpec for ( $( $name, )+ ) {
            fn to_presence_spec() -> PresenceSpec {
                let mut out = PresenceSpec::default();
                $( { out = out.merge_and(<$name as ToPresenceSpec>::to_presence_spec()); } )+
                out
            }
        }
    };
}
impl_to_presence_for_tuple!(A);
impl_to_presence_for_tuple!(A, B);
impl_to_presence_for_tuple!(A, B, C);
impl_to_presence_for_tuple!(A, B, C, D);
impl_to_presence_for_tuple!(A, B, C, D, E);
impl_to_presence_for_tuple!(A, B, C, D, E, F);
impl_to_presence_for_tuple!(A, B, C, D, E, F, G);
impl_to_presence_for_tuple!(A, B, C, D, E, F, G, H);

// Or of a tuple: build an OR expression of each alternative's presence constraints
macro_rules! impl_to_presence_for_or_tuple {
    ( $( $name:ident ),+ ) => {
        impl<$( $name: ToPresenceSpec ),+> ToPresenceSpec for Or<( $( $name, )+ )> {
            fn to_presence_spec() -> PresenceSpec {
                let parts: Vec<Option<FilterExpression>> = vec![
                    $( <$name as ToPresenceSpec>::to_presence_spec().to_expr(), )+
                ];
                let mut or_expr: Option<FilterExpression> = None;
                for p in parts.into_iter().flatten() {
                    or_expr = Some(match or_expr { Some(cur) => cur.or(p), None => p });
                }
                PresenceSpec { withs: vec![], withouts: vec![], expr: or_expr }
            }
        }
    };
}
impl_to_presence_for_or_tuple!(A, B);
impl_to_presence_for_or_tuple!(A, B, C);
impl_to_presence_for_or_tuple!(A, B, C, D);
impl_to_presence_for_or_tuple!(A, B, C, D, E);
impl_to_presence_for_or_tuple!(A, B, C, D, E, F);
impl_to_presence_for_or_tuple!(A, B, C, D, E, F, G);
impl_to_presence_for_or_tuple!(A, B, C, D, E, F, G, H);

// Helper: collect component names referenced by presence expressions (component object checks).
pub fn collect_presence_components(expr: &FilterExpression, acc: &mut Vec<&'static str>) {
    match expr {
        FilterExpression::Field {
            component_name,
            field_name,
        } => {
            if field_name.is_empty() && !acc.contains(component_name) {
                acc.push(component_name);
            }
        }
        FilterExpression::BinaryOperator { lhs, rhs, .. } => {
            collect_presence_components(lhs, acc);
            collect_presence_components(rhs, acc);
        }
        _ => {}
    }
}
