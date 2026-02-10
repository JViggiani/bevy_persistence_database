// Map Bevy QueryData types to component Persist::name()s for backend fetch lists.
use crate::bevy::components::Guid;
use crate::core::persist::Persist;

pub trait QueryDataToComponents {
    fn push_names(acc: &mut Vec<&'static str>);
}

// &T
impl<T: bevy::prelude::Component + Persist> QueryDataToComponents for &T {
    fn push_names(acc: &mut Vec<&'static str>) {
        acc.push(T::name());
    }
}
// &mut T
impl<T: bevy::prelude::Component + Persist> QueryDataToComponents for &mut T {
    fn push_names(acc: &mut Vec<&'static str>) {
        acc.push(T::name());
    }
}
// Option<&T>
impl<T: bevy::prelude::Component + Persist> QueryDataToComponents for Option<&T> {
    fn push_names(acc: &mut Vec<&'static str>) {
        acc.push(T::name());
    }
}
// Option<&mut T>
impl<T: bevy::prelude::Component + Persist> QueryDataToComponents for Option<&mut T> {
    fn push_names(acc: &mut Vec<&'static str>) {
        acc.push(T::name());
    }
}

// Special-case Guid: it is a component but not persisted as a document field.
// Treat it as non-fetching so PersistentQuery<&Guid, _> compiles without Persist.
impl QueryDataToComponents for &Guid {
    fn push_names(_acc: &mut Vec<&'static str>) {}
}
impl QueryDataToComponents for &mut Guid {
    fn push_names(_acc: &mut Vec<&'static str>) {}
}
impl QueryDataToComponents for Option<&Guid> {
    fn push_names(_acc: &mut Vec<&'static str>) {}
}
impl QueryDataToComponents for Option<&mut Guid> {
    fn push_names(_acc: &mut Vec<&'static str>) {}
}

// Tuples
macro_rules! impl_q_to_components_tuple {
    ( $( $name:ident ),+ ) => {
        impl<$( $name: QueryDataToComponents ),+> QueryDataToComponents for ( $( $name, )+ ) {
            fn push_names(acc: &mut Vec<&'static str>) {
                $( $name::push_names(acc); )+
            }
        }
    };
}
impl_q_to_components_tuple!(A);
impl_q_to_components_tuple!(A, B);
impl_q_to_components_tuple!(A, B, C);
impl_q_to_components_tuple!(A, B, C, D);
impl_q_to_components_tuple!(A, B, C, D, E);
impl_q_to_components_tuple!(A, B, C, D, E, F);
impl_q_to_components_tuple!(A, B, C, D, E, F, G);
impl_q_to_components_tuple!(A, B, C, D, E, F, G, H);
