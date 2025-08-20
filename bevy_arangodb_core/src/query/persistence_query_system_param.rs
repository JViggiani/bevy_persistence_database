//! Implements a Bevy SystemParam for querying entities from both world and database
//! in a seamless, integrated way.

use bevy::ecs::query::{QueryData, QueryFilter};
use bevy::ecs::system::SystemParam;
use bevy::prelude::{Entity, Query, Res};

use crate::plugins::persistence_plugin::TokioRuntime;
use crate::query::filter_expression::FilterExpression;
use crate::{DatabaseConnectionResource};

use crate::query::cache::{CachePolicy, PersistenceQueryCache};
use crate::query::deferred_ops::DeferredWorldOperations;
use crate::query::immediate_world_ptr::ImmediateWorldPtr;
use crate::query::presence_spec::{ToPresenceSpec, FilterSupported};
use crate::query::presence_spec::collect_presence_components;
use crate::query::query_data_to_components::QueryDataToComponents;
use crate::query::tls_config::{
    set_filter, take_filter, set_cache_policy, take_cache_policy,
    drain_additional_components, drain_without_components,
};


/// System parameter for querying entities from both the world and database
#[derive(SystemParam)]
pub struct PersistentQuery<'w, 's, Q: QueryData + 'static, F: QueryFilter + 'static = ()> {
    /// The underlying world query
    pub(crate) query: Query<'w, 's, (Entity, Q), F>,
    /// The database connection
    pub(crate) db: Res<'w, DatabaseConnectionResource>,
    /// The query cache - using immutable access with interior mutability
    pub(crate) cache: Res<'w, PersistenceQueryCache>,
    /// Runtime to drive async DB calls
    pub(crate) runtime: Res<'w, TokioRuntime>,
    /// Add access to the deferred ops queue (immutable; interior mutability)
    pub(crate) ops: Res<'w, DeferredWorldOperations>,
    /// Optional: immediate world access for in-system materialization
    pub(crate) world_ptr: Option<Res<'w, ImmediateWorldPtr>>,
}

impl<'w, 's, Q: QueryData<ReadOnly = Q> + 'static, F: QueryFilter + 'static>
    PersistentQuery<'w, 's, Q, F>
where
    F: ToPresenceSpec + FilterSupported,
    Q: QueryDataToComponents,
{
    /// Explicit load trigger that performs DB I/O (if needed) and returns self for pass-through use.
    /// This does not directly mutate the world; world mutations are applied by the plugin in PostUpdate.
    pub fn ensure_loaded(&mut self) -> &mut Self {
        bevy::log::debug!("PersistentQuery::ensure_loaded called");

        // Drain transient config from TLS for this call
        let mut fetch_names: Vec<&'static str> = Vec::new();
        let mut presence_names: Vec<&'static str> = drain_additional_components();
        let mut without_names: Vec<&'static str> = drain_without_components();
        let tls_filter_expression: Option<FilterExpression> = take_filter();
        let cache_policy: CachePolicy = take_cache_policy();

        // 1) Type-driven component extraction from Q (fetch targets, not presence gates)
        Q::push_names(&mut fetch_names);

        // 2) Merge type-driven presence from F (presence gates + ORs)
        let type_presence = <F as ToPresenceSpec>::to_presence_spec();
        presence_names.extend(type_presence.withs().iter().copied());
        without_names.extend(type_presence.withouts().iter().copied());

        // Collect components referenced by presence expr branches so we fetch them too
        if let Some(expr) = type_presence.expr() {
            collect_presence_components(expr, &mut fetch_names);
        }

        // Ensure fetch includes presence-gated components
        for &n in &presence_names {
            if !fetch_names.contains(&n) {
                fetch_names.push(n);
            }
        }

        // Dedup
        Self::sort_dedup(&mut fetch_names);
        Self::sort_dedup(&mut presence_names);
        Self::sort_dedup(&mut without_names);

        // 3) Combine presence-derived expression (from Or cases) with TLS filter via AND
        let combined_expr: Option<FilterExpression> = match (type_presence.expr().cloned(), tls_filter_expression) {
            (Some(a), Some(b)) => Some(a.and(b)),
            (Some(a), None) => Some(a),
            (None, Some(b)) => Some(b),
            (None, None) => None,
        };

        // Reuse the shared executor
        self.execute_combined_load(
            cache_policy,
            presence_names,
            without_names,
            fetch_names,
            combined_expr,
            &[], // no extra salt
            false, // don't force full docs for plain ensure_loaded
        );

        self
    }

    /// Add a value filter pushed down to the backend.
    /// Alias for `where(...)` to avoid raw-identifier call sites.
    pub fn filter(self, expr: FilterExpression) -> Self {
        self.r#where(expr)
    }

    /// Add a value filter pushed down to the backend.
    pub fn r#where(self, expr: FilterExpression) -> Self {
        set_filter(expr);
        self
    }

    /// Force a refresh from the database, bypassing the cache.
    pub fn force_refresh(self) -> Self {
        set_cache_policy(CachePolicy::ForceRefresh);
        self
    }

    /// Small helper: sort + dedup in-place.
    #[inline]
    fn sort_dedup<T: Ord>(v: &mut Vec<T>) {
        v.sort_unstable();
        v.dedup();
    }
}

// World-only pass-through: Deref to the inner Query so `iter/get/single/...` are available.
impl<'w, 's, Q: QueryData + 'static, F: QueryFilter + 'static> std::ops::Deref
    for PersistentQuery<'w, 's, Q, F>
{
    type Target = Query<'w, 's, (Entity, Q), F>;
    fn deref(&self) -> &Self::Target {
        &self.query
    }
}

impl<'w, 's, Q: QueryData + 'static, F: QueryFilter + 'static> std::ops::DerefMut
    for PersistentQuery<'w, 's, Q, F>
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.query
    }
}