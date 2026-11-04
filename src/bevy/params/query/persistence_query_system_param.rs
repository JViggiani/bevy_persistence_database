//! A Bevy `SystemParam` that reads entities from the world and requests database loads.
//!
//! Reads go through the inner `Query` only. `load` queues a load command; the rows are
//! materialized when this system's commands apply, so systems ordered after it see them in
//! the same frame and this system sees them on its next run.

use std::any::{TypeId, type_name};

use bevy::ecs::query::{QueryData, QueryFilter};
use bevy::ecs::system::SystemParam;
use bevy::prelude::{Commands, Entity, Local, Query, Res, World};

use crate::bevy::plugins::persistence_plugin::PersistencePluginConfig;
use crate::core::query::FilterExpression;

use super::cache::CachePolicy;
use super::load_options::LoadOptions;
use super::loader::{ComponentSelection, EntityLoad};
use super::presence_spec::{ToPresenceSpec, collect_presence_components};
use super::query_data_to_components::QueryDataToComponents;

/// System parameter for querying entities from both the world and database
#[derive(SystemParam)]
pub struct PersistentQueryParam<'w, 's, Q: QueryData + 'static, F: QueryFilter + 'static = ()> {
    /// The underlying world query
    pub(crate) query: Query<'w, 's, (Entity, Q), F>,
    /// Where load requests are queued
    pub(crate) commands: Commands<'w, 's>,
    /// Plugin configuration for defaults such as store
    pub(crate) config: Res<'w, PersistencePluginConfig>,
    /// Builder settings for the next load
    pub(crate) options: Local<'s, LoadOptions>,
}

/// Convenient alias mirroring Bevy's `Query<'w, 's, ...>` shape so lifetime
/// elision works at call sites (e.g. `PersistentQuery<&Health>`).
pub type PersistentQuery<'w, 's, Q, F = ()> = PersistentQueryParam<'w, 's, Q, F>;

impl<'w, 's, Q, F> PersistentQuery<'w, 's, Q, F>
where
    Q: QueryData + QueryDataToComponents,
    F: QueryFilter + ToPresenceSpec,
{
    /// Request a database load for this query's components, filters and builder settings.
    ///
    /// The load runs when this system's commands apply. A load whose results are already
    /// cached is skipped unless [`force_refresh`](Self::force_refresh) was set.
    pub fn load(&mut self) -> &mut Self {
        let options = std::mem::take(&mut *self.options);

        let mut fetch = Vec::new();
        Q::push_names(&mut fetch);
        let presence = <F as ToPresenceSpec>::to_presence_spec();
        if let Some(expr) = presence.expr() {
            collect_presence_components(expr, &mut fetch);
        }
        let selection = ComponentSelection {
            with: presence.withs().to_vec(),
            without: presence.withouts().to_vec(),
            fetch,
        };

        let load = EntityLoad::new(
            type_name::<Q>(),
            &[],
            selection,
            presence.expr().cloned(),
            false,
            options,
            &self.config.default_store,
        );
        self.commands.queue(move |world: &mut World| load.run(world));
        self
    }

    /// Request a load fetched in pages of `page_size` documents, to bound memory per query.
    pub fn paginated_load(&mut self, page_size: usize) -> &mut Self {
        self.options.page_size = Some(page_size);
        self.load()
    }

    /// Add a value filter pushed down to the backend.
    /// Alias for `where(...)` to avoid raw-identifier call sites.
    pub fn filter(self, expr: FilterExpression) -> Self {
        self.r#where(expr)
    }

    /// Add a value filter pushed down to the backend.
    pub fn r#where(mut self, expr: FilterExpression) -> Self {
        self.options.filter = Some(expr);
        self
    }

    /// Force a refresh from the database, bypassing the cache.
    pub fn force_refresh(mut self) -> Self {
        self.options.cache_policy = CachePolicy::ForceRefresh;
        self
    }

    /// Select the store to query against for the next load.
    pub fn store(mut self, store: impl Into<String>) -> Self {
        self.options.store = Some(store.into());
        self
    }

    /// Request loading of a specific relationship type to the given depth.
    pub fn with_relationship_depth<R: 'static>(mut self, depth: usize) -> Self {
        self.options
            .relationships
            .per_type
            .insert(TypeId::of::<R>(), depth);
        self
    }

    /// Request loading of all registered relationship types to the given depth.
    pub fn with_all_relationship_depth(mut self, depth: usize) -> Self {
        self.options.relationships.all = Some(depth);
        self
    }
}

// World reads: deref to the inner Query.
impl<'w, 's, Q: QueryData + 'static, F: QueryFilter + 'static> std::ops::Deref
    for PersistentQueryParam<'w, 's, Q, F>
{
    type Target = Query<'w, 's, (Entity, Q), F>;
    fn deref(&self) -> &Self::Target {
        &self.query
    }
}

impl<'w, 's, Q: QueryData + 'static, F: QueryFilter + 'static> std::ops::DerefMut
    for PersistentQueryParam<'w, 's, Q, F>
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.query
    }
}
