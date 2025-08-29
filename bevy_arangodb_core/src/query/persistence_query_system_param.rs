//! Implements a Bevy SystemParam for querying entities from both world and database
//! in a seamless, integrated way.

use bevy::ecs::query::{QueryData, QueryFilter};
use bevy::ecs::system::SystemParam;
use bevy::prelude::{Entity, Query, Res, World};

use crate::plugins::persistence_plugin::TokioRuntime;
use crate::query::filter_expression::FilterExpression;
use crate::{DatabaseConnectionResource};

use crate::query::cache::{CachePolicy, PersistenceQueryCache};
use crate::query::deferred_ops::DeferredWorldOperations;
use crate::query::immediate_world_ptr::ImmediateWorldPtr;
use crate::query::presence_spec::ToPresenceSpec;
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

impl<'w, 's, Q, F> PersistentQuery<'w, 's, Q, F>
where
    Q: QueryData<ReadOnly = Q> + 'static + QueryDataToComponents + bevy::ecs::query::ReadOnlyQueryData,
    F: QueryFilter + 'static + ToPresenceSpec,
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

    /// Collect matching entity IDs immediately using a fresh QueryState.
    /// No mutation of `self`, so callers don't need `mut`.
    fn entities_now(&self) -> Vec<Entity> {
        bevy::log::debug!("PersistentQuery::entities_now called; has_world_ptr={}", self.world_ptr.is_some());
        if let Some(ptr) = &self.world_ptr {
            // Need to use mutable world reference for world.query()
            let world: &mut World = ptr.as_world_mut();
            
            // IMPORTANT: Use the same filter type F as the original query
            let v: Vec<Entity> = world.query_filtered::<(Entity, Q), F>()
                .iter(world)
                .map(|(e, _)| e)
                .collect();
                
            bevy::log::debug!("PersistentQuery::entities_now returning {} entities", v.len());
            return v;
        }
        // If no immediate world access, fall back to the stale query
        self.query.iter().map(|(e, _)| e).collect()
    }

    /// Returns an iterator over all matching entities with up-to-date world state,
    /// including entities loaded in the current frame.
    /// This overrides the `iter()` method from `Deref<Target=Query>`.
    pub fn iter(&self) -> Box<dyn Iterator<Item = <(Entity, Q) as QueryData>::Item<'_>> + '_> {
        bevy::log::debug!("PersistentQuery::iter called (fresh version)");
        
        if let Some(ptr) = &self.world_ptr {
            let world = ptr.as_world_mut();
            
            // Get entities first, then filter map through the query
            let entities = self.entities_now();
            bevy::log::debug!("PersistentQuery::iter using {} entities from entities_now()", entities.len());
            
            // Create fresh query state and use it directly
            let mut query_state = world.query_filtered::<(Entity, Q), F>();
            
            // Collect into Vec to avoid lifetime issues with the query state
            let results: Vec<_> = entities.into_iter()
                .filter_map(|entity| query_state.get(world, entity).ok())
                .collect();
            
            bevy::log::debug!("PersistentQuery::iter collected {} results", results.len());
            return Box::new(results.into_iter());
        }
        
        // Fall back to the original query if no immediate world access
        Box::new(self.query.iter())
    }

    /// Returns a mutable iterator over all matching entities with up-to-date world state.
    /// This overrides the `iter_mut()` method from `Deref<Target=Query>`.
    pub fn iter_mut(&mut self) -> Box<dyn Iterator<Item = <(Entity, Q) as QueryData>::Item<'_>> + '_> {
        bevy::log::debug!("PersistentQuery::iter_mut called (fresh version)");
        
        if let Some(ptr) = &self.world_ptr {
            let world = ptr.as_world_mut();
            
            // Get entities first, then filter map through the query
            let entities = self.entities_now();
            bevy::log::debug!("PersistentQuery::iter_mut using {} entities from entities_now()", entities.len());
            
            // Create fresh query state and use it directly
            let mut query_state = world.query_filtered::<(Entity, Q), F>();
            
            // Collect into Vec to avoid lifetime issues with the query state
            let results: Vec<_> = entities.into_iter()
                .filter_map(|entity| query_state.get(world, entity).ok())
                .collect();
            
            bevy::log::debug!("PersistentQuery::iter_mut collected {} results", results.len());
            return Box::new(results.into_iter());
        }
        
        // Fall back to the original query if no immediate world access
        Box::new(self.query.iter_mut())
    }

    /// Gets data for a specific entity with up-to-date world state.
    /// This overrides the `get()` method from `Deref<Target=Query>`.
    pub fn get(&self, entity: Entity) -> Result<<(Entity, Q) as QueryData>::Item<'_>, bevy::ecs::query::QueryEntityError> {
        bevy::log::debug!("PersistentQuery::get called (fresh version)");
        if let Some(ptr) = &self.world_ptr {
            let world = ptr.as_world_mut();
            let mut query_state = world.query_filtered::<(Entity, Q), F>();
            query_state.get(world, entity)
        } else {
            self.query.get(entity)
        }
    }

    /// Gets mutable data for a specific entity with up-to-date world state.
    /// This overrides the `get_mut()` method from `Deref<Target=Query>`.
    pub fn get_mut(&mut self, entity: Entity) -> Result<<(Entity, Q) as QueryData>::Item<'_>, bevy::ecs::query::QueryEntityError> {
        bevy::log::debug!("PersistentQuery::get_mut called (fresh version)");
        if let Some(ptr) = &self.world_ptr {
            let world = ptr.as_world_mut();
            let mut query_state = world.query_filtered::<(Entity, Q), F>();
            query_state.get(world, entity)
        } else {
            self.query.get_mut(entity)
        }
    }

    /// Returns a single entity result with up-to-date world state.
    /// This overrides the `single()` method from `Deref<Target=Query>`.
    pub fn single(&self) -> Result<<(Entity, Q) as QueryData>::Item<'_>, bevy::ecs::query::QuerySingleError> {
        bevy::log::debug!("PersistentQuery::single called (fresh version)");
        if let Some(ptr) = &self.world_ptr {
            let world = ptr.as_world_mut();
            let mut query_state = world.query_filtered::<(Entity, Q), F>();
            query_state.single(world)
        } else {
            self.query.single()
        }
    }

    /// Returns a single mutable entity result with up-to-date world state.
    /// This overrides the `single_mut()` method from `Deref<Target=Query>`.
    pub fn single_mut(&mut self) -> Result<<(Entity, Q) as QueryData>::Item<'_>, bevy::ecs::query::QuerySingleError> {
        bevy::log::debug!("PersistentQuery::single_mut called (fresh version)");
        if let Some(ptr) = &self.world_ptr {
            let world = ptr.as_world_mut();
            let mut query_state = world.query_filtered::<(Entity, Q), F>();
            query_state.single(world)
        } else {
            self.query.single_mut()
        }
    }

    /// Gets data for multiple entities with up-to-date world state.
    /// This overrides the `get_many()` method from `Deref<Target=Query>`.
    pub fn get_many<const N: usize>(
        &self,
        entities: [Entity; N],
    ) -> Result<
        [<< (Entity, Q) as QueryData>::ReadOnly as QueryData>::Item<'_>; N],
        bevy::ecs::query::QueryEntityError,
    > {
        bevy::log::debug!(
            "PersistentQuery::get_many called; has_world_ptr={}; N={}; entities={:?}",
            self.world_ptr.is_some(),
            N,
            entities
        );
        if let Some(ptr) = &self.world_ptr {
            let world = ptr.as_world_mut();
            
            // Log entity existence and component match for debugging
            for (i, &entity) in entities.iter().enumerate() {
                if !world.entities().contains(entity) {
                    bevy::log::warn!("Entity at index {} ({:?}) does not exist in world", i, entity);
                }
            }
            
            let mut query_state = world.query_filtered::<(Entity, Q), F>();
            match query_state.get_many(world, entities) {
                Ok(result) => {
                    bevy::log::debug!("get_many succeeded");
                    Ok(result)
                }
                Err(err) => {
                    bevy::log::warn!("get_many failed: {:?}", err);
                    Err(err)
                }
            }
        } else {
            self.query.get_many(entities)
        }
    }

    /// Gets mutable data for multiple entities with up-to-date world state.
    /// This overrides the `get_many_mut()` method from `Deref<Target=Query>`.
    pub fn get_many_mut<const N: usize>(
        &mut self,
        entities: [Entity; N],
    ) -> Result<
        [<< (Entity, Q) as QueryData>::ReadOnly as QueryData>::Item<'_>; N],
        bevy::ecs::query::QueryEntityError,
    > {
        bevy::log::debug!(
            "PersistentQuery::get_many_mut called; has_world_ptr={}; N={}",
            self.world_ptr.is_some(),
            N
        );
        if let Some(ptr) = &self.world_ptr {
            let world = ptr.as_world_mut();
            let mut query_state = world.query_filtered::<(Entity, Q), F>();
            query_state.get_many(world, entities)
        } else {
            self.query.get_many_mut(entities)
        }
    }
    
    /// Iterates over a specific set of entities that match the query.
    /// This overrides the `iter_many()` method from `Deref<Target=Query>`.
    pub fn iter_many<EntityList: IntoIterator<Item = Entity>>(
        &self,
        entities: EntityList,
    ) -> Box<dyn Iterator<Item = <(Entity, Q) as QueryData>::Item<'_>> + '_> {
        bevy::log::debug!("PersistentQuery::iter_many called (fresh version)");
        
        if let Some(ptr) = &self.world_ptr {
            let world = ptr.as_world_mut();
            
            // Collect entities into a Vec
            let entity_vec: Vec<Entity> = entities.into_iter().collect();
            bevy::log::debug!("PersistentQuery::iter_many using {} entities", entity_vec.len());
            
            // Create fresh query state and use it directly
            let mut query_state = world.query_filtered::<(Entity, Q), F>();
            
            // Collect into Vec to avoid lifetime issues with the query state
            let results: Vec<_> = entity_vec.into_iter()
                .filter_map(|entity| query_state.get(world, entity).ok())
                .collect();
            
            bevy::log::debug!("PersistentQuery::iter_many collected {} results", results.len());
            return Box::new(results.into_iter());
        }
        
        // Fall back to the original query if no immediate world access
        let entity_vec: Vec<Entity> = entities.into_iter().collect();
        Box::new(self.query.iter_many(entity_vec))
    }

    /// Iterates with mutable access over a specific set of entities that match the query.
    /// This overrides the `iter_many_mut()` method from `Deref<Target=Query>`.
    pub fn iter_many_mut<EntityList: IntoIterator<Item = Entity>>(
        &mut self,
        entities: EntityList,
    ) -> Box<dyn Iterator<Item = <(Entity, Q) as QueryData>::Item<'_>> + '_> {
        bevy::log::debug!("PersistentQuery::iter_many_mut called (fresh version)");
        
        if let Some(ptr) = &self.world_ptr {
            let world = ptr.as_world_mut();
            
            // Collect entities into a Vec
            let entity_vec: Vec<Entity> = entities.into_iter().collect();
            bevy::log::debug!("PersistentQuery::iter_many_mut using {} entities", entity_vec.len());
            
            // Create fresh query state and use it directly
            let mut query_state = world.query_filtered::<(Entity, Q), F>();
            
            // Collect into Vec to avoid lifetime issues with the query state
            let results: Vec<_> = entity_vec.into_iter()
                .filter_map(|entity| query_state.get(world, entity).ok())
                .collect();
            
            bevy::log::debug!("PersistentQuery::iter_many_mut collected {} results", results.len());
            return Box::new(results.into_iter());
        }
        
        // Fall back to the original query if no immediate world access
        let entity_vec: Vec<Entity> = entities.into_iter().collect();
        Box::new(self.query.iter_many_mut(entity_vec))
    }

    /// Returns an iterator over all combinations of N entities with up-to-date world state.
    /// This overrides the `iter_combinations()` method from `Deref<Target=Query>`.
    pub fn iter_combinations<const N: usize>(&self) -> Box<dyn Iterator<Item = [<(Entity, Q) as QueryData>::Item<'_>; N]> + '_> {
        bevy::log::debug!("PersistentQuery::iter_combinations called (fresh version)");
        // We'll keep this implementation since it doesn't have reference lifetime issues
        if let Some(_ptr) = &self.world_ptr {
            // Get all entities and manually build combinations
            let entities = self.entities_now();
            
            // Generate combinations of entity IDs
            struct Combinations<I> {
                indices: Vec<usize>,
                items: Vec<I>,
                done: bool,
                n: usize,
            }
            
            impl<I: Clone> Iterator for Combinations<I> {
                type Item = Vec<I>;
                
                fn next(&mut self) -> Option<Self::Item> {
                    if self.done || self.items.len() < self.n {
                        return None;
                    }
                    
                    let result: Vec<I> = self.indices.iter().map(|&i| self.items[i].clone()).collect();
                    
                    // Find the rightmost index that can be incremented
                    let mut i = self.n - 1;
                    while i > 0 && self.indices[i] == self.items.len() - (self.n - i) {
                        i -= 1;
                    }
                    
                    // If we couldn't find one, we're done
                    if i == 0 && self.indices[0] == self.items.len() - self.n {
                        self.done = true;
                    } else {
                        // Increment the index and reset all to the right
                        self.indices[i] += 1;
                        for j in i+1..self.n {
                            self.indices[j] = self.indices[j-1] + 1;
                        }
                    }
                    
                    Some(result)
                }
            }
            
            let combinations = Combinations {
                indices: (0..N).collect(),
                items: entities,
                done: false,
                n: N,
            };
            
            // Map combinations of entity IDs to combinations of query results
            Box::new(combinations.filter_map(move |combo| {
                if combo.len() != N {
                    return None;
                }
                
                let entity_array: [Entity; N] = match combo.try_into() {
                    Ok(arr) => arr,
                    Err(_) => return None,
                };
                
                match self.get_many(entity_array) {
                    Ok(results) => Some(results),
                    Err(_) => None,
                }
            }))
        } else {
            // Without immediate world access, use regular query
            Box::new(self.query.iter_combinations::<N>())
        }
    }

    /// Returns an iterator over all combinations of N entities with mutable access and up-to-date world state.
    /// This overrides the `iter_combinations_mut()` method from `Deref<Target=Query>`.
    pub fn iter_combinations_mut<const N: usize>(&mut self) -> Box<dyn Iterator<Item = [<(Entity, Q) as QueryData>::Item<'_>; N]> + '_> {
        bevy::log::debug!("PersistentQuery::iter_combinations_mut called (fresh version)");
        if let Some(_ptr) = &self.world_ptr {
            // Same implementation as iter_combinations since we create fresh QueryState anyway
            self.iter_combinations::<N>()
        } else {
            // Without immediate world access, use regular query
            Box::new(self.query.iter_combinations_mut::<N>())
        }
    }

    /// Checks if the given entity matches the query.
    /// This overrides the `contains()` method from `Deref<Target=Query>`.
    pub fn contains(&self, entity: Entity) -> bool {
        bevy::log::debug!("PersistentQuery::contains called (fresh version)");
        if let Some(ptr) = &self.world_ptr {
            let world = ptr.as_world_mut();
            
            // The contains method needs ticks for change detection
            let last_change_tick = world.last_change_tick();
            let change_tick = world.change_tick();
            
            let query_state = world.query_filtered::<(Entity, Q), F>();
            query_state.contains(entity, world, last_change_tick, change_tick)
        } else {
            self.query.contains(entity)
        }
    }
}

// World-only pass-through: Deref to the inner Query for compatibility.
// Note: Methods directly on PersistentQuery (iter, get, etc.) override these
// and provide up-to-date views of the world.
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