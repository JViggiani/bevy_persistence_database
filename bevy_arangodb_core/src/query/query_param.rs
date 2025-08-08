//! Implements a Bevy SystemParam for querying entities from both world and database
//! in a seamless, integrated way.

use std::collections::HashSet;
use bevy::prelude::{Commands, Entity, Query, Res, ResMut, Resource, Local};
use bevy::ecs::query::{QueryData, QueryFilter};
use bevy::ecs::system::SystemParam;

use crate::query::expression::Expression;
use crate::query::persistence_query::WithComponentExt;
use crate::{DatabaseConnectionResource, PersistenceSession};
use crate::query::PersistenceQuery;

/// Caching policy for persistent queries
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CachePolicy {
    /// Use cache if possible, only query DB when needed
    UseCache,
    /// Always refresh from the database
    ForceRefresh,
}

impl Default for CachePolicy {
    fn default() -> Self {
        CachePolicy::UseCache
    }
}

/// Cache for persistent queries to avoid duplicating database calls
#[derive(Resource, Default)]
pub struct PersistenceQueryCache {
    cache: HashSet<u64>,
}

// Separate struct for fields that don't implement SystemParam
// This will be the "state" part of our SystemParam
#[derive(Default)]
pub struct PersistentQueryState {
    loaded_this_frame: bool,
    additional_components: Vec<&'static str>,
    filter_expr: Option<Expression>,
    cache_policy: CachePolicy,
    query_hash: Option<u64>,
}

/// System parameter for querying entities from both the world and database
#[derive(SystemParam)]
pub struct PersistentQuery<'w, 's, Q: QueryData + 'static, F: QueryFilter + 'static = ()> {
    /// The underlying world query
    query: Query<'w, 's, (Entity, Q), F>,
    /// The database connection
    db: Res<'w, DatabaseConnectionResource>,
    /// The persistence session
    session: Res<'w, PersistenceSession>,
    /// The query cache
    cache: ResMut<'w, PersistenceQueryCache>,
    /// Commands for spawning entities
    commands: Commands<'w, 's>,
    /// Local state that persists across invocations
    state: Local<'s, PersistentQueryState>,
}

impl<'w, 's, Q: QueryData<ReadOnly = Q> + 'static, F: QueryFilter + 'static> PersistentQuery<'w, 's, Q, F> {
    /// Iterate over entities with the given components, loading from the database if necessary
    pub fn iter_with_loading(&mut self) -> impl Iterator<Item = (Entity, Q::Item<'_>)> {
        // Only load from DB once per frame
        if !self.state.loaded_this_frame {
            // Skip if we've seen this query before and the cache policy allows using cache
            let should_query_db = match (self.state.cache_policy, self.state.query_hash) {
                (CachePolicy::ForceRefresh, _) => true,
                (CachePolicy::UseCache, Some(hash)) => !self.cache.cache.contains(&hash),
                (CachePolicy::UseCache, None) => true,
            };
            
            if should_query_db {
                // Get component names from the query type
                let mut comp_names: Vec<&'static str> = Vec::new();
                
                // TODO: Use reflection or type name to get component names
                // For now, we rely on additional components being added explicitly
                comp_names.extend(self.state.additional_components.iter().copied());
                
                // Build the persistence query
                let mut query = PersistenceQuery::new(self.db.0.clone());
                
                // Add components
                for comp_name in &comp_names {
                    query = query.with_component(comp_name);
                }
                
                // Add filter if present
                if let Some(expr) = &self.state.filter_expr {
                    query = query.filter(expr.clone());
                }
                
                // Load entities using the helper method that works with immutable session access
                query.fetch_into_world_with_commands(
                    &self.db.0,
                    &self.session,
                    &mut self.commands
                );
                
                // Update cache if needed
                if let Some(hash) = self.state.query_hash {
                    self.cache.cache.insert(hash);
                }
                
                // Mark as loaded for this frame
                self.state.loaded_this_frame = true;
            }
        }
        
        // Return iterator over query results
        self.query.iter()
    }
    
    /// Add a component to load by name
    pub fn with_component(mut self, component_name: &'static str) -> Self {
        self.state.additional_components.push(component_name);
        // Invalidate hash
        self.state.query_hash = None;
        self
    }
    
    /// Add a filter expression
    pub fn filter(mut self, expression: Expression) -> Self {
        self.state.filter_expr = Some(expression);
        // Invalidate hash
        self.state.query_hash = None;
        self
    }
    
    /// Force a refresh from the database, bypassing the cache
    pub fn force_refresh(mut self) -> Self {
        self.state.cache_policy = CachePolicy::ForceRefresh;
        self
    }
}