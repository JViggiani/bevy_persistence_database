//! Implements a Bevy SystemParam for querying entities from both world and database
//! in a seamless, integrated way.

use std::collections::HashSet;
use std::sync::Mutex;
use bevy::prelude::{Commands, Entity, Query, Res, Resource, Local, ResMut, Mut};
use bevy::ecs::query::{QueryData, QueryFilter};
use bevy::ecs::system::SystemParam;
use bevy::ecs::system::Command;

use crate::query::expression::Expression;
use crate::query::persistence_query::WithComponentExt;
use crate::{DatabaseConnectionResource, PersistenceSession, Guid, BEVY_PERSISTENCE_VERSION_FIELD};
use crate::query::PersistenceQuery;
use crate::versioning::version_manager::VersionKey;

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
/// Uses interior mutability to allow multiple systems to access it safely
#[derive(Resource, Default)]
pub struct PersistenceQueryCache {
    cache: Mutex<HashSet<u64>>,
}

impl PersistenceQueryCache {
    /// Check if a query hash is in the cache
    pub fn contains(&self, hash: u64) -> bool {
        bevy::log::trace!("PersistenceQueryCache::contains - attempting to lock cache");
        let result = self.cache.lock().unwrap().contains(&hash);
        bevy::log::trace!("PersistenceQueryCache::contains - lock released, result: {}", result);
        result
    }
    
    /// Insert a query hash into the cache
    pub fn insert(&self, hash: u64) {
        bevy::log::trace!("PersistenceQueryCache::insert - attempting to lock cache");
        self.cache.lock().unwrap().insert(hash);
        bevy::log::trace!("PersistenceQueryCache::insert - lock released, hash {} inserted", hash);
    }
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
    /// The persistence session - needs to be mutable to update entity_keys and version_manager
    session: ResMut<'w, PersistenceSession>,
    /// The query cache - using immutable access with interior mutability
    cache: Res<'w, PersistenceQueryCache>,
    /// Commands for spawning entities
    commands: Commands<'w, 's>,
    /// Local state that persists across invocations
    state: Local<'s, PersistentQueryState>,
}

// A custom command to deserialize a component
struct DeserializeComponentCommand {
    entity: Entity,
    component_name: &'static str,
    json_value: serde_json::Value,
}

impl Command for DeserializeComponentCommand {
    fn apply(self, world: &mut bevy::prelude::World) {
        let entity = self.entity;
        let component_name = self.component_name;
        let json_value = self.json_value;

        // Use resource_scope to safely borrow the session and world together
        world.resource_scope(|world, session: Mut<PersistenceSession>| {
            if let Some(deserializer) = session.component_deserializers.get(component_name) {
                match deserializer(world, entity, json_value) {
                    Ok(_) => {
                        bevy::log::debug!("Added component {} to entity {:?}", component_name, entity);
                    }
                    Err(e) => {
                        bevy::log::error!("Failed to deserialize component {}: {}", component_name, e);
                    }
                }
            } else {
                bevy::log::warn!("No deserializer found for component: {}", component_name);
            }
        });
    }
}

impl<'w, 's, Q: QueryData<ReadOnly = Q> + 'static, F: QueryFilter + 'static> PersistentQuery<'w, 's, Q, F> {
    /// Iterate over entities with the given components, loading from the database if necessary
    /// This will block until database loading is complete to ensure consistency
    pub fn iter_with_loading(&mut self) -> impl Iterator<Item = (Entity, Q::Item<'_>)> {
        bevy::log::debug!("PersistentQuery::iter_with_loading called");
        
        // Only load from DB once per frame
        if !self.state.loaded_this_frame {
            bevy::log::debug!("First call this frame, checking if DB query needed");
            
            // Skip if we've seen this query before and the cache policy allows using cache
            let should_query_db = match (self.state.cache_policy, self.state.query_hash) {
                (CachePolicy::ForceRefresh, _) => {
                    bevy::log::debug!("Force refresh policy - will query DB");
                    true
                },
                (CachePolicy::UseCache, Some(hash)) => {
                    bevy::log::debug!("Checking cache for hash: {}", hash);
                    let in_cache = self.cache.contains(hash);
                    bevy::log::debug!("Cache check complete, in_cache: {}", in_cache);
                    !in_cache
                },
                (CachePolicy::UseCache, None) => {
                    bevy::log::debug!("No query hash set - will query DB");
                    true
                },
            };
            
            if should_query_db {
                bevy::log::debug!("Querying database");
                
                // Extract component names from the query type using type registration info
                let mut comp_names: Vec<&'static str> = Vec::new();
                
                // For now, just use the explicitly requested components
                comp_names.extend(self.state.additional_components.iter().copied());
                
                // If no components were specified, we still proceed and will insert any registered
                // components found in the documents.
                if comp_names.is_empty() {
                    bevy::log::info!("No explicit component filters; will load documents and insert any registered components present.");
                }
                
                bevy::log::debug!("Will query for components (if any): {:?}", comp_names);
                
                // Build the synchronous query
                let mut query = PersistenceQuery::new(self.db.0.clone());
                
                for comp_name in &comp_names {
                    query = query.with_component(comp_name);
                }
                
                if let Some(expr) = &self.state.filter_expr {
                    query = query.filter(expr.clone());
                }
                
                // Execute synchronously - this will block until complete
                bevy::log::info!("Executing synchronous database query for components: {:?}", comp_names);
                let (aql, bind_vars) = query.build_aql(true);
                
                match self.db.0.query_documents_sync(aql, bind_vars) {
                    Ok(documents) => {
                        bevy::log::debug!("Retrieved {} documents synchronously", documents.len());
                        
                        // Process each document, creating entities and components
                        self.process_documents(documents, &comp_names);
                        
                        if let Some(hash) = self.state.query_hash {
                            self.cache.insert(hash);
                        }
                    },
                    Err(e) => {
                        bevy::log::error!("Error fetching documents synchronously: {}", e);
                    }
                }
                
                // Mark as loaded for this frame
                self.state.loaded_this_frame = true;
            } else {
                bevy::log::debug!("Skipping DB query - using cached results");
            }
        } else {
            bevy::log::debug!("Already loaded this frame, returning cached query results");
        }
        
        // Return iterator over query results - now includes freshly loaded entities WITH their components
        self.query.iter()
    }
    
    // Private helper to process documents and create entities/components
    fn process_documents(&mut self, documents: Vec<serde_json::Value>, comp_names: &[&'static str]) {
        // Step 1: First create all the entities we need
        let key_field = self.db.0.document_key_field();
        let mut key_to_entity = std::collections::HashMap::new();

        for doc in &documents {
            if let Some(key_str) = doc.get(key_field).and_then(|v| v.as_str()) {
                let key = key_str.to_string();
                if let Some(entity) = self.session.entity_keys.iter()
                    .find(|(_, k)| **k == key)
                    .map(|(e, _)| *e)
                {
                    key_to_entity.insert(key.clone(), entity);
                } else {
                    let entity = self.commands.spawn(Guid::new(key.clone())).id();
                    self.session.entity_keys.insert(entity, key.clone());
                    key_to_entity.insert(key.clone(), entity);
                }
            }
        }

        // Step 2: Process versions and queue component additions
        for doc in documents {
            if let Some(key_str) = doc.get(key_field).and_then(|v| v.as_str()) {
                let key = key_str.to_string();
                let version = doc.get(BEVY_PERSISTENCE_VERSION_FIELD)
                    .and_then(|v| v.as_u64())
                    .unwrap_or(1);

                if let Some(&entity_id) = key_to_entity.get(&key) {
                    self.session.version_manager.set_version(
                        VersionKey::Entity(key.clone()),
                        version,
                    );

                    // Determine which components to insert:
                    // - if comp_names is non-empty, use it
                    // - otherwise, insert all registered components present in the document
                    if !comp_names.is_empty() {
                        for &comp_name in comp_names {
                            if let Some(val) = doc.get(comp_name) {
                                self.commands.queue(DeserializeComponentCommand {
                                    entity: entity_id,
                                    component_name: comp_name,
                                    json_value: val.clone(),
                                });
                            }
                        }
                    } else {
                        for (registered_name, _) in self.session.component_deserializers.iter() {
                            if let Some(val) = doc.get(registered_name) {
                                // registered_name is String; convert to &'static str is not possible.
                                // But our deserializer map is String-keyed; the command stores &'static str.
                                // Instead, directly queue a command per discovered component using the string key.
                                // We use a small bridging: clone the value and look up by string at apply time.
                                // Reuse DeserializeComponentCommand by passing a &'static str only for known static names.
                                // For dynamic keys, dispatch via a tiny inline closure command.
                                let registered_name_owned = registered_name.clone();
                                let val_clone = val.clone();
                                self.commands.queue(move |world: &mut bevy::prelude::World| {
                                    world.resource_scope(|world, session: Mut<PersistenceSession>| {
                                        if let Some(deser) = session.component_deserializers.get(&registered_name_owned) {
                                            if let Err(e) = deser(world, entity_id, val_clone.clone()) {
                                                bevy::log::error!("Failed to deserialize component {}: {}", registered_name_owned, e);
                                            }
                                        }
                                    });
                                });
                            }
                        }
                    }
                }
            }
        }
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