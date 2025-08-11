//! Implements a Bevy SystemParam for querying entities from both world and database
//! in a seamless, integrated way.

use std::collections::HashSet;
use std::sync::Mutex;
use bevy::prelude::{Entity, Query, Res, Resource, Mut, World};
use bevy::ecs::query::{QueryData, QueryFilter};
use bevy::ecs::system::SystemParam;

use crate::query::expression::Expression;
use crate::query::persistence_query::WithComponentExt;
use crate::{DatabaseConnectionResource, PersistenceSession, Guid, BEVY_PERSISTENCE_VERSION_FIELD};
use crate::query::PersistenceQuery;
use crate::versioning::version_manager::VersionKey;
use crate::plugins::persistence_plugin::TokioRuntime;
use std::hash::{Hash, Hasher};

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

/// Thread-safe queue of world mutations to be applied later in the frame.
#[derive(Resource, Default)]
pub(crate) struct DeferredWorldOps(Mutex<Vec<Box<dyn FnOnce(&mut World) + Send>>>);

impl DeferredWorldOps {
    pub fn push(&self, op: Box<dyn FnOnce(&mut World) + Send>) {
        self.0.lock().unwrap().push(op);
    }

    /// Drain and return all pending world operations.
    pub fn drain(&self) -> Vec<Box<dyn FnOnce(&mut World) + Send>> {
        let mut guard = self.0.lock().unwrap();
        let mut out = Vec::new();
        std::mem::swap(&mut *guard, &mut out);
        out
    }
}

// Transient per-thread configuration used between builder calls and iter_with_loading.
// This avoids Local<T> so multiple PersistentQuery params can coexist in one system.
thread_local! {
    static PQ_ADDITIONAL_COMPONENTS: std::cell::RefCell<Vec<&'static str>> = Default::default();
    static PQ_FILTER_EXPR: std::cell::RefCell<Option<Expression>> = const { std::cell::RefCell::new(None) };
    static PQ_CACHE_POLICY: std::cell::RefCell<CachePolicy> = const { std::cell::RefCell::new(CachePolicy::UseCache) };
}

/// System parameter for querying entities from both the world and database
#[derive(SystemParam)]
pub struct PersistentQuery<'w, 's, Q: QueryData + 'static, F: QueryFilter + 'static = ()> {
    /// The underlying world query
    query: Query<'w, 's, (Entity, Q), F>,
    /// The database connection
    db: Res<'w, DatabaseConnectionResource>,
    /// The query cache - using immutable access with interior mutability
    cache: Res<'w, PersistenceQueryCache>,
    /// Runtime to drive async DB calls
    runtime: Res<'w, TokioRuntime>,
    /// Add access to the deferred ops queue (immutable; interior mutability)
    ops: Res<'w, DeferredWorldOps>,
}

impl<'w, 's, Q: QueryData<ReadOnly = Q> + 'static, F: QueryFilter + 'static> PersistentQuery<'w, 's, Q, F> {
    /// Iterate over entities with the given components, loading from the database if necessary.
    /// World mutations are queued and applied later in the frame by the plugin.
    pub fn iter_with_loading(&mut self) -> impl Iterator<Item = (Entity, Q::Item<'_>)> {
        bevy::log::debug!("PersistentQuery::iter_with_loading called");

        // Drain transient config from TLS for this call
        let mut comp_names: Vec<&'static str> = Vec::new();
        PQ_ADDITIONAL_COMPONENTS.with(|c| comp_names.extend(c.borrow_mut().drain(..)));
        let filter_expr: Option<Expression> = PQ_FILTER_EXPR.with(|f| f.borrow_mut().take());
        let cache_policy: CachePolicy = PQ_CACHE_POLICY.with(|p| *p.borrow());
        // Reset cache policy to default for the next call
        PQ_CACHE_POLICY.with(|p| *p.borrow_mut() = CachePolicy::UseCache);

        // Compute a hash from Q + explicit comps + filter to drive the cache
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        std::any::type_name::<Q>().hash(&mut hasher);
        for &name in &comp_names {
            name.hash(&mut hasher);
        }
        if let Some(expr) = &filter_expr {
            format!("{:?}", expr).hash(&mut hasher);
        }
        let query_hash = hasher.finish();
        bevy::log::trace!("Computed query hash: {}", query_hash);

        // Decide if we need to hit the DB based on cache policy
        let should_query_db = match cache_policy {
            CachePolicy::ForceRefresh => {
                bevy::log::debug!("Force refresh policy - will query DB");
                true
            }
            CachePolicy::UseCache => {
                bevy::log::debug!("Checking cache for hash: {}", query_hash);
                let in_cache = self.cache.contains(query_hash);
                bevy::log::debug!("Cache check complete, in_cache: {}", in_cache);
                !in_cache
            }
        };

        if should_query_db {
            bevy::log::debug!("Querying database");

            if comp_names.is_empty() {
                bevy::log::info!("No explicit component filters; will load documents and insert any registered components present.");
            }
            bevy::log::debug!("Will query for components (if any): {:?}", comp_names);

            // Build the query
            let mut query = PersistenceQuery::new(self.db.0.clone());
            for comp_name in &comp_names {
                query = query.with_component(comp_name);
            }
            if let Some(expr) = &filter_expr {
                query = query.filter(expr.clone());
            }

            // Execute using the plugin runtime
            bevy::log::info!("Executing database query for components: {:?}", comp_names);
            let (aql, bind_vars) = query.build_aql(true);

            match self.runtime.block_on(self.db.0.query_documents(aql, bind_vars)) {
                Ok(documents) => {
                    bevy::log::debug!("Retrieved {} documents (async via runtime)", documents.len());
                    // Allow overwrite only when ForceRefresh was requested
                    let allow_overwrite = matches!(cache_policy, CachePolicy::ForceRefresh);
                    self.process_documents(documents, &comp_names, allow_overwrite);
                    self.cache.insert(query_hash);
                }
                Err(e) => {
                    bevy::log::error!("Error fetching documents: {}", e);
                }
            }
        } else {
            bevy::log::debug!("Skipping DB query - using cached results");
        }

        // Return iterator over query results - now includes freshly loaded entities WITH their components
        self.query.iter()
    }

    // Queue per-document closures that will apply all mutations atomically when drained.
    fn process_documents(&mut self, documents: Vec<serde_json::Value>, comp_names: &[&'static str], allow_overwrite: bool) {
        let key_field = self.db.0.document_key_field();
        let explicit_components = comp_names.to_vec();

        for doc in documents {
            let key = match doc.get(key_field).and_then(|v| v.as_str()) {
                Some(k) => k.to_string(),
                None => continue,
            };
            let version = doc
                .get(BEVY_PERSISTENCE_VERSION_FIELD)
                .and_then(|v| v.as_u64())
                .unwrap_or(1);

            let doc_clone = doc.clone();
            let comps = explicit_components.clone();
            let allow = allow_overwrite;

            self.ops.push(Box::new(move |world: &mut World| {
                world.resource_scope(|world, mut session: Mut<PersistenceSession>| {
                    // Resolve or spawn the entity for this key and detect existence
                    let (entity, existed) = if let Some((e, _)) = session
                        .entity_keys
                        .iter()
                        .find(|(_, k)| **k == key)
                        .map(|(e, k)| (*e, k.clone()))
                    {
                        (e, true)
                    } else {
                        let e = world.spawn(Guid::new(key.clone())).id();
                        session.entity_keys.insert(e, key.clone());
                        (e, false)
                    };

                    // If the entity already exists and we're not force-refreshing, do nothing.
                    if existed && !allow {
                        bevy::log::trace!("Skipping update for existing entity {} (no force refresh)", key);
                        return;
                    }

                    // Cache version (for new entities or when forcing refresh)
                    session
                        .version_manager
                        .set_version(VersionKey::Entity(key.clone()), version);

                    // Insert components
                    if !comps.is_empty() {
                        for &comp_name in &comps {
                            // When forcing refresh, we overwrite; otherwise this path is only for brand-new entities
                            if let Some(val) = doc_clone.get(comp_name) {
                                if let Some(deser) = session.component_deserializers.get(comp_name) {
                                    if let Err(e) = deser(world, entity, val.clone()) {
                                        bevy::log::error!(
                                            "Failed to deserialize component {}: {}",
                                            comp_name,
                                            e
                                        );
                                    }
                                }
                            }
                        }
                    } else {
                        for (registered_name, deser) in session.component_deserializers.iter() {
                            if let Some(val) = doc_clone.get(registered_name) {
                                if let Err(e) = deser(world, entity, val.clone()) {
                                    bevy::log::error!(
                                        "Failed to deserialize component {}: {}",
                                        registered_name,
                                        e
                                    );
                                }
                            }
                        }
                    }
                });
            }));
        }
    }

    /// Add a component to load by name
    pub fn with_component(self, component_name: &'static str) -> Self {
        PQ_ADDITIONAL_COMPONENTS.with(|c| c.borrow_mut().push(component_name));
        self
    }

    /// Add a filter expression
    pub fn filter(self, expression: Expression) -> Self {
        PQ_FILTER_EXPR.with(|f| *f.borrow_mut() = Some(expression));
        self
    }

    /// Force a refresh from the database, bypassing the cache
    pub fn force_refresh(self) -> Self {
        PQ_CACHE_POLICY.with(|p| *p.borrow_mut() = CachePolicy::ForceRefresh);
        self
    }
}