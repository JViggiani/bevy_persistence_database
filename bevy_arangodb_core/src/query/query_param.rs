//! Implements a Bevy SystemParam for querying entities from both world and database
//! in a seamless, integrated way.
//!
//! TODO(deprecation): Remove runtime DSL helpers once type-driven filters fully replace them.

use std::collections::HashSet;
use std::sync::Mutex;
use bevy::prelude::{Entity, Query, Res, Resource, Mut, World, With, Without, Or};
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
    // track components to exclude (presence absence)
    static PQ_WITHOUT_COMPONENTS: std::cell::RefCell<Vec<&'static str>> = Default::default();
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

impl<'w, 's, Q: QueryData<ReadOnly = Q> + 'static, F: QueryFilter + 'static> PersistentQuery<'w, 's, Q, F>
where
    F: ToPresenceSpec + FilterSupported,
    Q: QueryDataToComponents,
{
    /// Iterate over entities with the given components, loading from the database if necessary.
    /// World mutations are queued and applied later in the frame by the plugin.
    pub fn iter_with_loading(&mut self) -> impl Iterator<Item = (Entity, Q::Item<'_>)> {
        bevy::log::debug!("PersistentQuery::iter_with_loading called");

        // Drain transient config from TLS for this call
        // Fetch-only targets from Q
        let mut fetch_names: Vec<&'static str> = Vec::new();
        // Presence gates from TLS .with_component() (deprecated)
        let mut presence_names: Vec<&'static str> = Vec::new();
        PQ_ADDITIONAL_COMPONENTS.with(|c| presence_names.extend(c.borrow_mut().drain(..)));
        let mut without_names: Vec<&'static str> = Vec::new();
        PQ_WITHOUT_COMPONENTS.with(|w| without_names.extend(w.borrow_mut().drain(..)));
        let tls_filter_expr: Option<Expression> = PQ_FILTER_EXPR.with(|f| f.borrow_mut().take());
        let cache_policy: CachePolicy = PQ_CACHE_POLICY.with(|p| *p.borrow());
        // Reset cache policy to default for the next call
        PQ_CACHE_POLICY.with(|p| *p.borrow_mut() = CachePolicy::UseCache);

        // 1) Type-driven component extraction from Q (fetch targets, not presence gates)
        Q::push_names(&mut fetch_names);

        // 2) Merge type-driven presence from F (presence gates + ORs)
        let type_presence = <F as ToPresenceSpec>::to_presence_spec();
        presence_names.extend(type_presence.withs.iter().copied());
        without_names.extend(type_presence.withouts.iter().copied());

        // 2b) Ensure fetch list includes presence-gated components for deserialization
        for &n in &presence_names {
            if !fetch_names.contains(&n) {
                fetch_names.push(n);
            }
        }

        // Deduplicate names after merging
        fetch_names.sort_unstable();
        fetch_names.dedup();
        presence_names.sort_unstable();
        presence_names.dedup();
        without_names.sort_unstable();
        without_names.dedup();

        // 3) Combine presence-derived expression (from Or cases) with TLS filter via AND
        let combined_expr: Option<Expression> = match (type_presence.expr, tls_filter_expr) {
            (Some(a), Some(b)) => Some(a.and(b)),
            (Some(a), None) => Some(a),
            (None, Some(b)) => Some(b),
            (None, None) => None,
        };

        // Compute a hash from Q + presence + fetch + filter to drive the cache
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        std::any::type_name::<Q>().hash(&mut hasher);
        for &name in &presence_names { name.hash(&mut hasher); }
        for &name in &without_names { name.hash(&mut hasher); }
        for &name in &fetch_names { name.hash(&mut hasher); }
        if let Some(expr) = &combined_expr { format!("{:?}", expr).hash(&mut hasher); }
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

            if presence_names.is_empty() {
                bevy::log::info!("No explicit component filters; will load documents and insert any registered components present.");
            }
            bevy::log::debug!("Will query for components (if any): {:?}", presence_names);

            // Build the query
            let mut query = PersistenceQuery::new(self.db.0.clone());
            // Presence gating (doc.`T` != null)
            for comp_name in &presence_names {
                query = query.with_component(comp_name);
            }
            // Absence gating (doc.`T` == null)
            for without in &without_names {
                query = query.without_component(without);
            }
            // Fetch-only targets (do not add presence filters)
            for name in &fetch_names {
                // Skip those already added as presence-gated to avoid dup
                if !presence_names.contains(name) {
                    query = query.fetch_only_component(name);
                }
            }
            if let Some(expr) = &combined_expr {
                query = query.filter(expr.clone());
            }

            // Execute using the plugin runtime
            bevy::log::info!("Executing database query for components: {:?}", presence_names);
            let (aql, bind_vars) = query.build_aql(true);

            match self.runtime.block_on(self.db.0.query_documents(aql, bind_vars)) {
                Ok(documents) => {
                    bevy::log::debug!("Retrieved {} documents (async via runtime)", documents.len());
                    let allow_overwrite = matches!(cache_policy, CachePolicy::ForceRefresh);
                    // Deserialize all requested components (presence + fetch-only)
                    self.process_documents(documents, &fetch_names, allow_overwrite);
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
    #[deprecated(since = "0.1.0", note = "Deprecated runtime helper; prefer type-driven Q")]
    pub fn with_component(self, component_name: &'static str) -> Self {
        PQ_ADDITIONAL_COMPONENTS.with(|c| c.borrow_mut().push(component_name));
        self
    }

    /// Add a filter expression
    #[deprecated(since = "0.1.0", note = "Deprecated DSL; prefer type-driven filters")]
    pub fn filter(self, expression: Expression) -> Self {
        PQ_FILTER_EXPR.with(|f| *f.borrow_mut() = Some(expression));
        self
    }

    /// Force a refresh from the database, bypassing the cache
    pub fn force_refresh(self) -> Self {
        PQ_CACHE_POLICY.with(|p| *p.borrow_mut() = CachePolicy::ForceRefresh);
        self
    }

    /// Exclude entities that have the given component T (presence absence).
    #[deprecated(since = "0.1.0", note = "Deprecated runtime helper; prefer type-driven F")]
    pub fn without<T: bevy::prelude::Component + crate::Persist>(self) -> Self {
        PQ_WITHOUT_COMPONENTS.with(|w| w.borrow_mut().push(T::name()));
        self
    }
}

// Presence spec extracted from type-level filters
#[derive(Default)]
struct PresenceSpec {
    withs: Vec<&'static str>,
    withouts: Vec<&'static str>,
    expr: Option<Expression>,
}

impl PresenceSpec {
    fn merge_and(mut self, other: PresenceSpec) -> PresenceSpec {
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

    fn to_expr(&self) -> Option<Expression> {
        // Build an AND of all presence lists, then AND with self.expr if present
        let mut acc: Option<Expression> = None;
        for n in &self.withs {
            let e = Expression::with_component(*n);
            acc = Some(match acc {
                Some(cur) => cur.and(e),
                None => e,
            });
        }
        for n in &self.withouts {
            let e = Expression::without_component(*n);
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

// Guard trait for supported filter forms. Strict: only implemented for supported shapes.
pub trait FilterSupported {}
impl FilterSupported for () {}
impl<T: bevy::prelude::Component + crate::Persist> FilterSupported for With<T> {}
impl<T: bevy::prelude::Component + crate::Persist> FilterSupported for Without<T> {}
impl<T: FilterSupportedTuple> FilterSupported for Or<T> {}
// tuples themselves are supported (AND) if each element is supported
impl<T: FilterSupportedTuple> FilterSupported for T {}

// Helper trait to mark tuples as supported
pub trait FilterSupportedTuple {}
macro_rules! impl_filter_supported_tuple {
    ( $( $name:ident ),+ ) => {
        impl<$( $name: FilterSupported ),+> FilterSupportedTuple for ( $( $name, )+ ) {}
    };
}
impl_filter_supported_tuple!(A);
impl_filter_supported_tuple!(A,B);
impl_filter_supported_tuple!(A,B,C);
impl_filter_supported_tuple!(A,B,C,D);
impl_filter_supported_tuple!(A,B,C,D,E);
impl_filter_supported_tuple!(A,B,C,D,E,F);
impl_filter_supported_tuple!(A,B,C,D,E,F,G);
impl_filter_supported_tuple!(A,B,C,D,E,F,G,H);

// Core trait: extract presence spec from F
pub trait ToPresenceSpec {
    fn to_presence_spec() -> PresenceSpec;
}
impl ToPresenceSpec for () {
    fn to_presence_spec() -> PresenceSpec { PresenceSpec::default() }
}
impl<T: bevy::prelude::Component + crate::Persist> ToPresenceSpec for With<T> {
    fn to_presence_spec() -> PresenceSpec {
        PresenceSpec { withs: vec![T::name()], withouts: vec![], expr: None }
    }
}
impl<T: bevy::prelude::Component + crate::Persist> ToPresenceSpec for Without<T> {
    fn to_presence_spec() -> PresenceSpec {
        PresenceSpec { withs: vec![], withouts: vec![T::name()], expr: None }
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
impl_to_presence_for_tuple!(A,B);
impl_to_presence_for_tuple!(A,B,C);
impl_to_presence_for_tuple!(A,B,C,D);
impl_to_presence_for_tuple!(A,B,C,D,E);
impl_to_presence_for_tuple!(A,B,C,D,E,F);
impl_to_presence_for_tuple!(A,B,C,D,E,F,G);
impl_to_presence_for_tuple!(A,B,C,D,E,F,G,H);

// Or of a tuple: build an OR expression of each alternative's presence constraints
macro_rules! impl_to_presence_for_or_tuple {
    ( $( $name:ident ),+ ) => {
        impl<$( $name: ToPresenceSpec ),+> ToPresenceSpec for Or<( $( $name, )+ )> {
            fn to_presence_spec() -> PresenceSpec {
                let parts: Vec<Option<Expression>> = vec![
                    $( <$name as ToPresenceSpec>::to_presence_spec().to_expr(), )+
                ];
                // Build OR chain of non-empty expressions
                let mut or_expr: Option<Expression> = None;
                for p in parts.into_iter().flatten() {
                    or_expr = Some(match or_expr {
                        Some(cur) => cur.or(p),
                        None => p,
                    });
                }
                // Within Or, we return only an expression to avoid AND-ing via flat with/without lists
                PresenceSpec { withs: vec![], withouts: vec![], expr: or_expr }
            }
        }
    };
}
impl_to_presence_for_or_tuple!(A,B);
impl_to_presence_for_or_tuple!(A,B,C);
impl_to_presence_for_or_tuple!(A,B,C,D);
impl_to_presence_for_or_tuple!(A,B,C,D,E);
impl_to_presence_for_or_tuple!(A,B,C,D,E,F);
impl_to_presence_for_or_tuple!(A,B,C,D,E,F,G);
impl_to_presence_for_or_tuple!(A,B,C,D,E,F,G,H);

// Extract component names to fetch from the QueryData type Q
pub trait QueryDataToComponents {
    fn push_names(acc: &mut Vec<&'static str>);
}

// &T
impl<T: bevy::prelude::Component + crate::Persist> QueryDataToComponents for &T {
    fn push_names(acc: &mut Vec<&'static str>) { acc.push(T::name()); }
}
// &mut T
impl<T: bevy::prelude::Component + crate::Persist> QueryDataToComponents for &mut T {
    fn push_names(acc: &mut Vec<&'static str>) { acc.push(T::name()); }
}
// Option<&T>
impl<T: bevy::prelude::Component + crate::Persist> QueryDataToComponents for Option<&T> {
    fn push_names(acc: &mut Vec<&'static str>) { acc.push(T::name()); }
}
// Option<&mut T>
impl<T: bevy::prelude::Component + crate::Persist> QueryDataToComponents for Option<&mut T> {
    fn push_names(acc: &mut Vec<&'static str>) { acc.push(T::name()); }
}

// Special-case Guid: it is a component but not persisted as a document field.
// Treat it as non-fetching so PersistentQuery<&Guid, _> compiles without Persist.
impl QueryDataToComponents for &crate::components::Guid {
    fn push_names(_acc: &mut Vec<&'static str>) {}
}
impl QueryDataToComponents for &mut crate::components::Guid {
    fn push_names(_acc: &mut Vec<&'static str>) {}
}
impl QueryDataToComponents for Option<&crate::components::Guid> {
    fn push_names(_acc: &mut Vec<&'static str>) {}
}
impl QueryDataToComponents for Option<&mut crate::components::Guid> {
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
impl_q_to_components_tuple!(A,B);
impl_q_to_components_tuple!(A,B,C);
impl_q_to_components_tuple!(A,B,C,D);
impl_q_to_components_tuple!(A,B,C,D,E);
impl_q_to_components_tuple!(A,B,C,D,E,F);
impl_q_to_components_tuple!(A,B,C,D,E,F,G);
impl_q_to_components_tuple!(A,B,C,D,E,F,G,H);