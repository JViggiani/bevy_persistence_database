//! Load commands queued by the persistence params. Each runs with exclusive `&mut World`
//! when the requesting system's commands apply, so database rows are materialized without
//! aliasing any system's borrows.

use std::any::TypeId;
use std::collections::HashMap;
use std::hash::{DefaultHasher, Hash, Hasher};

use bevy::prelude::{Entity, Mut, World};
use tokio::runtime::Runtime;

use crate::bevy::plugins::persistence_plugin::TokioRuntime;
use crate::core::db::DatabaseConnection;
use crate::core::db::connection::{DatabaseConnectionResource, DocumentKind, PersistenceError};
use crate::core::query::{
    EdgeQuerySpecification, FilterExpression, PaginationConfig, PersistenceQuerySpecification,
};
use crate::core::session::PersistenceSession;

use super::cache::{CachePolicy, PersistenceQueryCache};
use super::load_options::{LoadOptions, RelationshipLoadSpec, and_filters};

/// Component names that gate and fill one entity load.
pub(crate) struct ComponentSelection {
    /// Components a document must have.
    pub(crate) with: Vec<&'static str>,
    /// Components a document must not have.
    pub(crate) without: Vec<&'static str>,
    /// Components to deserialize.
    pub(crate) fetch: Vec<&'static str>,
}

/// One requested entity load: the backend query, its cache key and what to do with the rows.
pub(crate) struct EntityLoad {
    cache_key: u64,
    cache_policy: CachePolicy,
    spec: PersistenceQuerySpecification,
    page_size: Option<usize>,
    relationships: RelationshipLoadSpec,
}

impl EntityLoad {
    /// Build a load for query data `query_type` (salted by `salts`, e.g. a join's other side).
    pub(crate) fn new(
        query_type: &'static str,
        salts: &[&'static str],
        mut selection: ComponentSelection,
        type_filter: Option<FilterExpression>,
        force_full_docs: bool,
        options: LoadOptions,
        default_store: &str,
    ) -> Self {
        let LoadOptions {
            filter,
            cache_policy,
            store,
            page_size,
            relationships,
        } = options;
        let value_filters = and_filters(type_filter, filter);
        let store = store.unwrap_or_else(|| default_store.to_string());

        // Presence-gated components are always fetched too.
        selection.fetch.extend(selection.with.iter().copied());
        for names in [
            &mut selection.with,
            &mut selection.without,
            &mut selection.fetch,
        ] {
            names.sort_unstable();
            names.dedup();
        }

        let mut hasher = DefaultHasher::new();
        query_type.hash(&mut hasher);
        salts.hash(&mut hasher);
        store.hash(&mut hasher);
        selection.with.hash(&mut hasher);
        selection.without.hash(&mut hasher);
        selection.fetch.hash(&mut hasher);
        format!("{value_filters:?}").hash(&mut hasher);
        let mut per_type: Vec<_> = relationships.per_type.iter().collect();
        per_type.sort_unstable();
        per_type.hash(&mut hasher);
        relationships.all.hash(&mut hasher);

        let unconstrained = selection.with.is_empty() && selection.without.is_empty();
        Self {
            cache_key: hasher.finish(),
            cache_policy,
            spec: PersistenceQuerySpecification {
                store,
                kind: DocumentKind::Entity,
                presence_with: selection.with,
                presence_without: selection.without,
                fetch_only: selection.fetch,
                value_filters,
                return_full_docs: force_full_docs || unconstrained,
                pagination: None,
            },
            page_size,
            relationships,
        }
    }

    /// Run the load: skip on a cache hit, fetch (page by page if asked), materialize every
    /// document, refresh resources, then follow requested relationships.
    pub(crate) fn run(self, world: &mut World) {
        let use_cache = self.cache_policy == CachePolicy::UseCache;
        if use_cache && world.resource::<PersistenceQueryCache>().contains(self.cache_key) {
            bevy::log::trace!("PersistentQuery load cached: hash={:#x}", self.cache_key);
            return;
        }

        let db = world
            .resource::<DatabaseConnectionResource>()
            .connection
            .clone();
        let runtime = world.resource::<TokioRuntime>().runtime.clone();
        let spec = &self.spec;
        let key_field = db.document_key_field();
        let unconstrained = spec.presence_with.is_empty() && spec.presence_without.is_empty();
        let components: &[&'static str] = if unconstrained { &[] } else { &spec.fetch_only };
        let allow_overwrite = !use_cache || !spec.presence_with.is_empty();

        let loaded = world.resource_scope(|world, mut session: Mut<PersistenceSession>| {
            let mut loaded_keys = Vec::new();
            let mut apply = |world: &mut World, documents: Vec<serde_json::Value>| {
                for doc in &documents {
                    if let Some(key) = doc.get(key_field).and_then(|v| v.as_str()) {
                        loaded_keys.push(key.to_string());
                    }
                    apply_document(
                        world,
                        &mut session,
                        doc,
                        components,
                        allow_overwrite,
                        key_field,
                    );
                }
            };

            let fetched = match self.page_size {
                None => runtime
                    .block_on(db.execute_documents(spec))
                    .map(|documents| apply(world, documents)),
                Some(page_size) => {
                    for_each_page(&runtime, &*db, spec, page_size, |documents| {
                        apply(world, documents)
                    })
                }
            };
            if let Err(e) = fetched {
                bevy::log::error!("PersistentQuery load failed: {}", e);
                return false;
            }

            if let Err(e) =
                runtime.block_on(session.fetch_and_insert_resources(&*db, &spec.store, world))
            {
                bevy::log::error!("PersistentQuery resource refresh failed: {}", e);
            }

            if !self.relationships.is_empty() {
                let depths = self
                    .relationships
                    .resolve(session.relationship_type_entries());
                load_relationships_for_keys(
                    world,
                    &mut session,
                    &runtime,
                    &*db,
                    &spec.store,
                    &loaded_keys,
                    depths,
                );
            }
            true
        });

        if loaded && use_cache {
            world
                .resource::<PersistenceQueryCache>()
                .insert(self.cache_key);
        }
    }
}

/// Which relationship types a [`RelationshipLoad`] follows.
pub(crate) enum RelationshipTypes {
    One(TypeId),
    AllRegistered,
}

/// Relationship edges requested for entities already in the world.
pub(crate) struct RelationshipLoad {
    pub(crate) types: RelationshipTypes,
    pub(crate) entities: Vec<Entity>,
    pub(crate) depth: usize,
    pub(crate) store: String,
}

impl RelationshipLoad {
    /// Load each requested type's edges once per (type, sources, depth); a repeat is a cache hit.
    pub(crate) fn run(self, world: &mut World) {
        let db = world
            .resource::<DatabaseConnectionResource>()
            .connection
            .clone();
        let runtime = world.resource::<TokioRuntime>().runtime.clone();

        world.resource_scope(|world, mut session: Mut<PersistenceSession>| {
            let source_keys: Vec<String> = self
                .entities
                .iter()
                .filter_map(|entity| session.entity_key(*entity).cloned())
                .collect();
            if source_keys.is_empty() {
                return;
            }
            let type_ids: Vec<TypeId> = match self.types {
                RelationshipTypes::One(type_id) => vec![type_id],
                RelationshipTypes::AllRegistered => session
                    .relationship_type_entries()
                    .into_iter()
                    .map(|(type_id, _)| type_id)
                    .collect(),
            };

            for type_id in type_ids {
                let mut hasher = DefaultHasher::new();
                type_id.hash(&mut hasher);
                source_keys.hash(&mut hasher);
                self.depth.hash(&mut hasher);
                self.store.hash(&mut hasher);
                let cache_key = hasher.finish();
                let cache = world.resource::<PersistenceQueryCache>();
                if cache.contains(cache_key) {
                    continue;
                }
                cache.insert(cache_key);

                load_relationships_for_keys(
                    world,
                    &mut session,
                    &runtime,
                    &*db,
                    &self.store,
                    &source_keys,
                    HashMap::from([(type_id, self.depth)]),
                );
            }
        });
    }
}

fn apply_document(
    world: &mut World,
    session: &mut PersistenceSession,
    doc: &serde_json::Value,
    components: &[&'static str],
    allow_overwrite: bool,
    key_field: &str,
) {
    match session.materialize_entity_document(world, doc, key_field, components, allow_overwrite) {
        Ok(None) => {
            bevy::log::trace!("skipping document missing key '{}'", key_field);
        }
        Ok(Some(_)) => {}
        Err(e) => {
            let key = doc
                .get(key_field)
                .and_then(|v| v.as_str())
                .unwrap_or("<missing>");
            bevy::log::error!(
                "Failed to materialize entity document for key {}: {}",
                key,
                e
            );
        }
    }
}

/// Fetch `spec` one page at a time, handing each page to `apply` before fetching the next.
fn for_each_page(
    runtime: &Runtime,
    db: &dyn DatabaseConnection,
    spec: &PersistenceQuerySpecification,
    page_size: usize,
    mut apply: impl FnMut(Vec<serde_json::Value>),
) -> Result<(), PersistenceError> {
    let total = runtime.block_on(db.count_documents(spec))?;
    let pages = total.div_ceil(page_size);
    bevy::log::debug!("Paginated load: total={} pages={}", total, pages);
    for page_number in 0..pages {
        let mut page_spec = spec.clone();
        page_spec.pagination = Some(PaginationConfig {
            page_size,
            page_number,
        });
        apply(runtime.block_on(db.execute_documents(&page_spec))?);
    }
    Ok(())
}

fn ensure_entity_loaded_by_key(
    world: &mut World,
    session: &mut PersistenceSession,
    runtime: &Runtime,
    db: &dyn DatabaseConnection,
    store: &str,
    key: &str,
) -> Option<Entity> {
    if let Some(existing) = session.entity_by_key(key) {
        if world.get_entity(existing).is_ok() {
            return Some(existing);
        }
    }

    let materialized = runtime
        .block_on(db.fetch_document(store, key))
        .and_then(|fetched| match fetched {
            Some((doc, _)) => session.materialize_entity_document_for_key(
                world,
                key,
                doc,
                db.document_key_field(),
                &[],
            ),
            None => Ok(None),
        });
    match materialized {
        Ok(entity) => entity,
        Err(e) => {
            bevy::log::error!("Failed to load relationship document {}: {}", key, e);
            None
        }
    }
}

/// Load the edges of each `(type, depth)` from `source_keys`, materializing any endpoint
/// not yet in the world, and attach them to the source entities.
fn load_relationships_for_keys(
    world: &mut World,
    session: &mut PersistenceSession,
    runtime: &Runtime,
    db: &dyn DatabaseConnection,
    store: &str,
    source_keys: &[String],
    depths: HashMap<TypeId, usize>,
) {
    if source_keys.is_empty() {
        return;
    }

    for (type_id, depth) in depths {
        let Some(rel_name) = session.relationship_type_name(&type_id) else {
            continue;
        };

        let spec = EdgeQuerySpecification {
            store: store.to_string(),
            relationship_types: vec![rel_name.to_string()],
            from_guids: source_keys.to_vec(),
            to_guids: Vec::new(),
            depth,
        };

        let edges = match runtime.block_on(db.query_edges(&spec)) {
            Ok(edges) => edges,
            Err(e) => {
                bevy::log::error!("Failed loading relationship edges for {}: {}", rel_name, e);
                continue;
            }
        };

        let mut grouped: HashMap<String, Vec<(String, Option<serde_json::Value>)>> =
            HashMap::new();
        for edge in edges {
            grouped
                .entry(edge.from_guid)
                .or_default()
                .push((edge.to_guid, edge.payload));
        }

        for source_key in source_keys {
            let Some(source_entity) =
                ensure_entity_loaded_by_key(world, session, runtime, db, store, source_key)
            else {
                continue;
            };

            let raw_targets = grouped.remove(source_key).unwrap_or_default();
            let mut resolved_targets = Vec::with_capacity(raw_targets.len());
            for (target_key, payload) in raw_targets {
                if let Some(target_entity) =
                    ensure_entity_loaded_by_key(world, session, runtime, db, store, &target_key)
                {
                    resolved_targets.push((target_entity, payload));
                }
            }

            if let Err(e) =
                session.apply_relationship_targets(type_id, world, source_entity, resolved_targets)
            {
                bevy::log::error!(
                    "Failed to materialize relationship {} on {:?}: {}",
                    rel_name,
                    source_entity,
                    e
                );
            }
        }
    }
}
