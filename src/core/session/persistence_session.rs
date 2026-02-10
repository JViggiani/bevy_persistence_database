//! Core ECS‐to‐Arango bridge: defines `PersistenceSession`.
//! Handles local cache, change tracking, and commit logic (create/update/delete).

use crate::bevy::plugins::persistence_plugin::{TokioRuntime, TriggerCommit, register_commit_listener};
use crate::core::db::connection::{
    BEVY_PERSISTENCE_DATABASE_BEVY_TYPE_FIELD, BEVY_PERSISTENCE_DATABASE_METADATA_FIELD,
    BEVY_PERSISTENCE_DATABASE_VERSION_FIELD, DatabaseConnection, DocumentKind, PersistenceError,
    TransactionOperation,
};
use crate::core::persist::Persist;
use crate::core::versioning::version_manager::{VersionKey, VersionManager};
use bevy::prelude::{App, Component, Entity, Resource, World, info};
use rayon::ThreadPoolBuilder;
use rayon::prelude::*;
use serde_json::Value;
use std::{
    any::TypeId,
    collections::{HashMap, HashSet},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};
use tokio::sync::oneshot;
use tokio::time::timeout;

/// A unique ID generator for correlating commit requests and responses.
static NEXT_CORRELATION_ID: AtomicU64 = AtomicU64::new(1);

type ComponentSerializer =
    Box<dyn Fn(Entity, &World) -> Result<Option<(String, Value)>, PersistenceError> + Send + Sync>;
type ComponentDeserializer =
    Box<dyn Fn(&mut World, Entity, Value) -> Result<(), PersistenceError> + Send + Sync>;
type ResourceSerializer = Box<
    dyn Fn(&World, &PersistenceSession) -> Result<Option<(String, Value)>, PersistenceError>
        + Send
        + Sync,
>;
type ResourceDeserializer =
    Box<dyn Fn(&mut World, Value) -> Result<(), PersistenceError> + Send + Sync>;
type ResourceRemover = Box<dyn Fn(&mut World) + Send + Sync>;

#[derive(Default)]
struct ChangeTracking {
    dirty_entity_components: HashMap<Entity, HashSet<TypeId>>,
    despawned_entities: HashSet<Entity>,
    dirty_resources: HashSet<TypeId>,
    despawned_resources: HashSet<TypeId>,
}

#[derive(Default)]
struct ComponentRegistry {
    serializers: HashMap<TypeId, ComponentSerializer>,
    deserializers: HashMap<String, ComponentDeserializer>,
    type_id_to_name: HashMap<TypeId, &'static str>,
    name_to_type_id: HashMap<String, TypeId>,
    presence: HashMap<String, Box<dyn Fn(&World, Entity) -> bool + Send + Sync>>,
}

#[derive(Default)]
struct ResourceRegistry {
    serializers: HashMap<TypeId, ResourceSerializer>,
    deserializers: HashMap<String, ResourceDeserializer>,
    name_to_type_id: HashMap<String, TypeId>,
    type_id_to_name: HashMap<TypeId, &'static str>,
    removers: HashMap<TypeId, ResourceRemover>,
    presence: HashMap<TypeId, Box<dyn Fn(&World) -> bool + Send + Sync>>,
    last_seen_present: HashMap<TypeId, bool>,
}

struct PersistenceCache {
    entity_keys: HashMap<Entity, String>,
    version_manager: VersionManager,
}

impl Default for PersistenceCache {
    fn default() -> Self {
        Self {
            entity_keys: HashMap::new(),
            version_manager: VersionManager::new(),
        }
    }
}

pub(crate) struct DirtyState {
    dirty_entity_components: HashMap<Entity, HashSet<TypeId>>,
    despawned_entities: HashSet<Entity>,
    dirty_resources: HashSet<TypeId>,
    despawned_resources: HashSet<TypeId>,
}

impl DirtyState {
    pub(crate) fn from_parts(
        dirty_entity_components: HashMap<Entity, HashSet<TypeId>>,
        despawned_entities: HashSet<Entity>,
        dirty_resources: HashSet<TypeId>,
        despawned_resources: HashSet<TypeId>,
    ) -> Self {
        Self {
            dirty_entity_components,
            despawned_entities,
            dirty_resources,
            despawned_resources,
        }
    }

    pub(crate) fn into_parts(
        self,
    ) -> (
        HashMap<Entity, HashSet<TypeId>>,
        HashSet<Entity>,
        HashSet<TypeId>,
        HashSet<TypeId>,
    ) {
        (
            self.dirty_entity_components,
            self.despawned_entities,
            self.dirty_resources,
            self.despawned_resources,
        )
    }
}

/// Manages a "unit of work": local World cache + change tracking + async runtime.
#[derive(Resource)]
pub struct PersistenceSession {
    tracking: ChangeTracking,
    components: ComponentRegistry,
    resources: ResourceRegistry,
    cache: PersistenceCache,
}

pub(crate) struct CommitData {
    pub(crate) operations: Vec<TransactionOperation>,
    pub(crate) new_entities: Vec<Entity>,
}

impl PersistenceSession {
    /// Registers a component type for persistence.
    ///
    /// This method sets up both serialization and deserialization for any
    /// component that implements the `Persist` marker trait.
    pub fn register_component<T: Component + Persist>(&mut self) {
        let ser_key = T::name();
        let type_id = TypeId::of::<T>();
        self.components.type_id_to_name.insert(type_id, ser_key);
        // reverse lookup
        self.components
            .name_to_type_id
            .insert(ser_key.to_string(), type_id);
        // presence checker
        self.components.presence.insert(
            ser_key.to_string(),
            Box::new(|world: &World, entity: Entity| world.entity(entity).contains::<T>()),
        );
        self.components.serializers.insert(
            type_id,
            Box::new(
                move |entity, world| -> Result<Option<(String, Value)>, PersistenceError> {
                    if let Some(c) = world.get::<T>(entity) {
                        let v = serde_json::to_value(c)
                            .map_err(|_| PersistenceError::new("Serialization failed"))?;
                        if v.is_null() {
                            return Err(PersistenceError::new("Could not serialize"));
                        }
                        Ok(Some((ser_key.to_string(), v)))
                    } else {
                        Ok(None)
                    }
                },
            ),
        );

        let de_key = T::name();
        self.components.deserializers.insert(
            de_key.to_string(),
            Box::new(|world, entity, json_val| {
                let comp: T = serde_json::from_value(json_val)
                    .map_err(|e| PersistenceError::new(e.to_string()))?;
                world.entity_mut(entity).insert(comp);
                Ok(())
            }),
        );
    }

    /// Registers a resource type for persistence.
    ///
    /// This method sets up both serialization and deserialization for any
    /// resource that implements the `Persist` marker trait.
    pub fn register_resource<R: Resource + Persist>(&mut self) {
        let ser_key = R::name();
        let type_id = std::any::TypeId::of::<R>();
        self.resources.type_id_to_name.insert(type_id, ser_key);
        self.resources.presence.insert(
            type_id,
            Box::new(|world: &World| world.get_resource::<R>().is_some()),
        );
        // Insert serializer into map keyed by TypeId
        self.resources.serializers.insert(
            type_id,
            Box::new(move |world, _session| {
                // Fetch and serialize the resource
                if let Some(r) = world.get_resource::<R>() {
                    let v = serde_json::to_value(r)
                        .map_err(|e| PersistenceError::new(e.to_string()))?;
                    if v.is_null() {
                        return Err(PersistenceError::new("Could not serialize"));
                    }
                    Ok(Some((ser_key.to_string(), v)))
                } else {
                    Ok(None)
                }
            }),
        );

        let de_key = R::name();
        self.resources.deserializers.insert(
            de_key.to_string(),
            Box::new(|world, json_val| {
                let res: R = serde_json::from_value(json_val)
                    .map_err(|e| PersistenceError::new(e.to_string()))?;
                world.insert_resource(res);
                Ok(())
            }),
        );
        self.resources
            .name_to_type_id
            .insert(de_key.to_string(), type_id);
        
        // Register remover function
        self.resources.removers.insert(
            type_id,
            Box::new(|world| {
                world.remove_resource::<R>();
            }),
        );
    }

    /// Manually mark a resource as needing persistence.
    pub fn mark_resource_dirty<R: Resource + Persist>(&mut self) {
        self.tracking.dirty_resources.insert(TypeId::of::<R>());
    }

    /// Manually mark a persisted resource as having been removed.
    pub fn mark_resource_despawned<R: Resource + Persist>(&mut self) {
        self.tracking.despawned_resources.insert(TypeId::of::<R>());
    }

    pub(crate) fn mark_resource_despawned_type_id(&mut self, type_id: TypeId) {
        self.tracking.despawned_resources.insert(type_id);
    }

    /// Mark a specific persisted component type as dirty for an entity.
    pub(crate) fn mark_entity_component_dirty(&mut self, entity: Entity, component: TypeId) {
        self.tracking
            .dirty_entity_components
            .entry(entity)
            .or_default()
            .insert(component);
    }

    /// Return the tracked version for a persisted resource type, if cached.
    pub fn resource_version<R: Resource + 'static>(&self) -> Option<u64> {
        self.cache
            .version_manager
            .get_version(&VersionKey::Resource(TypeId::of::<R>()))
    }

    /// Lookup the registered `TypeId` for a persisted resource by name.
    pub fn resource_type_id(&self, name: &str) -> Option<TypeId> {
        self.resources.name_to_type_id.get(name).copied()
    }

    /// Returns an iterator over all registered persisted resource TypeIds.
    /// Useful for cleanup operations like conflict reprocessing.
    pub fn persisted_resource_types(&self) -> impl Iterator<Item = TypeId> + '_ {
        self.resources.serializers.keys().copied()
    }

    /// Returns the number of registered persisted resources.
    pub fn persisted_resource_count(&self) -> usize {
        self.resources.removers.len()
    }

    /// Removes all persisted resources by calling each registered remover function.
    /// This method borrows self immutably and calls each remover with the world.
    pub fn remove_all_persisted_resources(&self, world: &mut World) {
        for remover in self.resources.removers.values() {
            remover(world);
        }
    }

    /// Run the registered resource deserializer for a given persisted resource name.
    pub fn deserialize_resource_by_name(
        &self,
        world: &mut World,
        name: &str,
        value: Value,
    ) -> Result<(), PersistenceError> {
        if let Some(deser) = self.resources.deserializers.get(name) {
            deser(world, value)
        } else {
            Err(PersistenceError::new(format!(
                "no deserializer registered for resource {name}"
            )))
        }
    }

    /// Manually mark an entity as having been removed.
    pub fn mark_despawned(&mut self, entity: Entity) {
        self.tracking.despawned_entities.insert(entity);
    }

    pub(crate) fn take_dirty_state(&mut self) -> DirtyState {
        DirtyState {
            dirty_entity_components: std::mem::take(&mut self.tracking.dirty_entity_components),
            despawned_entities: std::mem::take(&mut self.tracking.despawned_entities),
            dirty_resources: std::mem::take(&mut self.tracking.dirty_resources),
            despawned_resources: std::mem::take(&mut self.tracking.despawned_resources),
        }
    }

    pub(crate) fn restore_dirty_state(&mut self, state: DirtyState) {
        for (entity, dirty) in state.dirty_entity_components {
            self.tracking
                .dirty_entity_components
                .entry(entity)
                .or_default()
                .extend(dirty);
        }
        self.tracking
            .despawned_entities
            .extend(state.despawned_entities);
        self.tracking.dirty_resources.extend(state.dirty_resources);
        self.tracking
            .despawned_resources
            .extend(state.despawned_resources);
    }

    pub(crate) fn resource_presence_snapshot(&self, world: &World) -> Vec<(TypeId, bool)> {
        self.resources
            .presence
            .iter()
            .map(|(type_id, presence_fn)| (*type_id, presence_fn(world)))
            .collect()
    }

    pub(crate) fn update_resource_presence(&mut self, type_id: TypeId, is_present: bool) -> bool {
        self.resources
            .last_seen_present
            .insert(type_id, is_present)
            .unwrap_or(false)
    }

    pub(crate) fn version_manager(&self) -> &VersionManager {
        &self.cache.version_manager
    }

    pub(crate) fn version_manager_mut(&mut self) -> &mut VersionManager {
        &mut self.cache.version_manager
    }

    pub(crate) fn entity_keys(&self) -> &HashMap<Entity, String> {
        &self.cache.entity_keys
    }

    pub(crate) fn entity_key(&self, entity: Entity) -> Option<&String> {
        self.cache.entity_keys.get(&entity)
    }

    pub(crate) fn insert_entity_key(&mut self, entity: Entity, key: String) {
        self.cache.entity_keys.insert(entity, key);
    }

    pub(crate) fn component_deserializer(
        &self,
        name: &str,
    ) -> Option<&ComponentDeserializer> {
        self.components.deserializers.get(name)
    }

    pub(crate) fn component_deserializers(
        &self,
    ) -> impl Iterator<Item = (&String, &ComponentDeserializer)> {
        self.components.deserializers.iter()
    }

    #[cfg(test)]
    pub(crate) fn resource_deserializer(&self, name: &str) -> Option<&ResourceDeserializer> {
        self.resources.deserializers.get(name)
    }

    #[cfg(test)]
    pub(crate) fn clear_dirty_entity_components(&mut self) {
        self.tracking.dirty_entity_components.clear();
    }

    #[cfg(test)]
    pub(crate) fn is_entity_dirty(&self, entity: Entity) -> bool {
        self.tracking.dirty_entity_components.contains_key(&entity)
    }

    #[cfg(test)]
    pub(crate) fn is_dirty_entity_components_empty(&self) -> bool {
        self.tracking.dirty_entity_components.is_empty()
    }

    #[cfg(test)]
    pub(crate) fn is_despawned_entities_empty(&self) -> bool {
        self.tracking.despawned_entities.is_empty()
    }

    #[cfg(test)]
    pub(crate) fn is_resource_dirty(&self, type_id: TypeId) -> bool {
        self.tracking.dirty_resources.contains(&type_id)
    }

    /// Testing constructor w/ mock DB.
    #[cfg(test)]
    pub fn new_mocked() -> Self {
        Self::new()
    }

    /// Create a new session.
    pub fn new() -> Self {
        Self {
            tracking: ChangeTracking::default(),
            components: ComponentRegistry::default(),
            resources: ResourceRegistry::default(),
            cache: PersistenceCache::default(),
        }
    }

    /// Fetch the document for the given key from `db` and deserialize its components
    /// into `world` for `entity`. Also caches the document version.
    pub async fn fetch_and_insert_document(
        &mut self,
        db: &(dyn DatabaseConnection + 'static),
        store: &str,
        world: &mut World,
        key: &str,
        entity: Entity,
        component_names: &[&'static str],
    ) -> Result<(), PersistenceError> {
        if let Some((doc, version)) = db.fetch_document(store, key).await? {
            // Cache the version
            self.cache
                .version_manager
                .set_version(VersionKey::Entity(key.to_string()), version);

            // Deserialize requested components
            for &comp_name in component_names {
                if let Some(val) = doc.get(comp_name) {
                    if let Some(deser) = self.components.deserializers.get(comp_name) {
                        deser(world, entity, val.clone())?;
                    }
                }
            }
        }
        Ok(())
    }

    /// Fetch each registered resource's JSON blob from `db`
    /// and run the registered deserializer to insert it into `world`.
    pub async fn fetch_and_insert_resources(
        &mut self,
        db: &(dyn DatabaseConnection + 'static),
        store: &str,
        world: &mut World,
    ) -> Result<(), PersistenceError> {
        for (res_name, deser) in self.resources.deserializers.iter() {
            if let Some((val, version)) = db.fetch_resource(store, res_name).await? {
                // Cache the version based on the resource's TypeId using the map.
                if let Some(type_id) = self.resources.name_to_type_id.get(res_name) {
                    self.cache
                        .version_manager
                        .set_version(VersionKey::Resource(*type_id), version);
                }
                deser(world, val)?;
            }
        }
        Ok(())
    }

    /// Fetch each named component from `db` for the given document `key` and
    /// run the registered deserializer to insert it into `world` for `entity`.
    pub async fn fetch_and_insert_components(
        &self,
        db: &(dyn DatabaseConnection + 'static),
        store: &str,
        world: &mut World,
        key: &str,
        entity: Entity,
        component_names: &[&'static str],
    ) -> Result<(), PersistenceError> {
        for &comp_name in component_names {
            if let Some(val) = db.fetch_component(store, key, comp_name).await? {
                if let Some(deser) = self.components.deserializers.get(comp_name) {
                    deser(world, entity, val)?;
                }
            }
        }
        Ok(())
    }

    /// Prepare all operations (deletions, creations, updates of entities and resources)
    /// using a Rayon pool of `thread_count` threads.
    pub(crate) fn _prepare_commit(
        session: &PersistenceSession,
        world: &World,
        dirty_entity_components: &HashMap<Entity, HashSet<TypeId>>,
        despawned_entities: &HashSet<Entity>,
        dirty_resources: &HashSet<TypeId>,
        despawned_resources: &HashSet<TypeId>,
        thread_count: usize,
        key_field: &str,
        store: &str,
    ) -> Result<CommitData, PersistenceError> {
        if store.is_empty() {
            return Err(PersistenceError::new("store must be provided for commit"));
        }
        let mut operations = Vec::new();
        let mut newly_created_entities = Vec::new();

        fn insert_meta(
            data: &mut serde_json::Map<String, Value>,
            kind: DocumentKind,
            version: u64,
        ) {
            let mut meta = serde_json::Map::new();
            meta.insert(
                BEVY_PERSISTENCE_DATABASE_VERSION_FIELD.to_string(),
                serde_json::json!(version),
            );
            meta.insert(BEVY_PERSISTENCE_DATABASE_BEVY_TYPE_FIELD.to_string(), serde_json::json!(kind.as_str()));
            data.insert(BEVY_PERSISTENCE_DATABASE_METADATA_FIELD.to_string(), Value::Object(meta));
        }

        // 1) Deletions (order matters less, do sequentially)
        for &entity in despawned_entities {
            if let Some(key) = session.cache.entity_keys.get(&entity) {
                let version_key = VersionKey::Entity(key.clone());
                let current_version = session
                    .cache
                    .version_manager
                    .get_version(&version_key)
                    .ok_or_else(|| PersistenceError::new("Missing version for deletion"))?;
                operations.push(TransactionOperation::DeleteDocument {
                    store: store.to_string(),
                    kind: crate::core::db::connection::DocumentKind::Entity,
                    key: key.clone(),
                    expected_current_version: current_version,
                });
            }
        }

        for &resource_type_id in despawned_resources {
            let Some(name) = session.resources.type_id_to_name.get(&resource_type_id) else {
                continue;
            };
            let version_key = VersionKey::Resource(resource_type_id);
            let Some(current_version) = session.cache.version_manager.get_version(&version_key) else {
                continue;
            };
            operations.push(TransactionOperation::DeleteDocument {
                store: store.to_string(),
                kind: crate::core::db::connection::DocumentKind::Resource,
                key: name.to_string(),
                expected_current_version: current_version,
            });
        }

        // Build a Rayon pool
        let pool = ThreadPoolBuilder::new()
            .num_threads(thread_count)
            .build()
            .map_err(|e| PersistenceError::new(format!("ThreadPool error: {}", e)))?;

        // 2) Creations & Updates (entities)
        // Fix: Handle errors properly in the closure
        let entity_ops_result: Result<Vec<_>, PersistenceError> = pool.install(|| {
            dirty_entity_components
                .par_iter()
                .map(|(&entity, dirty_components)| {
                    let mut data_map = serde_json::Map::new();

                    let component_type_ids: Vec<TypeId> =
                        dirty_components.iter().copied().collect();

                    for component_type_id in component_type_ids {
                        let Some(serializer) = session
                            .components
                            .serializers
                            .get(&component_type_id)
                        else {
                            continue;
                        };
                        if let Some((field_name, value)) = serializer(entity, world)? {
                            data_map.insert(field_name, value);
                        }
                    }
                    if data_map.is_empty() {
                        return Ok(None);
                    }
                    if let Some(key) = session.cache.entity_keys.get(&entity) {
                        // update existing
                        let version_key = VersionKey::Entity(key.clone());
                        let current_version = session
                            .cache
                            .version_manager
                            .get_version(&version_key)
                            .ok_or_else(|| PersistenceError::new("Missing version for update"))?;
                        let next_version = current_version + 1;
                        insert_meta(&mut data_map, DocumentKind::Entity, next_version);
                        Ok(Some((
                            TransactionOperation::UpdateDocument {
                                store: store.to_string(),
                                kind: DocumentKind::Entity,
                                key: key.clone(),
                                expected_current_version: current_version,
                                patch: Value::Object(data_map),
                            },
                            None,
                        )))
                    } else {
                        // create new document
                        insert_meta(&mut data_map, DocumentKind::Entity, 1);
                        let document = Value::Object(data_map);
                        Ok(Some((
                            TransactionOperation::CreateDocument {
                                store: store.to_string(),
                                kind: DocumentKind::Entity,
                                data: document,
                            },
                            Some(entity),
                        )))
                    }
                })
                .filter_map(|res| res.transpose())
                .collect::<Result<Vec<(TransactionOperation, Option<Entity>)>, PersistenceError>>()
        });

        let (entity_ops, created): (Vec<TransactionOperation>, Vec<Option<Entity>>) =
            match entity_ops_result {
                Ok(ops_and_entities) => ops_and_entities.into_iter().unzip(),
                Err(e) => return Err(e),
            };

        operations.extend(entity_ops);
        newly_created_entities.extend(created.into_iter().flatten());

        // 3) Resources (in parallel)
        let resource_ops_result: Result<Vec<_>, PersistenceError> = pool.install(|| {
            let mut resource_ops = Vec::new();

            for &resource_type_id in dirty_resources {
                if despawned_resources.contains(&resource_type_id) {
                    continue;
                }
                if let Some(serializer) = session.resources.serializers.get(&resource_type_id) {
                    match serializer(world, session) {
                        Ok(Some((name, mut value))) => {
                            let version_key = VersionKey::Resource(resource_type_id);
                            if let Some(current_version) =
                                session.cache.version_manager.get_version(&version_key)
                            {
                                // update existing resource
                                let next_version = current_version + 1;
                                if let Some(obj) = value.as_object_mut() {
                                    insert_meta(obj, DocumentKind::Resource, next_version);
                                }
                                resource_ops.push(TransactionOperation::UpdateDocument {
                                    store: store.to_string(),
                                    kind: DocumentKind::Resource,
                                    key: name,
                                    expected_current_version: current_version,
                                    patch: value,
                                });
                            } else {
                                // create new resource
                                if let Some(obj) = value.as_object_mut() {
                                    // Use backend-specific key field instead of hardcoding "_key"
                                    obj.insert(key_field.to_string(), Value::String(name.clone()));
                                    insert_meta(obj, DocumentKind::Resource, 1);
                                }
                                resource_ops.push(TransactionOperation::CreateDocument {
                                    store: store.to_string(),
                                    kind: DocumentKind::Resource,
                                    data: value,
                                });
                            }
                        }
                        Ok(None) => {} // Nothing to do if serializer returns None
                        Err(e) => return Err(e),
                    }
                }
            }

            Ok(resource_ops)
        });

        let resource_ops = resource_ops_result?;
        operations.extend(resource_ops);

        info!(
            "[_prepare_commit] Prepared {} operations.",
            operations.len()
        );
        Ok(CommitData {
            operations,
            new_entities: newly_created_entities,
        })
    }
}

/// Awaitable commit that uses the supplied connection for this request.
pub async fn commit(
    app: &mut App,
    connection: Arc<dyn DatabaseConnection>,
    store: impl Into<String>,
) -> Result<(), PersistenceError> {
    let store = store.into();
    if store.is_empty() {
        return Err(PersistenceError::new("commit store must be provided"));
    }
    let correlation_id = NEXT_CORRELATION_ID.fetch_add(1, Ordering::Relaxed);
    let (tx, mut rx) = oneshot::channel();

    // Insert the sender into the world so the listener system can find it.
    register_commit_listener(app.world_mut(), correlation_id, tx);

    // Send the message to trigger the commit.
    app.world_mut().write_message(TriggerCommit {
        correlation_id: Some(correlation_id),
        target_connection: connection,
        store: store.clone(),
    });

    // The timeout is applied to the entire commit-and-wait process.
    timeout(std::time::Duration::from_secs(60), async {
        // Loop, calling app.update() and checking the receiver.
        // Yield to the executor each time to avoid blocking.
        loop {
            app.update();

            // Check if the receiver has a value without blocking.
            match rx.try_recv() {
                Ok(result) => {
                    info!("Received commit result for correlation ID {}", correlation_id);
                    return result;
                }
                Err(oneshot::error::TryRecvError::Empty) => {
                    // No result yet, yield and try again on the next loop iteration.
                    tokio::task::yield_now().await;
                }
                Err(oneshot::error::TryRecvError::Closed) => {
                    // The sender was dropped, which indicates an error.
                    return Err(PersistenceError::new(
                        "Commit channel closed unexpectedly. The commit listener might have panicked.",
                    ));
                }
            }
        }
    })
    .await
    .map_err(|_| PersistenceError::new("Commit timed out after 60 seconds"))?
}

// Add synchronous conveniences that use the plugin’s runtime
pub fn commit_sync(
    app: &mut App,
    connection: Arc<dyn DatabaseConnection>,
    store: impl Into<String>,
) -> Result<(), PersistenceError> {
    let rt = { app.world().resource::<TokioRuntime>().runtime.clone() };
    rt.block_on(commit(app, connection, store))
}

#[cfg(test)]
mod arango_session {
    use super::*;
    use crate::core::persist::Persist;
    use crate::bevy::registration::COMPONENT_REGISTRY;
    use bevy::prelude::World;
    use bevy_persistence_database_derive::persist;
    use serde_json::json;
    use std::any::TypeId;

    fn setup() {
        // Clear the global registry to avoid test pollution from other modules
        let mut registry = COMPONENT_REGISTRY.lock().unwrap();
        registry.clear();
    }

    #[persist(resource)]
    struct MyRes {
        value: i32,
    }
    #[persist(component)]
    struct MyComp {
        value: i32,
    }

    #[test]
    fn new_session_is_empty() {
        setup();
        let session = PersistenceSession::new_mocked();
        assert!(session.is_dirty_entity_components_empty());
        assert!(session.is_despawned_entities_empty());
    }

    #[test]
    fn deserializer_inserts_component() {
        setup();
        let mut world = World::new();
        let entity = world.spawn_empty().id();

        let mut session = PersistenceSession::new();
        session.register_component::<MyComp>();

        let deserializer = session.component_deserializer(MyComp::name()).unwrap();
        deserializer(&mut world, entity, json!({"value": 42})).unwrap();

        assert_eq!(world.get::<MyComp>(entity).unwrap().value, 42);
    }

    #[test]
    fn deserializer_inserts_resource() {
        setup();
        let mut world = World::new();

        let mut session = PersistenceSession::new();
        session.register_resource::<MyRes>();

        let deserializer = session.resource_deserializer(MyRes::name()).unwrap();
        deserializer(&mut world, json!({"value": 5})).unwrap();

        assert_eq!(world.resource::<MyRes>().value, 5);
    }

    #[test]
    fn prepare_commit_emits_resource_delete_when_marked_despawned() {
        setup();
        let world = World::new();

        let mut session = PersistenceSession::new();
        session.register_resource::<MyRes>();

        let tid = TypeId::of::<MyRes>();
        session
            .version_manager_mut()
            .set_version(VersionKey::Resource(tid), 3);

        let dirty_entity_components: HashMap<Entity, HashSet<TypeId>> = HashMap::new();
        let despawned_entities = HashSet::new();
        let dirty_resources = HashSet::new();
        let mut despawned_resources = HashSet::new();
        despawned_resources.insert(tid);

        let data = PersistenceSession::_prepare_commit(
            &session,
            &world,
            &dirty_entity_components,
            &despawned_entities,
            &dirty_resources,
            &despawned_resources,
            1,
            "_key",
            "store",
        )
        .unwrap();

        assert_eq!(data.operations.len(), 1);
        match &data.operations[0] {
            TransactionOperation::DeleteDocument {
                store,
                kind,
                key,
                expected_current_version,
            } => {
                assert_eq!(store, "store");
                assert!(matches!(kind, DocumentKind::Resource));
                assert_eq!(key, MyRes::name());
                assert_eq!(*expected_current_version, 3);
            }
            other => panic!("expected resource delete operation, got {other:?}"),
        }
    }
}
