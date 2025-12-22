//! Core ECS‐to‐Arango bridge: defines `PersistenceSession`.
//! Handles local cache, change tracking, and commit logic (create/update/delete).

use crate::db::connection::{
    BEVY_PERSISTENCE_VERSION_FIELD, BEVY_TYPE_FIELD, DatabaseConnection, DocumentKind,
    PersistenceError, TransactionOperation,
};
use crate::persist::Persist;
use crate::plugins::TriggerCommit;
use crate::plugins::persistence_plugin::{CommitEventListeners, TokioRuntime};
use crate::versioning::version_manager::{VersionKey, VersionManager};
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

/// Manages a "unit of work": local World cache + change tracking + async runtime.
#[derive(Resource)]
pub struct PersistenceSession {
    pub(crate) dirty_entities: HashSet<Entity>,
    pub despawned_entities: HashSet<Entity>,
    pub entity_keys: HashMap<Entity, String>,
    pub dirty_resources: HashSet<TypeId>,
    pub(crate) version_manager: VersionManager,
    component_serializers: HashMap<TypeId, ComponentSerializer>,
    pub(crate) component_deserializers: HashMap<String, ComponentDeserializer>,
    pub(crate) component_type_id_to_name: HashMap<TypeId, &'static str>,
    // New: reverse lookup and presence checkers
    pub(crate) component_name_to_type_id: HashMap<String, TypeId>,
    pub(crate) component_presence:
        HashMap<String, Box<dyn Fn(&World, Entity) -> bool + Send + Sync>>,
    resource_serializers: HashMap<TypeId, ResourceSerializer>,
    resource_deserializers: HashMap<String, ResourceDeserializer>,
    resource_name_to_type_id: HashMap<String, TypeId>,
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
        self.component_type_id_to_name.insert(type_id, ser_key);
        // reverse lookup
        self.component_name_to_type_id
            .insert(ser_key.to_string(), type_id);
        // presence checker
        self.component_presence.insert(
            ser_key.to_string(),
            Box::new(|world: &World, entity: Entity| world.entity(entity).contains::<T>()),
        );
        self.component_serializers.insert(
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
        self.component_deserializers.insert(
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
        // Insert serializer into map keyed by TypeId
        self.resource_serializers.insert(
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
        self.resource_deserializers.insert(
            de_key.to_string(),
            Box::new(|world, json_val| {
                let res: R = serde_json::from_value(json_val)
                    .map_err(|e| PersistenceError::new(e.to_string()))?;
                world.insert_resource(res);
                Ok(())
            }),
        );
        self.resource_name_to_type_id
            .insert(de_key.to_string(), type_id);
    }

    /// Manually mark a resource as needing persistence.
    pub fn mark_resource_dirty<R: Resource>(&mut self) {
        self.dirty_resources.insert(TypeId::of::<R>());
    }

    /// Manually mark an entity as having been removed.
    pub fn mark_despawned(&mut self, entity: Entity) {
        self.despawned_entities.insert(entity);
    }

    /// Testing constructor w/ mock DB.
    #[cfg(test)]
    pub fn new_mocked() -> Self {
        Self::new()
    }

    /// Create a new session.
    pub fn new() -> Self {
        Self {
            component_serializers: HashMap::new(),
            component_deserializers: HashMap::new(),
            component_type_id_to_name: HashMap::new(),
            component_name_to_type_id: HashMap::new(),
            component_presence: HashMap::new(),
            resource_serializers: HashMap::new(),
            resource_deserializers: HashMap::new(),
            resource_name_to_type_id: HashMap::new(),
            dirty_entities: HashSet::new(),
            despawned_entities: HashSet::new(),
            dirty_resources: HashSet::new(),
            entity_keys: HashMap::new(),
            version_manager: VersionManager::new(),
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
            self.version_manager
                .set_version(VersionKey::Entity(key.to_string()), version);

            // Deserialize requested components
            for &comp_name in component_names {
                if let Some(val) = doc.get(comp_name) {
                    if let Some(deser) = self.component_deserializers.get(comp_name) {
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
        for (res_name, deser) in self.resource_deserializers.iter() {
            if let Some((val, version)) = db.fetch_resource(store, res_name).await? {
                // Cache the version based on the resource's TypeId using the map.
                if let Some(type_id) = self.resource_name_to_type_id.get(res_name) {
                    self.version_manager
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
                if let Some(deser) = self.component_deserializers.get(comp_name) {
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
        dirty_entities: &HashSet<Entity>,
        despawned_entities: &HashSet<Entity>,
        dirty_resources: &HashSet<TypeId>,
        thread_count: usize,
        key_field: &str,
        store: &str,
    ) -> Result<CommitData, PersistenceError> {
        if store.is_empty() {
            return Err(PersistenceError::new("store must be provided for commit"));
        }
        let mut operations = Vec::new();
        let mut newly_created_entities = Vec::new();

        // 1) Deletions (order matters less, do sequentially)
        for &entity in despawned_entities {
            if let Some(key) = session.entity_keys.get(&entity) {
                let version_key = VersionKey::Entity(key.clone());
                let current_version = session
                    .version_manager
                    .get_version(&version_key)
                    .ok_or_else(|| PersistenceError::new("Missing version for deletion"))?;
                operations.push(TransactionOperation::DeleteDocument {
                    store: store.to_string(),
                    kind: crate::db::connection::DocumentKind::Entity,
                    key: key.clone(),
                    expected_current_version: current_version,
                });
            }
        }

        // Build a Rayon pool
        let pool = ThreadPoolBuilder::new()
            .num_threads(thread_count)
            .build()
            .map_err(|e| PersistenceError::new(format!("ThreadPool error: {}", e)))?;

        // 2) Creations & Updates (entities)
        // Fix: Handle errors properly in the closure
        let entity_ops_result: Result<Vec<_>, PersistenceError> = pool.install(|| {
            dirty_entities
                .par_iter()
                .map(|&entity| {
                    let mut data_map = serde_json::Map::new();
                    // serialize each component
                    for serializer in session.component_serializers.values() {
                        if let Some((field_name, value)) = serializer(entity, world)? {
                            data_map.insert(field_name, value);
                        }
                    }
                    if data_map.is_empty() {
                        return Ok(None);
                    }
                    if let Some(key) = session.entity_keys.get(&entity) {
                        // update existing
                        let version_key = VersionKey::Entity(key.clone());
                        let current_version = session
                            .version_manager
                            .get_version(&version_key)
                            .ok_or_else(|| PersistenceError::new("Missing version for update"))?;
                        let next_version = current_version + 1;
                        data_map.insert(
                            BEVY_PERSISTENCE_VERSION_FIELD.to_string(),
                            serde_json::json!(next_version),
                        );
                        data_map.insert(
                            BEVY_TYPE_FIELD.to_string(),
                            serde_json::json!(DocumentKind::Entity.as_str()),
                        );
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
                        data_map.insert(
                            BEVY_PERSISTENCE_VERSION_FIELD.to_string(),
                            serde_json::json!(1u64),
                        );
                        data_map.insert(
                            BEVY_TYPE_FIELD.to_string(),
                            serde_json::json!(DocumentKind::Entity.as_str()),
                        );
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
                if let Some(serializer) = session.resource_serializers.get(&resource_type_id) {
                    match serializer(world, session) {
                        Ok(Some((name, mut value))) => {
                            let version_key = VersionKey::Resource(resource_type_id);
                            if let Some(current_version) =
                                session.version_manager.get_version(&version_key)
                            {
                                // update existing resource
                                let next_version = current_version + 1;
                                if let Some(obj) = value.as_object_mut() {
                                    obj.insert(
                                        BEVY_PERSISTENCE_VERSION_FIELD.to_string(),
                                        serde_json::json!(next_version),
                                    );
                                    obj.insert(
                                        BEVY_TYPE_FIELD.to_string(),
                                        serde_json::json!(DocumentKind::Resource.as_str()),
                                    );
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
                                    obj.insert(
                                        BEVY_PERSISTENCE_VERSION_FIELD.to_string(),
                                        serde_json::json!(1u64),
                                    );
                                    // Use backend-specific key field instead of hardcoding "_key"
                                    obj.insert(key_field.to_string(), Value::String(name.clone()));
                                    obj.insert(
                                        BEVY_TYPE_FIELD.to_string(),
                                        serde_json::json!(DocumentKind::Resource.as_str()),
                                    );
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
    app.world_mut()
        .resource_mut::<CommitEventListeners>()
        .0
        .insert(correlation_id, tx);

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
    let rt = { app.world().resource::<TokioRuntime>().0.clone() };
    rt.block_on(commit(app, connection, store))
}

#[cfg(test)]
mod arango_session {
    use super::*;
    use crate::persist::Persist;
    use crate::registration::COMPONENT_REGISTRY;
    use bevy::prelude::World;
    use bevy_persistence_database_derive::persist;
    use serde_json::json;

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
        assert!(session.dirty_entities.is_empty());
        assert!(session.despawned_entities.is_empty());
    }

    #[test]
    fn deserializer_inserts_component() {
        setup();
        let mut world = World::new();
        let entity = world.spawn_empty().id();

        let mut session = PersistenceSession::new();
        session.register_component::<MyComp>();

        let deserializer = session.component_deserializers.get(MyComp::name()).unwrap();
        deserializer(&mut world, entity, json!({"value": 42})).unwrap();

        assert_eq!(world.get::<MyComp>(entity).unwrap().value, 42);
    }

    #[test]
    fn deserializer_inserts_resource() {
        setup();
        let mut world = World::new();

        let mut session = PersistenceSession::new();
        session.register_resource::<MyRes>();

        let deserializer = session.resource_deserializers.get(MyRes::name()).unwrap();
        deserializer(&mut world, json!({"value": 5})).unwrap();

        assert_eq!(world.resource::<MyRes>().value, 5);
    }
}
