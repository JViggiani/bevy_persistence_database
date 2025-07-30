//! Core ECS‐to‐Arango bridge: defines `PersistenceSession`.
//! Handles local cache, change tracking, and commit logic (create/update/delete).

use crate::db::connection::{
    DatabaseConnection, DocumentKey, DocumentRev, PersistenceError, TransactionOperation,
};
use crate::plugins::TriggerCommit;
use bevy::prelude::{info, debug, warn, App, Component, Entity, Resource, World};
use serde_json::Value;
use std::{
    any::TypeId,
    collections::{HashMap, HashSet},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};
use crate::persist::Persist;
use crate::plugins::persistence_plugin::CommitEventListeners;
use tokio::sync::oneshot;
use tracing::{trace};

/// A unique ID generator for correlating commit requests and responses.
static NEXT_CORRELATION_ID: AtomicU64 = AtomicU64::new(1);

type ComponentSerializer   = Box<dyn Fn(Entity, &World) -> Result<Option<(String, Value)>, PersistenceError> + Send + Sync>;
type ComponentDeserializer = Box<dyn Fn(&mut World, Entity, Value) -> Result<(), PersistenceError> + Send + Sync>;
type ResourceSerializer    = Box<dyn Fn(&World, &PersistenceSession) -> Result<Option<(String, Value)>, PersistenceError> + Send + Sync>;
type ResourceDeserializer  = Box<dyn Fn(&mut World, Value) -> Result<(), PersistenceError> + Send + Sync>;

/// Holds the database key and revision for a cached entity.
#[derive(Clone, Debug)]
pub(crate) struct EntityMetadata {
    pub(crate) key: DocumentKey,
    pub(crate) rev: DocumentRev,
}

/// Manages a “unit of work”: local World cache + change tracking + async runtime.
#[derive(Resource)]
pub struct PersistenceSession {
    pub db: Arc<dyn DatabaseConnection>,
    pub(crate) dirty_entities: HashSet<Entity>,
    pub(crate) despawned_entities: HashSet<Entity>,
    pub(crate) entity_meta: HashMap<Entity, EntityMetadata>,
    pub(crate) dirty_resources: HashSet<TypeId>,
    pub(crate) despawned_resources: HashSet<TypeId>,

    // map from TypeId to full‐serializer (for new entities)
    component_serializers: HashMap<TypeId, ComponentSerializer>,
    // map from component name -> serializer (for updates)
    component_serializers_by_name: HashMap<String, ComponentSerializer>,

    // track exactly which components changed on which entity
    pub(crate) component_changes: HashMap<Entity, HashSet<String>>,

    pub(crate) component_deserializers: HashMap<String, ComponentDeserializer>,
    resource_serializers: HashMap<TypeId, ResourceSerializer>,
    resource_deserializers: HashMap<String, ResourceDeserializer>,
    resource_names: HashMap<TypeId, DocumentKey>,
}

pub(crate) struct CommitData {
    pub(crate) operations: Vec<TransactionOperation>,
    pub(crate) new_entities: Vec<Entity>,
    pub(crate) updated_entities: Vec<Entity>,
    pub(crate) deleted_entities: Vec<Entity>,
}

impl PersistenceSession {
    /// Registers a component type for persistence.
    ///
    /// This method sets up both serialization and deserialization for any
    /// component that implements the `Persist` marker trait.
    pub fn register_component<T: Component + Persist>(&mut self) {
        let name = T::name().to_string();
        let type_id = TypeId::of::<T>();

        // Create serializer closure once
        let ser_name = name.clone();
        let serializer: ComponentSerializer = Box::new(move |entity, world| {
            if let Some(c) = world.get::<T>(entity) {
                let v = serde_json::to_value(c)
                    .map_err(|_| PersistenceError::Generic("Serialization failed".into()))?;
                Ok(Some((ser_name.clone(), v)))
            } else {
                Ok(None)
            }
        });

        // Store in both maps
        self.component_serializers.insert(type_id, serializer);
        
        // Create a second serializer for the by-name map
        let ser_name2 = name.clone();
        let serializer2: ComponentSerializer = Box::new(move |entity, world| {
            if let Some(c) = world.get::<T>(entity) {
                let v = serde_json::to_value(c)
                    .map_err(|_| PersistenceError::Generic("Serialization failed".into()))?;
                Ok(Some((ser_name2.clone(), v)))
            } else {
                Ok(None)
            }
        });
        self.component_serializers_by_name.insert(name, serializer2);

        let de_key = T::name();
        self.component_deserializers.insert(de_key.to_string(), Box::new(
            |world, entity, json_val| {
                let comp: T = serde_json::from_value(json_val)
                    .map_err(|e| PersistenceError::Generic(e.to_string()))?;
                // Use the trait method to insert the component without triggering change detection.
                T::insert_bypassing_change_detection(world, entity, comp);
                Ok(())
            },
        ));
    }

    /// Registers a resource type for persistence.
    ///
    /// This method sets up both serialization and deserialization for any
    /// resource that implements the `Persist` marker trait.
    pub fn register_resource<R: Resource + Persist>(&mut self) {
        let ser_key = R::name();
        let type_id = std::any::TypeId::of::<R>();
        self.resource_names.insert(type_id, ser_key.to_string());
        // Insert serializer into map keyed by TypeId
        self.resource_serializers.insert(type_id, Box::new(move |world, session| {
            // Only serialize if resource marked dirty
            if !session.dirty_resources.contains(&type_id) {
                return Ok(None);
            }
            // Fetch and serialize the resource
            if let Some(r) = world.get_resource::<R>() {
                let v = serde_json::to_value(r)
                    .map_err(|e| PersistenceError::Generic(e.to_string()))?;
                if v.is_null() {
                    return Err(PersistenceError::Generic("Could not serialize".into()));
                }
                Ok(Some((ser_key.to_string(), v)))
            } else {
                Ok(None)
            }
        }));

        let de_key = R::name();
        self.resource_deserializers.insert(de_key.to_string(), Box::new(
            |world, json_val| {
                let res: R = serde_json::from_value(json_val)
                    .map_err(|e| PersistenceError::Generic(e.to_string()))?;
                world.insert_resource(res);
                Ok(())
            },
        ));
    }

    /// Manually mark a resource as needing persistence.
    pub fn mark_resource_dirty<R: Resource>(&mut self) {
        self.dirty_resources.insert(TypeId::of::<R>());
    }

    /// Manually mark an entity as having been removed.
    pub fn mark_despawned(&mut self, entity: Entity) {
        self.despawned_entities.insert(entity);
    }

    /// Manually mark a resource as having been removed from the world.
    ///
    /// This is necessary because, unlike with components, Bevy does not provide
    /// a built-in way to automatically detect when a resource has been removed.
    pub fn mark_resource_despawned<R: Resource + Persist>(&mut self) {
        self.despawned_resources.insert(TypeId::of::<R>());
    }

    /// Testing constructor w/ mock DB.
    #[cfg(test)]
    pub fn new_mocked(db: Arc<dyn DatabaseConnection>) -> Self {
        let mut s = Self::new(db);
        // override any default init if needed
        s
    }

    /// Create a new session.
    pub fn new(db: Arc<dyn DatabaseConnection>) -> Self {
        Self {
            db,
            component_serializers: HashMap::new(),
            component_serializers_by_name: HashMap::new(),
            component_changes: HashMap::new(),
            component_deserializers: HashMap::new(),
            resource_serializers: HashMap::new(),
            resource_deserializers: HashMap::new(),
            dirty_entities: HashSet::new(),
            despawned_entities: HashSet::new(),
            dirty_resources: HashSet::new(),
            despawned_resources: HashSet::new(),
            entity_meta: HashMap::new(),
            resource_names: HashMap::new(),
        }
    }

    /// Fetch each named component from `db` for the given document `key` and
    /// run the registered deserializer to insert it into `world` for `entity`.
    pub async fn fetch_and_insert_components(
        &self,
        db: &(dyn DatabaseConnection + 'static),
        world: &mut World,
        key: &str,
        entity: Entity,
        component_names: &[&'static str],
    ) -> Result<(), PersistenceError> {
        for &comp_name in component_names {
            if let Some(val) = db.fetch_component(key, comp_name).await? {
                if let Some(deser) = self.component_deserializers.get(comp_name) {
                    deser(world, entity, val)?;
                }
            }
        }
        Ok(())
    }

    /// Fetch each registered resource’s JSON blob from `db`
    /// and run the registered deserializer to insert it into `world`.
    pub async fn fetch_and_insert_resources(
        &self,
        db: &(dyn DatabaseConnection + 'static),
        world: &mut World,
    ) -> Result<(), PersistenceError> {
        for (res_name, deser) in self.resource_deserializers.iter() {
            if let Some(val) = db.fetch_resource(res_name).await? {
                deser(world, val)?;
            }
        }
        Ok(())
    }
}

/// Serialize all data. This will fail early if any serialization fails.
pub(crate) fn _prepare_commit(
    session: &PersistenceSession,
    world: &World,
) -> Result<CommitData, PersistenceError> {
    let mut delete_ops = Vec::new();
    let mut create_ops = Vec::new();
    let mut update_ops = Vec::new();
    let mut new_entities = Vec::new();
    let mut updated_entities = Vec::new();
    let mut deleted_entities = Vec::new();

    // Deletions
    for &e in &session.despawned_entities {
        if let Some(meta) = session.entity_meta.get(&e) {
            delete_ops.push(TransactionOperation::DeleteDocument {
                key: meta.key.clone(),
                rev: meta.rev.clone(),
            });
            deleted_entities.push(e);
        }
    }
    // Creations & Updates
    for &e in &session.dirty_entities {
        if let Some(meta) = session.entity_meta.get(&e) {
            // gather exactly those names marked changed
            let mut patch = HashMap::new();
            if let Some(names) = session.component_changes.get(&e) {
                for name in names {
                    if let Some(ser) = session.component_serializers_by_name.get(name) {
                        if let Some((n, v)) = ser(e, world)? {
                            patch.insert(n, v);
                        }
                    }
                }
            }
            if patch.is_empty() {
                continue;
            }
            trace!("Preparing UPDATE for entity {:?} (key: {}, rev: {})", e, meta.key, meta.rev);
            update_ops.push(TransactionOperation::UpdateDocument {
                key: meta.key.clone(),
                rev: meta.rev.clone(),
                patch,
            });
            updated_entities.push(e);
        } else {
            // This is a new entity. Serialize all its components.
            let mut map = serde_json::Map::new();
            for ser in session.component_serializers.values() {
                if let Some((n, v)) = ser(e, world)? {
                    map.insert(n, v);
                }
            }

            if map.is_empty() {
                continue; // Nothing to persist for this entity.
            }
            let doc = Value::Object(map);
            create_ops.push(TransactionOperation::CreateDocument(doc));
            new_entities.push(e);
        }
    }

    let mut operations = Vec::new();
    operations.extend(delete_ops);
    operations.extend(create_ops);
    operations.extend(update_ops);

    // Resources
    for &tid in &session.dirty_resources {
        if let Some(ser) = session.resource_serializers.get(&tid) {
            if let Some((n, v)) = ser(world, session)? {
                operations.push(TransactionOperation::UpsertResource(n.to_string(), v));
            }
        }
    }
    // Resource Deletions
    for &tid in &session.despawned_resources {
        if let Some(name) = session.resource_names.get(&tid) {
            operations.push(TransactionOperation::DeleteResource(name.clone()));
        }
    }
    info!("[_prepare_commit] Prepared {} operations.", operations.len());
    Ok(CommitData {
        operations,
        new_entities,
        updated_entities,
        deleted_entities,
    })
}

/// This function provides a clean `await`-able interface for the event-driven
/// commit system. It sends a `TriggerCommit` event, then waits for a
/// `CommitCompleted` event with a matching correlation ID.
pub async fn commit_and_wait(app: &mut App) -> Result<(), PersistenceError> {
    // Run an update immediately before starting the commit process.
    // This ensures that any entity/resource changes made just before calling `commit`
    // are detected by Bevy's change tracking systems.
    app.update();

    let correlation_id = NEXT_CORRELATION_ID.fetch_add(1, Ordering::Relaxed);
    let (tx, mut rx) = oneshot::channel();

    debug!("Creating listener for commit with correlation ID {}", correlation_id);

    // Insert the sender into the world so the listener system can find it.
    app.world_mut()
        .resource_mut::<CommitEventListeners>()
        .0
        .insert(correlation_id, tx);

    debug!("Sending TriggerCommit event with correlation ID {}", correlation_id);
    // Send the event to trigger the commit.
    app.world_mut().send_event(TriggerCommit {
        correlation_id: Some(correlation_id),
    });

    let mut iterations = 0;
    // Loop, calling app.update() and checking the receiver.
    loop {
        iterations += 1;
        if iterations % 100 == 0 {
            warn!("commit_and_wait for ID {} has been polling for {} iterations", correlation_id, iterations);
        }

        // Always run an update to drive the Bevy schedule forward.
        // This processes events, runs systems, and polls tasks.
        app.update();

        // Use a non-blocking check on the receiver.
        match rx.try_recv() {
            Ok(res) => {
                info!("Received commit result for correlation ID {} after {} iterations", correlation_id, iterations);
                // The result is received. The commands to insert Guids have been queued
                // in the previous `app.update()`. We run one more `app.update()` to
                // ensure those commands are applied to the World.
                app.update();
                debug!("Final update complete for commit {}, returning result", correlation_id);
                return res;
            }
            Err(oneshot::error::TryRecvError::Empty) => {
                // Not ready yet, sleep briefly to yield control and prevent a busy-wait.
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
            Err(oneshot::error::TryRecvError::Closed) => {
                // The sender was dropped, which means the commit failed without sending a message.
                return Err(PersistenceError::Generic(
                    "Commit channel closed unexpectedly".to_string(),
                ));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::connection::MockDatabaseConnection;
    use crate::persist::Persist;
    use crate::registration::COMPONENT_REGISTRY;
    use bevy::prelude::World;
    use bevy_arangodb_derive::persist;
    use serde_json::json;

    fn setup() {
        // Clear the global registry to avoid test pollution from other modules
        let mut registry = COMPONENT_REGISTRY.lock().unwrap();
        registry.clear();
    }

    #[persist(resource)]
    struct MyRes { value: i32 }
    #[persist(component)]
    struct MyComp { value: i32 }

    #[test]
    fn new_session_is_empty() {
        setup();
        let mock_db = MockDatabaseConnection::new();
        let session = PersistenceSession::new_mocked(Arc::new(mock_db));
        assert!(session.dirty_entities.is_empty());
        assert!(session.despawned_entities.is_empty());
    }

    #[test]
    fn deserializer_inserts_component() {
        setup();
        let mut world = World::new();
        let entity = world.spawn_empty().id();

        let mut session = PersistenceSession::new(Arc::new(MockDatabaseConnection::new()));
        session.register_component::<MyComp>();

        let deserializer = session.component_deserializers.get(MyComp::name()).unwrap();
        deserializer(&mut world, entity, json!({"value": 42})).unwrap();

        assert_eq!(world.get::<MyComp>(entity).unwrap().value, 42);
    }

    #[test]
    fn deserializer_inserts_resource() {
        setup();
        let mut world = World::new();

        let mut session = PersistenceSession::new(Arc::new(MockDatabaseConnection::new()));
        session.register_resource::<MyRes>();

        let deserializer = session.resource_deserializers.get(MyRes::name()).unwrap();
        deserializer(&mut world, json!({"value": 5})).unwrap();

        assert_eq!(world.resource::<MyRes>().value, 5);
    }
}