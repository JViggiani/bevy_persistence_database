//! Core ECS‐to‐Arango bridge: defines `ArangoSession` and the abstract
//! `DatabaseConnection` trait.
//! Handles local cache, change tracking, and commit logic (create/update/delete).

// only pull in `automock` when compiling tests
#[cfg(test)]
use mockall::automock;

use downcast_rs::{Downcast, impl_downcast};
use bevy::prelude::{Component, Entity, Resource, World, App};
use futures::future::BoxFuture;
use serde_json::Value;
use std::{
    any::TypeId,
    collections::{HashMap, HashSet},
    fmt,
    sync::Arc,
};
use crate::persist::Persist;

type ComponentSerializer   = Box<dyn Fn(Entity, &World) -> Result<Option<(String, Value)>, ArangoError> + Send + Sync>;
type ComponentDeserializer = Box<dyn Fn(&mut World, Entity, Value) -> Result<(), ArangoError> + Send + Sync>;
type ResourceSerializer    = Box<dyn Fn(&World, &ArangoSession) -> Result<Option<(String, Value)>, ArangoError> + Send + Sync>;
type ResourceDeserializer  = Box<dyn Fn(&mut World, Value) -> Result<(), ArangoError> + Send + Sync>;

/// An error type for database operations.
#[derive(Debug)]
pub struct ArangoError(pub String);

impl fmt::Display for ArangoError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ArangoDB Error: {}", self.0)
    }
}

impl std::error::Error for ArangoError {}

/// Abstracts database operations via async returns but remains object-safe.
#[cfg_attr(test, automock)]
pub trait DatabaseConnection: Send + Sync + Downcast + fmt::Debug {
    /// Create a new document (entity). Returns the new doc's key.
    fn create_document(
        &self,
        data: Value,
    ) -> BoxFuture<'static, Result<String, ArangoError>>;

    /// Update an existing document.
    fn update_document(
        &self,
        entity_key: &str,
        patch: Value,
    ) -> BoxFuture<'static, Result<(), ArangoError>>;

    /// Delete a document (entity).
    fn delete_document(
        &self,
        entity_key: &str,
    ) -> BoxFuture<'static, Result<(), ArangoError>>;

    /// Execute a raw AQL query returning document keys.
    fn query(
        &self,
        aql: String,
        bind_vars: std::collections::HashMap<String, Value>,
    ) -> BoxFuture<'static, Result<Vec<String>, ArangoError>>;

    /// Fetch a single component’s JSON blob (or `None` if missing).
    fn fetch_component(
        &self,
        entity_key: &str,
        comp_name: &str,
    ) -> BoxFuture<'static, Result<Option<Value>, ArangoError>>;

    /// Fetch a single resource’s JSON blob (or `None` if missing).
    fn fetch_resource(
        &self,
        resource_name: &str,
    ) -> BoxFuture<'static, Result<Option<Value>, ArangoError>>;

    /// Creates or replaces a resource document.
    fn upsert_resource(
        &self,
        resource_name: &str,
        data: Value,
    ) -> BoxFuture<'static, Result<(), ArangoError>>;

    /// Clear all documents from the entities collection.
    fn clear_entities(&self) -> BoxFuture<'static, Result<(), ArangoError>>;

    /// Clear all documents from the resources collection.
    fn clear_resources(&self) -> BoxFuture<'static, Result<(), ArangoError>>;
}
impl_downcast!(DatabaseConnection);

/// Manages a “unit of work”: local World cache + change tracking + async runtime.
#[derive(Resource)]
pub struct ArangoSession {
    pub db: Arc<dyn DatabaseConnection>,
    pub(crate) dirty_entities: HashSet<Entity>,
    pub despawned_entities: HashSet<Entity>,
    pub entity_keys: HashMap<Entity, String>,
    pub dirty_resources: HashSet<TypeId>,
    component_serializers: HashMap<TypeId, ComponentSerializer>,
    component_deserializers: HashMap<String, ComponentDeserializer>,
    resource_serializers: HashMap<TypeId, ResourceSerializer>,
    resource_deserializers: HashMap<String, ResourceDeserializer>,
}

struct CommitData {
    deletes: Vec<Entity>,
    updates: Vec<(Entity, Value)>,
    resources: Vec<(String, Value)>,
}

impl ArangoSession {
    /// Registers a component type for persistence.
    ///
    /// This method sets up both serialization and deserialization for any
    /// component that implements the `Persist` marker trait.
    pub fn register_component<T: Component + Persist>(&mut self) {
        let ser_key = T::name();
        let type_id = TypeId::of::<T>();
        self.component_serializers.insert(type_id, Box::new(
            move |entity, world| -> Result<Option<(String, Value)>, ArangoError> {
                if let Some(c) = world.get::<T>(entity) {
                    let v = serde_json::to_value(c).map_err(|_| ArangoError("Serialization failed".into()))?;
                    if v.is_null() {
                        return Err(ArangoError("Could not serialize".into()));
                    }
                    Ok(Some((ser_key.to_string(), v)))
                } else {
                    Ok(None)
                }
            },
        ));

        let de_key = T::name();
        self.component_deserializers.insert(de_key.to_string(), Box::new(
            |world, entity, json_val| {
                let comp: T = serde_json::from_value(json_val).map_err(|e| ArangoError(e.to_string()))?;
                world.entity_mut(entity).insert(comp);
                Ok(())
            }
        ));
    }

    /// Registers a resource type for persistence.
    ///
    /// This method sets up both serialization and deserialization for any
    /// resource that implements the `Persist` marker trait.
    pub fn register_resource<R: Resource + Persist>(&mut self) {
        let ser_key = R::name();
        let type_id = std::any::TypeId::of::<R>();
        // Insert serializer into map keyed by TypeId
        self.resource_serializers.insert(type_id, Box::new(move |world, session| {
            // Only serialize if resource marked dirty
            if !session.dirty_resources.contains(&type_id) {
                return Ok(None);
            }
            // Fetch and serialize the resource
            if let Some(r) = world.get_resource::<R>() {
                let v = serde_json::to_value(r).map_err(|e| ArangoError(e.to_string()))?;
                if v.is_null() {
                    return Err(ArangoError("Could not serialize".into()));
                }
                Ok(Some((ser_key.to_string(), v)))
            } else {
                Ok(None)
            }
        }));

        let de_key = R::name();
        self.resource_deserializers.insert(de_key.to_string(), Box::new(
            |world, json_val| {
                let res: R = serde_json::from_value(json_val).map_err(|e| ArangoError(e.to_string()))?;
                world.insert_resource(res);
                Ok(())
            }
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

    /// Testing constructor w/ mock DB.
    #[cfg(test)]
    pub fn new_mocked(db: Arc<dyn DatabaseConnection>) -> Self {
        Self {
            db,
            component_serializers: HashMap::new(),
            component_deserializers: HashMap::new(),
            resource_serializers: HashMap::new(),
            resource_deserializers: HashMap::new(),
            dirty_entities: HashSet::new(),
            despawned_entities: HashSet::new(),
            dirty_resources: HashSet::new(),
            entity_keys: HashMap::new(),
        }
    }

    /// Create a new session.
    pub fn new(db: Arc<dyn DatabaseConnection>) -> Self {
        Self {
            db,
            component_serializers: HashMap::new(),
            component_deserializers: HashMap::new(),
            resource_serializers: HashMap::new(),
            resource_deserializers: HashMap::new(),
            dirty_entities: HashSet::new(),
            despawned_entities: HashSet::new(),
            dirty_resources: HashSet::new(),
            entity_keys: HashMap::new(),
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
    ) -> Result<(), ArangoError> {
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
    ) -> Result<(), ArangoError> {
        for (res_name, deser) in self.resource_deserializers.iter() {
            if let Some(val) = db.fetch_resource(res_name).await? {
                deser(world, val)?;
            }
        }
        Ok(())
    }
}

/// Serialize all data. This will fail early if any serialization fails.
fn _prepare_commit(session: &ArangoSession, world: &World) -> Result<CommitData, ArangoError> {
    let mut updates = Vec::new();
    for entity in session.dirty_entities.iter() {
        let mut components_json = serde_json::Map::new();
        for serializer in session.component_serializers.values() {
            if let Some((name, value)) = serializer(*entity, world)? {
                components_json.insert(name, value);
            }
        }
        updates.push((*entity, Value::Object(components_json)));
    }

    // Serialize dirty resources
    let mut resources = Vec::new();
    for type_id in session.dirty_resources.iter() {
        if let Some(serializer) = session.resource_serializers.get(type_id) {
            if let Some((name, value)) = serializer(world, session)? {
                resources.push((name, value));
            }
        }
    }

    Ok(CommitData {
        deletes: session.despawned_entities.iter().copied().collect(),
        updates,
        resources,
    })
}

/// Persist new, changed, or despawned entities to the database.
pub(crate) async fn commit(
    session: &mut ArangoSession,
    world: &mut World,
) -> Result<(), ArangoError> {
    // short‐circuit if nothing to do
    if session.dirty_entities.is_empty()
        && session.despawned_entities.is_empty()
        && session.dirty_resources.is_empty()
    {
        return Ok(());
    }

    // Step 1: Serialize all dirty data from the World.
    // This is done first to ensure that if serialization fails, we haven't
    // made any changes to the database yet.
    let commit_data = _prepare_commit(session, world)?;

    // Step 2: Process Deletions.
    // Remove entities that were despawned in Bevy from the database.
    for entity in commit_data.deletes.iter() {
        if let Some(key) = session.entity_keys.get(entity) {
            session.db.delete_document(key)
                .await
                .map_err(|_| ArangoError("DB operation failed".into()))?;
        }
    }

    // Step 3: Process Creations and Updates.
    // This loop handles both new entities and existing ones with changed components.
        for (entity, data) in commit_data.updates.iter() {
        if let Some(key) = session.entity_keys.get(entity) {
            // This is an existing entity, so we update its document.
            session.db.update_document(key, data.clone())
                .await
                .map_err(|_| ArangoError("DB operation failed".into()))?;
        } else {
            // This is a new entity, so we create a new document.
            let new_key = session.db.create_document(data.clone())
                .await
                .map_err(|_| ArangoError("DB operation failed".into()))?;
            // We need to add a Guid component to the Bevy entity to link it to the DB.
            if let Some(mut entity_mut) = world.get_entity_mut(*entity) {
                entity_mut.insert(crate::Guid::new(new_key.clone()));
            }
            session.entity_keys.insert(*entity, new_key);
        }
    }

    // Step 4: Process Resource Updates.
    // Upsert any resources that have been marked as dirty.
    for (name, data) in commit_data.resources.iter() {
        session.db.upsert_resource(name, data.clone())
            .await
            .map_err(|_| ArangoError("DB operation failed".into()))?;
    }

    // Step 5: Clean up tracking sets.
    // Now that the changes are committed, we can clear the dirty flags.
    session.dirty_entities.clear();
    session.despawned_entities.clear();
    session.dirty_resources.clear();

    Ok(())
}

/// Persist new, changed, or despawned entities to the database.
///
/// This function handles the `async` complexity of removing the `ArangoSession`
/// resource, performing the database operations, and then re-inserting it.
pub async fn commit_app(app: &mut App) -> Result<(), ArangoError> {
    let mut session = app.world.remove_resource::<ArangoSession>().unwrap();
    let world_ptr = &mut app.world as *mut _;

    // SAFETY: We removed the session, so it's safe to get &mut World
    let world = unsafe { &mut *world_ptr };
    let result = commit(&mut session, world).await;

    app.world.insert_resource(session);
    result
}


#[cfg(test)]
mod arango_session {
    use super::*;
    use crate::persist::Persist;
    use bevy::prelude::{Component, World, App};
    use bevy_arangodb_derive::persist;
    use mockall::predicate::*;
    use serde::{Serialize, Deserialize, Deserializer};
    use serde_json::json;

    #[persist(resource)]
    struct MyRes { value: i32 }
    #[persist(component)]
    struct MyComp { value: i32 }

    #[test]
    fn new_session_is_empty() {
        let mock_db = MockDatabaseConnection::new();
        let session = ArangoSession::new_mocked(Arc::new(mock_db));
        assert!(session.dirty_entities.is_empty());
        assert!(session.despawned_entities.is_empty());
    }

    #[test]
    fn deserializer_inserts_component() {
        let mut world = World::new();
        let entity = world.spawn_empty().id();

        let mut session = ArangoSession::new(Arc::new(MockDatabaseConnection::new()));
        session.register_component::<MyComp>();

        let deserializer = session.component_deserializers.get(MyComp::name()).unwrap();
        deserializer(&mut world, entity, json!({"value": 42})).unwrap();

        assert_eq!(world.get::<MyComp>(entity).unwrap().value, 42);
    }

    #[test]
    fn deserializer_inserts_resource() {
        let mut world = World::new();

        let mut session = ArangoSession::new(Arc::new(MockDatabaseConnection::new()));
        session.register_resource::<MyRes>();

        let deserializer = session.resource_deserializers.get(MyRes::name()).unwrap();
        deserializer(&mut world, json!({"value": 5})).unwrap();

        assert_eq!(world.resource::<MyRes>().value, 5);
    }

    #[tokio::test]
    async fn commit_serializes_resources() {
        let mut db = MockDatabaseConnection::new();
        db.expect_upsert_resource()
            .withf(|k, d| k==MyRes::name() && d==&json!({"value":5}))
            .times(1)
            .returning(|_, _| Box::pin(async { Ok(()) }));

        let mut app = App::new();
        app.insert_resource(MyRes { value: 5 });
        let mut session = ArangoSession::new(Arc::new(db));
        session.register_resource::<MyRes>();
        session.mark_resource_dirty::<MyRes>(); // Manually mark as dirty for test
        app.insert_resource(session);

        commit_app(&mut app).await.unwrap();
    }

    #[tokio::test]
    async fn commit_skips_clean_resources() {
        let mut db = MockDatabaseConnection::new();
        db.expect_upsert_resource().times(0);

        let mut app = App::new();
        app.insert_resource(MyRes { value: 5 });
        let mut session = ArangoSession::new(Arc::new(db));
        session.register_resource::<MyRes>();
        app.insert_resource(session);

        // Don't mark as dirty
        commit_app(&mut app).await.unwrap();
    }

    #[tokio::test]
    async fn commit_creates_new_entity() {
        let mut db = MockDatabaseConnection::new();
        db.expect_create_document()
            .withf(move |data| {
                data.get(MyComp::name()) == Some(&json!({"value": 10}))
            })
            .times(1)
            .returning(|_| Box::pin(async { Ok("new_key".to_string()) }));

        let mut app = App::new();
        let mut session = ArangoSession::new(Arc::new(db));
        session.register_component::<MyComp>();
        app.insert_resource(session);

        let entity = app.world.spawn(MyComp { value: 10 }).id();
        app.world.get_resource_mut::<ArangoSession>().unwrap().dirty_entities.insert(entity);

        commit_app(&mut app).await.unwrap();
    }

    #[tokio::test]
    async fn commit_assigns_guid_to_new_entity() {
        let mut db = MockDatabaseConnection::new();
        db.expect_create_document()
            .returning(|_| Box::pin(async { Ok("new_key".to_string()) }));

        let mut app = App::new();
        let mut session = ArangoSession::new(Arc::new(db));
        session.register_component::<MyComp>();
        app.insert_resource(session);

        let entity = app.world.spawn(MyComp { value: 10 }).id();
        app.world.get_resource_mut::<ArangoSession>().unwrap().dirty_entities.insert(entity);

        // Before commit, no Guid
        assert!(app.world.get::<crate::Guid>(entity).is_none());

        commit_app(&mut app).await.unwrap();

        // After commit, Guid should be present
        let guid = app.world.get::<crate::Guid>(entity).unwrap();
        assert_eq!(guid.id(), "new_key");
    }

    #[tokio::test]
    async fn commit_updates_existing_entity() {
        let mut db = MockDatabaseConnection::new();
        db.expect_update_document()
            .withf(move |key, data| {
                key == "existing_key" &&
                data.get(MyComp::name()) == Some(&json!({"value": 20}))
            })
            .times(1)
            .returning(|_, _| Box::pin(async move { Ok(()) }));

        let mut app = App::new();
        let mut session = ArangoSession::new(Arc::new(db));
        session.register_component::<MyComp>();
        let entity = Entity::from_raw(0);
        session.entity_keys.insert(entity, "existing_key".to_string());
        app.insert_resource(session);

        app.world.spawn(MyComp { value: 20 });
        app.world.get_resource_mut::<ArangoSession>().unwrap().dirty_entities.insert(entity);

        commit_app(&mut app).await.unwrap();
    }

    #[tokio::test]
    async fn commit_serializes_entity_with_key() {
        let mut db = MockDatabaseConnection::new();
        db.expect_create_document()
            .withf(move |data| {
                data.get(MyComp::name()) == Some(&json!({"value": 10}))
            })
            .times(1)
            .returning(|_| Box::pin(async { Ok("new_key".to_string()) }));

        let mut app = App::new();
        let mut session = ArangoSession::new(Arc::new(db));
        session.register_component::<MyComp>();
        app.insert_resource(session);

        let entity = app.world.spawn(MyComp { value: 10 }).id();
        app.world.get_resource_mut::<ArangoSession>().unwrap().dirty_entities.insert(entity);

        commit_app(&mut app).await.unwrap();
    }

    #[tokio::test]
    async fn commit_deletes_entity() {
        let mut db = MockDatabaseConnection::new();
        db.expect_delete_document()
            .withf(|key| key == "key_to_delete")
            .times(1)
            .returning(|_| Box::pin(async { Ok(()) }));

        let mut app = App::new();
        let mut session = ArangoSession::new(Arc::new(db));
        session.register_component::<MyComp>();
        let entity = Entity::from_raw(0);
        session.entity_keys.insert(entity, "key_to_delete".to_string());
        app.insert_resource(session);

        app.world.despawn(entity);
        // Manually mark as despawned because the system isn't running
        let mut session = app.world.get_resource_mut::<ArangoSession>().unwrap();
        session.mark_despawned(entity);

        commit_app(&mut app).await.unwrap();
    }

    #[persist(component)]
    struct A(i32);
    #[persist(component)]
    struct B(String);

    #[tokio::test]
    async fn commit_serializes_multiple_components() {
        let mut db = MockDatabaseConnection::new();
        db.expect_create_document()
            .withf(move |data| {
                let key_a = A::name();
                let key_b = B::name();
                data.get(key_a) == Some(&json!(1)) &&
                data.get(key_b) == Some(&json!("hello"))
            })
            .times(1)
            .returning(|_| Box::pin(async { Ok("new_key".to_string()) }));

        let mut app = App::new();
        let mut session = ArangoSession::new(Arc::new(db));
        session.register_component::<A>();
        session.register_component::<B>();
        app.insert_resource(session);

        let entity = app.world.spawn((A(1), B("hello".to_string()))).id();
        app.world.get_resource_mut::<ArangoSession>().unwrap().dirty_entities.insert(entity);

        commit_app(&mut app).await.unwrap();
    }

    #[persist(component)]
    struct NestedVec(Vec<String>);
    #[persist(component)]
    struct NestedMap(HashMap<String, i32>);

    #[tokio::test]
    async fn commit_serializes_nested_containers() {
        let mut db = MockDatabaseConnection::new();
        db.expect_create_document()
            .withf(move |data| {
                let key_nv = NestedVec::name();
                let key_nm = NestedMap::name();
                let expected_vec = json!(["a", "b"]);
                let mut map = HashMap::new();
                map.insert("x".to_string(), 1);
                let expected_map = json!(map);

                data.get(key_nv) == Some(&expected_vec) &&
                data.get(key_nm) == Some(&expected_map)
            })
            .times(1)
            .returning(|_| Box::pin(async { Ok("new_key".to_string()) }));

        let mut app = App::new();
        let mut session = ArangoSession::new(Arc::new(db));
        session.register_component::<NestedVec>();
        session.register_component::<NestedMap>();
        app.insert_resource(session);

        let entity = app.world.spawn((
            NestedVec(vec!["a".to_string(), "b".to_string()]),
            NestedMap({
                let mut map = HashMap::new();
                map.insert("x".to_string(), 1);
                map
            })
        )).id();
        app.world.get_resource_mut::<ArangoSession>().unwrap().dirty_entities.insert(entity);

        commit_app(&mut app).await.unwrap();
    }

    // Here we are testing the case where serialization fails.
    // We are deriving `Serialize` and `Deserialize` for a component manually because 
    // we want to simulate a serialization error.
    #[derive(Component)]
    struct ErrComp;
    impl Serialize for ErrComp {
        fn serialize<S>(&self, _serializer: S) -> Result<S::Ok, S::Error>
        where S: serde::Serializer {
            Err(serde::ser::Error::custom("test error"))
        }
    }
    impl<'de> Deserialize<'de> for ErrComp {
        fn deserialize<D>(_deserializer: D) -> Result<Self, D::Error>
        where D: Deserializer<'de> {
            Ok(ErrComp)
        }
    }
    // Manually implement `Persist` for `ErrComp` for this test
    impl Persist for ErrComp {
        fn name() -> &'static str { "ErrComp" }
    }

    #[tokio::test]
    #[should_panic(expected = "Serialization failed")]
    async fn commit_bubbles_serialize_error() {
        let mut app = App::new();
        let mut session = ArangoSession::new(Arc::new(MockDatabaseConnection::new()));
        session.register_component::<ErrComp>();
        app.insert_resource(session);

        let entity = app.world.spawn(ErrComp).id();
        app.world.get_resource_mut::<ArangoSession>().unwrap().dirty_entities.insert(entity);

        commit_app(&mut app).await.unwrap();
    }

    #[tokio::test]
    #[should_panic(expected = "DB operation failed")]
    async fn commit_bubbles_db_error() {
        #[persist(component)]
        struct A(i32);

        let mut db = MockDatabaseConnection::new();
        db.expect_create_document()
            .times(1)
            .returning(|_| Box::pin(async { Err(ArangoError("db error".to_string())) }));

        let mut app = App::new();
        let mut session = ArangoSession::new(Arc::new(db));
        session.register_component::<A>();
        app.insert_resource(session);

        let entity = app.world.spawn(A(1)).id();
        app.world.get_resource_mut::<ArangoSession>().unwrap().dirty_entities.insert(entity);

        commit_app(&mut app).await.unwrap();
    }

    #[persist(component)]
    enum TestEnum {
        A,
        B(i32),
    }

    #[test]
    fn enum_component_round_trip_serializes_correctly() {
        let mut db = MockDatabaseConnection::new();
        db.expect_create_document()
            .withf(move |data| {
                let key_enum = TestEnum::name();
                let expected_a = json!("A");
                let expected_b = json!({"B": 42});
                // This test is a bit tricky, we don't know the order.
                // Let's just check if one of them matches.
                data.get(key_enum) == Some(&expected_a) || data.get(key_enum) == Some(&expected_b)
            })
            .times(2)
            .returning(|_| Box::pin(async { Ok("new_key".to_string()) }));

        let mut app = App::new();
        let mut session = ArangoSession::new(Arc::new(db));
        session.register_component::<TestEnum>();
        app.insert_resource(session);

        let entity_a = app.world.spawn(TestEnum::A).id();
        let entity_b = app.world.spawn(TestEnum::B(42)).id();
        let mut session = app.world.get_resource_mut::<ArangoSession>().unwrap();
        session.dirty_entities.insert(entity_a);
        session.dirty_entities.insert(entity_b);

        futures::executor::block_on(commit_app(&mut app)).unwrap();
    }

    #[persist(component)]
    struct BigVec { items: Vec<i32> }

    #[test]
    fn large_payload_serializes_without_panic() {
        let mut db = MockDatabaseConnection::new();
        db.expect_create_document()
            .withf(move |data| {
                let key_big = BigVec::name();
                data.get(key_big).is_some()
            })
            .times(1)
            .returning(|_| Box::pin(async { Ok("new_key".to_string()) }));

        let mut app = App::new();
        let mut session = ArangoSession::new(Arc::new(db));
        session.register_component::<BigVec>();
        app.insert_resource(session);

        let items = (0..10000).collect();
        let entity = app.world.spawn(BigVec { items }).id();
        app.world.get_resource_mut::<ArangoSession>().unwrap().dirty_entities.insert(entity);

        futures::executor::block_on(commit_app(&mut app)).unwrap();
    }
}