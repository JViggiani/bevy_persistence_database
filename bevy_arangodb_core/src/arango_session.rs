//! Core ECS‐to‐Arango bridge: defines `ArangoSession` and the abstract
//! `DatabaseConnection` trait.
//! Handles local cache, change tracking, and commit logic (create/update/delete).

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
use downcast_rs::{impl_downcast, Downcast};

#[cfg(test)]
use mockall::automock;

#[derive(Debug)]
pub struct ArangoError(pub String);

impl fmt::Display for ArangoError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ArangoDB error: {}", self.0)
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
    pub loaded_entities: HashSet<Entity>,    // track pre-loaded entities
    pub dirty_resources: HashSet<TypeId>, // track dirty resources
    component_serializers: Vec<Box<dyn Fn(Entity, &World) -> Result<Option<(String, Value)>, ArangoError> + Send + Sync>>,
    pub component_deserializers: HashMap<String, Box<dyn Fn(&mut World, Entity, Value) -> Result<(), ArangoError> + Send + Sync>>,
    resource_serializers: Vec<Box<dyn Fn(&World, &ArangoSession) -> Result<Option<(String, Value)>, ArangoError> + Send + Sync>>,
    resource_deserializers: HashMap<String, Box<dyn Fn(&mut World, Value) -> Result<(), ArangoError> + Send + Sync>>,
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
        self.component_serializers.push(Box::new(
            move |entity, world| -> Result<Option<(String, Value)>, ArangoError> {
                if let Some(c) = world.get::<T>(entity) {
                    let v = serde_json::to_value(c).map_err(|e| ArangoError(e.to_string()))?;
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
        self.resource_serializers.push(Box::new(move |world, session| {
            if !session.dirty_resources.contains(&type_id) {
                return Ok(None);
            }
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

    /// Mark an entity as already present in the database.
    pub fn mark_loaded(&mut self, entity: Entity) {
        self.loaded_entities.insert(entity);
    }

    /// Testing constructor w/ mock DB.
    #[cfg(test)]
    pub fn new_mocked(db: Arc<dyn DatabaseConnection>) -> Self {
        Self {
            db,
            component_serializers: Vec::new(),
            component_deserializers: HashMap::new(),
            resource_serializers: Vec::new(),
            resource_deserializers: HashMap::new(),
            dirty_entities: HashSet::new(),
            despawned_entities: HashSet::new(),
            loaded_entities: HashSet::new(),
            dirty_resources: HashSet::new(),
        }
    }

    /// Create a new session.
    pub fn new(db: Arc<dyn DatabaseConnection>) -> Self {
        Self {
            db,
            component_serializers: Vec::new(),
            component_deserializers: HashMap::new(),
            resource_serializers: Vec::new(),
            resource_deserializers: HashMap::new(),
            dirty_entities: HashSet::new(),
            despawned_entities: HashSet::new(),
            loaded_entities: HashSet::new(),
            dirty_resources: HashSet::new(),
        }
    }
}

/// Serialize all data. This will fail early if any serialization fails.
fn _prepare_commit(session: &ArangoSession, world: &World) -> Result<CommitData, ArangoError> {
    let deleted_set: HashSet<Entity> = session.despawned_entities.iter().cloned().collect();

    let updates = session
        .dirty_entities
        .iter()
        .filter(|e| !deleted_set.contains(e))
        .map(|entity| {
            let mut map = serde_json::Map::new();
            for func in &session.component_serializers {
                if let Some((k, v)) = func(*entity, world)? {
                    // Do not include the `_key` in the update patch payload.
                    if k != "_key" {
                        map.insert(k, v);
                    }
                }
            }
            Ok((*entity, Value::Object(map)))
        })
        .collect::<Result<Vec<_>, ArangoError>>()?;

    let mut resource_data = Vec::new();
    for func in &session.resource_serializers {
        if let Some((k, v)) = func(world, session)? {
            resource_data.push((k, v));
        }
    }

    Ok(CommitData {
        deletes: session.despawned_entities.iter().cloned().collect(),
        updates,
        resources: resource_data,
    })
}

/// Persist new, changed, or despawned entities to the database.
pub(crate) async fn commit(session: &mut ArangoSession, world: &mut World) -> Result<(), ArangoError> {
    let CommitData { deletes, updates, resources } = _prepare_commit(session, world)?;

    // updates
    for (entity, data) in updates {
        if let Some(guid) = world.get::<crate::Guid>(entity) {
            session.db.update_document(guid.id(), data).await?;
        } else {
            let key = session.db.create_document(data).await?;
            world.entity_mut(entity).insert(crate::Guid::new(key));
        }
    }
    // deletes
    for entity in deletes {
        if let Some(guid) = world.get::<crate::Guid>(entity) {
            session.db.delete_document(guid.id()).await?;
        }
    }
    // resources
    for (name, data) in resources {
        session.db.upsert_resource(&name, data).await?;
    }
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

    // SAFETY: We have removed the ArangoSession, so we can now safely get a mutable
    // reference to the world. The session resource is not accessed by any other
    // part of the app while it's removed.
    let world = unsafe { &mut *world_ptr };
    let result = commit(&mut session, world).await;

    app.world.insert_resource(session);
    result
}


#[cfg(test)]
mod arango_session {
    use super::*;
    use tokio;            
    use serde::{Serialize, Deserialize};
    use serde_json::json;
    use std::sync::Arc;
    use std::collections::HashMap;
    use futures::executor::block_on;
    use bevy_arangodb_derive::Persist;

    #[derive(Resource, Serialize, Deserialize, PartialEq, Debug, Persist)]
    struct MyRes { value: i32 }

    #[derive(bevy::prelude::Component, serde::Serialize, serde::Deserialize, PartialEq, Debug, Persist)]
    struct MyComp { value: i32 }

    #[test]
    fn deserializer_inserts_component() {
        let mut session = ArangoSession::new_mocked(Arc::new(MockDatabaseConnection::new()));
        session.register_component::<MyComp>();
        let mut world = World::new();
        let entity = world.spawn(()).id();
        let json_val = json!({"value": 123});

        let deserializer = session.component_deserializers.get(MyComp::name()).unwrap();
        deserializer(&mut world, entity, json_val).unwrap();

        let comp = world.get::<MyComp>(entity).unwrap();
        assert_eq!(comp.value, 123);
    }

    #[test]
    fn deserializer_inserts_resource() {
        let mut session = ArangoSession::new_mocked(Arc::new(MockDatabaseConnection::new()));
        session.register_resource::<MyRes>();
        let mut world = World::new();
        let json_val = json!({"value": 456});

        let deserializer = session.resource_deserializers.get(MyRes::name()).unwrap();
        deserializer(&mut world, json_val).unwrap();

        let res = world.get_resource::<MyRes>().unwrap();
        assert_eq!(res.value, 456);
    }

    #[tokio::test]
    async fn commit_serializes_resources() {
        let mut mock_db = MockDatabaseConnection::new();
        mock_db.expect_upsert_resource()
            .withf(|k, d| k==MyRes::name() && d==&json!({"value":5}))
            .returning(|_,_| Box::pin(async{Ok(())}));

        let mut session = ArangoSession::new_mocked(Arc::new(mock_db));
        session.register_resource::<MyRes>();
        let mut world = World::new();
        world.insert_resource(MyRes{value:5});
        session.mark_resource_dirty::<MyRes>(); // Mark the resource as dirty
        assert!(commit(&mut session, &mut world).await.is_ok());
    }

    #[tokio::test]
    async fn commit_skips_clean_resources() {
        let mut mock_db = MockDatabaseConnection::new();
        // Expect upsert_resource to never be called
        mock_db.expect_upsert_resource().never();

        let mut session = ArangoSession::new_mocked(Arc::new(mock_db));
        session.register_resource::<MyRes>();
        let mut world = World::new();
        world.insert_resource(MyRes{value:5});
        // Do NOT mark the resource as dirty
        assert!(commit(&mut session, &mut world).await.is_ok());
    }

    #[allow(dead_code)]
    #[derive(bevy::prelude::Component)]
    struct Foo(i32);

    #[tokio::test]
    async fn commit_creates_new_entity() {
        let mut mock_db = MockDatabaseConnection::new();
        mock_db.expect_create_document()
            .withf(|data| data.as_object().unwrap().is_empty())
            .times(1)
            .returning(|_| Box::pin(async { Ok("new_key".to_string()) }));

        let mut session = ArangoSession::new_mocked(Arc::new(mock_db));
        let mut world = World::new();
        let id = world.spawn(()).id();
        session.dirty_entities.insert(id);
        assert!(commit(&mut session, &mut world).await.is_ok());
    }

    #[tokio::test]
    async fn commit_assigns_guid_to_new_entity() {
        let mut mock_db = MockDatabaseConnection::new();
        mock_db.expect_create_document()
            .returning(|_| Box::pin(async { Ok("new_key_123".to_string()) }));

        let mut session = ArangoSession::new_mocked(Arc::new(mock_db));
        let mut world = World::new();
        let id = world.spawn(()).id();
        session.dirty_entities.insert(id);
        
        // Before commit, no Guid
        assert!(world.get::<crate::Guid>(id).is_none());

        commit(&mut session, &mut world).await.unwrap();

        // After commit, Guid should be present
        let guid = world.get::<crate::Guid>(id).unwrap();
        assert_eq!(guid.id(), "new_key_123");
    }

    #[tokio::test]
    async fn commit_updates_existing_entity() {
        let mut mock_db = MockDatabaseConnection::new();
        mock_db
            .expect_update_document()
            .withf(|key, data| key == "0" && data.as_object().unwrap().is_empty())
            .times(1)
            .returning(|_, _| Box::pin(async move { Ok(()) }));

        let mut session = ArangoSession::new_mocked(Arc::new(mock_db));
        let mut world = World::new();
        let id = world.spawn(crate::Guid::new("0".to_string())).id();
        session.dirty_entities.insert(id);
        assert!(commit(&mut session, &mut world).await.is_ok());
    }

    #[tokio::test]
    async fn commit_serializes_entity_with_key() {
        let mut mock_db = MockDatabaseConnection::new();
        mock_db.expect_create_document()
            .withf(|data| {
                data.get(MyComp::name()) == Some(&json!({"value": 10}))
            })
            .times(1)
            .returning(|_| Box::pin(async { Ok("new_key".to_string()) }));

        let mut session = ArangoSession::new_mocked(Arc::new(mock_db));
        session.register_component::<MyComp>();
        let mut world = World::new();
        let id = world.spawn(MyComp { value: 10 }).id();
        session.dirty_entities.insert(id);
        assert!(commit(&mut session, &mut world).await.is_ok());
    }

    #[tokio::test]
    async fn commit_deletes_entity() {
        let mut mock_db = MockDatabaseConnection::new();
        mock_db
            .expect_delete_document()
            .withf(|key| key == "0")
            .times(1)
            .returning(|_| Box::pin(async { Ok(()) }));

        let mut session = ArangoSession::new_mocked(Arc::new(mock_db));
        let mut world = World::new();
        let id = world.spawn(crate::Guid::new("0".to_string())).id();
        session.mark_despawned(id);
        assert!(commit(&mut session, &mut world).await.is_ok());
    }

    #[test]
    fn new_session_is_empty() {
        let mock_db = MockDatabaseConnection::new();
        let session = ArangoSession::new_mocked(Arc::new(mock_db));
        assert!(session.dirty_entities.is_empty());
        assert!(session.despawned_entities.is_empty());
    }

    #[test]
    fn commit_clears_tracking_sets() {
        let mut mock_db = MockDatabaseConnection::new();
        mock_db.expect_delete_document().returning(|_| Box::pin(async { Ok(()) }));
        mock_db.expect_create_document().returning(|_| Box::pin(async { Ok("new_key".to_string()) }));
        mock_db.expect_update_document().returning(|_,_| Box::pin(async { Ok(()) }));

        let mut session = ArangoSession::new_mocked(Arc::new(mock_db));
        let mut world = World::new();

        let id_new = world.spawn(()).id();
        let id_old = world.spawn(crate::Guid::new("old_key".to_string())).id();
        session.dirty_entities.insert(id_new);
        session.dirty_entities.insert(id_old);
        session.mark_despawned(id_old);

        assert!(block_on(commit(&mut session, &mut world)).is_ok());

        assert!(session.dirty_entities.is_empty());
        assert!(session.despawned_entities.is_empty());
    }

    #[test]
    fn commit_handles_multiple_entities() {
        let mut mock_db = MockDatabaseConnection::new();
        mock_db
            .expect_create_document()
            .times(1)
            .returning(|_| Box::pin(async { Ok("new_key".to_string()) }));
        mock_db
            .expect_update_document()
            .withf(|key, _| key == "1")
            .times(1)
            .returning(|_, _| Box::pin(async { Ok(()) }));

        let mut session = ArangoSession::new_mocked(Arc::new(mock_db));
        let mut world = World::new();
        let id0 = world.spawn(()).id();
        let id1 = world.spawn(crate::Guid::new("1".to_string())).id();

        session.dirty_entities.insert(id0);
        session.dirty_entities.insert(id1);

        assert!(block_on(commit(&mut session, &mut world)).is_ok());
    }

    #[test]
    fn nested_serde_roundtrip() {
        use serde::{Serialize, Deserialize};
        #[derive(Serialize, Deserialize, PartialEq, Debug)]
        struct Nested { a: u8, b: Vec<String> }

        let orig = Nested { a: 42, b: vec!["foo".into(), "bar".into()] };
        let val = serde_json::to_value(&orig).expect("serialize failed");
        let back: Nested = serde_json::from_value(val).expect("deserialize failed");
        assert_eq!(orig, back);
    }

    #[derive(bevy::prelude::Component, serde::Serialize, serde::Deserialize, Persist)]
    struct A(i32);
    #[derive(bevy::prelude::Component, serde::Serialize, serde::Deserialize, Persist)]
    struct B(String);

    #[test]
    fn commit_serializes_multiple_components() {
        let mut mock_db = MockDatabaseConnection::new();
        // derive the full names for A and B
        let key_a = A::name();
        let key_b = B::name();
        mock_db
            .expect_create_document()
            .withf(move |data| {
                data.get(key_a) == Some(&json!(10)) &&
                data.get(key_b) == Some(&json!("hello"))
            })
            .returning(|_| Box::pin(async { Ok("new_key".to_string()) }));

        let mut session = ArangoSession::new_mocked(Arc::new(mock_db));
        let mut world = World::new();
        // register serializers for A and B
        session.register_component::<A>();
        session.register_component::<B>();
        let entity = world.spawn((A(10), B("hello".into()))).id();
        session.dirty_entities.insert(entity);
        assert!(block_on(commit(&mut session, &mut world)).is_ok());
    }

    #[derive(bevy::prelude::Component, serde::Serialize, serde::Deserialize, Persist)]
    struct NestedVec(Vec<String>);
    #[derive(bevy::prelude::Component, serde::Serialize, serde::Deserialize, Persist)]
    struct NestedMap(HashMap<String, i32>);

    #[test]
    fn commit_serializes_nested_containers() {
        let mut mock_db = MockDatabaseConnection::new();
        let key_nv = NestedVec::name();
        let key_nm = NestedMap::name();

        mock_db.expect_create_document()
            .withf(move |data| {
                data.get(key_nv) == Some(&json!(["a", "b", "c"])) &&
                data.get(key_nm) == Some(&json!({"x":1,"y":2}))
            })
            .returning(|_| Box::pin(async { Ok("new_key".to_string()) }));

        let mut session = ArangoSession::new_mocked(Arc::new(mock_db));
        let mut world = World::new();
        session.register_component::<NestedVec>();
        session.register_component::<NestedMap>();

        let mut map = HashMap::new();
        map.insert("x".to_string(), 1);
        map.insert("y".to_string(), 2);
        let entity = world
            .spawn((NestedVec(vec!["a".into(),"b".into(),"c".into()]), NestedMap(map)))
            .id();
        session.dirty_entities.insert(entity);
        assert!(block_on(commit(&mut session, &mut world)).is_ok());
    }

    #[derive(bevy::prelude::Component, serde::Serialize, serde::Deserialize, Persist)]
    struct ErrComp(f32); // f32::INFINITY will fail to serialize

    #[test]
    fn commit_bubbles_serialize_error() {
        let mut mock_db = MockDatabaseConnection::new();
        // DB shouldn't be called
        mock_db.expect_create_document().never();
        mock_db.expect_update_document().never();
        mock_db.expect_delete_document().never();

        let mut session = ArangoSession::new_mocked(Arc::new(mock_db));
        let mut world = World::new();
        session.register_component::<ErrComp>();
        let entity = world.spawn((ErrComp(f32::INFINITY),)).id();
        session.dirty_entities.insert(entity);

        let err = block_on(commit(&mut session, &mut world)).unwrap_err();
        assert!(err.to_string().contains("Could not serialize"));
    }

    #[test]
    fn commit_bubbles_db_error() {
        let mut mock_db = MockDatabaseConnection::new();
        mock_db
            .expect_create_document()
            .returning(|_| Box::pin(async { Err(ArangoError("up".into())) }));

        let mut session = ArangoSession::new_mocked(Arc::new(mock_db));
        let mut world = World::new();
        #[derive(bevy::prelude::Component, serde::Serialize, serde::Deserialize, Persist)]
        struct A(i32);
        session.register_component::<A>();
        let entity = world.spawn((A(1),)).id();
        session.dirty_entities.insert(entity);

        let err = block_on(commit(&mut session, &mut world)).unwrap_err();
        assert_eq!(err.to_string(), "ArangoDB error: up");
    }

    #[test]
    fn commit_entity_with_zero_components_sends_only_key() {
        let mut mock = MockDatabaseConnection::new();
        mock.expect_create_document()
            .withf(|payload: &Value| {
                payload.as_object().unwrap().is_empty()
            })
            .times(1)
            .returning(|_| Box::pin(async { Ok("new_key".to_string()) }));

        let mut session = ArangoSession::new_mocked(Arc::new(mock));
        let mut world = World::new();
        let entity = world.spawn(()).id();
        session.dirty_entities.insert(entity);
        assert!(block_on(commit(&mut session, &mut world)).is_ok());
    }

    #[derive(bevy::prelude::Component, Serialize, Deserialize, PartialEq, Debug, Persist)]
    enum TestEnum {
        A,
        B { x: i32 },
        C(String),
    }

    #[test]
    fn enum_component_round_trip_serializes_correctly() {
        let mut mock = MockDatabaseConnection::new();
        // capture the full type name
        let key_enum = TestEnum::name();
        mock.expect_create_document()
            .withf(move |payload| {
                let obj = payload.as_object().unwrap();
                // lookup by full type name, not just "TestEnum"
                matches!(obj.get(key_enum).unwrap(), Value::String(_) | Value::Object(_))
            })
            .times(1)
            .returning(|_| Box::pin(async { Ok("new_key".to_string()) }));

        let mut session = ArangoSession::new_mocked(Arc::new(mock));
        let mut world = World::new();
        session.register_component::<TestEnum>();

        let entity = world
            .spawn((TestEnum::B { x: 42 },))
            .id();
        session.dirty_entities.insert(entity);
        assert!(block_on(commit(&mut session, &mut world)).is_ok());
    }

    #[derive(bevy::prelude::Component, Serialize, Deserialize, Persist)]
    struct BigVec { items: Vec<i32> }

    #[test]
    fn large_payload_serializes_without_panic() {
        let mut mock = MockDatabaseConnection::new();
        // capture the full type name
        let key_big = BigVec::name();
        mock.expect_create_document()
            .withf(move |payload| {
                let obj = payload.as_object().unwrap();
                // lookup the nested object by full type name
                let big = obj.get(key_big).unwrap().as_object().unwrap();
                let items = big.get("items").unwrap().as_array().unwrap();
                items.len() == 1000
            })
            .times(1)
            .returning(|_| Box::pin(async { Ok("new_key".to_string()) }));

        let mut session = ArangoSession::new_mocked(Arc::new(mock));
        let mut world = World::new();
        session.register_component::<BigVec>();

        let mut b = world.spawn(());
        let data: Vec<i32> = (0..1000).collect();
        let entity = b.insert(BigVec { items: data }).id();
        session.dirty_entities.insert(entity);
        assert!(block_on(commit(&mut session, &mut world)).is_ok());
    }
}