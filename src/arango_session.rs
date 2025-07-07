//! Core ECS‐to‐Arango bridge: defines `ArangoSession` and the abstract
//! `DatabaseConnection` trait.
//! Handles local cache, change tracking, and commit logic (create/update/delete).

use bevy::prelude::{Entity, World, Resource};
use futures::future::BoxFuture;
use serde_json::Value;
use std::{collections::{HashSet, HashMap}, fmt, sync::Arc};

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
pub trait DatabaseConnection: Send + Sync {
    /// Create a new document (entity) with the given key.
    fn create_document(
        &self,
        entity_key: &str,
        data: Value,
    ) -> BoxFuture<'static, Result<(), ArangoError>>;

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
    fn query_arango(
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

    /// Clear all documents from the entities collection.
    fn clear_entities(&self) -> BoxFuture<'static, Result<(), ArangoError>>;
}

/// Manages a “unit of work”: local World cache + change tracking + async runtime.
pub struct ArangoSession {
    pub local_world: World,
    pub db: Arc<dyn DatabaseConnection>,
    pub dirty_entities: HashSet<Entity>,
    pub despawned_entities: HashSet<Entity>,
    pub loaded_entities: HashSet<Entity>,    // track pre-loaded entities
    component_serializers: Vec<Box<dyn Fn(Entity, &World) -> Result<Option<(String, Value)>, ArangoError> + Send + Sync>>,
    pub component_deserializers: HashMap<String, Box<dyn Fn(&mut World, Entity, Value) -> Result<(), ArangoError> + Send + Sync>>,
    resource_serializers: Vec<Box<dyn Fn(&World) -> Result<Option<(String, Value)>, ArangoError> + Send + Sync>>,
    resource_deserializers: HashMap<String, Box<dyn Fn(&mut World, Value) -> Result<(), ArangoError> + Send + Sync>>,
}

struct CommitData {
    deletes: Vec<Entity>,
    updates: Vec<(Entity, Value)>,
    resources: Option<Value>,
}

impl ArangoSession {
    /// Register a component type for persistence in `commit()`.
    pub fn register_serializer<T>(&mut self)
    where
        T: bevy::ecs::component::Component + serde::Serialize + serde::de::DeserializeOwned + 'static,
    {
        let name = std::any::type_name::<T>().to_string();
        self.component_serializers.push(Box::new(
            move |entity, world| -> Result<Option<(String, Value)>, ArangoError> {
                if let Some(c) = world.get::<T>(entity) {
                    let v = serde_json::to_value(c)
                        .map_err(|e| ArangoError(e.to_string()))?;
                    // Treat null (e.g. non-finite floats) as serialization error
                    if v.is_null() {
                        return Err(ArangoError("Could not serialize".into()));
                    }
                    Ok(Some((name.clone(), v)))
                } else {
                    Ok(None)
                }
            },
        ));
        self.register_deserializer::<T>();
    }

    /// Register a component type for deserialization.
    pub fn register_deserializer<T>(&mut self)
    where
        T: bevy::ecs::component::Component + serde::de::DeserializeOwned + 'static,
    {
        let name = std::any::type_name::<T>().to_string();
        self.component_deserializers.insert(name, Box::new(
            |world, entity, json_val| {
                let comp: T = serde_json::from_value(json_val)
                    .map_err(|e| ArangoError(e.to_string()))?;
                world.entity_mut(entity).insert(comp);
                Ok(())
            }
        ));
    }

    /// Register a resource type for persistence in `commit()`.
    pub fn register_resource_serializer<R>(&mut self)
    where
        R: Resource + serde::Serialize + serde::de::DeserializeOwned + 'static,
    {
        let name = std::any::type_name::<R>().to_string();
        self.resource_serializers.push(Box::new(move |world| {
            if let Some(r) = world.get_resource::<R>() {
                let v = serde_json::to_value(r)
                    .map_err(|e| ArangoError(e.to_string()))?;
                // Treat null (e.g. non-finite floats) as serialization error
                if v.is_null() {
                    return Err(ArangoError("Could not serialize".into()));
                }
                Ok(Some((name.clone(), v)))
            } else {
                Ok(None)
            }
        }));
        self.register_resource_deserializer::<R>();
    }

    /// Register a resource type for deserialization.
    pub fn register_resource_deserializer<R>(&mut self)
        where
            R: Resource + serde::de::DeserializeOwned + 'static,
    {
        let name = std::any::type_name::<R>().to_string();
        self.resource_deserializers.insert(name, Box::new(
            |world, json_val| {
                let res: R = serde_json::from_value(json_val)
                    .map_err(|e| ArangoError(e.to_string()))?;
                world.insert_resource(res);
                Ok(())
            }
        ));
    }

    /// Manually mark an entity as needing persistence.
    pub fn mark_dirty(&mut self, entity: Entity) {
        self.dirty_entities.insert(entity);
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
            local_world: World::new(),
            db,
            component_serializers: Vec::new(),
            component_deserializers: HashMap::new(),
            resource_serializers: Vec::new(),
            resource_deserializers: HashMap::new(),
            dirty_entities: HashSet::new(),
            despawned_entities: HashSet::new(),
            loaded_entities: HashSet::new(),
        }
    }

    /// Create a new session.
    pub fn new(db: Arc<dyn DatabaseConnection>) -> Self {
        Self {
            local_world: World::new(),
            db,
            component_serializers: Vec::new(),
            component_deserializers: HashMap::new(),
            resource_serializers: Vec::new(),
            resource_deserializers: HashMap::new(),
            dirty_entities: HashSet::new(),
            despawned_entities: HashSet::new(),
            loaded_entities: HashSet::new(),
        }
    }

    /// Serialize all data. This will fail early if any serialization fails.
    fn _prepare_commit(&self) -> Result<CommitData, ArangoError> {
        let deleted_set: HashSet<Entity> = self.despawned_entities.iter().cloned().collect();

        let updates = self
            .dirty_entities
            .iter()
            .filter(|e| !deleted_set.contains(e))
            .map(|entity| {
                let mut map = serde_json::Map::new();
                for func in &self.component_serializers {
                    if let Some((k, v)) = func(*entity, &self.local_world)? {
                        map.insert(k, v);
                    }
                }
                Ok((*entity, Value::Object(map)))
            })
            .collect::<Result<Vec<_>, ArangoError>>()?;

        let mut resource_data = None;
        if !self.resource_serializers.is_empty() {
            let mut map = serde_json::Map::new();
            for func in &self.resource_serializers {
                if let Some((k, v)) = func(&self.local_world)? {
                    map.insert(k, v);
                }
            }
            if !map.is_empty() {
                resource_data = Some(Value::Object(map));
            }
        }

        Ok(CommitData {
            deletes: self.despawned_entities.iter().cloned().collect(),
            updates,
            resources: resource_data,
        })
    }

    /// Persist new, changed, or despawned entities to the database.
    pub async fn commit(&mut self) -> Result<(), ArangoError> {
        let CommitData { deletes, updates, resources } = self._prepare_commit()?;

        // updates
        for (entity, data) in updates {
            let key = entity.index().to_string();
            if self.loaded_entities.contains(&entity) {
                self.db.update_document(&key, data).await?;
            } else {
                let mut new_data = data;
                if let Value::Object(ref mut map) = new_data {
                    map.insert("_key".into(), Value::String(key.clone()));
                }
                self.db.create_document(&key, new_data).await?;
            }
        }
        // deletes
        for entity in deletes {
            let key = entity.index().to_string();
            self.db.delete_document(&key).await?;
        }
        // resources
        if let Some(data) = resources {
            self.db.create_document("resources", data).await?;
        }
        self.dirty_entities.clear();
        self.despawned_entities.clear();
        Ok(())
    }
}

#[cfg(test)]
mod arango_session {
    use super::*;
    use tokio;            // for #[tokio::test]
    use serde::{Serialize, Deserialize};
    use serde_json::json;
    use std::sync::Arc;
    use std::collections::HashMap;
    use futures::executor::block_on;
    use std::any::type_name;

    #[derive(Resource, Serialize, Deserialize, PartialEq, Debug)]
    struct MyRes { value: i32 }

    #[derive(bevy::prelude::Component, serde::Serialize, serde::Deserialize, PartialEq, Debug)]
    struct MyComp { value: i32 }

    #[test]
    fn deserializer_inserts_component() {
        let mut session = ArangoSession::new_mocked(Arc::new(MockDatabaseConnection::new()));
        session.register_deserializer::<MyComp>();
        let entity = session.local_world.spawn(()).id();
        let json_val = json!({"value": 123});

        let deserializer = session.component_deserializers.get(type_name::<MyComp>()).unwrap();
        deserializer(&mut session.local_world, entity, json_val).unwrap();

        let comp = session.local_world.get::<MyComp>(entity).unwrap();
        assert_eq!(comp.value, 123);
    }

    #[test]
    fn deserializer_inserts_resource() {
        let mut session = ArangoSession::new_mocked(Arc::new(MockDatabaseConnection::new()));
        session.register_resource_deserializer::<MyRes>();
        let json_val = json!({"value": 456});

        let deserializer = session.resource_deserializers.get(type_name::<MyRes>()).unwrap();
        deserializer(&mut session.local_world, json_val).unwrap();

        let res = session.local_world.get_resource::<MyRes>().unwrap();
        assert_eq!(res.value, 456);
    }

    #[tokio::test]
    async fn commit_serializes_resources() {
        let mut mock_db = MockDatabaseConnection::new();
        mock_db.expect_create_document()
            .withf(|k, d| k=="resources" && d.get(type_name::<MyRes>())==Some(&json!({"value":5})))
            .returning(|_,_| Box::pin(async{Ok(())}));

        let mut session = ArangoSession::new_mocked(Arc::new(mock_db));
        session.register_resource_serializer::<MyRes>();
        session.local_world.insert_resource(MyRes{value:5});
        assert!(session.commit().await.is_ok());
    }

    #[allow(dead_code)]
    #[derive(bevy::prelude::Component)]
    struct Foo(i32);

    #[tokio::test]
    async fn commit_creates_new_entity() {
        let mut mock_db = MockDatabaseConnection::new();
        mock_db.expect_create_document()
            .withf(|key, data| key=="0" && data==&json!({"_key": "0"}))
            .times(1)
            .returning(|_, _| Box::pin(async { Ok(()) }));

        let mut session = ArangoSession::new_mocked(Arc::new(mock_db));
        let id = session.local_world.spawn(()).id();
        session.mark_dirty(id);
        assert!(session.commit().await.is_ok());
    }

    #[tokio::test]
    async fn commit_updates_existing_entity() {
        let mut mock_db = MockDatabaseConnection::new();
        mock_db
            .expect_update_document()
            .withf(|key, data| key == "0" && data == &json!({}))
            .times(1)
            .returning(|_, _| Box::pin(async move { Ok(()) }));

        let mut session = ArangoSession::new_mocked(Arc::new(mock_db));
        let id = session.local_world.spawn(()).id();
        session.mark_loaded(id);      // simulate entity already in DB
        session.mark_dirty(id);
        assert!(session.commit().await.is_ok());
    }

    #[tokio::test]
    async fn commit_serializes_entity_with_key() {
        let mut mock_db = MockDatabaseConnection::new();
        mock_db.expect_create_document()
            .withf(|key, data| {
                key == "0" &&
                data.get("_key") == Some(&json!("0")) &&
                data.get(type_name::<MyComp>()) == Some(&json!({"value": 10}))
            })
            .times(1)
            .returning(|_, _| Box::pin(async { Ok(()) }));

        let mut session = ArangoSession::new_mocked(Arc::new(mock_db));
        session.register_serializer::<MyComp>();
        let id = session.local_world.spawn(MyComp { value: 10 }).id();
        session.mark_dirty(id);
        assert!(session.commit().await.is_ok());
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
        let id = session.local_world.spawn(()).id();
        session.mark_loaded(id);
        session.mark_despawned(id);
        assert!(session.commit().await.is_ok());
    }

    #[test]
    fn mark_dirty_tracks_entity() {
        let mock_db = MockDatabaseConnection::new();
        let mut session = ArangoSession::new_mocked(Arc::new(mock_db));

        let e = session.local_world.spawn(Foo(42));
        let id = e.id();
        session.mark_dirty(id);
        assert!(
            session.dirty_entities.contains(&id),
            "Entity should be marked dirty"
        );
    }

    #[test]
    fn new_session_is_empty() {
        let mock_db = MockDatabaseConnection::new();
        let session = ArangoSession::new_mocked(Arc::new(mock_db));
        assert_eq!(session.local_world.entities().len(), 0);
        assert!(session.dirty_entities.is_empty());
        assert!(session.despawned_entities.is_empty());
    }

    #[test]
    fn commit_clears_tracking_sets() {
        let mut mock_db = MockDatabaseConnection::new();
        mock_db.expect_delete_document().returning(|_| Box::pin(async { Ok(()) }));
        mock_db.expect_create_document().returning(|_, _| Box::pin(async { Ok(()) }));

        let mut session = ArangoSession::new_mocked(Arc::new(mock_db));

        let id_new = session.local_world.spawn(()).id();
        let id_old = session.local_world.spawn(()).id();
        session.mark_loaded(id_old);
        session.mark_dirty(id_new);
        session.mark_dirty(id_old);
        session.mark_despawned(id_old);

        assert!(block_on(session.commit()).is_ok());

        assert!(session.dirty_entities.is_empty());
        assert!(session.despawned_entities.is_empty());
    }

    #[test]
    fn commit_handles_multiple_entities() {
        let mut mock_db = MockDatabaseConnection::new();
        mock_db
            .expect_create_document()
            .withf(|key, _| key == "0")
            .times(1)
            .returning(|_, _| Box::pin(async { Ok(()) }));
        mock_db
            .expect_update_document()
            .withf(|key, _| key == "1")
            .times(1)
            .returning(|_, _| Box::pin(async { Ok(()) }));

        let mut session = ArangoSession::new_mocked(Arc::new(mock_db));
        let id0 = session.local_world.spawn(()).id();
        let id1 = session.local_world.spawn(()).id();

        session.mark_loaded(id1);
        session.mark_dirty(id0);
        session.mark_dirty(id1);

        assert!(block_on(session.commit()).is_ok());
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

    #[derive(bevy::prelude::Component, serde::Serialize, serde::Deserialize)]
    struct A(i32);
    #[derive(bevy::prelude::Component, serde::Serialize, serde::Deserialize)]
    struct B(String);

    #[test]
    fn commit_serializes_multiple_components() {
        let mut mock_db = MockDatabaseConnection::new();
        // derive the full names for A and B
        let key_a = std::any::type_name::<A>();
        let key_b = std::any::type_name::<B>();
        mock_db
            .expect_create_document()
            .withf(move |_key, data| {
                data.get(key_a) == Some(&json!(10)) &&
                data.get(key_b) == Some(&json!("hello"))
            })
            .returning(|_, _| Box::pin(async { Ok(()) }));

        let mut session = ArangoSession::new_mocked(Arc::new(mock_db));
        // register serializers for A and B
        session.register_serializer::<A>();
        session.register_serializer::<B>();
        let entity = session.local_world.spawn((A(10), B("hello".into()))).id();
        session.mark_dirty(entity);
        assert!(block_on(session.commit()).is_ok());
    }

    #[derive(bevy::prelude::Component, serde::Serialize, serde::Deserialize)]
    struct NestedVec(Vec<String>);
    #[derive(bevy::prelude::Component, serde::Serialize, serde::Deserialize)]
    struct NestedMap(HashMap<String, i32>);

    #[test]
    fn commit_serializes_nested_containers() {
        let mut mock_db = MockDatabaseConnection::new();
        let key_nv = std::any::type_name::<NestedVec>();
        let key_nm = std::any::type_name::<NestedMap>();

        mock_db.expect_create_document()
            .withf(move |_k, data| {
                data.get(key_nv) == Some(&json!(["a", "b", "c"])) &&
                data.get(key_nm) == Some(&json!({"x":1,"y":2}))
            })
            .returning(|_, _| Box::pin(async { Ok(()) }));

        let mut session = ArangoSession::new_mocked(Arc::new(mock_db));
        session.register_serializer::<NestedVec>();
        session.register_serializer::<NestedMap>();

        let mut map = HashMap::new();
        map.insert("x".to_string(), 1);
        map.insert("y".to_string(), 2);
        let entity = session.local_world
            .spawn((NestedVec(vec!["a".into(),"b".into(),"c".into()]), NestedMap(map)))
            .id();
        session.mark_dirty(entity);
        assert!(block_on(session.commit()).is_ok());
    }

    #[derive(bevy::prelude::Component, serde::Serialize, serde::Deserialize)]
    struct ErrComp(f32); // f32::INFINITY will fail to serialize

    #[test]
    fn commit_bubbles_serialize_error() {
        let mut mock_db = MockDatabaseConnection::new();
        // DB shouldn't be called
        mock_db.expect_create_document().never();
        mock_db.expect_update_document().never();
        mock_db.expect_delete_document().never();

        let mut session = ArangoSession::new_mocked(Arc::new(mock_db));
        session.register_serializer::<ErrComp>();
        let entity = session.local_world.spawn((ErrComp(f32::INFINITY),)).id();
        session.mark_dirty(entity);

        let err = block_on(session.commit()).unwrap_err();
        assert!(err.to_string().contains("Could not serialize"));
    }

    #[test]
    fn commit_bubbles_db_error() {
        let mut mock_db = MockDatabaseConnection::new();
        mock_db
            .expect_create_document()
            .returning(|_, _| Box::pin(async { Err(ArangoError("up".into())) }));

        let mut session = ArangoSession::new_mocked(Arc::new(mock_db));
        #[derive(bevy::prelude::Component, serde::Serialize, serde::Deserialize)]
        struct A(i32);
        session.register_serializer::<A>();
        let entity = session.local_world.spawn((A(1),)).id();
        session.mark_dirty(entity);

        let err = block_on(session.commit()).unwrap_err();
        assert_eq!(err.to_string(), "ArangoDB error: up");
    }

    #[test]
    fn commit_entity_with_zero_components_sends_only_key() {
        let mut mock = MockDatabaseConnection::new();
        mock.expect_create_document()
            .withf(|_collection: &str, payload: &Value| {
                if let Value::Object(map) = payload {
                    map.len() == 1 && map.contains_key("_key")
                } else {
                    false
                }
            })
            .times(1)
            .returning(|_, _| Box::pin(async { Ok(()) }));

        let mut session = ArangoSession::new_mocked(Arc::new(mock));
        let entity = session.local_world.spawn(()).id();
        session.mark_dirty(entity);
        assert!(block_on(session.commit()).is_ok());
    }

    #[derive(bevy::prelude::Component, Serialize, Deserialize, PartialEq, Debug)]
    enum TestEnum {
        A,
        B { x: i32 },
        C(String),
    }

    #[test]
    fn enum_component_round_trip_serializes_correctly() {
        let mut mock = MockDatabaseConnection::new();
        // capture the full type name
        let key_enum = std::any::type_name::<TestEnum>();
        mock.expect_create_document()
            .withf(move |_col, payload| {
                let obj = payload.as_object().unwrap();
                // lookup by full type name, not just "TestEnum"
                matches!(obj.get(key_enum).unwrap(), Value::String(_) | Value::Object(_))
            })
            .times(1)
            .returning(|_, _| Box::pin(async { Ok(()) }));

        let mut session = ArangoSession::new_mocked(Arc::new(mock));
        session.register_serializer::<TestEnum>();

        let entity = session.local_world
            .spawn((TestEnum::B { x: 42 },))
            .id();
        session.mark_dirty(entity);
        assert!(block_on(session.commit()).is_ok());
    }

    #[derive(bevy::prelude::Component, Serialize, Deserialize)]
    struct BigVec { items: Vec<i32> }

    #[test]
    fn large_payload_serializes_without_panic() {
        let mut mock = MockDatabaseConnection::new();
        // capture the full type name
        let key_big = std::any::type_name::<BigVec>();
        mock.expect_create_document()
            .withf(move |_col, payload| {
                let obj = payload.as_object().unwrap();
                // lookup the nested object by full type name
                let big = obj.get(key_big).unwrap().as_object().unwrap();
                let items = big.get("items").unwrap().as_array().unwrap();
                items.len() == 1000
            })
            .times(1)
            .returning(|_, _| Box::pin(async { Ok(()) }));

        let mut session = ArangoSession::new_mocked(Arc::new(mock));
        session.register_serializer::<BigVec>();

        let mut b = session.local_world.spawn(());
        let data: Vec<i32> = (0..1000).collect();
        let entity = b.insert(BigVec { items: data }).id();
        session.mark_dirty(entity);
        assert!(block_on(session.commit()).is_ok());
    }
}