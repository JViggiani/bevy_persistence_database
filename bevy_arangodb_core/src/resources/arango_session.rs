//! Core ECS‐to‐Arango bridge: defines `ArangoSession`.
//! Handles local cache, change tracking, and commit logic (create/update/delete).

use crate::db::connection::{DatabaseConnection, TransactionOperation, ArangoError};
use bevy::prelude::{Component, Entity, Resource, World, App};
use serde_json::Value;
use std::{
    any::TypeId,
    collections::{HashMap, HashSet},
    sync::Arc,
};
use crate::persist::Persist;

type ComponentSerializer   = Box<dyn Fn(Entity, &World) -> Result<Option<(String, Value)>, ArangoError> + Send + Sync>;
type ComponentDeserializer = Box<dyn Fn(&mut World, Entity, Value) -> Result<(), ArangoError> + Send + Sync>;
type ResourceSerializer    = Box<dyn Fn(&World, &ArangoSession) -> Result<Option<(String, Value)>, ArangoError> + Send + Sync>;
type ResourceDeserializer  = Box<dyn Fn(&mut World, Value) -> Result<(), ArangoError> + Send + Sync>;

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
    operations: Vec<TransactionOperation>,
    new_entities: Vec<Entity>,
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
fn _prepare_commit(
    session: &ArangoSession,
    world: &World,
) -> Result<CommitData, ArangoError> {
    let mut operations = Vec::new();
    let mut new_entities = Vec::new();

    // Deletions
    for &e in &session.despawned_entities {
        if let Some(key) = session.entity_keys.get(&e) {
            operations.push(TransactionOperation::DeleteDocument(key.clone()));
        }
    }
    // Creations & Updates
    for &e in &session.dirty_entities {
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
        if let Some(key) = session.entity_keys.get(&e) {
            operations.push(TransactionOperation::UpdateDocument(key.clone(), doc));
        } else {
            // For new entities, only create a document if there's something to persist.
            if !doc.as_object().unwrap().is_empty() {
                operations.push(TransactionOperation::CreateDocument(doc));
                new_entities.push(e);
            }
        }
    }
    // Resources
    for &tid in &session.dirty_resources {
        if let Some(ser) = session.resource_serializers.get(&tid) {
            if let Some((n, v)) = ser(world, session)? {
                operations.push(TransactionOperation::UpsertResource(n.to_string(), v));
            }
        }
    }
    Ok(CommitData { operations, new_entities })
}

/// Persist new, changed, or despawned entities to the database.
pub(crate) async fn commit(
    session: &mut ArangoSession,
    world: &mut World,
) -> Result<(), ArangoError> {
    if session.dirty_entities.is_empty()
        && session.despawned_entities.is_empty()
        && session.dirty_resources.is_empty()
    {
        return Ok(());
    }

    let data = _prepare_commit(session, world)?;
    let new_keys = session.db.execute_transaction(data.operations).await?;

    // assign new GUIDs
    for (entity, key) in data.new_entities.into_iter().zip(new_keys.into_iter()) {
        if let Some(mut e) = world.get_entity_mut(entity) {
            e.insert(crate::Guid::new(key.clone()));
        }
        session.entity_keys.insert(entity, key);
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

    // SAFETY: We removed the session, so it's safe to get &mut World
    let world = unsafe { &mut *world_ptr };
    let result = commit(&mut session, world).await;

    app.world.insert_resource(session);
    result
}


#[cfg(test)]
mod arango_session {
    use super::*;
    use crate::db::connection::MockDatabaseConnection;
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
        db.expect_execute_transaction()
            .withf(|ops| ops.iter().any(|op| match op {
                TransactionOperation::UpsertResource(name, data) =>
                    name == MyRes::name() && data == &json!({"value":5}),
                _ => false,
            }))
            .times(1)
            .returning(|_| Box::pin(async { Ok(vec![]) }));

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
        db.expect_execute_transaction().times(0);

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
        db.expect_execute_transaction()
            .withf(|ops| ops.iter().any(|op| matches!(op, TransactionOperation::CreateDocument(_))))
            .times(1)
            .returning(|_| Box::pin(async { Ok(vec!["new_key".into()]) }));

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
        db.expect_execute_transaction()
            .times(1)
            .returning(|_| Box::pin(async { Ok(vec!["new_key".into()]) }));

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
        db.expect_execute_transaction()
            .withf(|ops| ops.iter().any(|op| match op {
                TransactionOperation::UpdateDocument(key, patch) =>
                    key == "existing_key" && patch.get(MyComp::name()) == Some(&json!({"value":20})),
                _ => false,
            }))
            .times(1)
            .returning(|_| Box::pin(async { Ok(vec![]) }));

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
        db.expect_execute_transaction()
            .withf(|ops| ops.iter().any(|op| matches!(op, TransactionOperation::CreateDocument(_))))
            .times(1)
            .returning(|_| Box::pin(async { Ok(vec!["new_key".into()]) }));

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
        db.expect_execute_transaction()
            .withf(|ops| {
                ops.iter().any(|op| {
                    if let TransactionOperation::DeleteDocument(key) = op {
                        key == "key_to_delete"
                    } else {
                        false
                    }
                })
            })
            .times(1)
            .returning(|_| Box::pin(async { Ok(vec![]) }));

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
        db.expect_execute_transaction()
            .withf(move |ops| {
                let key_a = A::name();
                let key_b = B::name();
                ops.iter().any(|op| match op {
                    TransactionOperation::CreateDocument(doc) =>
                        doc.get(key_a) == Some(&json!(1)) &&
                        doc.get(key_b) == Some(&json!("hello")),
                    _ => false,
                })
            })
            .times(1)
            .returning(|_| Box::pin(async { Ok(vec!["new_key".into()]) }));

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
        db.expect_execute_transaction()
            .withf(move |ops| {
                let key_nv = NestedVec::name();
                let key_nm = NestedMap::name();
                let expected_vec = json!(["a", "b"]);
                let mut map = HashMap::new();
                map.insert("x".to_string(), 1);
                let expected_map = json!(map);

                ops.iter().any(|op| match op {
                    TransactionOperation::CreateDocument(doc) =>
                        doc.get(key_nv) == Some(&expected_vec) &&
                        doc.get(key_nm) == Some(&expected_map),
                    _ => false,
                })
            })
            .times(1)
            .returning(|_| Box::pin(async { Ok(vec!["new_key".into()]) }));

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
    #[should_panic(expected = "db error")]
    async fn commit_bubbles_db_error() {
        #[persist(component)]
        struct A(i32);

        let mut db = MockDatabaseConnection::new();
        db.expect_execute_transaction()
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
        db.expect_execute_transaction()
            .withf(move |ops| {
                let key_enum = TestEnum::name();
                let expected_a = json!("A");
                let expected_b = json!({"B": 42});
                ops.iter().any(|op| match op {
                    TransactionOperation::CreateDocument(doc) =>
                        doc.get(key_enum) == Some(&expected_a) || doc.get(key_enum) == Some(&expected_b),
                    _ => false,
                })
            })
            .times(1) // only one transaction call for both enums
            .returning(|_| Box::pin(async { Ok(vec!["new_key".into()]) }));

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
        db.expect_execute_transaction()
            .withf(move |ops| {
                let key_big = BigVec::name();
                ops.iter().any(|op| match op {
                    TransactionOperation::CreateDocument(doc) =>
                        doc.get(key_big).is_some(),
                    _ => false,
                })
            })
            .times(1)
            .returning(|_| Box::pin(async { Ok(vec!["new_key".into()]) }));

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