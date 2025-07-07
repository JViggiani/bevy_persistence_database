//! Core ECS‐to‐Arango bridge: defines `ArangoSession` and the abstract
//! `DatabaseConnection` trait.
//! Handles local cache, change tracking, and commit logic (create/update/delete).

use bevy::prelude::{Entity, World, Resource};
use futures::future::BoxFuture;
use serde_json::Value;
use std::{collections::HashSet, fmt, sync::Arc};
use tokio::runtime::Runtime;

#[cfg(test)]
use tokio::runtime::Builder;

#[cfg(test)]
use mockall::automock;

#[derive(Debug)]
pub struct ArangoError(String);

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
}

/// Manages a “unit of work”: local World cache + change tracking + async runtime.
pub struct ArangoSession {
    pub local_world: World,
    pub db: Arc<dyn DatabaseConnection>,
    pub dirty_entities: HashSet<Entity>,
    pub despawned_entities: HashSet<Entity>,
    pub loaded_entities: HashSet<Entity>,    // track pre-loaded entities
    /// Async runtime for driving database futures.
    pub runtime: Runtime,
    component_serializers: Vec<Box<dyn Fn(Entity, &World) -> Option<(String, Value)> + Send + Sync>>,
    resource_serializers: Vec<Box<dyn Fn(&World) -> Option<(String, Value)> + Send + Sync>>,
}

impl ArangoSession {
    /// Register a component type for persistence in `commit()`.
    pub fn register_serializer<T>(&mut self)
    where
        T: bevy::ecs::component::Component + serde::Serialize + 'static,
    {
       // Use the full Rust path as the JSON key
       let name = std::any::type_name::<T>().to_string();
        self.component_serializers.push(Box::new(move |entity, world| {
            world.get::<T>(entity)
                .map(|c| (name.clone(), serde_json::to_value(c).unwrap()))
        }));
    }

    /// Register a resource type for persistence in `commit()`.
    pub fn register_resource_serializer<R>(&mut self)
    where
        R: Resource + serde::Serialize + 'static,
    {
        let name = std::any::type_name::<R>().to_string();
        self.resource_serializers.push(Box::new(move |world| {
            world.get_resource::<R>().map(|r| (name.clone(), serde_json::to_value(r).unwrap()))
        }));
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
            resource_serializers: Vec::new(),
            dirty_entities: HashSet::new(),
            despawned_entities: HashSet::new(),
            loaded_entities: HashSet::new(),
            runtime: Builder::new_multi_thread()
                .enable_all()
                .build()
                .expect("Failed to create Tokio runtime"),
        }
    }

    /// Persist new, changed, or despawned entities to the database.
    pub fn commit(&mut self) {
        // First process deletions
        let to_delete: Vec<Entity> = self.despawned_entities.drain().collect();
        let deleted_set: std::collections::HashSet<Entity> = to_delete.iter().cloned().collect();
        for entity in to_delete {
            let key = entity.index().to_string();
            self.runtime
                .block_on(self.db.delete_document(&key))
                .expect("delete_document failed");
        }

        // Then process creates/updates
        let to_update = self
            .dirty_entities
            .drain()
            .filter(|e| !deleted_set.contains(e))
            .collect::<Vec<_>>();

        for entity in to_update {
            // aggregate registered components into JSON
            let mut map = serde_json::Map::new();
            for func in &self.component_serializers {
                if let Some((k, v)) = func(entity, &self.local_world) {
                    map.insert(k, v);
                }
            }
            let data = Value::Object(map);
            let key = entity.index().to_string();
            if self.loaded_entities.contains(&entity) {
                self.runtime
                    .block_on(self.db.update_document(&key, data))
                    .expect("update_document failed");
            } else {
                self.runtime
                    .block_on(self.db.create_document(&key, data))
                    .expect("create_document failed");
            }
        }

        // Persist registered resources as one Arango document under key "resources"
        if !self.resource_serializers.is_empty() {
            let mut map = serde_json::Map::new();
            for func in &self.resource_serializers {
                if let Some((k, v)) = func(&self.local_world) {
                    map.insert(k, v);
                }
            }
            let data = Value::Object(map);
            // write to special "resources" collection
            self.runtime
                .block_on(self.db.create_document("resources", data))
                .expect("create_document for resources failed");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Serialize, Deserialize};
    use std::sync::Arc;
    use serde_json::json;

    #[derive(Resource, Serialize, Deserialize, PartialEq, Debug)]
    struct MyRes { value: i32 }

    #[test]
    fn commit_serializes_resources() {
        let mut mock_db = MockDatabaseConnection::new();
        mock_db.expect_create_document()
            .withf(|key, data| {
                key == "resources" &&
                data.get(std::any::type_name::<MyRes>()) == Some(&json!({"value":5}))
            })
            .returning(|_, _| Box::pin(async { Ok(()) }));

        let mut session = ArangoSession::new_mocked(Arc::new(mock_db));
        session.register_resource_serializer::<MyRes>();

        // insert resource into local_world
        session.local_world.insert_resource(MyRes { value: 5 });
        session.commit();
    }

    #[allow(dead_code)]
    #[derive(bevy::prelude::Component)]
    struct Foo(i32);

    #[test]
    fn commit_creates_new_entity() {
        let mut mock_db = MockDatabaseConnection::new();
        mock_db
            .expect_create_document()
            .withf(|key, data| key == "0" && data == &json!({}))
            .times(1)
            .returning(|_, _| Box::pin(async { Ok(()) }));

        let mut session = ArangoSession::new_mocked(Arc::new(mock_db));
        let id = session.local_world.spawn(()).id();
        session.mark_dirty(id);
        session.commit();
    }

    #[test]
    fn commit_updates_existing_entity() {
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
        session.commit();
    }

    #[test]
    fn commit_deletes_entity() {
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
        session.commit();
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

        session.commit();

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

        session.commit();
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
        session.commit();
    }
}

/// Note: Components and resources you wish to persist **must** derive
/// `serde::Serialize` and `serde::Deserialize`.
mod arango_query;
mod arango_connection;
pub use arango_query::ArangoQuery;
pub use arango_connection::ArangoDbConnection;