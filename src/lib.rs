//! Core ECS‐to‐Arango bridge: defines `ArangoSession` and the abstract
//! `DatabaseConnection` trait. Handles local cache, change tracking,
//! and commit logic (create/update/delete) against any backend.

use bevy::prelude::{Entity, World};
use futures::future::BoxFuture;
use serde_json::{json, Value};
use std::{collections::HashSet, fmt};
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
}

/// Manages a “unit of work”: local World cache + change tracking + async runtime.
pub struct ArangoSession {
    pub local_world: World,
    pub db: Box<dyn DatabaseConnection>,
    pub dirty_entities: HashSet<Entity>,
    pub despawned_entities: HashSet<Entity>,
    pub loaded_entities: HashSet<Entity>,    // track pre-loaded entities
    /// Async runtime for driving database futures.
    pub runtime: Runtime,
}

impl ArangoSession {
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
    pub fn new_mocked(db: Box<dyn DatabaseConnection>) -> Self {
        Self {
            local_world: World::new(),
            db,
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
            let key = entity.index().to_string();
            let data = json!({}); // TODO: actual component data
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
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

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

        let mut session = ArangoSession::new_mocked(Box::new(mock_db));
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

        let mut session = ArangoSession::new_mocked(Box::new(mock_db));
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

        let mut session = ArangoSession::new_mocked(Box::new(mock_db));
        let id = session.local_world.spawn(()).id();
        session.mark_loaded(id);
        session.mark_despawned(id);
        session.commit();
    }

    #[test]
    fn mark_dirty_tracks_entity() {
        // Setup
        let mock_db = Box::new(MockDatabaseConnection::new());
        let mut session = ArangoSession::new_mocked(mock_db);

        // 1) spawn a component in local_world
        let e = session.local_world.spawn(Foo(42));  // removed `mut`
        let id = e.id();

        // 2) mark it dirty
        session.mark_dirty(id);

        // 3) assert tracking
        assert!(
            session.dirty_entities.contains(&id),
            "Entity should be marked dirty"
        );
    }

    #[test]
    fn new_session_is_empty() {
        let mock_db = Box::new(MockDatabaseConnection::new());
        let session = ArangoSession::new_mocked(mock_db);
        assert_eq!(session.local_world.entities().len(), 0);
        assert!(session.dirty_entities.is_empty());
        assert!(session.despawned_entities.is_empty());
    }

    #[test]
    fn commit_clears_tracking_sets() {
        // Setup a mock that will accept one delete and one create
        let mut mock_db = MockDatabaseConnection::new();
        mock_db
            .expect_delete_document()
            .returning(|_| Box::pin(async { Ok(()) }));
        mock_db
            .expect_create_document()
            .returning(|_, _| Box::pin(async { Ok(()) }));

        let mut session = ArangoSession::new_mocked(Box::new(mock_db));
        let id_new = session.local_world.spawn(()).id();
        let id_old = session.local_world.spawn(()).id();
        session.mark_loaded(id_old);
        session.mark_dirty(id_new);
        session.mark_dirty(id_old);
        session.mark_despawned(id_old);

        session.commit();

        // After commit, both sets should be empty
        assert!(session.dirty_entities.is_empty(), "dirty_entities should be drained");
        assert!(session.despawned_entities.is_empty(), "despawned_entities should be drained");
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

        let mut session = ArangoSession::new_mocked(Box::new(mock_db));
        let id0 = session.local_world.spawn(()).id();
        let id1 = session.local_world.spawn(()).id();

        session.mark_loaded(id1);
        session.mark_dirty(id0);
        session.mark_dirty(id1);

        session.commit();
    }
}

// Include the query builder and real‐DB connection modules:
mod arango_query;
mod arango_connection;
pub use arango_query::ArangoQuery;
pub use arango_connection::ArangoDbConnection;