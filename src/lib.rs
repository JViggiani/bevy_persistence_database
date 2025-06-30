use bevy::prelude::{Entity, World};
use futures::future::BoxFuture;
use serde_json::Value;
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
    /// Create a new document (entity).
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
}

/// Manages a “unit of work”: local World cache + change tracking + async runtime.
pub struct ArangoSession {
    pub local_world: World,
    pub db: Box<dyn DatabaseConnection>,
    pub dirty_entities: HashSet<Entity>,
    pub despawned_entities: HashSet<Entity>,
    /// Async runtime for driving database futures.
    pub runtime: Runtime,
}

impl ArangoSession {
    /// Testing constructor w/ mock DB.
    #[cfg(test)]
    pub fn new_mocked(db: Box<dyn DatabaseConnection>) -> Self {
        Self {
            local_world: World::new(),
            db,
            dirty_entities: HashSet::new(),
            despawned_entities: HashSet::new(),
            runtime: Builder::new_multi_thread()
                .enable_all()
                .build()
                .expect("Failed to create Tokio runtime"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_session_is_empty() {
        let mock_db = Box::new(MockDatabaseConnection::new());
        let session = ArangoSession::new_mocked(mock_db);
        assert_eq!(session.local_world.entities().len(), 0);
        assert!(session.dirty_entities.is_empty());
        assert!(session.despawned_entities.is_empty());
    }
}