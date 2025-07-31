//! Defines the abstract `DatabaseConnection` trait.

use downcast_rs::{Downcast, impl_downcast};
use futures::future::BoxFuture;
use mockall::automock;
use serde_json::Value;
use std::fmt;

/// The field name used for optimistic locking version tracking.
pub const BEVY_PERSISTENCE_VERSION_FIELD: &str = "bevy_persistence_version";

/// An enum representing the collections used by this library.
pub enum Collection {
    /// The collection where all Bevy entities are stored as documents.
    Entities,
    /// The special document key for storing Bevy resources.
    Resources,
}

impl std::fmt::Display for Collection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Collection::Entities => write!(f, "entities"),
            Collection::Resources => write!(f, "resources"),
        }
    }
}

/// An error type for database operations.
#[derive(Debug, Clone)]
pub enum PersistenceError {
    /// A general error with a message.
    General(String),
    /// A version conflict occurred during an update or delete operation.
    Conflict { key: String },
}

impl fmt::Display for PersistenceError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PersistenceError::General(msg) => write!(f, "Persistence Error: {}", msg),
            PersistenceError::Conflict { key } => write!(f, "Version conflict for key: {}", key),
        }
    }
}

impl std::error::Error for PersistenceError {}

// For backward compatibility
impl From<String> for PersistenceError {
    fn from(msg: String) -> Self {
        PersistenceError::General(msg)
    }
}

impl PersistenceError {
    /// Creates a new general error.
    pub fn new(msg: impl Into<String>) -> Self {
        PersistenceError::General(msg.into())
    }
}

/// Represents one DB operation in our atomic transaction.
#[derive(serde::Serialize, Debug, Clone)]
pub enum TransactionOperation {
    CreateDocument(Value),
    UpdateDocument { key: String, expected_version: u64, patch: Value },
    DeleteDocument { key: String, expected_version: u64 },
    CreateResource { key: String, data: Value },
    UpdateResource { key: String, expected_version: u64, data: Value },
}

/// Abstracts database operations via async returns but remains object-safe.
#[automock]
pub trait DatabaseConnection: Send + Sync + Downcast + fmt::Debug {
    fn execute_transaction(
        &self,
        operations: Vec<TransactionOperation>,
    ) -> BoxFuture<'static, Result<Vec<String>, PersistenceError>>;

    fn query(
        &self,
        aql: String,
        bind_vars: std::collections::HashMap<String, Value>,
    ) -> BoxFuture<'static, Result<Vec<String>, PersistenceError>>;

    fn fetch_document(
        &self,
        entity_key: &str,
    ) -> BoxFuture<'static, Result<Option<(Value, u64)>, PersistenceError>>;

    fn fetch_component(
        &self,
        entity_key: &str,
        comp_name: &str,
    ) -> BoxFuture<'static, Result<Option<Value>, PersistenceError>>;

    fn fetch_resource(
        &self,
        resource_name: &str,
    ) -> BoxFuture<'static, Result<Option<(Value, u64)>, PersistenceError>>;

    fn clear_entities(&self) -> BoxFuture<'static, Result<(), PersistenceError>>;

    fn clear_resources(&self) -> BoxFuture<'static, Result<(), PersistenceError>>;
}
impl_downcast!(DatabaseConnection);
