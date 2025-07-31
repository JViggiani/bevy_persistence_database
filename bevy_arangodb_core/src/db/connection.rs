//! Defines the abstract `DatabaseConnection` trait.

use downcast_rs::{Downcast, impl_downcast};
use futures::future::BoxFuture;
use mockall::automock;
use serde_json::Value;
use std::fmt;

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
    Conflict { key: String },
    Other(String),
}

impl fmt::Display for PersistenceError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PersistenceError::Conflict { key } => write!(f, "Conflict detected for key: {}", key),
            PersistenceError::Other(s) => write!(f, "Persistence Error: {}", s),
        }
    }
}
impl std::error::Error for PersistenceError {}

/// Represents one DB operation in our atomic transaction.
#[derive(serde::Serialize, Debug, Clone)]
pub enum TransactionOperation {
    CreateDocument(Value),
    UpdateDocument {
        key: String,
        rev: String,
        patch: Value,
    },
    DeleteDocument(String),
    UpsertResource(String, Value),
}

/// The result of a successful transaction execution.
#[derive(Debug, Default)]
pub struct TransactionResult {
    /// A list of `(_key, _rev, _id)` tuples for newly created documents.
    pub created: Vec<(String, String, String)>,
    /// A list of `(_key, _rev, _id)` tuples for updated documents.
    pub updated: Vec<(String, String, String)>,
}

/// Abstracts database operations via async returns but remains object-safe.
#[automock]
pub trait DatabaseConnection: Send + Sync + Downcast + fmt::Debug {
    fn execute_transaction(
        &self,
        operations: Vec<TransactionOperation>,
    ) -> BoxFuture<'static, Result<TransactionResult, PersistenceError>>;

    fn query(
        &self,
        aql: String,
        bind_vars: std::collections::HashMap<String, Value>,
    ) -> BoxFuture<'static, Result<Vec<String>, PersistenceError>>;

    fn fetch_document(
        &self,
        entity_key: &str,
    ) -> BoxFuture<'static, Result<Option<(Value, String, String)>, PersistenceError>>;

    fn fetch_component(
        &self,
        entity_key: &str,
        comp_name: &str,
    ) -> BoxFuture<'static, Result<Option<Value>, PersistenceError>>;

    fn fetch_resource(
        &self,
        resource_name: &str,
    ) -> BoxFuture<'static, Result<Option<Value>, PersistenceError>>;

    fn clear_entities(&self) -> BoxFuture<'static, Result<(), PersistenceError>>;

    fn clear_resources(&self) -> BoxFuture<'static, Result<(), PersistenceError>>;
}
impl_downcast!(DatabaseConnection);
