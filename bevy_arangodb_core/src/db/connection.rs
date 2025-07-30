//! Defines the abstract `DatabaseConnection` trait.

use downcast_rs::{Downcast, impl_downcast};
use futures::future::BoxFuture;
use mockall::automock;
use serde_json::Value;
use std::fmt;

/// A type alias for a document's key (`_key`).
pub type DocumentKey = String;
/// A type alias for a document's revision (`_rev`).
pub type DocumentRev = String;

/// A struct holding the key and revision of a newly created document.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DocumentId {
    pub key: DocumentKey,
    pub rev: DocumentRev,
}

/// The result of a transaction, separating created and updated document IDs.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct TransactionResult {
    pub created: Vec<DocumentId>,
    pub updated: Vec<DocumentId>,
}

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
#[derive(Debug, PartialEq, Clone)]
pub enum PersistenceError {
    /// A generic error message.
    Generic(String),
    /// A concurrency conflict, indicating that the document was modified by another process.
    Conflict { key: String },
}

impl fmt::Display for PersistenceError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PersistenceError::Generic(s) => write!(f, "Persistence Error: {}", s),
            PersistenceError::Conflict { key } => {
                write!(f, "Concurrency conflict for document key '{}'", key)
            }
        }
    }
}
impl std::error::Error for PersistenceError {}

/// Represents one DB operation in our atomic transaction.
#[derive(serde::Serialize, Debug, Clone)]
pub enum TransactionOperation {
    CreateDocument(Value),
    UpdateDocument {
        key: DocumentKey,
        rev: DocumentRev,
        patch: std::collections::HashMap<String, Value>,
    },
    DeleteDocument {
        key: DocumentKey,
        rev: DocumentRev,
    },
    DeleteResource(DocumentKey),
    UpsertResource(DocumentKey, Value),
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
    ) -> BoxFuture<'static, Result<Vec<DocumentKey>, PersistenceError>>;

    fn fetch_document(
        &self,
        entity_key: &str,
    ) -> BoxFuture<'static, Result<Option<(Value, DocumentRev)>, PersistenceError>>;

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
