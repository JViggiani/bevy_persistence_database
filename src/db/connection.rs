//! Defines the abstract `DatabaseConnection` trait.

use futures::future::BoxFuture;
use mockall::automock;
use serde_json::Value;
use std::fmt;
use std::sync::Arc;
use bevy::prelude::Resource;
use crate::query::persistence_query_specification::PersistenceQuerySpecification;

/// The field name used for optimistic locking version tracking.
pub const BEVY_PERSISTENCE_VERSION_FIELD: &str = "bevy_persistence_version";

/// Field indicating whether a document represents an entity or a resource.
pub const BEVY_TYPE_FIELD: &str = "bevy_type";

/// Logical discriminator for persisted documents.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
pub enum DocumentKind {
    Entity,
    Resource,
}

impl DocumentKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            DocumentKind::Entity => "entity",
            DocumentKind::Resource => "resource",
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

impl PersistenceError {
    /// Creates a new general error.
    pub fn new(msg: impl Into<String>) -> Self {
        PersistenceError::General(msg.into())
    }
}

/// Represents one DB operation in our atomic transaction.
#[derive(serde::Serialize, Debug, Clone)]
pub enum TransactionOperation {
    CreateDocument {
        store: String,
        kind: DocumentKind,
        data: Value,
    },
    UpdateDocument {
        store: String,
        kind: DocumentKind,
        key: String,
        expected_current_version: u64,
        patch: Value,
    },
    DeleteDocument {
        store: String,
        kind: DocumentKind,
        key: String,
        expected_current_version: u64,
    },
}

impl TransactionOperation {
    /// Accessor for the store targeted by this operation.
    pub fn store(&self) -> &str {
        match self {
            TransactionOperation::CreateDocument { store, .. }
            | TransactionOperation::UpdateDocument { store, .. }
            | TransactionOperation::DeleteDocument { store, .. } => store,
        }
    }
}

/// Abstracts database operations via async returns but remains object-safe.
#[automock]
pub trait DatabaseConnection: Send + Sync + std::fmt::Debug {
    /// Returns the name of the field used as the primary key for documents.
    /// The backend must include this field in any full-document results.
    fn document_key_field(&self) -> &'static str;

    /// Execute a backend-agnostic spec and return document keys only.
    fn execute_keys(
        &self,
        spec: &PersistenceQuerySpecification,
    ) -> BoxFuture<'static, Result<Vec<String>, PersistenceError>>;

    /// Execute a backend-agnostic spec and return full documents.
    fn execute_documents(
        &self,
        spec: &PersistenceQuerySpecification,
    ) -> BoxFuture<'static, Result<Vec<Value>, PersistenceError>>;

    /// Synchronous variant for tests/system-param runtime.
    /// Default implementation just panics - implementations should override this
    fn execute_documents_sync(
        &self,
        _spec: &PersistenceQuerySpecification,
    ) -> Result<Vec<Value>, PersistenceError> {
        panic!("execute_documents_sync not implemented");
    }

    fn execute_transaction(
        &self,
        operations: Vec<TransactionOperation>,
    ) -> BoxFuture<'static, Result<Vec<String>, PersistenceError>>;

    fn fetch_document(
        &self,
        store: &str,
        entity_key: &str,
    ) -> BoxFuture<'static, Result<Option<(Value, u64)>, PersistenceError>>;

    fn fetch_component(
        &self,
        store: &str,
        entity_key: &str,
        comp_name: &str,
    ) -> BoxFuture<'static, Result<Option<Value>, PersistenceError>>;

    fn fetch_resource(
        &self,
        store: &str,
        resource_name: &str,
    ) -> BoxFuture<'static, Result<Option<(Value, u64)>, PersistenceError>>;

    fn clear_store(&self, store: &str, kind: DocumentKind) -> BoxFuture<'static, Result<(), PersistenceError>>;

    /// Count documents matching a specification without fetching them
    fn count_documents(
        &self,
        spec: &PersistenceQuerySpecification,
    ) -> BoxFuture<'static, Result<usize, PersistenceError>>;
}

/// A resource wrapper around the DatabaseConnection
#[derive(Resource)]
pub struct DatabaseConnectionResource(pub Arc<dyn DatabaseConnection>);

impl std::ops::Deref for DatabaseConnectionResource {
    type Target = Arc<dyn DatabaseConnection>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}