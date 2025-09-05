use std::sync::{Arc, atomic::{AtomicUsize, Ordering}};
use bevy_persistence_database::{DatabaseConnection, PersistenceError, TransactionOperation};
use bevy_persistence_database::query::persistence_query_specification::PersistenceQuerySpecification;
use futures::future::BoxFuture;
use serde_json::Value;

/// Wrapper that counts execute_documents calls and forwards to a real backend.
#[derive(Debug)]
pub struct CountingDbConnection {
    pub inner: Arc<dyn DatabaseConnection>,
    pub queries: Arc<AtomicUsize>,
}

impl CountingDbConnection {
    pub fn new(inner: Arc<dyn DatabaseConnection>, queries: Arc<AtomicUsize>) -> Self {
        Self { inner, queries }
    }
}

impl DatabaseConnection for CountingDbConnection {
    fn document_key_field(&self) -> &'static str {
        self.inner.document_key_field()
    }

    fn execute_keys(
        &self,
        spec: &PersistenceQuerySpecification,
    ) -> BoxFuture<'static, Result<Vec<String>, PersistenceError>> {
        self.inner.execute_keys(spec)
    }

    fn execute_documents(
        &self,
        spec: &PersistenceQuerySpecification,
    ) -> BoxFuture<'static, Result<Vec<Value>, PersistenceError>> {
        self.queries.fetch_add(1, Ordering::SeqCst);
        self.inner.execute_documents(spec)
    }

    fn execute_documents_sync(
        &self,
        spec: &PersistenceQuerySpecification,
    ) -> Result<Vec<Value>, PersistenceError> {
        self.inner.execute_documents_sync(spec)
    }

    fn execute_transaction(
        &self,
        operations: Vec<TransactionOperation>,
    ) -> BoxFuture<'static, Result<Vec<String>, PersistenceError>> {
        self.inner.execute_transaction(operations)
    }

    fn fetch_document(
        &self,
        entity_key: &str,
    ) -> BoxFuture<'static, Result<Option<(Value, u64)>, PersistenceError>> {
        self.inner.fetch_document(entity_key)
    }

    fn fetch_component(
        &self,
        entity_key: &str,
        comp_name: &str,
    ) -> BoxFuture<'static, Result<Option<Value>, PersistenceError>> {
        self.inner.fetch_component(entity_key, comp_name)
    }

    fn fetch_resource(
        &self,
        resource_name: &str,
    ) -> BoxFuture<'static, Result<Option<(Value, u64)>, PersistenceError>> {
        self.inner.fetch_resource(resource_name)
    }

    fn clear_entities(&self) -> BoxFuture<'static, Result<(), PersistenceError>> {
        self.inner.clear_entities()
    }

    fn clear_resources(&self) -> BoxFuture<'static, Result<(), PersistenceError>> {
        self.inner.clear_resources()
    }

    fn count_documents(
        &self,
        spec: &PersistenceQuerySpecification,
    ) -> BoxFuture<'static, Result<usize, PersistenceError>> {
        // Increment the counter since this is a DB operation
        self.queries.fetch_add(1, Ordering::SeqCst);
        
        // Forward the call to the inner connection
        self.inner.count_documents(spec)
    }
}
