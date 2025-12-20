//! Real ArangoDB backend: implements `DatabaseConnection` using `arangors`.
//! Inject this into `PersistenceSession` in production to persist components.

use arangors::{
    client::reqwest::ReqwestClient,
    AqlQuery, ClientError, Connection, Database,
    transaction::{TransactionCollections, TransactionSettings},
};
use crate::db::DatabaseConnection;
use crate::db::connection::{PersistenceError, TransactionOperation, DocumentKind, BEVY_PERSISTENCE_VERSION_FIELD, BEVY_TYPE_FIELD};
use crate::query::persistence_query_specification::PersistenceQuerySpecification;
use crate::query::filter_expression::{FilterExpression, BinaryOperator};
use futures::future::BoxFuture;
use futures::FutureExt;
use serde_json::Value;
use std::collections::HashMap;
use std::fmt;
use once_cell::sync::Lazy;
use crate::db::shared::{GroupedOperations, check_operation_success, OperationType};

// Local helper to pull out the version field
fn extract_version(doc: &Value, key: &str) -> Result<u64, PersistenceError> {
    doc.get(BEVY_PERSISTENCE_VERSION_FIELD)
        .and_then(|v| v.as_u64())
        .ok_or_else(|| {
            PersistenceError::new(format!(
                "Document '{}' is missing version field '{}'",
                key, BEVY_PERSISTENCE_VERSION_FIELD
            ))
        })
}

// Local constants and enums to avoid magic strings
const JSON_KEY_FIELD: &str = "key";
const AQL_BIND_DOCS: &str = "docs";
const AQL_BIND_PATCHES: &str = "patches";
const AQL_BIND_DELETES: &str = "deletes";
const AQL_BIND_STORE: &str = "store";
const AQL_BIND_KIND: &str = "kind";

fn insert_store_bind(bind_vars: &mut HashMap<String, Value>, store: &str) {
    bind_vars.insert(format!("@{}", AQL_BIND_STORE), Value::String(store.to_string()));
}

/// A real ArangoDB backend for `DatabaseConnection`.
pub struct ArangoDbConnection {
    db: Database<ReqwestClient>,
}

impl fmt::Debug for ArangoDbConnection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ArangoDbConnection")
            .finish_non_exhaustive()
    }
}

impl ArangoDbConnection {
    /// Connects to ArangoDB via JWT, selects the specified database, and
    /// ensures the required collections exist.
    pub async fn connect(
        url: &str,
        user: &str,
        pass: &str,
        db_name: &str,
    ) -> Result<Self, PersistenceError> {
        let conn = Connection::establish_jwt(url, user, pass)
            .await
            .map_err(|e| PersistenceError::new(e.to_string()))?;
        let db: Database<ReqwestClient> = conn
            .db(db_name)
            .await
            .map_err(|e| PersistenceError::new(e.to_string()))?;

        Ok(Self { db })
    }

    async fn ensure_collection(db: &Database<ReqwestClient>, name: &str) -> Result<(), PersistenceError> {
        match db.create_collection(name).await {
            Ok(_) => Ok(()),
            Err(e) => {
                if let ClientError::Arango(arango_error) = &e {
                    if arango_error.error_num() == 1207 {
                        return Ok(());
                    }
                }
                Err(PersistenceError::new(e.to_string()))
            }
        }
    }

    /// Ensure a database exists, creating it if necessary.
    pub async fn ensure_database(
        url: &str,
        user: &str,
        pass: &str,
        db_name: &str,
    ) -> Result<(), PersistenceError> {
        // Establish a root connection to the server
        let conn = Connection::establish_jwt(url, user, pass)
            .await
            .map_err(|e| PersistenceError::new(e.to_string()))?;

        // Try to create the database; ignore duplicate-name errors
        match conn.create_database(db_name).await {
            Ok(_) => Ok(()),
            Err(e) => {
                if let ClientError::Arango(ref arango_error) = e {
                    // 1207 = ARANGO_DUPLICATE_NAME
                    if arango_error.error_num() == 1207 {
                        return Ok(());
                    }
                }
                Err(PersistenceError::new(format!(
                    "Failed to ensure database '{}': {}",
                    db_name, e
                )))
            }
        }
    }

    fn translate_filter_expression(
        expr: &FilterExpression,
        bind_vars: &mut HashMap<String, Value>,
        key_field: &str,
    ) -> String {
        match expr {
            FilterExpression::Literal(v) => {
                let name = format!("bevy_persistence_database_bind_{}", bind_vars.len());
                bind_vars.insert(name.clone(), v.clone());
                format!("@{}", name)
            }
            FilterExpression::Field { component_name, field_name } => {
                if field_name.is_empty() {
                    format!("doc.`{}`", component_name)
                } else {
                    format!("doc.`{}`.`{}`", component_name, field_name)
                }
            }
            FilterExpression::DocumentKey => format!("doc.{}", key_field),
            FilterExpression::BinaryOperator { op, lhs, rhs } => {
                let l = Self::translate_filter_expression(lhs, bind_vars, key_field);
                let r = Self::translate_filter_expression(rhs, bind_vars, key_field);
                let op_str = match op {
                    BinaryOperator::Eq => "==",
                    BinaryOperator::Ne => "!=",
                    BinaryOperator::Gt => ">",
                    BinaryOperator::Gte => ">=",
                    BinaryOperator::Lt => "<",
                    BinaryOperator::Lte => "<=",
                    BinaryOperator::And => "AND",
                    BinaryOperator::Or => "OR",
                    BinaryOperator::In => "IN",
                };
                format!("({} {} {})", l, op_str, r)
            }
        }
    }

    // Private: build AQL and bind vars for a given spec
    fn build_filter_static(
        spec: &PersistenceQuerySpecification,
        bind_vars: &mut HashMap<String, Value>,
        key_field: &str,
    ) -> String {
        let mut filters: Vec<String> = Vec::new();

        bind_vars.insert(AQL_BIND_KIND.into(), Value::String(spec.kind.as_str().to_string()));
        filters.push(format!("doc.`{}` == @{}", BEVY_TYPE_FIELD, AQL_BIND_KIND));

        if !spec.presence_with.is_empty() {
            let s = spec.presence_with
                .iter()
                .map(|n| format!("doc.`{}` != null", n))
                .collect::<Vec<_>>()
                .join(" AND ");
            filters.push(format!("({})", s));
        }
        if !spec.presence_without.is_empty() {
            let s = spec.presence_without
                .iter()
                .map(|n| format!("doc.`{}` == null", n))
                .collect::<Vec<_>>()
                .join(" AND ");
            filters.push(format!("({})", s));
        }
        if let Some(expr) = &spec.value_filters {
            let s = Self::translate_filter_expression(expr, bind_vars, key_field);
            filters.push(s);
        }
        if filters.is_empty() {
            "FILTER true".to_string()
        } else {
            format!("FILTER {}", filters.join(" AND "))
        }
    }

    // Non-async builder used by tests to inspect generated AQL/binds without hitting Arango
    #[cfg(test)]
    fn build_query_internal(
        spec: &PersistenceQuerySpecification,
        key_field: &str,
    ) -> (String, HashMap<String, Value>) {
        let spec = spec.clone();
        let mut bind_vars = HashMap::new();
        insert_store_bind(&mut bind_vars, &spec.store);
        let filter = Self::build_filter_static(&spec, &mut bind_vars, key_field);

        let aql = if spec.return_full_docs {
            format!(
                "FOR doc IN @@{}\n  {}\n  RETURN MERGE(doc, {{ \"{}\": doc.`{}` }})",
                AQL_BIND_STORE,
                filter,
                key_field,
                key_field
            )
        } else {
            format!(
                "FOR doc IN @@{}\n  {}\n  RETURN doc.{}",
                AQL_BIND_STORE,
                filter,
                key_field
            )
        };

        (aql, bind_vars)
    }

    // Private helper to truncate any collection
    fn clear_collection(&self, name: &str) -> BoxFuture<'static, Result<(), PersistenceError>> {
        let db = self.db.clone();
        let name = name.to_string();
        async move {
            let col = db
                .collection(&name)
                .await
                .map_err(|e| PersistenceError::new(e.to_string()))?;
            col.truncate()
                .await
                .map(|_| ())
                .map_err(|e| PersistenceError::new(e.to_string()))
        }
        .boxed()
    }

    // Private helper to fetch a full document + version
    fn fetch_with_version(
        &self,
        store: &str,
        key: &str,
        kind: DocumentKind,
    ) -> BoxFuture<'static, Result<Option<(Value, u64)>, PersistenceError>> {
        let db = self.db.clone();
        let name = store.to_string();
        let key = key.to_string();
        async move {
            ArangoDbConnection::ensure_collection(&db, &name).await?;
            let col = db
                .collection(&name)
                .await
                .map_err(|e| PersistenceError::new(e.to_string()))?;
            match col.document::<Value>(&key).await {
                Ok(doc) => {
                    let matches_kind = doc.document.get(BEVY_TYPE_FIELD)
                        .and_then(|v| v.as_str())
                        .map(|s| s == kind.as_str())
                        .unwrap_or(false);
                    if !matches_kind {
                        return Ok(None);
                    }
                    let version = extract_version(&doc.document, &key)?;
                    Ok(Some((doc.document, version)))
                }
                Err(e) => {
                    if let ClientError::Arango(api_err) = &e {
                        if api_err.error_num() == 1202 {
                            return Ok(None);
                        }
                    }
                    Err(PersistenceError::new(e.to_string()))
                }
            }
        }
        .boxed()
    }
}

// Shared multi-thread runtime for sync operations (avoid per-call runtimes)
static SYNC_RT: Lazy<tokio::runtime::Runtime> = Lazy::new(|| {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("Failed to build sync Tokio runtime")
});

impl DatabaseConnection for ArangoDbConnection {
    fn document_key_field(&self) -> &'static str {
        "_key"
    }

    fn execute_keys(
        &self,
        spec: &PersistenceQuerySpecification,
    ) -> BoxFuture<'static, Result<Vec<String>, PersistenceError>> {
        let db = self.db.clone();
        let mut spec = spec.clone();
        spec.return_full_docs = false;
        let mut bind_vars = HashMap::new();
        insert_store_bind(&mut bind_vars, &spec.store);
        let filter = Self::build_filter_static(&spec, &mut bind_vars, self.document_key_field());
        let mut aql = String::new();
        aql.push_str(&format!("FOR doc IN @@{}\n  {}\n  RETURN doc.{}", AQL_BIND_STORE, filter, self.document_key_field()));
        async move {
            ArangoDbConnection::ensure_collection(&db, &spec.store).await?;
            let query = AqlQuery::builder()
                .query(&aql)
                .bind_vars(
                    bind_vars.iter()
                        .map(|(k,v)| (k.as_str(), v.clone()))
                        .collect()
                )
                .build();
            let result: Vec<String> = db
                .aql_query(query)
                .await
                .map_err(|e| PersistenceError::new(e.to_string()))?;
            Ok(result)
        }
        .boxed()
    }

    fn execute_documents(
        &self,
        spec: &PersistenceQuerySpecification,
    ) -> BoxFuture<'static, Result<Vec<Value>, PersistenceError>> {
        let db = self.db.clone();
        let mut spec = spec.clone();
        spec.return_full_docs = true;
        let mut bind_vars = HashMap::new();
        insert_store_bind(&mut bind_vars, &spec.store);
        let filter = Self::build_filter_static(&spec, &mut bind_vars, self.document_key_field());
        let mut aql = String::new();
        if spec.return_full_docs {
            let kf = self.document_key_field();
            aql.push_str(&format!(
                "FOR doc IN @@{}\n  {}\n  RETURN MERGE(doc, {{ \"{}\": doc.`{}` }})",
                AQL_BIND_STORE, filter, kf, kf
            ));
        } else {
            aql.push_str(&format!("FOR doc IN @@{}\n  {}\n  RETURN doc.{}", AQL_BIND_STORE, filter, self.document_key_field()));
        }
        async move {
            ArangoDbConnection::ensure_collection(&db, &spec.store).await?;
            let query = AqlQuery::builder()
                .query(&aql)
                .bind_vars(
                    bind_vars.iter()
                        .map(|(k,v)| (k.as_str(), v.clone()))
                        .collect()
                )
                .build();
            let result: Vec<Value> = db
                .aql_query(query)
                .await
                .map_err(|e| PersistenceError::new(e.to_string()))?;
            Ok(result)
        }
        .boxed()
    }

    fn execute_documents_sync(
        &self,
        spec: &PersistenceQuerySpecification,
    ) -> Result<Vec<Value>, PersistenceError> {
        let mut spec = spec.clone();
        spec.return_full_docs = true;
        let mut bind_vars = HashMap::new();
        insert_store_bind(&mut bind_vars, &spec.store);
        let filter = Self::build_filter_static(&spec, &mut bind_vars, self.document_key_field());
        let kf = self.document_key_field();
        let aql = format!(
            "FOR doc IN @@{}\n  {}\n  RETURN MERGE(doc, {{ \"{}\": doc.`{}` }})",
            AQL_BIND_STORE, filter, kf, kf
        );
        SYNC_RT.block_on(async {
            ArangoDbConnection::ensure_collection(&self.db, &spec.store).await?;
            let query = AqlQuery::builder()
                .query(&aql)
                .bind_vars(
                    bind_vars.iter()
                        .map(|(k,v)| (k.as_str(), v.clone()))
                        .collect()
                )
                .build();

            self.db
                .aql_query(query)
                .await
                .map_err(|e| PersistenceError::new(e.to_string()))
        })
    }

    fn fetch_document(
        &self,
        store: &str,
        entity_key: &str,
    ) -> BoxFuture<'static, Result<Option<(Value, u64)>, PersistenceError>> {
        self.fetch_with_version(store, entity_key, DocumentKind::Entity)
    }

    fn fetch_component(
        &self,
        store: &str,
        entity_key: &str,
        comp_name: &str,
    ) -> BoxFuture<'static, Result<Option<Value>, PersistenceError>> {
        let db = self.db.clone();
        let key = entity_key.to_string();
        let comp = comp_name.to_string();
        let store_name = store.to_string();
        async move {
            ArangoDbConnection::ensure_collection(&db, &store_name).await?;
            let col = db
                .collection(&store_name)
                .await
                .map_err(|e| PersistenceError::new(e.to_string()))?;
            match col.document::<Value>(&key).await {
                Ok(doc) => {
                    let matches_kind = doc.document.get(BEVY_TYPE_FIELD)
                        .and_then(|v| v.as_str())
                        .map(|s| s == DocumentKind::Entity.as_str())
                        .unwrap_or(false);
                    if !matches_kind {
                        return Ok(None);
                    }
                    Ok(doc.document.get(&comp).cloned())
                }
                Err(e) => {
                    if let ClientError::Arango(api_err) = &e {
                        if api_err.error_num() == 1202 {
                            // entity not found
                            return Ok(None);
                        }
                    }
                    Err(PersistenceError::new(e.to_string()))
                }
            }
        }
        .boxed()
    }

    fn fetch_resource(
        &self,
        store: &str,
        resource_name: &str,
    ) -> BoxFuture<'static, Result<Option<(Value, u64)>, PersistenceError>> {
        self.fetch_with_version(store, resource_name, DocumentKind::Resource)
    }

    fn clear_store(&self, store: &str, _kind: DocumentKind) -> BoxFuture<'static, Result<(), PersistenceError>> {
        self.clear_collection(store)
    }

    fn execute_transaction(
        &self,
        operations: Vec<TransactionOperation>,
    ) -> BoxFuture<'static, Result<Vec<String>, PersistenceError>> {
        let db = self.db.clone();
        // The DB-level key attribute (e.g., `_key`) for returns
        let key_attr = self.document_key_field();
        async move {
            let store = operations
                .get(0)
                .map(|op| op.store().to_string())
                .ok_or_else(|| PersistenceError::new("execute_transaction requires at least one operation"))?;
            if store.is_empty() {
                return Err(PersistenceError::new("store must be non-empty"));
            }
            if operations.iter().any(|op| op.store() != store) {
                return Err(PersistenceError::new("all operations in a transaction must target the same store"));
            }

            ArangoDbConnection::ensure_collection(&db, &store).await?;

            let collections = TransactionCollections::builder()
                .write(vec![store.clone()])
                .build();
            let settings = TransactionSettings::builder()
                .collections(collections)
                .build();

            let trx = db
                .begin_transaction(settings)
                .await
                .map_err(|e| PersistenceError::new(e.to_string()))?;

            let groups = GroupedOperations::from_operations(operations, JSON_KEY_FIELD);
            let mut new_keys: Vec<String> = Vec::new();

            // 1) Entity creates
            if !groups.entity_creates.is_empty() {
                let aql = format!(
                    "FOR d IN @{bind} INSERT d INTO {col} RETURN NEW.`{key}`",
                    bind = AQL_BIND_DOCS,
                    col = store,
                    key = key_attr
                );
                let mut bind_vars: std::collections::HashMap<String, Value> = std::collections::HashMap::new();
                bind_vars.insert(AQL_BIND_DOCS.into(), Value::Array(groups.entity_creates.clone()));
                let query = AqlQuery::builder()
                    .query(&aql)
                    .bind_vars(bind_vars.iter().map(|(k, v)| (k.as_str(), v.clone())).collect())
                    .build();
                let keys: Vec<String> = trx
                    .aql_query(query)
                    .await
                    .map_err(|e| PersistenceError::new(e.to_string()))?;
                new_keys.extend(keys);
            }

            // 2) Entity updates
            if !groups.entity_updates.is_empty() {
                let requested = groups.extract_keys(&groups.entity_updates, JSON_KEY_FIELD);
                let aql = format!(
                    "FOR p IN @{patches}
                       LET doc = DOCUMENT('{col}', p.{key})
                       FILTER doc != null AND doc.{type_field} == @kind AND doc.{ver} == p.expected
                       UPDATE doc WITH p.patch IN {col} OPTIONS {{ mergeObjects: true }}
                       RETURN p.{key}",
                    patches = AQL_BIND_PATCHES,
                    col = store,
                    key = JSON_KEY_FIELD,
                    ver = BEVY_PERSISTENCE_VERSION_FIELD,
                    type_field = BEVY_TYPE_FIELD,
                );
                let mut bind_vars: std::collections::HashMap<String, Value> = std::collections::HashMap::new();
                bind_vars.insert(AQL_BIND_PATCHES.into(), Value::Array(groups.entity_updates.clone()));
                bind_vars.insert(AQL_BIND_KIND.into(), Value::String(DocumentKind::Entity.as_str().to_string()));
                let query = AqlQuery::builder()
                    .query(&aql)
                    .bind_vars(bind_vars.iter().map(|(k, v)| (k.as_str(), v.clone())).collect())
                    .build();
                let updated: Vec<String> = trx
                    .aql_query(query)
                    .await
                    .map_err(|e| PersistenceError::new(e.to_string()))?;
                check_operation_success(requested, updated, &OperationType::Update, store.as_str())?;
            }

            // 3) Entity deletes
            if !groups.entity_deletes.is_empty() {
                let requested = groups.extract_keys(&groups.entity_deletes, JSON_KEY_FIELD);
                let aql = format!(
                    "FOR p IN @{deletes}
                       LET doc = DOCUMENT('{col}', p.{key})
                       FILTER doc != null AND doc.{type_field} == @kind AND doc.{ver} == p.expected
                       REMOVE doc IN {col}
                       RETURN p.{key}",
                    deletes = AQL_BIND_DELETES,
                    col = store,
                    key = JSON_KEY_FIELD,
                    ver = BEVY_PERSISTENCE_VERSION_FIELD,
                    type_field = BEVY_TYPE_FIELD,
                );
                let mut bind_vars: std::collections::HashMap<String, Value> = std::collections::HashMap::new();
                bind_vars.insert(AQL_BIND_DELETES.into(), Value::Array(groups.entity_deletes.clone()));
                bind_vars.insert(AQL_BIND_KIND.into(), Value::String(DocumentKind::Entity.as_str().to_string()));
                let query = AqlQuery::builder()
                    .query(&aql)
                    .bind_vars(bind_vars.iter().map(|(k, v)| (k.as_str(), v.clone())).collect())
                    .build();
                let removed: Vec<String> = trx
                    .aql_query(query)
                    .await
                    .map_err(|e| PersistenceError::new(e.to_string()))?;
                check_operation_success(requested, removed, &OperationType::Delete, store.as_str())?;
            }

            // 4) Resource creates
            if !groups.resource_creates.is_empty() {
                let aql = format!(
                    "FOR d IN @{bind} INSERT d INTO {col}",
                    bind = AQL_BIND_DOCS,
                    col = store
                );
                let mut bind_vars: std::collections::HashMap<String, Value> = std::collections::HashMap::new();
                bind_vars.insert(AQL_BIND_DOCS.into(), Value::Array(groups.resource_creates.clone()));
                let query = AqlQuery::builder()
                    .query(&aql)
                    .bind_vars(bind_vars.iter().map(|(k, v)| (k.as_str(), v.clone())).collect())
                    .build();
                let _: Vec<Value> = trx
                    .aql_query(query)
                    .await
                    .map_err(|e| PersistenceError::new(e.to_string()))?;
            }

            // 5) Resource updates
            if !groups.resource_updates.is_empty() {
                let requested = groups.extract_keys(&groups.resource_updates, JSON_KEY_FIELD);
                let aql = format!(
                    "FOR p IN @{patches}
                       LET doc = DOCUMENT('{col}', p.{key})
                       FILTER doc != null AND doc.{type_field} == @kind AND doc.{ver} == p.expected
                       UPDATE doc WITH p.patch IN {col} OPTIONS {{ mergeObjects: true }}
                       RETURN p.{key}",
                    patches = AQL_BIND_PATCHES,
                    col = store,
                    key = JSON_KEY_FIELD,
                    ver = BEVY_PERSISTENCE_VERSION_FIELD,
                    type_field = BEVY_TYPE_FIELD,
                );
                let mut bind_vars: std::collections::HashMap<String, Value> = std::collections::HashMap::new();
                bind_vars.insert(AQL_BIND_PATCHES.into(), Value::Array(groups.resource_updates.clone()));
                bind_vars.insert(AQL_BIND_KIND.into(), Value::String(DocumentKind::Resource.as_str().to_string()));
                let query = AqlQuery::builder()
                    .query(&aql)
                    .bind_vars(bind_vars.iter().map(|(k, v)| (k.as_str(), v.clone())).collect())
                    .build();
                let updated: Vec<String> = trx
                    .aql_query(query)
                    .await
                    .map_err(|e| PersistenceError::new(e.to_string()))?;
                check_operation_success(requested, updated, &OperationType::Update, store.as_str())?;
            }

            // 6) Resource deletes
            if !groups.resource_deletes.is_empty() {
                let requested = groups.extract_keys(&groups.resource_deletes, JSON_KEY_FIELD);
                let aql = format!(
                    "FOR p IN @{deletes}
                       LET doc = DOCUMENT('{col}', p.{key})
                       FILTER doc != null AND doc.{type_field} == @kind AND doc.{ver} == p.expected
                       REMOVE doc IN {col}
                       RETURN p.{key}",
                    deletes = AQL_BIND_DELETES,
                    col = store,
                    key = JSON_KEY_FIELD,
                    ver = BEVY_PERSISTENCE_VERSION_FIELD,
                    type_field = BEVY_TYPE_FIELD,
                );
                let mut bind_vars: std::collections::HashMap<String, Value> = std::collections::HashMap::new();
                bind_vars.insert(AQL_BIND_DELETES.into(), Value::Array(groups.resource_deletes.clone()));
                bind_vars.insert(AQL_BIND_KIND.into(), Value::String(DocumentKind::Resource.as_str().to_string()));
                let query = AqlQuery::builder()
                    .query(&aql)
                    .bind_vars(bind_vars.iter().map(|(k, v)| (k.as_str(), v.clone())).collect())
                    .build();
                let removed: Vec<String> = trx
                    .aql_query(query)
                    .await
                    .map_err(|e| PersistenceError::new(e.to_string()))?;
                check_operation_success(requested, removed, &OperationType::Delete, store.as_str())?;
            }

            trx.commit()
                .await
                .map_err(|e| PersistenceError::new(e.to_string()))?;
            Ok(new_keys)
        }
        .boxed()
    }

    fn count_documents(
        &self,
        spec: &PersistenceQuerySpecification,
    ) -> BoxFuture<'static, Result<usize, PersistenceError>> {
        let db = self.db.clone();
        let mut bind_vars = HashMap::new();
        insert_store_bind(&mut bind_vars, &spec.store);
        let filter = Self::build_filter_static(spec, &mut bind_vars, self.document_key_field());
        let store = spec.store.clone();

        let count_aql = format!(
            "RETURN LENGTH(\n  FOR doc IN @@{}\n  {}\n  RETURN 1\n)",
            AQL_BIND_STORE,
            filter
        );

        bevy::log::debug!("[arango] count_documents AQL: {}", count_aql);

        async move {
            ArangoDbConnection::ensure_collection(&db, &store).await?;
            let query = AqlQuery::builder()
                .query(&count_aql)
                .bind_vars(
                    bind_vars.iter()
                        .map(|(k,v)| (k.as_str(), v.clone()))
                        .collect()
                )
                .build();

            let result: Vec<usize> = db
                .aql_query(query)
                .await
                .map_err(|e| PersistenceError::new(e.to_string()))?;

            Ok(result.first().copied().unwrap_or(0))
        }.boxed()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::persistence_query_specification::PersistenceQuerySpecification;
    use crate::query::filter_expression::FilterExpression;
    use serde_json::Value;
    use std::collections::HashMap;

    /// Helper to call the private builder without touching `db`.
    fn build(spec: PersistenceQuerySpecification) -> (String, HashMap<String, Value>) {
        ArangoDbConnection::build_query_internal(&spec, "_key")
    }

    #[test]
    fn presence_only_filters_and_keys() {
        let mut spec = PersistenceQuerySpecification::default();
        spec.presence_with = vec!["Health"];
        spec.return_full_docs = false;
        let (aql, binds) = build(spec);

        assert!(aql.contains("FOR doc IN @@store"));
        assert!(aql.contains("FILTER doc.`bevy_type` == @kind AND (doc.`Health` != null)"));
        assert!(aql.contains("RETURN doc._key"));
        assert_eq!(binds.len(), 2, "expect store and kind binds only");
    }

    #[test]
    fn presence_and_value_filter_pushes_bind_and_expr() {
        let mut spec = PersistenceQuerySpecification::default();
        spec.presence_with = vec!["Position"];
        // example value filter: Position.x < 3.5
        let expr = FilterExpression::field("Position", "x").lt(3.5);
        spec.value_filters = Some(expr.clone());
        spec.return_full_docs = false;

        let (aql, binds) = build(spec);
        // ensure presence, kind, and value predicate appear
        assert!(aql.contains("(doc.`Position` != null)"));
        assert!(aql.contains("doc.`bevy_type` == @kind"));
        assert!(aql.contains("<"));
        // binds: store, kind, value
        assert_eq!(binds.len(), 3);
    }

    #[test]
    fn or_value_filter_generates_or_clause() {
        let mut spec = PersistenceQuerySpecification::default();
        // OR filter: key == "a" OR key == "b"
        let f1 = FilterExpression::DocumentKey.eq("a");
        let f2 = FilterExpression::DocumentKey.eq("b");
        spec.value_filters = Some(f1.or(f2));
        spec.return_full_docs = false;

        let (aql, binds) = build(spec);
        assert!(aql.contains("OR"));
        // binds: store, kind, "a", "b"
        assert_eq!(binds.len(), 4);
    }

    #[test]
    fn return_full_docs_merges_doc_and_key() {
        let mut spec = PersistenceQuerySpecification::default();
        spec.return_full_docs = true;
        // no presence/value filters -> FILTER true
        let (aql, binds) = build(spec);

        assert!(aql.contains("FILTER doc.`bevy_type` == @kind"));
        // check MERGE(doc, { "_key": doc.`_key` })
        assert!(aql.contains("RETURN MERGE(doc,"));
        assert!(aql.contains("\"_key\": doc.`_key`"));
        assert_eq!(binds.len(), 2, "store and kind");
    }
}