//! Real ArangoDB backend: implements `DatabaseConnection` using `arangors`.
//! Inject this into `PersistenceSession` in production to persist components.

use arangors::{
    client::reqwest::ReqwestClient,
    AqlQuery, ClientError, Connection, Database,
    transaction::{TransactionCollections, TransactionSettings},
};
use crate::db::DatabaseConnection;
use crate::db::connection::{PersistenceError, TransactionOperation, Collection, BEVY_PERSISTENCE_VERSION_FIELD};
use crate::query::spec::{QuerySpec, ValueExpr, BinaryOp};
use futures::future::BoxFuture;
use futures::FutureExt;
use serde_json::Value;
use std::collections::HashMap;
use std::fmt;
use once_cell::sync::Lazy;

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

        // Ensure all required collections exist.
        let collections_to_ensure = vec![
            Collection::Entities.to_string(),
            Collection::Resources.to_string(),
        ];
        for col_name in collections_to_ensure {
            match db.create_collection(&col_name).await {
                Ok(_) => {} // Collection created successfully
                Err(e) => {
                    // If the error is "duplicate name", the collection already exists, which is fine.
                    if let ClientError::Arango(arango_error) = e {
                        if arango_error.error_num() != 1207 { // 1207 is "duplicate name"
                            return Err(PersistenceError::new(arango_error.to_string()));
                        }
                    } else {
                        return Err(PersistenceError::new(e.to_string()));
                    }
                }
            }
        }

        Ok(Self { db })
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

    fn translate_value_expr(
        expr: &ValueExpr,
        bind_vars: &mut HashMap<String, Value>,
        key_field: &str,
    ) -> String {
        match expr {
            ValueExpr::Literal(v) => {
                let name = format!("bevy_arangodb_bind_{}", bind_vars.len());
                bind_vars.insert(name.clone(), v.clone());
                format!("@{}", name)
            }
            ValueExpr::Field { component_name, field_name } => {
                if field_name.is_empty() {
                    format!("doc.`{}`", component_name)
                } else {
                    format!("doc.`{}`.`{}`", component_name, field_name)
                }
            }
            ValueExpr::DocumentKey => format!("doc.{}", key_field),
            ValueExpr::BinaryOp { op, lhs, rhs } => {
                let l = Self::translate_value_expr(lhs, bind_vars, key_field);
                let r = Self::translate_value_expr(rhs, bind_vars, key_field);
                let op_str = match op {
                    BinaryOp::Eq => "==",
                    BinaryOp::Ne => "!=",
                    BinaryOp::Gt => ">",
                    BinaryOp::Gte => ">=",
                    BinaryOp::Lt => "<",
                    BinaryOp::Lte => "<=",
                    BinaryOp::And => "AND",
                    BinaryOp::Or => "OR",
                    BinaryOp::In => "IN",
                };
                format!("({} {} {})", l, op_str, r)
            }
        }
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

    fn query_keys(
        &self,
        aql: String,
        bind_vars: HashMap<String, Value>,
    ) -> BoxFuture<'static, Result<Vec<String>, PersistenceError>> {
        let db = self.db.clone();
        async move {
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

    fn query_documents(
        &self,
        aql: String,
        bind_vars: HashMap<String, Value>,
    ) -> BoxFuture<'static, Result<Vec<Value>, PersistenceError>> {
        let db = self.db.clone();
        async move {
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

    fn fetch_document(
        &self,
        entity_key: &str,
    ) -> BoxFuture<'static, Result<Option<(Value, u64)>, PersistenceError>> {
        let db = self.db.clone();
        let key = entity_key.to_string();
        async move {
            let col = db
                .collection(&Collection::Entities.to_string())
                .await
                .map_err(|e| PersistenceError::new(e.to_string()))?;
            match col.document::<Value>(&key).await {
                Ok(doc) => {
                    let version = doc.document
                        .get(BEVY_PERSISTENCE_VERSION_FIELD)
                        .and_then(|v| v.as_u64())
                        .ok_or_else(|| PersistenceError::new(format!("Document '{}' is missing version field '{}'", key, BEVY_PERSISTENCE_VERSION_FIELD)))?;
                    Ok(Some((doc.document, version)))
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

    fn fetch_component(
        &self,
        entity_key: &str,
        comp_name: &str,
    ) -> BoxFuture<'static, Result<Option<Value>, PersistenceError>> {
        let db = self.db.clone();
        let key = entity_key.to_string();
        let comp = comp_name.to_string();
        async move {
            let col = db
                .collection(&Collection::Entities.to_string())
                .await
                .map_err(|e| PersistenceError::new(e.to_string()))?;
            match col.document::<Value>(&key).await {
                Ok(doc) => Ok(doc.document.get(&comp).cloned()),
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
        resource_name: &str,
    ) -> BoxFuture<'static, Result<Option<(Value, u64)>, PersistenceError>> {
        let db = self.db.clone();
        let res_key = resource_name.to_string();
        async move {
            let col = db
                .collection(&Collection::Resources.to_string())
                .await
                .map_err(|e| PersistenceError::new(e.to_string()))?;
            // A document may not exist, so we handle the error.
            match col.document::<Value>(&res_key).await {
                Ok(doc) => {
                    let version = doc.document
                        .get(BEVY_PERSISTENCE_VERSION_FIELD)
                        .and_then(|v| v.as_u64())
                        .ok_or_else(|| PersistenceError::new(format!("Resource '{}' is missing version field '{}'", res_key, BEVY_PERSISTENCE_VERSION_FIELD)))?;
                    Ok(Some((doc.document, version)))
                }
                Err(e) => {
                    if let ClientError::Arango(ref api_err) = e {
                        if api_err.error_num() == 1202 { // 1202 is "document not found"
                            return Ok(None);
                        }
                    }
                    Err(PersistenceError::new(e.to_string()))
                }
            }
        }
        .boxed()
    }

    fn clear_entities(&self) -> BoxFuture<'static, Result<(), PersistenceError>> {
        let db = self.db.clone();
        async move {
            let col = db
                .collection(&Collection::Entities.to_string())
                .await
                .map_err(|e| PersistenceError::new(e.to_string()))?;
            col.truncate()
                .await
                .map_err(|e| PersistenceError::new(e.to_string()))?;
            Ok(())
        }
        .boxed()
    }

    fn clear_resources(&self) -> BoxFuture<'static, Result<(), PersistenceError>> {
        let db = self.db.clone();
        async move {
            let col = db
                .collection(&Collection::Resources.to_string())
                .await
                .map_err(|e| PersistenceError::new(e.to_string()))?;
            col.truncate()
                .await
                .map_err(|e| PersistenceError::new(e.to_string()))?;
            Ok(())
        }
        .boxed()
    }

    fn execute_transaction(
        &self,
        operations: Vec<TransactionOperation>,
    ) -> BoxFuture<'static, Result<Vec<String>, PersistenceError>> {
        let db = self.db.clone();
        async move {
            let collections = TransactionCollections::builder()
                .write(vec![
                    Collection::Entities.to_string(),
                    Collection::Resources.to_string(),
                ])
                .build();
            let settings = TransactionSettings::builder()
                .collections(collections)
                .build();

            let trx = db
                .begin_transaction(settings)
                .await
                .map_err(|e| PersistenceError::new(e.to_string()))?;
            let mut new_keys = Vec::new();

            for op in operations {
                match op {
                    TransactionOperation::CreateDocument { collection, data } => {
                        let col = trx
                            .collection(&collection.to_string())
                            .await
                            .map_err(|e| PersistenceError::new(e.to_string()))?;
                        let meta = col
                            .create_document(data, Default::default())
                            .await
                            .map_err(|e| PersistenceError::new(e.to_string()))?;
                        let key = meta
                            .header()
                            .ok_or_else(|| PersistenceError::new("Missing header"))?
                            ._key
                            .clone();
                        if matches!(collection, Collection::Entities) {
                            new_keys.push(key);
                        }
                    }
                    TransactionOperation::UpdateDocument {
                        collection,
                        key,
                        expected_current_version,
                        patch,
                    } => {
                        let aql = format!(
                            "LET doc = DOCUMENT('{}', @key) \
                             FILTER doc != null AND doc.{} == @expected_current_version \
                             UPDATE doc WITH @patch IN {} \
                             RETURN OLD",
                            collection, BEVY_PERSISTENCE_VERSION_FIELD, collection
                        );

                        let bind_vars: HashMap<String, Value> = [
                            ("key".to_string(), Value::String(key.clone())),
                            ("expected_current_version".to_string(), Value::Number(expected_current_version.into())),
                            ("patch".to_string(), patch),
                        ].into_iter().collect();

                        let query = AqlQuery::builder()
                            .query(&aql)
                            .bind_vars(
                                bind_vars.iter()
                                    .map(|(k,v)| (k.as_str(), v.clone()))
                                    .collect()
                            )
                            .build();

                        let result: Vec<Value> = trx
                            .aql_query(query)
                            .await
                            .map_err(|e| PersistenceError::new(e.to_string()))?;

                        if result.is_empty() {
                            return Err(PersistenceError::Conflict { key });
                        }
                    }
                    TransactionOperation::DeleteDocument {
                        collection,
                        key,
                        expected_current_version,
                    } => {
                        let aql = format!(
                            "LET doc = DOCUMENT('{}', @key) \
                             FILTER doc != null AND doc.{} == @expected_current_version \
                             REMOVE doc IN {} \
                             RETURN OLD",
                            collection, BEVY_PERSISTENCE_VERSION_FIELD, collection
                        );

                        let bind_vars: HashMap<String, Value> = [
                            ("key".to_string(), Value::String(key.clone())),
                            ("expected_current_version".to_string(), Value::Number(expected_current_version.into())),
                        ].into_iter().collect();

                        let query = AqlQuery::builder()
                            .query(&aql)
                            .bind_vars(
                                bind_vars.iter()
                                    .map(|(k,v)| (k.as_str(), v.clone()))
                                    .collect()
                            )
                            .build();

                        let result: Vec<Value> = trx
                            .aql_query(query)
                            .await
                            .map_err(|e| PersistenceError::new(e.to_string()))?;

                        if result.is_empty() {
                            return Err(PersistenceError::Conflict { key });
                        }
                    }
                }
            }

            trx.commit()
                .await
                .map_err(|e| PersistenceError::new(e.to_string()))?;
            Ok(new_keys)
        }
        .boxed()
    }

    fn query_documents_sync(
        &self,
        aql: String,
        bind_vars: HashMap<String, Value>,
    ) -> Result<Vec<Value>, PersistenceError> {
        // Drive the async query on the shared multi-thread runtime.
        SYNC_RT.block_on(async {
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

    fn build_query(&self, spec: &QuerySpec) -> (String, HashMap<String, Value>) {
        let mut bind_vars = HashMap::new();
        let mut aql = format!("FOR doc IN {}", Collection::Entities);
        let mut filters: Vec<String> = Vec::new();

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
            let s = Self::translate_value_expr(expr, &mut bind_vars, self.document_key_field());
            filters.push(s);
        }
        if filters.is_empty() {
            aql.push_str("\n  FILTER true");
        } else {
            aql.push_str("\n  FILTER ");
            aql.push_str(&filters.join(" AND "));
        }
        if spec.return_full_docs {
            aql.push_str("\n  RETURN doc");
        } else {
            aql.push_str(&format!("\n  RETURN doc.{}", self.document_key_field()));
        }
        (aql, bind_vars)
    }
}