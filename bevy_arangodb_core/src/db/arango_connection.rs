//! Real ArangoDB backend: implements `DatabaseConnection` using `arangors`.
//! Inject this into `PersistenceSession` in production to persist components.

use arangors::{
    client::reqwest::ReqwestClient,
    AqlQuery, ClientError, Connection, Database,
    transaction::{TransactionCollections, TransactionSettings},
};
use crate::db::DatabaseConnection;
use crate::db::connection::{PersistenceError, TransactionOperation, Collection, BEVY_PERSISTENCE_VERSION_FIELD};
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

    fn translate_filter_expression(
        expr: &FilterExpression,
        bind_vars: &mut HashMap<String, Value>,
        key_field: &str,
    ) -> String {
        match expr {
            FilterExpression::Literal(v) => {
                let name = format!("bevy_arangodb_bind_{}", bind_vars.len());
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
    fn build_query_internal(
        &self,
        spec: &PersistenceQuerySpecification,
    ) -> (String, HashMap<String, Value>) {
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
            let s = Self::translate_filter_expression(expr, &mut bind_vars, self.document_key_field());
            filters.push(s);
        }
        if filters.is_empty() {
            aql.push_str("\n  FILTER true");
        } else {
            aql.push_str("\n  FILTER ");
            aql.push_str(&filters.join(" AND "));
        }
        if spec.return_full_docs {
            // Always include the key field in returned documents so immediate apply can resolve entities.
            // Use backticks to access system attribute names like `_key`.
            let kf = self.document_key_field();
            aql.push_str(&format!("\n  RETURN MERGE(doc, {{ \"{}\": doc.`{}` }})", kf, kf));
        } else {
            aql.push_str(&format!("\n  RETURN doc.{}", self.document_key_field()));
        }
        bevy::log::debug!(
            "[arango] AQL generated (return_full_docs={}):\n{}\nbind_vars={}",
            spec.return_full_docs,
            aql,
            bind_vars.len()
        );
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
        coll: Collection,
        key: &str,
    ) -> BoxFuture<'static, Result<Option<(Value, u64)>, PersistenceError>> {
        let db = self.db.clone();
        let name = coll.to_string();
        let key = key.to_string();
        async move {
            let col = db
                .collection(&name)
                .await
                .map_err(|e| PersistenceError::new(e.to_string()))?;
            match col.document::<Value>(&key).await {
                Ok(doc) => {
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
        // enforce key-only selection
        let mut spec = spec.clone();
        spec.return_full_docs = false;
        let (aql, bind_vars) = self.build_query_internal(&spec);
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

    fn execute_documents(
        &self,
        spec: &PersistenceQuerySpecification,
    ) -> BoxFuture<'static, Result<Vec<Value>, PersistenceError>> {
        let db = self.db.clone();
        let mut spec = spec.clone();
        spec.return_full_docs = true;
        let (aql, bind_vars) = self.build_query_internal(&spec);
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

    fn execute_documents_sync(
        &self,
        spec: &PersistenceQuerySpecification,
    ) -> Result<Vec<Value>, PersistenceError> {
        let mut spec = spec.clone();
        spec.return_full_docs = true;
        let (aql, bind_vars) = self.build_query_internal(&spec);
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

    fn fetch_document(
        &self,
        entity_key: &str,
    ) -> BoxFuture<'static, Result<Option<(Value, u64)>, PersistenceError>> {
        self.fetch_with_version(Collection::Entities, entity_key)
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
        self.fetch_with_version(Collection::Resources, resource_name)
    }

    fn clear_entities(&self) -> BoxFuture<'static, Result<(), PersistenceError>> {
        self.clear_collection(&Collection::Entities.to_string())
    }

    fn clear_resources(&self) -> BoxFuture<'static, Result<(), PersistenceError>> {
        self.clear_collection(&Collection::Resources.to_string())
    }

    fn execute_transaction(
        &self,
        operations: Vec<TransactionOperation>,
    ) -> BoxFuture<'static, Result<Vec<String>, PersistenceError>> {
        let db = self.db.clone();
        // The DB-level key attribute (e.g., `_key`) for returns
        let key_attr = self.document_key_field();
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

            // Use "key" as the JSON property in the grouped operations to match AQL `p.key`
            let groups = GroupedOperations::from_operations(operations, JSON_KEY_FIELD);
            let mut new_keys: Vec<String> = Vec::new();

            // 1) Entity creates
            if !groups.entity_creates.is_empty() {
                let aql = format!(
                    "FOR d IN @{bind} INSERT d INTO {col} RETURN NEW.`{key}`",
                    bind = AQL_BIND_DOCS,
                    col = Collection::Entities,
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
                       FILTER doc != null AND doc.{ver} == p.expected
                       UPDATE doc WITH p.patch IN {col} OPTIONS {{ mergeObjects: true }}
                       RETURN p.{key}",
                    patches = AQL_BIND_PATCHES,
                    col = Collection::Entities,
                    key = JSON_KEY_FIELD,
                    ver = BEVY_PERSISTENCE_VERSION_FIELD
                );
                let mut bind_vars: std::collections::HashMap<String, Value> = std::collections::HashMap::new();
                bind_vars.insert(AQL_BIND_PATCHES.into(), Value::Array(groups.entity_updates.clone()));
                let query = AqlQuery::builder()
                    .query(&aql)
                    .bind_vars(bind_vars.iter().map(|(k, v)| (k.as_str(), v.clone())).collect())
                    .build();
                let updated: Vec<String> = trx
                    .aql_query(query)
                    .await
                    .map_err(|e| PersistenceError::new(e.to_string()))?;
                let col_name = Collection::Entities.to_string();
                check_operation_success(requested, updated, &OperationType::Update, col_name.as_str())?;
            }

            // 3) Entity deletes
            if !groups.entity_deletes.is_empty() {
                let requested = groups.extract_keys(&groups.entity_deletes, JSON_KEY_FIELD);
                let aql = format!(
                    "FOR p IN @{deletes}
                       LET doc = DOCUMENT('{col}', p.{key})
                       FILTER doc != null AND doc.{ver} == p.expected
                       REMOVE doc IN {col}
                       RETURN p.{key}",
                    deletes = AQL_BIND_DELETES,
                    col = Collection::Entities,
                    key = JSON_KEY_FIELD,
                    ver = BEVY_PERSISTENCE_VERSION_FIELD
                );
                let mut bind_vars: std::collections::HashMap<String, Value> = std::collections::HashMap::new();
                bind_vars.insert(AQL_BIND_DELETES.into(), Value::Array(groups.entity_deletes.clone()));
                let query = AqlQuery::builder()
                    .query(&aql)
                    .bind_vars(bind_vars.iter().map(|(k, v)| (k.as_str(), v.clone())).collect())
                    .build();
                let removed: Vec<String> = trx
                    .aql_query(query)
                    .await
                    .map_err(|e| PersistenceError::new(e.to_string()))?;
                let col_name = Collection::Entities.to_string();
                check_operation_success(requested, removed, &OperationType::Delete, col_name.as_str())?;
            }

            // 4) Resource creates
            if !groups.resource_creates.is_empty() {
                let aql = format!(
                    "FOR d IN @{bind} INSERT d INTO {col}",
                    bind = AQL_BIND_DOCS,
                    col = Collection::Resources
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
                       FILTER doc != null AND doc.{ver} == p.expected
                       UPDATE doc WITH p.patch IN {col} OPTIONS {{ mergeObjects: true }}
                       RETURN p.{key}",
                    patches = AQL_BIND_PATCHES,
                    col = Collection::Resources,
                    key = JSON_KEY_FIELD,
                    ver = BEVY_PERSISTENCE_VERSION_FIELD
                );
                let mut bind_vars: std::collections::HashMap<String, Value> = std::collections::HashMap::new();
                bind_vars.insert(AQL_BIND_PATCHES.into(), Value::Array(groups.resource_updates.clone()));
                let query = AqlQuery::builder()
                    .query(&aql)
                    .bind_vars(bind_vars.iter().map(|(k, v)| (k.as_str(), v.clone())).collect())
                    .build();
                let updated: Vec<String> = trx
                    .aql_query(query)
                    .await
                    .map_err(|e| PersistenceError::new(e.to_string()))?;
                let col_name = Collection::Resources.to_string();
                check_operation_success(requested, updated, &OperationType::Update, col_name.as_str())?;
            }

            // 6) Resource deletes
            if !groups.resource_deletes.is_empty() {
                let requested = groups.extract_keys(&groups.resource_deletes, JSON_KEY_FIELD);
                let aql = format!(
                    "FOR p IN @{deletes}
                       LET doc = DOCUMENT('{col}', p.{key})
                       FILTER doc != null AND doc.{ver} == p.expected
                       REMOVE doc IN {col}
                       RETURN p.{key}",
                    deletes = AQL_BIND_DELETES,
                    col = Collection::Resources,
                    key = JSON_KEY_FIELD,
                    ver = BEVY_PERSISTENCE_VERSION_FIELD
                );
                let mut bind_vars: std::collections::HashMap<String, Value> = std::collections::HashMap::new();
                bind_vars.insert(AQL_BIND_DELETES.into(), Value::Array(groups.resource_deletes.clone()));
                let query = AqlQuery::builder()
                    .query(&aql)
                    .bind_vars(bind_vars.iter().map(|(k, v)| (k.as_str(), v.clone())).collect())
                    .build();
                let removed: Vec<String> = trx
                    .aql_query(query)
                    .await
                    .map_err(|e| PersistenceError::new(e.to_string()))?;
                let col_name = Collection::Resources.to_string();
                check_operation_success(requested, removed, &OperationType::Delete, col_name.as_str())?;
            }

            trx.commit()
                .await
                .map_err(|e| PersistenceError::new(e.to_string()))?;
            Ok(new_keys)
        }
        .boxed()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::persistence_query_specification::PersistenceQuerySpecification;
    use crate::query::filter_expression::FilterExpression;
    use serde_json::Value;
    use std::{collections::HashMap, mem::MaybeUninit};

    /// Helper to call the private builder without touching `db`.
    fn build(spec: PersistenceQuerySpecification) -> (String, HashMap<String, Value>) {
        let uninit: MaybeUninit<ArangoDbConnection> = MaybeUninit::uninit();
        let conn: &ArangoDbConnection = unsafe { uninit.assume_init_ref() };
        conn.build_query_internal(&spec)
    }

    #[test]
    fn presence_only_filters_and_keys() {
        let mut spec = PersistenceQuerySpecification::default();
        spec.presence_with = vec!["Health"];
        spec.return_full_docs = false;
        let (aql, binds) = build(spec);

        assert!(aql.contains("FOR doc IN entities"));
        assert!(aql.contains("FILTER (doc.`Health` != null)"));
        assert!(aql.contains("RETURN doc._key"));
        assert!(binds.is_empty());
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
        // ensure both presence and value predicate appear
        assert!(aql.contains("(doc.`Position` != null)"));
        assert!(aql.contains("AND"));
        assert!(aql.contains("<"));
        // exactly one bind var
        assert_eq!(binds.len(), 1);
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
        // two binds: "a" and "b"
        assert_eq!(binds.len(), 2);
    }

    #[test]
    fn return_full_docs_merges_doc_and_key() {
        let mut spec = PersistenceQuerySpecification::default();
        spec.return_full_docs = true;
        // no presence/value filters -> FILTER true
        let (aql, binds) = build(spec);

        assert!(aql.contains("FILTER true"));
        // check MERGE(doc, { "_key": doc.`_key` })
        assert!(aql.contains("RETURN MERGE(doc,"));
        assert!(aql.contains("\"_key\": doc.`_key`"));
        assert!(binds.is_empty());
    }
}