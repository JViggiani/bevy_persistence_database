//! Real ArangoDB backend: implements `DatabaseConnection` using `arangors`.
//! Inject this into `PersistenceSession` in production to persist components.

use arangors::{
    client::reqwest::ReqwestClient,
    transaction::{TransactionCollections, TransactionSettings},
    AqlQuery, ClientError, Connection, Database,
};
use crate::db::DatabaseConnection;
use crate::db::connection::{PersistenceError, TransactionOperation, Collection, TransactionResult};
use futures::future::BoxFuture;
use futures::FutureExt;
use serde_json::Value;
use std::collections::HashMap;
use std::fmt;
use tracing::trace;

/// A real ArangoDB backend for `DatabaseConnection`.
pub struct ArangoDbConnection {
    pub db: Database<ReqwestClient>,
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
            .map_err(|e| PersistenceError::Other(e.to_string()))?;
        let db: Database<ReqwestClient> = conn
            .db(db_name)
            .await
            .map_err(|e| PersistenceError::Other(e.to_string()))?;

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
                            return Err(PersistenceError::Other(arango_error.to_string()));
                        }
                    } else {
                        return Err(PersistenceError::Other(e.to_string()));
                    }
                }
            }
        }

        Ok(Self { db })
    }
}

impl DatabaseConnection for ArangoDbConnection {
    fn query(
        &self,
        aql: String,
        bind_vars: HashMap<String, Value>,
    ) -> BoxFuture<'static, Result<Vec<String>, PersistenceError>> {
        let db = self.db.clone();
        async move {
            // convert String->Value into &'static str->Value
            let bind_refs: HashMap<&'static str, Value> = bind_vars
                .into_iter()
                .map(|(k, v)| {
                    let s: &'static str = Box::leak(k.into_boxed_str());
                    (s, v)
                })
                .collect();

            let query = AqlQuery::builder()
                .query(&aql)
                .bind_vars(bind_refs)
                .build();
            let docs: Vec<Value> = db
                .aql_query(query)
                .await
                .map_err(|e| PersistenceError::Other(e.to_string()))?;
            // extract keys as strings
            let keys = docs
                .into_iter()
                .filter_map(|v| v.as_str().map(ToOwned::to_owned))
                .collect();
            Ok(keys)
        }
        .boxed()
    }

    fn fetch_document(
        &self,
        entity_key: &str,
    ) -> BoxFuture<'static, Result<Option<(Value, String, String)>, PersistenceError>> {
        let db = self.db.clone();
        let key = entity_key.to_string();
        async move {
            let col = db
                .collection(&Collection::Entities.to_string())
                .await
                .map_err(|e| PersistenceError::Other(e.to_string()))?;
            match col.document::<Value>(&key).await {
                Ok(doc) => {
                    let rev = doc.header._rev.clone();
                    let id = doc.header._id.clone();
                    Ok(Some((doc.document, rev, id)))
                }
                Err(e) => {
                    if let ClientError::Arango(api_err) = &e {
                        if api_err.error_num() == 1202 {
                            // entity not found
                            return Ok(None);
                        }
                    }
                    Err(PersistenceError::Other(e.to_string()))
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
                .map_err(|e| PersistenceError::Other(e.to_string()))?;
            match col.document::<Value>(&key).await {
                Ok(doc) => Ok(doc.document.get(&comp).cloned()),
                Err(e) => {
                    if let ClientError::Arango(api_err) = &e {
                        if api_err.error_num() == 1202 {
                            // entity not found
                            return Ok(None);
                        }
                    }
                    Err(PersistenceError::Other(e.to_string()))
                }
            }
        }
        .boxed()
    }

    fn fetch_resource(
        &self,
        resource_name: &str,
    ) -> BoxFuture<'static, Result<Option<Value>, PersistenceError>> {
        let db = self.db.clone();
        let res_key = resource_name.to_string();
        async move {
            let col = db
                .collection(&Collection::Resources.to_string())
                .await
                .map_err(|e| PersistenceError::Other(e.to_string()))?;
            // A document may not exist, so we handle the error.
            match col.document::<Value>(&res_key).await {
                Ok(doc) => Ok(Some(doc.document)),
                Err(e) => {
                    if let ClientError::Arango(ref api_err) = e {
                        if api_err.error_num() == 1202 { // 1202 is "document not found"
                            return Ok(None);
                        }
                    }
                    Err(PersistenceError::Other(e.to_string()))
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
                .map_err(|e| PersistenceError::Other(e.to_string()))?;
            col.truncate()
                .await
                .map_err(|e| PersistenceError::Other(e.to_string()))?;
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
                .map_err(|e| PersistenceError::Other(e.to_string()))?;
            col.truncate()
                .await
                .map_err(|e| PersistenceError::Other(e.to_string()))?;
            Ok(())
        }
        .boxed()
    }

    fn execute_transaction(
        &self,
        operations: Vec<TransactionOperation>,
    ) -> BoxFuture<'static, Result<TransactionResult, PersistenceError>> {
        let db = self.db.clone();
        async move {
            let ent = Collection::Entities.to_string();
            let res = Collection::Resources.to_string();
            let collections = TransactionCollections::builder()
                .write(vec![ent.clone(), res.clone()])
                .build();
            let settings = TransactionSettings::builder()
                .collections(collections)
                .build();

            let trx = db
                .begin_transaction(settings)
                .await
                .map_err(|e| PersistenceError::Other(e.to_string()))?;
            let mut result = TransactionResult::default();

            for op in operations {
                match op {
                    TransactionOperation::CreateDocument(doc) => {
                        let col = trx.collection(&ent).await.map_err(|e| PersistenceError::Other(e.to_string()))?;
                        let meta = col.create_document(doc, Default::default()).await.map_err(|e| PersistenceError::Other(e.to_string()))?;
                        let header = meta.header().ok_or_else(|| PersistenceError::Other("Missing header".into()))?;
                        result.created.push((header._key.clone(), header._rev.clone(), header._id.clone()));
                    }
                    TransactionOperation::UpdateDocument { key, rev, patch } => {
                        // Use `UPDATE` for robust conditional updates. This allows for merging.
                        let aql = format!(
                            "UPDATE {{ _key: @key, _rev: @rev }} WITH @patch IN {} OPTIONS {{ ignoreRevs: false, mergeObjects: true }} RETURN {{ _rev: NEW._rev, _id: NEW._id }}",
                            ent
                        );
                        trace!(aql, "Executing conditional update AQL");

                        let mut bind_vars_map = HashMap::new();
                        bind_vars_map.insert("key".to_string(), Value::String(key.clone()));
                        bind_vars_map.insert("rev".to_string(), Value::String(rev));
                        bind_vars_map.insert("patch".to_string(), patch);

                        let bind_refs: HashMap<&'static str, Value> = bind_vars_map
                            .into_iter()
                            .map(|(k, v)| (Box::leak(k.into_boxed_str()) as &str, v))
                            .collect();

                        let query = AqlQuery::builder()
                            .query(&aql)
                            .bind_vars(bind_refs)
                            .build();

                        let query_res: Result<Vec<Value>, ClientError> = trx.aql_query(query).await;

                        match query_res {
                            Ok(new_meta) => {
                                if let Some(meta_val) = new_meta.into_iter().next() {
                                    if let (Some(new_rev), Some(new_id)) = (meta_val.get("_rev").and_then(Value::as_str), meta_val.get("_id").and_then(Value::as_str)) {
                                        result.updated.push((key, new_rev.to_string(), new_id.to_string()));
                                    } else {
                                        return Err(PersistenceError::Other(format!("Update for key {} succeeded but did not return a new revision and id", key)));
                                    }
                                } else {
                                    return Err(PersistenceError::Other(format!("Update for key {} succeeded but did not return a new revision", key)));
                                }
                            }
                            Err(e) => {
                                if let ClientError::Arango(arango_error) = &e {
                                    if arango_error.error_num() == 1200 { // Precondition failed
                                        return Err(PersistenceError::Conflict { key });
                                    }
                                }
                                return Err(PersistenceError::Other(e.to_string()));
                            }
                        }
                    }
                    TransactionOperation::DeleteDocument(key) => {
                        let col = trx.collection(&ent).await.map_err(|e| PersistenceError::Other(e.to_string()))?;
                        col.remove_document::<Value>(&key, Default::default(), None).await.map_err(|e| PersistenceError::Other(e.to_string()))?;
                    }
                    TransactionOperation::UpsertResource(key, data) => {
                        // An AQL UPSERT is the robust way to handle create-or-replace logic.
                        let aql = format!("UPSERT {{ _key: @key }} INSERT @doc REPLACE @doc IN {}", res);

                        // The document for INSERT must contain the _key.
                        let mut doc_with_key = data;
                        if let Some(obj) = doc_with_key.as_object_mut() {
                            obj.insert("_key".to_string(), Value::String(key.clone()));
                        }

                        let mut bind_vars_map = HashMap::new();
                        bind_vars_map.insert("key".to_string(), Value::String(key));
                        bind_vars_map.insert("doc".to_string(), doc_with_key);

                        let bind_refs: HashMap<&'static str, Value> = bind_vars_map
                            .into_iter()
                            .map(|(k, v)| {
                                let s: &'static str = Box::leak(k.into_boxed_str());
                                (s, v)
                            })
                            .collect();

                        let query = AqlQuery::builder()
                            .query(&aql)
                            .bind_vars(bind_refs)
                            .build();

                        trx.aql_query::<Value>(query)
                            .await
                            .map_err(|e| PersistenceError::Other(e.to_string()))?;
                    }
                }
            }

            trx.commit().await.map_err(|e| PersistenceError::Other(e.to_string()))?;
            Ok(result)
        }
        .boxed()
    }
}
