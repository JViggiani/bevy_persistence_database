//! Real ArangoDB backend: implements `DatabaseConnection` using `arangors`.
//! Inject this into `PersistenceSession` in production to persist components.

use arangors::{
    client::reqwest::ReqwestClient,
    document::options::UpdateOptions,
    transaction::{TransactionCollections, TransactionSettings},
    AqlQuery, ClientError, Connection, Database,
};
use crate::db::DatabaseConnection;
use crate::db::connection::{
    Collection, DocumentId, DocumentKey, DocumentRev, PersistenceError, TransactionOperation,
    TransactionResult,
};
use futures::future::BoxFuture;
use futures::FutureExt;
use serde_json::Value;
use std::collections::HashMap;
use std::fmt;

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
            .map_err(|e| PersistenceError::Generic(e.to_string()))?;
        let db: Database<ReqwestClient> = conn
            .db(db_name)
            .await
            .map_err(|e| PersistenceError::Generic(e.to_string()))?;

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
                            return Err(PersistenceError::Generic(arango_error.to_string()));
                        }
                    } else {
                        return Err(PersistenceError::Generic(e.to_string()));
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
    ) -> BoxFuture<'static, Result<Vec<DocumentKey>, PersistenceError>> {
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
                .map_err(|e| PersistenceError::Generic(e.to_string()))?;
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
    ) -> BoxFuture<'static, Result<Option<(Value, DocumentRev)>, PersistenceError>> {
        let db = self.db.clone();
        let key = entity_key.to_string();
        async move {
            let col = db
                .collection(&Collection::Entities.to_string())
                .await
                .map_err(|e| PersistenceError::Generic(e.to_string()))?;
            match col.document::<Value>(&key).await {
                Ok(doc) => {
                    let rev = doc.header._rev.clone();
                    Ok(Some((doc.document, rev)))
                }
                Err(e) => {
                    if let ClientError::Arango(api_err) = &e {
                        if api_err.error_num() == 1202 {
                            // entity not found
                            return Ok(None);
                        }
                    }
                    Err(PersistenceError::Generic(e.to_string()))
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
                .map_err(|e| PersistenceError::Generic(e.to_string()))?;
            match col.document::<Value>(&key).await {
                Ok(doc) => Ok(doc.document.get(&comp).cloned()),
                Err(e) => {
                    if let ClientError::Arango(api_err) = &e {
                        if api_err.error_num() == 1202 {
                            // entity not found
                            return Ok(None);
                        }
                    }
                    Err(PersistenceError::Generic(e.to_string()))
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
                .map_err(|e| PersistenceError::Generic(e.to_string()))?;
            // A document may not exist, so we handle the error.
            match col.document::<Value>(&res_key).await {
                Ok(doc) => Ok(Some(doc.document)),
                Err(e) => {
                    if let ClientError::Arango(ref api_err) = e {
                        if api_err.error_num() == 1202 { // 1202 is "document not found"
                            return Ok(None);
                        }
                    }
                    Err(PersistenceError::Generic(e.to_string()))
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
                .map_err(|e| PersistenceError::Generic(e.to_string()))?;
            col.truncate()
                .await
                .map_err(|e| PersistenceError::Generic(e.to_string()))?;
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
                .map_err(|e| PersistenceError::Generic(e.to_string()))?;
            col.truncate()
                .await
                .map_err(|e| PersistenceError::Generic(e.to_string()))?;
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
                .map_err(|e| PersistenceError::Generic(e.to_string()))?;
            let mut created_ids = Vec::new();
            let mut updated_ids = Vec::new();

            for op in operations {
                match op {
                    TransactionOperation::CreateDocument(doc) => {
                        let col = trx
                            .collection(&ent)
                            .await
                            .map_err(|e| PersistenceError::Generic(e.to_string()))?;
                        let meta = col
                            .create_document(doc, Default::default())
                            .await
                            .map_err(|e| PersistenceError::Generic(e.to_string()))?;
                        let header = meta
                            .header()
                            .ok_or_else(|| PersistenceError::Generic("Missing header".into()))?;
                        created_ids.push(DocumentId {
                            key: header._key.clone(),
                            rev: header._rev.clone(),
                        });
                    }
                    TransactionOperation::UpdateDocument { key, rev, patch } => {
                        let col = trx
                            .collection(&ent)
                            .await
                            .map_err(|e| PersistenceError::Generic(e.to_string()))?;
                        
                        // Convert the patch HashMap into a serde_json::Value object.
                        let mut patch_obj: serde_json::Map<String, Value> = patch.into_iter().collect();
                        // The _rev for optimistic locking must be part of the patch object.
                        patch_obj.insert("_rev".to_string(), Value::String(rev));
                        let patch_val = Value::Object(patch_obj);

                        // Full trace of the patch being sent
                        tracing::trace!("Executing UpdateDocument for key '{}' with patch: {}", key, serde_json::to_string(&patch_val).unwrap_or_else(|_| "patch serialization failed".to_string()));
                        
                        // Use `update_document`. The `_rev` for optimistic locking is inside the `patch`.
                        // We must set `ignore_revs(false)` to enable optimistic locking.
                        let options = UpdateOptions::builder()
                            .keep_null(false)
                            .ignore_revs(false)
                            .build();

                        match col.update_document::<Value>(&key, patch_val, options).await {
                            Ok(meta) => {
                                let header = meta.header().ok_or_else(|| {
                                    PersistenceError::Generic("Missing header on update".into())
                                })?;
                                updated_ids.push(DocumentId {
                                    key: header._key.clone(),
                                    rev: header._rev.clone(),
                                });
                            }
                            Err(e) => {
                                if let ClientError::Arango(api_err) = &e {
                                    if api_err.error_num() == 1200 {
                                        // 1200 is "precondition failed"
                                        return Err(PersistenceError::Conflict { key });
                                    }
                                }
                                return Err(PersistenceError::Generic(e.to_string()));
                            }
                        }
                    }
                    TransactionOperation::DeleteDocument { key, rev } => {
                        let col = trx
                            .collection(&ent)
                            .await
                            .map_err(|e| PersistenceError::Generic(e.to_string()))?;
                        
                        // Use `if_match` for optimistic locking on delete.
                        let if_match = Some(rev);
                        col.remove_document::<Value>(&key, Default::default(), if_match)
                            .await
                            .map_err(|e| PersistenceError::Generic(e.to_string()))?;
                    }
                    TransactionOperation::DeleteResource(key) => {
                        let col = trx
                            .collection(&res)
                            .await
                            .map_err(|e| PersistenceError::Generic(e.to_string()))?;
                        col.remove_document::<Value>(&key, Default::default(), None)
                            .await
                            .map_err(|e| PersistenceError::Generic(e.to_string()))?;
                    }
                    TransactionOperation::UpsertResource(key, data) => {
                        // An AQL UPSERT is the robust way to handle create-or-replace logic.
                        let aql = format!(
                            "UPSERT {{ _key: @key }} INSERT @doc REPLACE @doc IN {}",
                            res
                        );

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
                            .map_err(|e| PersistenceError::Generic(e.to_string()))?;
                    }
                }
            }

            trx.commit()
                .await
                .map_err(|e| PersistenceError::Generic(e.to_string()))?;
            Ok(TransactionResult {
                created: created_ids,
                updated: updated_ids,
            })
        }
        .boxed()
    }
}
