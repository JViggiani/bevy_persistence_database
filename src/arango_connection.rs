//! Real ArangoDB backend: implements `DatabaseConnection` using `arangors`.
//! Inject this into `ArangoSession` in production to persist components.

use arangors::{
    client::reqwest::ReqwestClient,
    document::options::InsertOptions,
    AqlQuery, ClientError, Connection, Database,
};
use arangors::document::Document;
use futures::future::BoxFuture;
use futures::FutureExt;
use serde_json::Value;
use std::collections::HashMap;
use crate::{DatabaseConnection, ArangoError, Collection};

/// A real ArangoDB backend for `DatabaseConnection`.
pub struct ArangoDbConnection {
    db: Database<ReqwestClient>,
}

impl ArangoDbConnection {
    /// Connects to ArangoDB via JWT, selects the specified database, and
    /// ensures the required collections exist.
    pub async fn connect(
        url: &str,
        user: &str,
        pass: &str,
        db_name: &str,
    ) -> Result<Self, ArangoError> {
        let conn = Connection::establish_jwt(url, user, pass)
            .await
            .map_err(|e| ArangoError(e.to_string()))?;
        let db: Database<ReqwestClient> = conn
            .db(db_name)
            .await
            .map_err(|e| ArangoError(e.to_string()))?;

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
                            return Err(ArangoError(arango_error.to_string()));
                        }
                    } else {
                        return Err(ArangoError(e.to_string()));
                    }
                }
            }
        }

        Ok(Self { db })
    }
}

impl DatabaseConnection for ArangoDbConnection {
    fn create_document(
        &self,
        data: Value,
    ) -> BoxFuture<'static, Result<String, ArangoError>> {
        let db = self.db.clone();
        async move {
            let col = db
                .collection(&Collection::Entities.to_string())
                .await
                .map_err(|e| ArangoError(e.to_string()))?;
            let doc_meta = col.create_document(data, Default::default())
                .await
                .map_err(|e| ArangoError(e.to_string()))?;
            Ok(doc_meta.header().unwrap()._key.clone())
        }
        .boxed()
    }

    fn update_document(
        &self,
        entity_key: &str,
        patch: Value,
    ) -> BoxFuture<'static, Result<(), ArangoError>> {
        let db = self.db.clone();
        let key_owned = entity_key.to_string();
        async move {
            let col = db
                .collection(&Collection::Entities.to_string())
                .await
                .map_err(|e| ArangoError(e.to_string()))?;
            col.update_document(&key_owned, patch, Default::default())
                .await
                .map_err(|e| ArangoError(e.to_string()))?;
            Ok(())
        }
        .boxed()
    }

    fn delete_document(
        &self,
        entity_key: &str,
    ) -> BoxFuture<'static, Result<(), ArangoError>> {
        let db = self.db.clone();
        let key_owned = entity_key.to_string();
        async move {
            let col = db
                .collection(&Collection::Entities.to_string())
                .await
                .map_err(|e| ArangoError(e.to_string()))?;
            // remove_document<T>: specify Value as the document type
            col.remove_document::<Value>(
                &key_owned,
                Default::default(),
                None, // no specific revision
            )
            .await
            .map_err(|e| ArangoError(e.to_string()))?;
            Ok(())
        }
        .boxed()
    }

    fn query_arango(
        &self,
        aql: String,
        bind_vars: HashMap<String, Value>,
    ) -> BoxFuture<'static, Result<Vec<String>, ArangoError>> {
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
                .map_err(|e| ArangoError(e.to_string()))?;
            // extract keys as strings
            let keys = docs
                .into_iter()
                .filter_map(|v| v.as_str().map(ToOwned::to_owned))
                .collect();
            Ok(keys)
        }
        .boxed()
    }

    fn fetch_component(
        &self,
        entity_key: &str,
        comp_name: &str,
    ) -> BoxFuture<'static, Result<Option<Value>, ArangoError>> {
        let db = self.db.clone();
        let key = entity_key.to_string();
        let comp = comp_name.to_string();
        async move {
            let col = db
                .collection(&Collection::Entities.to_string())
                .await
                .map_err(|e| ArangoError(e.to_string()))?;
            let doc: Document<Value> = col
                .document(&key)
                .await
                .map_err(|e| ArangoError(e.to_string()))?;
            // pick out the field if present
            Ok(doc
                .document
                .get(&comp)
                .cloned())
        }
        .boxed()
    }

    fn fetch_resource(
        &self,
        resource_name: &str,
    ) -> BoxFuture<'static, Result<Option<Value>, ArangoError>> {
        let db = self.db.clone();
        let res_key = resource_name.to_string();
        async move {
            let col = db
                .collection(&Collection::Resources.to_string())
                .await
                .map_err(|e| ArangoError(e.to_string()))?;
            // A document may not exist, so we handle the error.
            match col.document::<Value>(&res_key).await {
                Ok(doc) => Ok(Some(doc.document)),
                Err(e) => {
                    if let ClientError::Arango(ref api_err) = e {
                        if api_err.error_num() == 1202 { // 1202 is "document not found"
                            return Ok(None);
                        }
                    }
                    Err(ArangoError(e.to_string()))
                }
            }
        }
        .boxed()
    }

    fn upsert_resource(
        &self,
        resource_name: &str,
        mut data: Value,
    ) -> BoxFuture<'static, Result<(), ArangoError>> {
        let db = self.db.clone();
        let key = resource_name.to_string();
        async move {
            let col = db
                .collection(&Collection::Resources.to_string())
                .await
                .map_err(|e| ArangoError(e.to_string()))?;

            if let Some(obj) = data.as_object_mut() {
                obj.insert("_key".to_string(), Value::String(key));
            } else {
                return Err(ArangoError("Resource data is not a JSON object".into()));
            }

            let options = InsertOptions::builder().overwrite(true).build();
            col.create_document(data, options)
                .await
                .map_err(|e| ArangoError(e.to_string()))?;
            Ok(())
        }
        .boxed()
    }

    fn clear_entities(&self) -> BoxFuture<'static, Result<(), ArangoError>> {
        let db = self.db.clone();
        async move {
            let col = db
                .collection(&Collection::Entities.to_string())
                .await
                .map_err(|e| ArangoError(e.to_string()))?;
            col.truncate()
                .await
                .map_err(|e| ArangoError(e.to_string()))?;
            Ok(())
        }
        .boxed()
    }

    fn clear_resources(&self) -> BoxFuture<'static, Result<(), ArangoError>> {
        let db = self.db.clone();
        async move {
            let col = db
                .collection(&Collection::Resources.to_string())
                .await
                .map_err(|e| ArangoError(e.to_string()))?;
            col.truncate()
                .await
                .map_err(|e| ArangoError(e.to_string()))?;
            Ok(())
        }
        .boxed()
    }
}
