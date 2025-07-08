//! Real ArangoDB backend: implements `DatabaseConnection` using `arangors`.
//! Inject this into `ArangoSession` in production to persist components.

use arangors::{
    Connection, Database,
    client::reqwest::ReqwestClient,
    AqlQuery,
};
use arangors::document::Document;
use futures::future::BoxFuture;
use futures::FutureExt;
use serde_json::Value;
use std::collections::HashMap;
use crate::{DatabaseConnection, ArangoError};

/// A real ArangoDB backend for `DatabaseConnection`.
pub struct ArangoDbConnection {
    db: Database<ReqwestClient>,
}

impl ArangoDbConnection {
    /// Connects to ArangoDB via JWT and selects the specified database.
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
                .collection("entities")
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
                .collection("entities")
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
                .collection("entities")
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
                .collection("entities")
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
        let res_name = resource_name.to_string();
        async move {
            let col = db
                .collection("entities")
                .await
                .map_err(|e| ArangoError(e.to_string()))?;
            let doc: Document<Value> = col
                .document("resources")
                .await
                .map_err(|e| ArangoError(e.to_string()))?;
            Ok(doc
                .document
                .get(&res_name)
                .cloned())
        }
        .boxed()
    }

    fn clear_entities(&self) -> BoxFuture<'static, Result<(), ArangoError>> {
        let db = self.db.clone();
        async move {
            let col = db
                .collection("entities")
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
