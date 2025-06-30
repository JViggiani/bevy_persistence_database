use arangors::{Connection, Database};
use futures::future::BoxFuture;
use futures::FutureExt;
use serde_json::Value;
use crate::{DatabaseConnection, ArangoError};

/// A real ArangoDB backend for `DatabaseConnection`.
pub struct ArangoDbConnection {
    db: Database,
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
        let db = conn
            .db(db_name)
            .await
            .map_err(|e| ArangoError(e.to_string()))?;
        Ok(Self { db })
    }
}

impl DatabaseConnection for ArangoDbConnection {
    fn create_document(
        &self,
        entity_key: &str,
        data: Value,
    ) -> BoxFuture<'static, Result<(), ArangoError>> {
        // use a fixed collection name "entities"
        let col = self.db.collection("entities");
        async move {
            col.create_document(entity_key, &data, None)
                .await
                .map_err(|e| ArangoError(e.to_string()))?;
            Ok(())
        }
        .boxed()
    }

    fn update_document(
        &self,
        entity_key: &str,
        patch: Value,
    ) -> BoxFuture<'static, Result<(), ArangoError>> {
        let col = self.db.collection("entities");
        async move {
            col.update_document(entity_key, &patch, None)
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
        let col = self.db.collection("entities");
        async move {
            col.delete_document(entity_key, None)
                .await
                .map_err(|e| ArangoError(e.to_string()))?;
            Ok(())
        }
        .boxed()
    }
}
