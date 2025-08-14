//! Postgres backend: implements `DatabaseConnection` using `tokio-postgres`.
//! Storage model:
//!   entities(id text pk, bevy_persistence_version bigint not null, doc jsonb not null)
//!   resources(id text pk, bevy_persistence_version bigint not null, doc jsonb not null)

use crate::db::connection::{
    BEVY_PERSISTENCE_VERSION_FIELD, Collection, PersistenceError, TransactionOperation,
};
use crate::db::DatabaseConnection;
use crate::query::filter_expression::{BinaryOperator, FilterExpression};
use crate::query::persistence_query_specification::PersistenceQuerySpecification;
use futures::future::BoxFuture;
use futures::FutureExt;
use once_cell::sync::Lazy;
use serde_json::Value;
use std::fmt;
use std::sync::Arc;

use tokio_postgres::{types::ToSql, Client, NoTls};

// Dedicated runtime for sync operations and driving connections.
static PG_RT: Lazy<tokio::runtime::Runtime> = Lazy::new(|| {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("failed to build postgres tokio runtime")
});

/// Postgres implementation of DatabaseConnection
pub struct PostgresDbConnection {
    // Wrap client to share into async closures
    client: Arc<Client>,
}

impl fmt::Debug for PostgresDbConnection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PostgresDbConnection").finish_non_exhaustive()
    }
}

impl PostgresDbConnection {
    /// Ensure a database exists by name using a server connection (dbname=postgres).
    pub async fn ensure_database(
        host: &str,
        user: &str,
        pass: &str,
        db_name: &str,
        port: Option<u16>,
    ) -> Result<(), PersistenceError> {
        let mut conn_str = format!("host={} user={} password={} dbname=postgres", host, user, pass);
        if let Some(port) = port {
            conn_str.push_str(&format!(" port={}", port));
        }
        let (client, connection) = tokio_postgres::connect(&conn_str, NoTls)
            .await
            .map_err(|e| PersistenceError::new(format!("pg connect (server) failed: {}", e)))?;
        PG_RT.spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("postgres connection error: {}", e);
            }
        });

        // Check existence
        let exists = client
            .query_one(
                "SELECT 1 FROM pg_database WHERE datname = $1",
                &[&db_name],
            )
            .await
            .is_ok();

        if !exists {
            // CREATE DATABASE cannot run inside a transaction
            client
                .batch_execute(&format!(r#"CREATE DATABASE "{}""#, db_name.replace('"', "\"\"")))
                .await
                .map_err(|e| PersistenceError::new(format!("create database failed: {}", e)))?;
        }

        Ok(())
    }

    /// Connect to Postgres and ensure required tables/indexes in the target database.
    pub async fn connect(
        host: &str,
        user: &str,
        pass: &str,
        db_name: &str,
        port: Option<u16>,
    ) -> Result<Self, PersistenceError> {
        let mut conn_str = format!(
            "host={} user={} password={} dbname={}",
            host, user, pass, db_name
        );
        if let Some(port) = port {
            conn_str.push_str(&format!(" port={}", port));
        }
        let (client, connection) = tokio_postgres::connect(&conn_str, NoTls)
            .await
            .map_err(|e| PersistenceError::new(format!("pg connect failed: {}", e)))?;
        PG_RT.spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("postgres connection error: {}", e);
            }
        });

        // Ensure tables
        client
            .batch_execute(
                r#"
                CREATE TABLE IF NOT EXISTS entities(
                    id TEXT PRIMARY KEY,
                    bevy_persistence_version BIGINT NOT NULL,
                    doc JSONB NOT NULL
                );
                CREATE TABLE IF NOT EXISTS resources(
                    id TEXT PRIMARY KEY,
                    bevy_persistence_version BIGINT NOT NULL,
                    doc JSONB NOT NULL
                );
                "#,
            )
            .await
            .map_err(|e| PersistenceError::new(format!("schema init failed: {}", e)))?;

        // Optional JSONB GIN indexes
        let _ = client
            .batch_execute(
                r#"
                DO $$
                BEGIN
                    IF NOT EXISTS (
                        SELECT 1 FROM pg_indexes WHERE schemaname = 'public' AND indexname = 'entities_doc_gin'
                    ) THEN
                        CREATE INDEX entities_doc_gin ON entities USING GIN (doc);
                    END IF;
                    IF NOT EXISTS (
                        SELECT 1 FROM pg_indexes WHERE schemaname = 'public' AND indexname = 'resources_doc_gin'
                    ) THEN
                        CREATE INDEX resources_doc_gin ON resources USING GIN (doc);
                    END IF;
                END $$;
                "#,
            )
            .await;

        Ok(Self { client: Arc::new(client) })
    }

    fn table_name(collection: Collection) -> &'static str {
        match collection {
            Collection::Entities => "entities",
            Collection::Resources => "resources",
        }
    }

    // Build WHERE clause and params for a spec, using a starting param index (1-based).
    fn build_where_clause_offset(
        &self,
        spec: &PersistenceQuerySpecification,
        start_index: usize,
    ) -> (String, Vec<SqlParam>) {
        let mut parts: Vec<String> = Vec::new();
        let mut params: Vec<SqlParam> = Vec::new();

        let mut next_idx = start_index;

        // presence filters
        for comp in &spec.presence_with {
            parts.push(format!("doc ? ${}", next_idx));
            params.push(SqlParam::Text((*comp).to_string()));
            next_idx += 1;
        }
        for comp in &spec.presence_without {
            parts.push(format!("NOT (doc ? ${})", next_idx));
            params.push(SqlParam::Text((*comp).to_string()));
            next_idx += 1;
        }

        // value filters
        if let Some(expr) = &spec.value_filters {
            let expr_sql = Self::translate_expr_with_offset(expr, &mut params, &mut next_idx);
            if !expr_sql.is_empty() {
                parts.push(expr_sql);
            }
        }

        let where_sql = if parts.is_empty() {
            "TRUE".to_string()
        } else {
            parts.join(" AND ")
        };
        (where_sql, params)
    }

    // Same translate but aware of current param index
    fn translate_expr_with_offset(
        expr: &FilterExpression,
        params: &mut Vec<SqlParam>,
        next_idx: &mut usize,
    ) -> String {
        match expr {
            FilterExpression::Literal(v) => {
                let idx = *next_idx;
                *next_idx += 1;
                match v {
                    Value::String(s) => params.push(SqlParam::Text(s.clone())),
                    Value::Number(n) if n.is_i64() => params.push(SqlParam::I64(n.as_i64().unwrap())),
                    Value::Number(n) if n.is_u64() => params.push(SqlParam::I64(n.as_u64().unwrap() as i64)),
                    Value::Number(n) => params.push(SqlParam::F64(n.as_f64().unwrap())),
                    Value::Bool(b) => params.push(SqlParam::Bool(*b)),
                    Value::Null => params.push(SqlParam::Json(Value::Null)),
                    other => params.push(SqlParam::Json(other.clone())),
                }
                format!("${}", idx)
            }
            FilterExpression::Field { component_name, field_name } => {
                if field_name.is_empty() {
                    format!("doc -> '{}'", component_name)
                } else {
                    format!("doc -> '{}' ->> '{}'", component_name, field_name)
                }
            }
            FilterExpression::DocumentKey => "id".to_string(),
            FilterExpression::BinaryOperator { op, lhs, rhs } => {
                let l = Self::translate_expr_with_offset(lhs, params, next_idx);
                let r = Self::translate_expr_with_offset(rhs, params, next_idx);
                match op {
                    BinaryOperator::Eq => format!("({} = {})", Self::maybe_cast(&l, rhs), Self::rhs_cast(r, rhs)),
                    BinaryOperator::Ne => format!("({} <> {})", Self::maybe_cast(&l, rhs), Self::rhs_cast(r, rhs)),
                    BinaryOperator::Gt => format!("({} > {})", Self::cast_numeric(&l, rhs), Self::rhs_cast(r, rhs)),
                    BinaryOperator::Gte => format!("({} >= {})", Self::cast_numeric(&l, rhs), Self::rhs_cast(r, rhs)),
                    BinaryOperator::Lt => format!("({} < {})", Self::cast_numeric(&l, rhs), Self::rhs_cast(r, rhs)),
                    BinaryOperator::Lte => format!("({} <= {})", Self::cast_numeric(&l, rhs), Self::rhs_cast(r, rhs)),
                    BinaryOperator::And => format!("({} AND {})", l, r),
                    BinaryOperator::Or => format!("({} OR {})", l, r),
                    BinaryOperator::In => format!("({} = ANY({}))", Self::maybe_cast_array_lhs(&l, rhs), r),
                }
            }
        }
    }

    // If lhs uses doc->>'field', cast it by rhs type
    fn maybe_cast(lhs: &str, rhs: &FilterExpression) -> String {
        match rhs {
            FilterExpression::Literal(Value::Number(n)) if n.is_i64() || n.is_u64() => {
                format!("({})::bigint", lhs)
            }
            FilterExpression::Literal(Value::Number(_)) => format!("({})::double precision", lhs),
            FilterExpression::Literal(Value::Bool(_)) => format!("({})::boolean", lhs),
            _ => lhs.to_string(),
        }
    }
    fn cast_numeric(lhs: &str, rhs: &FilterExpression) -> String {
        match rhs {
            FilterExpression::Literal(Value::Number(n)) if n.is_i64() || n.is_u64() => {
                format!("({})::bigint", lhs)
            }
            _ => format!("({})::double precision", lhs),
        }
    }
    fn rhs_cast(r: String, rhs: &FilterExpression) -> String {
        match rhs {
            FilterExpression::Literal(Value::Number(n)) if n.is_i64() || n.is_u64() => r,
            FilterExpression::Literal(Value::Number(_)) => r,
            FilterExpression::Literal(Value::Bool(_)) => r,
            _ => r,
        }
    }
    fn maybe_cast_array_lhs(lhs: &str, rhs: &FilterExpression) -> String {
        match rhs {
            FilterExpression::Literal(Value::Array(arr)) => {
                // infer element type: prefer first element
                if let Some(first) = arr.get(0) {
                    match first {
                        Value::Number(n) if n.is_i64() || n.is_u64() => {
                            format!("({})::bigint", lhs)
                        }
                        Value::Number(_) => format!("({})::double precision", lhs),
                        Value::Bool(_) => format!("({})::boolean", lhs),
                        _ => lhs.to_string(),
                    }
                } else {
                    lhs.to_string()
                }
            }
            _ => lhs.to_string(),
        }
    }

    fn select_json_row_sql(&self) -> &'static str {
        // Uses positional params for both key-field name and version-field name at the end
        "SELECT (jsonb_build_object($1, id) || doc || jsonb_build_object($2, bevy_persistence_version)) AS json FROM entities WHERE "
    }

    // Convert SqlParam list to boxed ToSql values; keep vector alive while querying.
    fn build_param_boxes(params: &[SqlParam]) -> Vec<Box<dyn ToSql + Sync + Send>> {
        params
            .iter()
            .map(|p| match p {
                SqlParam::Text(s) => Box::new(s.clone()) as Box<dyn ToSql + Sync + Send>,
                SqlParam::I64(i) => Box::new(*i) as Box<dyn ToSql + Sync + Send>,
                SqlParam::F64(f) => Box::new(*f) as Box<dyn ToSql + Sync + Send>,
                SqlParam::Bool(b) => Box::new(*b) as Box<dyn ToSql + Sync + Send>,
                SqlParam::Json(v) => Box::new(v.clone()) as Box<dyn ToSql + Sync + Send>,
            })
            .collect()
    }
}

// Parameter holder (no ToSql impl; we box concrete values instead)
#[derive(Debug)]
enum SqlParam {
    Text(String),
    I64(i64),
    F64(f64),
    Bool(bool),
    Json(Value),
}

impl DatabaseConnection for PostgresDbConnection {
    fn document_key_field(&self) -> &'static str {
        "id"
    }

    fn execute_keys(
        &self,
        spec: &PersistenceQuerySpecification,
    ) -> BoxFuture<'static, Result<Vec<String>, PersistenceError>> {
        let client = self.client.clone();
        // WHERE params start at $1
        let (where_sql, params) = self.build_where_clause_offset(spec, 1);
        let sql = format!("SELECT id FROM entities WHERE {}", where_sql);

        async move {
            let boxed = PostgresDbConnection::build_param_boxes(&params);
            let refs: Vec<&(dyn ToSql + Sync)> = boxed.iter().map(|b| &**b as &(dyn ToSql + Sync)).collect();

            let rows = client
                .query(sql.as_str(), &refs)
                .await
                .map_err(|e| PersistenceError::new(format!("pg query keys failed: {}", e)))?;
            Ok(rows.into_iter().map(|r| r.get::<_, String>(0)).collect())
        }
        .boxed()
    }

    fn execute_documents(
        &self,
        spec: &PersistenceQuerySpecification,
    ) -> BoxFuture<'static, Result<Vec<Value>, PersistenceError>> {
        let client = self.client.clone();
        // Reserve $1 and $2 for key/version field names; WHERE params start at $3
        let (where_sql, where_params) = self.build_where_clause_offset(spec, 3);

        // Build full param list: [$1=key_field, $2=version_field, $3..=WHERE params...]
        let mut all_params = Vec::with_capacity(2 + where_params.len());
        all_params.push(SqlParam::Text(self.document_key_field().to_string()));
        all_params.push(SqlParam::Text(BEVY_PERSISTENCE_VERSION_FIELD.to_string()));
        all_params.extend(where_params);

        let sql = format!("{}{}", self.select_json_row_sql(), where_sql);

        async move {
            let boxed = PostgresDbConnection::build_param_boxes(&all_params);
            let refs: Vec<&(dyn ToSql + Sync)> = boxed.iter().map(|b| &**b as &(dyn ToSql + Sync)).collect();

            let rows = client
                .query(sql.as_str(), &refs)
                .await
                .map_err(|e| PersistenceError::new(format!("pg query docs failed: {}", e)))?;
            let mut out = Vec::with_capacity(rows.len());
            for row in rows {
                let v: Value = row.get(0);
                out.push(v);
            }
            Ok(out)
        }
        .boxed()
    }

    fn execute_documents_sync(
        &self,
        spec: &PersistenceQuerySpecification,
    ) -> Result<Vec<Value>, PersistenceError> {
        PG_RT.block_on(self.execute_documents(spec))
    }

    fn execute_transaction(
        &self,
        operations: Vec<TransactionOperation>,
    ) -> BoxFuture<'static, Result<Vec<String>, PersistenceError>> {
        use uuid::Uuid;

        let client = self.client.clone();
        async move {
            // Manual transaction
            if let Err(e) = client.batch_execute("BEGIN").await {
                return Err(PersistenceError::new(format!("begin tx failed: {}", e)));
            }

            let mut new_keys: Vec<String> = Vec::new();

            let res: Result<(), PersistenceError> = (|| async {
                for op in operations {
                    match op {
                        TransactionOperation::CreateDocument { collection, mut data } => {
                            let table = PostgresDbConnection::table_name(collection);
                            let key_field = "id";
                            let version = data
                                .get(BEVY_PERSISTENCE_VERSION_FIELD)
                                .and_then(|v| v.as_u64())
                                .unwrap_or(1) as i64;

                            let id = if matches!(collection, Collection::Entities) {
                                Uuid::new_v4().to_string()
                            } else {
                                data.get(key_field)
                                    .and_then(|v| v.as_str())
                                    .ok_or_else(|| PersistenceError::new("resource create missing id"))?
                                    .to_string()
                            };

                            if let Some(obj) = data.as_object_mut() {
                                obj.remove(key_field);
                            }

                            let p0: &(dyn ToSql + Sync) = &id;
                            let p1: &(dyn ToSql + Sync) = &version;
                            let p2: &(dyn ToSql + Sync) = &data;
                            let params: &[&(dyn ToSql + Sync)] = &[p0, p1, p2];

                            client
                                .query(
                                    &format!(
                                        "INSERT INTO {}(id, bevy_persistence_version, doc) VALUES ($1,$2,$3)",
                                        table
                                    ),
                                    params,
                                )
                                .await
                                .map_err(|e| PersistenceError::new(format!("insert failed: {}", e)))?;

                            if matches!(collection, Collection::Entities) {
                                new_keys.push(id);
                            }
                        }
                        TransactionOperation::UpdateDocument {
                            collection,
                            key,
                            expected_current_version,
                            patch,
                        } => {
                            let table = PostgresDbConnection::table_name(collection);
                            let next_version = (expected_current_version + 1) as i64;
                            let expected = expected_current_version as i64;

                            let sql = format!(
                                "UPDATE {} \
                                 SET doc = jsonb_strip_nulls(doc || $1), \
                                     bevy_persistence_version = $2 \
                                 WHERE id = $3 AND bevy_persistence_version = $4 \
                                 RETURNING 1",
                                table
                            );

                            let p0: &(dyn ToSql + Sync) = &patch;
                            let p1: &(dyn ToSql + Sync) = &next_version;
                            let p2: &(dyn ToSql + Sync) = &key;
                            let p3: &(dyn ToSql + Sync) = &expected;
                            let params: &[&(dyn ToSql + Sync)] = &[p0, p1, p2, p3];

                            let rows = client
                                .query(sql.as_str(), params)
                                .await
                                .map_err(|e| PersistenceError::new(format!("update failed: {}", e)))?;
                            if rows.is_empty() {
                                return Err(PersistenceError::Conflict { key });
                            }
                        }
                        TransactionOperation::DeleteDocument {
                            collection,
                            key,
                            expected_current_version,
                        } => {
                            let table = PostgresDbConnection::table_name(collection);
                            let expected = expected_current_version as i64;
                            let sql = format!(
                                "DELETE FROM {} WHERE id = $1 AND bevy_persistence_version = $2 RETURNING 1",
                                table
                            );

                            let p0: &(dyn ToSql + Sync) = &key;
                            let p1: &(dyn ToSql + Sync) = &expected;
                            let params: &[&(dyn ToSql + Sync)] = &[p0, p1];

                            let rows = client
                                .query(sql.as_str(), params)
                                .await
                                .map_err(|e| PersistenceError::new(format!("delete failed: {}", e)))?;
                            if rows.is_empty() {
                                return Err(PersistenceError::Conflict { key });
                            }
                        }
                    }
                }
                Ok(())
            })()
            .await;

            if let Err(e) = res {
                let _ = client.batch_execute("ROLLBACK").await;
                return Err(e);
            }

            if let Err(e) = client.batch_execute("COMMIT").await {
                return Err(PersistenceError::new(format!("commit failed: {}", e)));
            }
            Ok(new_keys)
        }
        .boxed()
    }

    fn fetch_document(
        &self,
        entity_key: &str,
    ) -> BoxFuture<'static, Result<Option<(Value, u64)>, PersistenceError>> {
        let client = self.client.clone();
        let id = entity_key.to_string();
        async move {
            let p0: &(dyn ToSql + Sync) = &id;
            let params: &[&(dyn ToSql + Sync)] = &[p0];

            let row = client
                .query_opt(
                    "SELECT doc, bevy_persistence_version FROM entities WHERE id = $1",
                    params,
                )
                .await
                .map_err(|e| PersistenceError::new(format!("fetch doc failed: {}", e)))?;

            if let Some(row) = row {
                let mut doc: Value = row.get(0);
                let version: i64 = row.get(1);
                if let Some(obj) = doc.as_object_mut() {
                    obj.insert("id".to_string(), Value::String(id));
                    obj.insert(
                        BEVY_PERSISTENCE_VERSION_FIELD.to_string(),
                        Value::Number((version as i64).into()),
                    );
                }
                Ok(Some((doc, version as u64)))
            } else {
                Ok(None)
            }
        }
        .boxed()
    }

    fn fetch_component(
        &self,
        entity_key: &str,
        comp_name: &str,
    ) -> BoxFuture<'static, Result<Option<Value>, PersistenceError>> {
        let client = self.client.clone();
        let id = entity_key.to_string();
        let comp = comp_name.to_string();
        async move {
            let p0: &(dyn ToSql + Sync) = &id;
            let p1: &(dyn ToSql + Sync) = &comp;
            let params: &[&(dyn ToSql + Sync)] = &[p0, p1];

            let row = client
                .query_opt(
                    "SELECT doc -> $2 FROM entities WHERE id = $1",
                    params,
                )
                .await
                .map_err(|e| PersistenceError::new(format!("fetch comp failed: {}", e)))?;
            Ok(row.and_then(|r| r.get::<_, Option<Value>>(0)))
        }
        .boxed()
    }

    fn fetch_resource(
        &self,
        resource_name: &str,
    ) -> BoxFuture<'static, Result<Option<(Value, u64)>, PersistenceError>> {
        let client = self.client.clone();
        let id = resource_name.to_string();
        async move {
            let p0: &(dyn ToSql + Sync) = &id;
            let params: &[&(dyn ToSql + Sync)] = &[p0];

            let row = client
                .query_opt(
                    "SELECT doc, bevy_persistence_version FROM resources WHERE id = $1",
                    params,
                )
                .await
                .map_err(|e| PersistenceError::new(format!("fetch resource failed: {}", e)))?;

            if let Some(row) = row {
                let mut doc: Value = row.get(0);
                let version: i64 = row.get(1);
                if let Some(obj) = doc.as_object_mut() {
                    obj.insert("id".to_string(), Value::String(id));
                    obj.insert(
                        BEVY_PERSISTENCE_VERSION_FIELD.to_string(),
                        Value::Number((version as i64).into()),
                    );
                }
                Ok(Some((doc, version as u64)))
            } else {
                Ok(None)
            }
        }
        .boxed()
    }

    fn clear_entities(&self) -> BoxFuture<'static, Result<(), PersistenceError>> {
        let client = self.client.clone();
        async move {
            client
                .batch_execute("TRUNCATE TABLE entities")
                .await
                .map_err(|e| PersistenceError::new(format!("truncate entities failed: {}", e)))?;
            Ok(())
        }
        .boxed()
    }

    fn clear_resources(&self) -> BoxFuture<'static, Result<(), PersistenceError>> {
        let client = self.client.clone();
        async move {
            client
                .batch_execute("TRUNCATE TABLE resources")
                .await
                .map_err(|e| PersistenceError::new(format!("truncate resources failed: {}", e)))?;
            Ok(())
        }
        .boxed()
    }
}
