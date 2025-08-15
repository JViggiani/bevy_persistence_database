//! Postgres backend: implements `DatabaseConnection` using `tokio-postgres`.
//! Storage model:
//!   entities(id text pk, bevy_persistence_version bigint not null, doc jsonb not null)
//!   resources(id text pk, bevy_persistence_version bigint not null, doc jsonb not null)

use crate::db::connection::{
    Collection, DatabaseConnection, PersistenceError, TransactionOperation,
    BEVY_PERSISTENCE_VERSION_FIELD,
};
use crate::query::filter_expression::{BinaryOperator, FilterExpression};
use crate::query::persistence_query_specification::PersistenceQuerySpecification;
use bevy::log::{debug, error, info};
use futures::future::BoxFuture;
use futures::FutureExt;
use serde_json::Value;
use std::fmt;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio_postgres::{Client, Config, NoTls};
use uuid::Uuid;


#[derive(Clone)]
pub struct PostgresDbConnection {
    client: Arc<Mutex<Client>>,
}

impl fmt::Debug for PostgresDbConnection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PostgresDbConnection").finish_non_exhaustive()
    }
}

impl PostgresDbConnection {
    pub async fn ensure_database(
        host: &str,
        user: &str,
        pass: &str,
        db_name: &str,
        port: Option<u16>,
    ) -> Result<(), PersistenceError> {
        // Connect to the default "postgres" database to create a new DB
        let mut cfg = Config::new();
        cfg.host(host)
            .user(user)
            .password(pass)
            .dbname("postgres");
        if let Some(p) = port {
            cfg.port(p);
        }
        let (client, connection) = cfg
            .connect(NoTls)
            .await
            .map_err(|e| PersistenceError::new(format!("pg connect (server) failed: {}", e)))?;
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                error!("postgres connection error (ensure_database): {}", e);
            }
        });

        let db_quoted = db_name.replace('"', "\"\"");
        let stmt = format!("CREATE DATABASE \"{}\"", db_quoted);
        debug!("[pg] {}", stmt);
        match client.batch_execute(&stmt).await {
            Ok(_) => info!("[pg] database {} created", db_name),
            Err(e) => {
                // 42P04 is duplicate_database
                if let Some(db_err) = e.code() {
                    if db_err.code() != "42P04" {
                        return Err(PersistenceError::new(format!(
                            "pg create db failed: {}",
                            e
                        )));
                    }
                }
            }
        }
        Ok(())
    }

    pub async fn connect(
        host: &str,
        user: &str,
        pass: &str,
        db_name: &str,
        port: Option<u16>,
    ) -> Result<Self, PersistenceError> {
        let mut cfg = Config::new();
        cfg.host(host).user(user).password(pass).dbname(db_name);
        if let Some(p) = port {
            cfg.port(p);
        }

        let (client, connection) = cfg
            .connect(NoTls)
            .await
            .map_err(|e| PersistenceError::new(format!("pg connect failed: {}", e)))?;
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                error!("postgres connection error: {}", e);
            }
        });

        let db = Self {
            client: Arc::new(Mutex::new(client)),
        };
        // Ensure schema exists
        db.ensure_schema().await?;
        Ok(db)
    }

    async fn ensure_schema(&self) -> Result<(), PersistenceError> {
        let client = self.client.lock().await;
        debug!("[pg] ensuring schema (tables)");
        client
            .batch_execute(
                r#"
                CREATE TABLE IF NOT EXISTS entities (
                    id TEXT PRIMARY KEY,
                    bevy_persistence_version BIGINT NOT NULL,
                    doc JSONB NOT NULL
                );
                CREATE TABLE IF NOT EXISTS resources (
                    id TEXT PRIMARY KEY,
                    bevy_persistence_version BIGINT NOT NULL,
                    doc JSONB NOT NULL
                );
            "#,
            )
            .await
            .map_err(|e| PersistenceError::new(format!("pg ensure schema failed: {}", e)))?;
        Ok(())
    }

    // Build WHERE and params for a spec
    fn build_where(spec: &PersistenceQuerySpecification) -> (String, Vec<String>) {
        let mut clauses: Vec<String> = Vec::new();
        let mut params: Vec<String> = Vec::new();

        // presence_with: "doc ? 'Component'"
        if !spec.presence_with.is_empty() {
            let cond = spec
                .presence_with
                .iter()
                .map(|n| format!("doc ? '{}'", n))
                .collect::<Vec<_>>()
                .join(" AND ");
            clauses.push(format!("({})", cond));
        }
        // presence_without: "NOT doc ? 'Component'"
        if !spec.presence_without.is_empty() {
            let cond = spec
                .presence_without
                .iter()
                .map(|n| format!("NOT (doc ? '{}')", n))
                .collect::<Vec<_>>()
                .join(" AND ");
            clauses.push(format!("({})", cond));
        }

        // value_filters
        if let Some(expr) = &spec.value_filters {
            let (sql, ps) = Self::translate_expr(expr);
            clauses.push(sql);
            params.extend(ps);
        }

        let where_sql = if clauses.is_empty() {
            "TRUE".to_string()
        } else {
            clauses.join(" AND ")
        };
        (where_sql, params)
    }

    // Translate FilterExpression to SQL with positional params (as text) + casts
    // We pass all params as text and cast explicitly (e.g. ($1::text)::double precision).
    fn translate_expr(expr: &FilterExpression) -> (String, Vec<String>) {
        fn field_path(component: &str, field: &str) -> String {
            if field.is_empty() {
                format!("doc -> '{}'", component)
            } else {
                format!("doc -> '{}' ->> '{}'", component, field)
            }
        }
        fn translate_rec(expr: &FilterExpression, args: &mut Vec<String>) -> String {
            match expr {
                FilterExpression::Literal(v) => {
                    // Always push as string; cast in SQL where needed
                    let idx = args.len() + 1;
                    let s = match v {
                        Value::String(s) => s.clone(),
                        Value::Number(n) => n.to_string(),
                        Value::Bool(b) => b.to_string(),
                        Value::Null => "null".to_string(),
                        other => other.to_string(),
                    };
                    args.push(s);
                    format!("${}", idx)
                }
                FilterExpression::DocumentKey => "id".to_string(),
                FilterExpression::Field { component_name, field_name } => field_path(component_name, field_name),
                FilterExpression::BinaryOperator { op, lhs, rhs } => {
                    let op_str = match op {
                        BinaryOperator::Eq => "=",
                        BinaryOperator::Ne => "!=",
                        BinaryOperator::Gt => ">",
                        BinaryOperator::Gte => ">=",
                        BinaryOperator::Lt => "<",
                        BinaryOperator::Lte => "<=",
                        BinaryOperator::And => "AND",
                        BinaryOperator::Or => "OR",
                        BinaryOperator::In => "IN",
                    };
                    match op {
                        BinaryOperator::And | BinaryOperator::Or => {
                            let l = translate_rec(lhs, args);
                            let r_raw = translate_rec(rhs, args);
                            format!("({} {} {})", l, op_str, r_raw)
                        }
                        BinaryOperator::Eq | BinaryOperator::Ne => {
                            // Handle presence checks BEFORE recursing to avoid binding a useless NULL param.
                            if let FilterExpression::Field { component_name, field_name } = &**lhs {
                                if field_name.is_empty() {
                                    if let FilterExpression::Literal(Value::Null) = &**rhs {
                                        if matches!(op, BinaryOperator::Eq) {
                                            // Component absent
                                            return format!("NOT (doc ? '{}')", component_name);
                                        } else {
                                            // Component present
                                            return format!("(doc ? '{}')", component_name);
                                        }
                                    }
                                }
                            }
                            // Fallback: recurse and build comparison with proper casts
                            let l = translate_rec(lhs, args);
                            let r_raw = translate_rec(rhs, args);

                            let left_is_key = matches!(&**lhs, FilterExpression::DocumentKey);
                            if left_is_key {
                                format!("({}) = ({}::text)", l, r_raw)
                            } else if let FilterExpression::Field { field_name, .. } = &**lhs {
                                if field_name.is_empty() {
                                    "(FALSE)".to_string()
                                } else {
                                    // If rhs looks boolean (by text), compare as boolean, otherwise compare as text.
                                    format!(
                                        "((lower(({}::text)) IN ('true','false')) AND ((({})::boolean) = ((({}::text))::boolean))) \
                                         OR ((lower(({}::text)) NOT IN ('true','false')) AND (({}) = ({}::text)))",
                                        r_raw, l, r_raw, r_raw, l, r_raw
                                    )
                                }
                            } else {
                                format!("({}) = ({}::text)", l, r_raw)
                            }
                        }
                        BinaryOperator::Gt | BinaryOperator::Gte | BinaryOperator::Lt | BinaryOperator::Lte => {
                            let l = translate_rec(lhs, args);
                            let r_raw = translate_rec(rhs, args);
                            // Cast left to numeric; cast param from text to numeric to avoid driver type mismatch
                            let left_num = format!("({})::double precision", l);
                            format!("({}) {} ((({}::text))::double precision)", left_num, op_str, r_raw)
                        }
                        BinaryOperator::In => {
                            "(FALSE)".to_string()
                        }
                    }
                }
            }
        }

        let mut params = Vec::new();
        let sql = translate_rec(expr, &mut params);
        (sql, params)
    }
}

impl DatabaseConnection for PostgresDbConnection {
    fn document_key_field(&self) -> &'static str {
        "id"
    }

    fn execute_keys(
        &self,
        spec: &PersistenceQuerySpecification,
    ) -> BoxFuture<'static, Result<Vec<String>, PersistenceError>> {
        let (where_sql, params) = Self::build_where(spec);
        let sql = format!("SELECT id FROM entities WHERE {}", where_sql);
        let client = self.client.clone();
        bevy::log::debug!("[pg] execute_keys: {}; params={:?}", sql, params);
        async move {
            let client = client.lock().await;
            // Build &[&(dyn ToSql + Sync)] from our Vec<String>
            let param_refs: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> =
                params.iter().map(|s| s as &(dyn tokio_postgres::types::ToSql + Sync)).collect();
            let rows = client
                .query(&sql, param_refs.as_slice())
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
        let (where_sql, params) = Self::build_where(spec);
        let sql = format!(
            "SELECT (doc || jsonb_build_object('id', id)) AS doc FROM entities WHERE {}",
            where_sql
        );
        let client = self.client.clone();
        bevy::log::debug!("[pg] execute_documents: {} ; params={:?}", sql, params);
        async move {
            let client = client.lock().await;
            let param_refs: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> =
                params.iter().map(|s| s as &(dyn tokio_postgres::types::ToSql + Sync)).collect();
            let rows = client
                .query(&sql, param_refs.as_slice())
                .await
                .map_err(|e| PersistenceError::new(format!("pg query docs failed: {}", e)))?;
            let mut out = Vec::with_capacity(rows.len());
            for r in rows {
                let v: Value = r.get("doc");
                out.push(v);
            }
            Ok(out)
        }
        .boxed()
    }

    fn execute_transaction(
        &self,
        operations: Vec<TransactionOperation>,
    ) -> BoxFuture<'static, Result<Vec<String>, PersistenceError>> {
        let client = self.client.clone();
        async move {
            let mut client = client.lock().await;
            let tx = client
                .transaction()
                .await
                .map_err(|e| PersistenceError::new(format!("pg begin tx failed: {}", e)))?;

            let mut new_keys: Vec<String> = Vec::new();

            for op in operations {
                match op {
                    TransactionOperation::CreateDocument { collection, data } => {
                        match collection {
                            Collection::Entities => {
                                // Generate id and ensure it is NOT stored inside doc JSON
                                let key = Uuid::new_v4().to_string();
                                // Extract version as u64
                                let version = data
                                    .get(BEVY_PERSISTENCE_VERSION_FIELD)
                                    .and_then(|v| v.as_u64())
                                    .unwrap_or(1) as i64;

                                // Do not insert "id" into doc JSON for Postgres entities
                                debug!(
                                    "[pg] create entity id={} version={} data={}",
                                    key, version, data
                                );
                                tx.execute(
                                    "INSERT INTO entities (id, bevy_persistence_version, doc) VALUES ($1, $2, $3::jsonb)",
                                    &[&key, &version, &data],
                                )
                                .await
                                .map_err(|e| PersistenceError::new(format!("pg insert entity failed: {}", e)))?;
                                new_keys.push(key);
                            }
                            Collection::Resources => {
                                // Resource key must be in the JSON (already inserted by caller)
                                let key = data
                                    .get("id")
                                    .and_then(|v| v.as_str())
                                    .ok_or_else(|| {
                                        PersistenceError::new(
                                            "resource JSON missing 'id' (key) field",
                                        )
                                    })?
                                    .to_string();
                                let version = data
                                    .get(BEVY_PERSISTENCE_VERSION_FIELD)
                                    .and_then(|v| v.as_u64())
                                    .unwrap_or(1) as i64;
                                debug!(
                                    "[pg] create resource id={} version={} data={}",
                                    key, version, data
                                );
                                tx.execute(
                                    "INSERT INTO resources (id, bevy_persistence_version, doc) VALUES ($1, $2, $3::jsonb)
                                     ON CONFLICT (id) DO UPDATE SET bevy_persistence_version = EXCLUDED.bevy_persistence_version,
                                       doc = resources.doc || EXCLUDED.doc",
                                    &[&key, &version, &data],
                                )
                                .await
                                .map_err(|e| PersistenceError::new(format!("pg upsert resource failed: {}", e)))?;
                            }
                        }
                    }
                    TransactionOperation::UpdateDocument {
                        collection,
                        key,
                        expected_current_version,
                        patch,
                    } => {
                        let exp = expected_current_version as i64;
                        let next_ver = exp + 1;
                        match collection {
                            Collection::Entities => {
                                debug!(
                                    "[pg] update entity id={} expected_ver={} patch={}",
                                    key, exp, patch
                                );
                                let n = tx
                                    .execute(
                                        "UPDATE entities
                                           SET doc = doc || $3::jsonb,
                                               bevy_persistence_version = $2 + 1
                                         WHERE id = $1 AND bevy_persistence_version = $2",
                                        // Pass jsonb as serde_json::Value
                                        &[&key, &exp, &patch],
                                    )
                                    .await
                                    .map_err(|e| {
                                        PersistenceError::new(format!(
                                            "pg update entity failed: {}",
                                            e
                                        ))
                                    })?;
                                if n == 0 {
                                    return Err(PersistenceError::Conflict { key });
                                }
                                // Ensure doc version field is incremented too
                                tx.execute(
                                    &format!(
                                        "UPDATE entities
                                           SET doc = jsonb_set(doc, '{{{}}}', to_jsonb($2::bigint), true)
                                         WHERE id = $1",
                                        BEVY_PERSISTENCE_VERSION_FIELD
                                    ),
                                    &[&key, &next_ver],
                                )
                                .await
                                .ok();
                            }
                            Collection::Resources => {
                                debug!(
                                    "[pg] update resource id={} expected_ver={} patch={}",
                                    key, exp, patch
                                );
                                let n = tx
                                    .execute(
                                        "UPDATE resources
                                           SET doc = doc || $3::jsonb,
                                               bevy_persistence_version = $2 + 1
                                         WHERE id = $1 AND bevy_persistence_version = $2",
                                        // Pass jsonb as serde_json::Value
                                        &[&key, &exp, &patch],
                                    )
                                    .await
                                    .map_err(|e| {
                                        PersistenceError::new(format!(
                                            "pg update resource failed: {}",
                                            e
                                        ))
                                    })?;
                                if n == 0 {
                                    return Err(PersistenceError::Conflict { key });
                                }
                                tx.execute(
                                    &format!(
                                        "UPDATE resources
                                           SET doc = jsonb_set(doc, '{{{}}}', to_jsonb($2::bigint), true)
                                         WHERE id = $1",
                                        BEVY_PERSISTENCE_VERSION_FIELD
                                    ),
                                    &[&key, &next_ver],
                                )
                                .await
                                .ok();
                            }
                        }
                    }
                    TransactionOperation::DeleteDocument {
                        collection,
                        key,
                        expected_current_version,
                    } => {
                        let exp = expected_current_version as i64;
                        match collection {
                            Collection::Entities => {
                                debug!("[pg] delete entity id={} expected_ver={}", key, exp);
                                let n = tx
                                    .execute(
                                        "DELETE FROM entities WHERE id = $1 AND bevy_persistence_version = $2",
                                        &[&key, &exp],
                                    )
                                    .await
                                    .map_err(|e| {
                                        PersistenceError::new(format!(
                                            "pg delete entity failed: {}",
                                            e
                                        ))
                                    })?;
                                if n == 0 {
                                    return Err(PersistenceError::Conflict { key });
                                }
                            }
                            Collection::Resources => {
                                debug!("[pg] delete resource id={} expected_ver={}", key, exp);
                                let n = tx
                                    .execute(
                                        "DELETE FROM resources WHERE id = $1 AND bevy_persistence_version = $2",
                                        &[&key, &exp],
                                    )
                                    .await
                                    .map_err(|e| {
                                        PersistenceError::new(format!(
                                            "pg delete resource failed: {}",
                                            e
                                        ))
                                    })?;
                                if n == 0 {
                                    return Err(PersistenceError::Conflict { key });
                                }
                            }
                        }
                    }
                }
            }

            tx.commit()
                .await
                .map_err(|e| PersistenceError::new(format!("pg commit failed: {}", e)))?;
            Ok(new_keys)
        }
        .boxed()
    }

    fn fetch_document(
        &self,
        entity_key: &str,
    ) -> BoxFuture<'static, Result<Option<(Value, u64)>, PersistenceError>> {
        let key = entity_key.to_string();
        let client = self.client.clone();
        async move {
            let client = client.lock().await;
            debug!("[pg] fetch_document {}", key);
            let row_opt = client
                .query_opt(
                    "SELECT doc, bevy_persistence_version FROM entities WHERE id = $1",
                    &[&key],
                )
                .await
                .map_err(|e| PersistenceError::new(format!("pg fetch_document failed: {}", e)))?;
            if let Some(row) = row_opt {
                let doc: Value = row.get(0);
                // Do NOT inject "id" into JSON; tests expect only component fields + version
                let ver: i64 = row.get(1);
                Ok(Some((doc, ver as u64)))
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
        let key = entity_key.to_string();
        let comp = comp_name.to_string();
        let client = self.client.clone();
        async move {
            let client = client.lock().await;
            debug!("[pg] fetch_component key={} comp={}", key, comp);
            let row_opt = client
                .query_opt(
                    "SELECT doc -> $2 FROM entities WHERE id = $1",
                    &[&key, &comp],
                )
                .await
                .map_err(|e| PersistenceError::new(format!("pg fetch_component failed: {}", e)))?;
            if let Some(row) = row_opt {
                let v: Option<Value> = row.get(0);
                Ok(v)
            } else {
                Ok(None)
            }
        }
        .boxed()
    }

    fn fetch_resource(
        &self,
        resource_name: &str,
    ) -> BoxFuture<'static, Result<Option<(Value, u64)>, PersistenceError>> {
        let key = resource_name.to_string();
        let client = self.client.clone();
        async move {
            let client = client.lock().await;
            debug!("[pg] fetch_resource {}", key);
            let row_opt = client
                .query_opt(
                    "SELECT doc, bevy_persistence_version FROM resources WHERE id = $1",
                    &[&key],
                )
                .await
                .map_err(|e| PersistenceError::new(format!("pg fetch_resource failed: {}", e)))?;
            if let Some(row) = row_opt {
                let doc: Value = row.get(0);
                // Do NOT inject "id" into JSON resource blob either
                let ver: i64 = row.get(1);
                Ok(Some((doc, ver as u64)))
            } else {
                Ok(None)
            }
        }
        .boxed()
    }

    fn clear_entities(&self) -> BoxFuture<'static, Result<(), PersistenceError>> {
        let client = self.client.clone();
        async move {
            let client = client.lock().await;
            debug!("[pg] clear_entities");
            client
                .batch_execute("TRUNCATE TABLE entities")
                .await
                .map_err(|e| PersistenceError::new(format!("pg clear_entities failed: {}", e)))?;
            Ok(())
        }
        .boxed()
    }

    fn clear_resources(&self) -> BoxFuture<'static, Result<(), PersistenceError>> {
        let client = self.client.clone();
        async move {
            let client = client.lock().await;
            debug!("[pg] clear_resources");
            client
                .batch_execute("TRUNCATE TABLE resources")
                .await
                .map_err(|e| PersistenceError::new(format!("pg clear_resources failed: {}", e)))?;
            Ok(())
        }
        .boxed()
    }
}