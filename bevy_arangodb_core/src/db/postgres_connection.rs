//! Postgres backend: implements `DatabaseConnection` using `tokio-postgres`.
//! Storage model:
//!   entities(id text pk, bevy_persistence_version bigint not null, doc jsonb not null)
//!   resources(id text pk, bevy_persistence_version bigint not null, doc jsonb not null)

use crate::db::connection::{
    DatabaseConnection, PersistenceError, TransactionOperation,
    BEVY_PERSISTENCE_VERSION_FIELD,
};
use crate::db::shared::{GroupedOperations, OperationType, check_operation_success};
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
use tokio_postgres::types::ToSql;

// Local constants to avoid magic strings
const KEY_COL: &str = "id";
const ENTITIES_TABLE: &str = "entities";
const RESOURCES_TABLE: &str = "resources";

// Typed parameter carrier for filter translation
enum SqlParam {
    Text(String),
    Bool(bool),
    F64(f64),
}
impl SqlParam {
    // Make boxed params Send so futures remain Send
    fn into_box(self) -> Box<dyn ToSql + Sync + Send> {
        match self {
            SqlParam::Text(s) => Box::new(s),
            SqlParam::Bool(b) => Box::new(b),
            SqlParam::F64(n) => Box::new(n),
        }
    }
}

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
                -- Generic GIN index to accelerate jsonb presence/containment predicates
                CREATE INDEX IF NOT EXISTS entities_doc_gin ON entities USING GIN (doc jsonb_path_ops);
            "#,
            )
            .await
            .map_err(|e| PersistenceError::new(format!("pg ensure schema failed: {}", e)))?;
        Ok(())
    }

    // Build WHERE and params for a spec
    fn build_where(spec: &PersistenceQuerySpecification) -> (String, Vec<SqlParam>) {
        let mut clauses: Vec<String> = Vec::new();
        let mut params: Vec<SqlParam> = Vec::new();

        // presence_with: ensure component exists (use jsonb existence operator)
        if !spec.presence_with.is_empty() {
            let cond = spec
                .presence_with
                .iter()
                .map(|n| format!("doc ? '{}'", n))
                .collect::<Vec<_>>()
                .join(" AND ");
            clauses.push(format!("({})", cond));
        }
        // presence_without: ensure component absent
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

    // Translate FilterExpression to SQL with typed params
    fn translate_expr(expr: &FilterExpression) -> (String, Vec<SqlParam>) {
        fn field_path(component: &str, field: &str) -> String {
            if field.is_empty() {
                // presence is handled specially; still return doc->'Comp' for casts
                format!("doc -> '{}'", component)
            } else {
                // text extract; cast in ops below
                format!("doc -> '{}' ->> '{}'", component, field)
            }
        }
        fn translate_rec(expr: &FilterExpression, args: &mut Vec<SqlParam>) -> String {
            match expr {
                FilterExpression::Literal(v) => {
                    let idx = args.len() + 1;
                    let p = match v {
                        Value::Bool(b) => SqlParam::Bool(*b),
                        Value::Number(n) => SqlParam::F64(n.as_f64().unwrap_or(0.0)),
                        Value::String(s) => SqlParam::Text(s.clone()),
                        Value::Null => SqlParam::Text("null".into()), // will be ignored in presence branches
                        other => SqlParam::Text(other.to_string()),
                    };
                    args.push(p);
                    format!("${}", idx)
                }
                FilterExpression::DocumentKey => KEY_COL.to_string(),
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
                            let r = translate_rec(rhs, args);
                            format!("({} {} {})", l, op_str, r)
                        }
                        BinaryOperator::Eq | BinaryOperator::Ne => {
                            // Presence optimization for component-only fields against NULL
                            if let FilterExpression::Field { component_name, field_name } = &**lhs {
                                if field_name.is_empty() {
                                    if let FilterExpression::Literal(Value::Null) = &**rhs {
                                        return if matches!(op, BinaryOperator::Eq) {
                                            // equals NULL => absent
                                            format!("NOT (doc ? '{}')", component_name)
                                        } else {
                                            // not equals NULL => present
                                            format!("(doc ? '{}')", component_name)
                                        };
                                    }
                                }
                            }
                            // Typed comparisons (fallbacks)
                            let left_is_key = matches!(&**lhs, FilterExpression::DocumentKey);
                            if left_is_key {
                                let l = translate_rec(lhs, args);
                                let r = translate_rec(rhs, args);
                                return format!("({}) = ({}::text)", l, r);
                            }
                            if let FilterExpression::Field { field_name, .. } = &**lhs {
                                // determine RHS param type by peeking last pushed param index
                                let before = args.len();
                                let r = translate_rec(rhs, args);
                                let rhs_is_bool = matches!(args.get(before), Some(SqlParam::Bool(_)));
                                let rhs_is_num  = matches!(args.get(before), Some(SqlParam::F64(_)));
                                if field_name.is_empty() {
                                    // component object equality unsupported -> false
                                    "(FALSE)".to_string()
                                } else if rhs_is_bool {
                                    let l = translate_rec(lhs, &mut Vec::new()); // rebuild path only
                                    format!("((({})::boolean) {} ({}::boolean))", l, op_str, r)
                                } else if rhs_is_num {
                                    let l = translate_rec(lhs, &mut Vec::new());
                                    format!("((({})::double precision) {} ({}::double precision))", l, op_str, r)
                                } else {
                                    let l = translate_rec(lhs, &mut Vec::new());
                                    format!("(({}) {} ({}::text))", l, op_str, r)
                                }
                            } else {
                                let l = translate_rec(lhs, args);
                                let r = translate_rec(rhs, args);
                                format!("({}) = ({}::text)", l, r)
                            }
                        }
                        BinaryOperator::Gt | BinaryOperator::Gte | BinaryOperator::Lt | BinaryOperator::Lte => {
                            // Force numeric compare where possible
                            let before = args.len();
                            let r = translate_rec(rhs, args);
                            let rhs_is_num  = matches!(args.get(before), Some(SqlParam::F64(_)));
                            let l = translate_rec(lhs, &mut Vec::new());
                            if rhs_is_num {
                                format!("(({})::double precision {} ({}::double precision))", l, op_str, r)
                            } else {
                                format!("(({})::text {} ({}::text))", l, op_str, r)
                            }
                        }
                        BinaryOperator::In => {
                            // Not needed currently; keep disabled for safety
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
        KEY_COL
    }

    fn execute_keys(
        &self,
        spec: &PersistenceQuerySpecification,
    ) -> BoxFuture<'static, Result<Vec<String>, PersistenceError>> {
        let (where_sql, params) = Self::build_where(spec);
        let sql = format!("SELECT {k} FROM {t} WHERE {w}", k = KEY_COL, t = ENTITIES_TABLE, w = where_sql);
        let client = self.client.clone();
        bevy::log::debug!("[pg] execute_keys: {}; params_len={}", sql, params.len());
        async move {
            let client = client.lock().await;
            // Box typed params as Send
            let boxed: Vec<Box<dyn ToSql + Sync + Send>> =
                params.into_iter().map(|p| p.into_box()).collect();
            // Build slice of &(dyn ToSql + Sync) from the Send boxes
            let param_refs: Vec<&(dyn ToSql + Sync)> =
                boxed.iter().map(|b| &**b as &(dyn ToSql + Sync)).collect();

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
        // WHERE and typed params
        let (where_sql, params) = Self::build_where(spec);

        // Build projection:
        // - If return_full_docs is true: return full doc + id + version.
        // - Else if fetch_only is non-empty: return only id, version, and requested component keys (when present).
        // - Else: return just id + version.
        let sql = if spec.return_full_docs {
            format!(
                "SELECT (doc || jsonb_build_object('{k}', {k}, '{ver}', bevy_persistence_version)) AS doc \
                 FROM {t} WHERE {w}",
                k = KEY_COL,
                ver = BEVY_PERSISTENCE_VERSION_FIELD,
                t = ENTITIES_TABLE,
                w = where_sql
            )
        } else if !spec.fetch_only.is_empty() {
            fn q(s: &str) -> String { s.replace('\'', "''") }
            let mut proj = format!(
                "SELECT jsonb_strip_nulls(jsonb_build_object('{k}', {k}, '{ver}', bevy_persistence_version",
                k = KEY_COL,
                ver = BEVY_PERSISTENCE_VERSION_FIELD
            );
            for name in &spec.fetch_only {
                let key = q(name);
                proj.push_str(&format!(
                    ", '{k}', CASE WHEN doc ? '{k}' THEN doc->'{k}' ELSE NULL END",
                    k = key
                ));
            }
            proj.push_str(&format!(")) AS doc FROM {t} WHERE {w}", t = ENTITIES_TABLE, w = where_sql));
            proj
        } else {
            format!(
                "SELECT jsonb_build_object('{k}', {k}, '{ver}', bevy_persistence_version) AS doc \
                 FROM {t} WHERE {w}",
                k = KEY_COL,
                ver = BEVY_PERSISTENCE_VERSION_FIELD,
                t = ENTITIES_TABLE,
                w = where_sql
            )
        };

        let client = self.client.clone();
        bevy::log::debug!(
            "[pg] execute_documents: {}; fetch_only={:?}; return_full_docs={}; params_len={}",
            sql,
            spec.fetch_only,
            spec.return_full_docs,
            params.len()
        );
        async move {
            let client = client.lock().await;
            // Box typed params as Send
            let boxed: Vec<Box<dyn ToSql + Sync + Send>> =
                params.into_iter().map(|p| p.into_box()).collect();
            // Downcast references to &(dyn ToSql + Sync) for tokio-postgres
            let param_refs: Vec<&(dyn ToSql + Sync)> =
                boxed.iter().map(|b| &**b as &(dyn ToSql + Sync)).collect();

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
    ) -> futures::future::BoxFuture<'static, Result<Vec<String>, PersistenceError>> {
        let client_arc = self.client.clone();
        async move {
            // Acquire client and begin a transaction
            let mut client = client_arc.lock().await;
            let tx = client
                .transaction()
                .await
                .map_err(|e| PersistenceError::new(format!("pg START TRANSACTION failed: {}", e)))?;

            let groups = GroupedOperations::from_operations(operations, KEY_COL);
            let mut new_entity_ids: Vec<String> = Vec::new();

            // 1) Entity creates
            if !groups.entity_creates.is_empty() {
                use uuid::Uuid;
                let ids: Vec<String> = groups.entity_creates
                    .iter()
                    .map(|_| Uuid::new_v4().to_string())
                    .collect();

                let ver_field = BEVY_PERSISTENCE_VERSION_FIELD;
                let input_docs: Vec<serde_json::Value> = groups.entity_creates
                    .iter()
                    .cloned()
                    .zip(ids.iter())
                    .map(|(doc, id)| {
                        let ver = doc.get(ver_field).and_then(|v| v.as_i64()).unwrap_or(1);
                        serde_json::json!({ "id": id, "ver": ver, "doc": doc })
                    })
                    .collect();
                let input_json = serde_json::Value::Array(input_docs);

                let sql = format!(r#"
                    WITH input AS (
                        SELECT (x->>'{k}')::text      AS {k},
                               (x->>'ver')::bigint    AS ver,
                               (x->'doc')::jsonb      AS doc
                        FROM jsonb_array_elements($1::jsonb) AS x
                    )
                    INSERT INTO {t} ({k}, bevy_persistence_version, doc)
                    SELECT {k}, ver, doc FROM input
                "#, k = KEY_COL, t = ENTITIES_TABLE);

                tx.execute(&sql, &[&input_json])
                    .await
                    .map_err(|e| PersistenceError::new(format!("pg batch insert ({}) failed: {}", ENTITIES_TABLE, e)))?;

                new_entity_ids = ids;
            }

            // 2) Entity updates
            if !groups.entity_updates.is_empty() {
                let requested = groups.extract_keys(&groups.entity_updates, KEY_COL);

                let upd_sql = format!(r#"
                    WITH input AS (
                        SELECT (x->>'{k}')::text        AS {k},
                               (x->>'expected')::bigint AS expected,
                               (x->'patch')::jsonb      AS patch
                        FROM jsonb_array_elements($1::jsonb) AS x
                    ),
                    updated AS (
                        UPDATE {t} e
                        SET doc = e.doc || i.patch,
                            bevy_persistence_version = i.expected + 1
                        FROM input i
                        WHERE e.{k} = i.{k} AND e.bevy_persistence_version = i.expected
                        RETURNING e.{k}
                    )
                    SELECT {k} FROM updated
                "#, k = KEY_COL, t = ENTITIES_TABLE);

                let rows = tx
                    .query(&upd_sql, &[&serde_json::Value::Array(groups.entity_updates.clone())])
                    .await
                    .map_err(|e| PersistenceError::new(format!("pg batch update ({}) failed: {}", ENTITIES_TABLE, e)))?;
                let updated: Vec<String> = rows.into_iter().map(|r| r.get::<_, String>(0)).collect();

                if let Err(e) = check_operation_success(requested, updated, &OperationType::Update, ENTITIES_TABLE) {
                    let _ = tx.rollback().await;
                    return Err(e);
                }
            }

            // 3) Entity deletes
            if !groups.entity_deletes.is_empty() {
                let requested = groups.extract_keys(&groups.entity_deletes, KEY_COL);

                let del_sql = format!(r#"
                    WITH input AS (
                        SELECT (x->>'{k}')::text        AS {k},
                               (x->>'expected')::bigint AS expected
                        FROM jsonb_array_elements($1::jsonb) AS x
                    ),
                    deleted AS (
                        DELETE FROM {t} e
                        USING input i
                        WHERE e.{k} = i.{k} AND e.bevy_persistence_version = i.expected
                        RETURNING e.{k}
                    )
                    SELECT {k} FROM deleted
                "#, k = KEY_COL, t = ENTITIES_TABLE);

                let rows = tx
                    .query(&del_sql, &[&serde_json::Value::Array(groups.entity_deletes.clone())])
                    .await
                    .map_err(|e| PersistenceError::new(format!("pg batch delete ({}) failed: {}", ENTITIES_TABLE, e)))?;
                let deleted: Vec<String> = rows.into_iter().map(|r| r.get::<_, String>(0)).collect();

                if let Err(e) = check_operation_success(requested, deleted, &OperationType::Delete, ENTITIES_TABLE) {
                    let _ = tx.rollback().await;
                    return Err(e);
                }
            }

            // 4) Resource creates
            if !groups.resource_creates.is_empty() {
                let ver_field = BEVY_PERSISTENCE_VERSION_FIELD;
                let input_docs: Vec<serde_json::Value> = groups.resource_creates
                    .iter()
                    .cloned()
                    .map(|doc| {
                        let id = doc
                            .get("id")
                            .and_then(|v| v.as_str())
                            .ok_or_else(|| PersistenceError::new("Resource create missing id"))?
                            .to_string();
                        let ver = doc.get(ver_field).and_then(|v| v.as_i64()).unwrap_or(1);
                        Ok(serde_json::json!({ "id": id, "ver": ver, "doc": doc }))
                    })
                    .collect::<Result<_, PersistenceError>>()?;
                let input_json = serde_json::Value::Array(input_docs);

                let sql = format!(r#"
                    WITH input AS (
                        SELECT (x->>'{k}')::text      AS {k},
                               (x->>'ver')::bigint    AS ver,
                               (x->'doc')::jsonb      AS doc
                        FROM jsonb_array_elements($1::jsonb) AS x
                    )
                    INSERT INTO {t} ({k}, bevy_persistence_version, doc)
                    SELECT {k}, ver, doc FROM input
                "#, k = KEY_COL, t = RESOURCES_TABLE);

                tx.execute(&sql, &[&input_json])
                    .await
                    .map_err(|e| PersistenceError::new(format!("pg batch insert ({}) failed: {}", RESOURCES_TABLE, e)))?;
            }

            // 5) Resource updates
            if !groups.resource_updates.is_empty() {
                let requested = groups.extract_keys(&groups.resource_updates, KEY_COL);

                let upd_sql = format!(r#"
                    WITH input AS (
                        SELECT (x->>'{k}')::text        AS {k},
                               (x->>'expected')::bigint AS expected,
                               (x->'patch')::jsonb      AS patch
                        FROM jsonb_array_elements($1::jsonb) AS x
                    ),
                    updated AS (
                        UPDATE {t} r
                        SET doc = r.doc || i.patch,
                            bevy_persistence_version = i.expected + 1
                        FROM input i
                        WHERE r.{k} = i.{k} AND r.bevy_persistence_version = i.expected
                        RETURNING r.{k}
                    )
                    SELECT {k} FROM updated
                "#, k = KEY_COL, t = RESOURCES_TABLE);

                let rows = tx
                    .query(&upd_sql, &[&serde_json::Value::Array(groups.resource_updates.clone())])
                    .await
                    .map_err(|e| PersistenceError::new(format!("pg batch update ({}) failed: {}", RESOURCES_TABLE, e)))?;
                let updated: Vec<String> = rows.into_iter().map(|r| r.get::<_, String>(0)).collect();

                if let Err(e) = check_operation_success(requested, updated, &OperationType::Update, RESOURCES_TABLE) {
                    let _ = tx.rollback().await;
                    return Err(e);
                }
            }

            // 6) Resource deletes
            if !groups.resource_deletes.is_empty() {
                let requested = groups.extract_keys(&groups.resource_deletes, KEY_COL);

                let del_sql = format!(r#"
                    WITH input AS (
                        SELECT (x->>'{k}')::text        AS {k},
                               (x->>'expected')::bigint AS expected
                        FROM jsonb_array_elements($1::jsonb) AS x
                    ),
                    deleted AS (
                        DELETE FROM {t} r
                        USING input i
                        WHERE r.{k} = i.{k} AND r.bevy_persistence_version = i.expected
                        RETURNING r.{k}
                    )
                    SELECT {k} FROM deleted
                "#, k = KEY_COL, t = RESOURCES_TABLE);

                let rows = tx
                    .query(&del_sql, &[&serde_json::Value::Array(groups.resource_deletes.clone())])
                    .await
                    .map_err(|e| PersistenceError::new(format!("pg batch delete ({}) failed: {}", RESOURCES_TABLE, e)))?;
                let deleted: Vec<String> = rows.into_iter().map(|r| r.get::<_, String>(0)).collect();

                if let Err(e) = check_operation_success(requested, deleted, &OperationType::Delete, RESOURCES_TABLE) {
                    let _ = tx.rollback().await;
                    return Err(e);
                }
            }

            // COMMIT and return entity ids
            tx.commit()
                .await
                .map_err(|e| PersistenceError::new(format!("pg COMMIT failed: {}", e)))?;
            Ok(new_entity_ids)
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
                    &format!("SELECT doc, bevy_persistence_version FROM {t} WHERE {k} = $1", t = ENTITIES_TABLE, k = KEY_COL),
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
                    &format!("SELECT doc -> $2 FROM {t} WHERE {k} = $1", t = ENTITIES_TABLE, k = KEY_COL),
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
                    &format!("SELECT doc, bevy_persistence_version FROM {t} WHERE {k} = $1", t = RESOURCES_TABLE, k = KEY_COL),
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
                .batch_execute(&format!("TRUNCATE TABLE {}", ENTITIES_TABLE))
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
                .batch_execute(&format!("TRUNCATE TABLE {}", RESOURCES_TABLE))
                .await
                .map_err(|e| PersistenceError::new(format!("pg clear_resources failed: {}", e)))?;
            Ok(())
        }
        .boxed()
    }
}