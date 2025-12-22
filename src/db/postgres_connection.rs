//! Postgres backend: implements `DatabaseConnection` using `tokio-postgres`.
//! Storage model: one table per store, keyed by id with bevy_type discriminator and jsonb payload.

use crate::db::connection::{
    BEVY_PERSISTENCE_VERSION_FIELD, BEVY_TYPE_FIELD, DatabaseConnection, DocumentKind,
    PersistenceError, TransactionOperation,
};
use crate::db::shared::{GroupedOperations, OperationType, check_operation_success};
use crate::query::filter_expression::{BinaryOperator, FilterExpression};
use crate::query::persistence_query_specification::PersistenceQuerySpecification;
use bevy::log::{debug, error, info};
use futures::FutureExt;
use futures::future::BoxFuture;
use serde_json::Value;
use std::fmt;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio_postgres::types::ToSql;
use tokio_postgres::{Client, Config, NoTls};

// Local constants to avoid magic strings
const KEY_COL: &str = "id";

// Typed parameter carrier for filter translation
enum SqlParam {
    Text(String),
    Bool(bool),
    F64(f64),
    TextArray(Vec<String>),
    BoolArray(Vec<bool>),
    F64Array(Vec<f64>),
}
impl SqlParam {
    // Make boxed params Send so futures remain Send
    fn into_box(self) -> Box<dyn ToSql + Sync + Send> {
        match self {
            SqlParam::Text(s) => Box::new(s),
            SqlParam::Bool(b) => Box::new(b),
            SqlParam::F64(n) => Box::new(n),
            SqlParam::TextArray(v) => Box::new(v),
            SqlParam::BoolArray(v) => Box::new(v),
            SqlParam::F64Array(v) => Box::new(v),
        }
    }
}

fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

#[derive(Clone)]
pub struct PostgresDbConnection {
    client: Arc<Mutex<Client>>,
}

impl fmt::Debug for PostgresDbConnection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PostgresDbConnection")
            .finish_non_exhaustive()
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
        cfg.host(host).user(user).password(pass).dbname("postgres");
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
                        return Err(PersistenceError::new(format!("pg create db failed: {}", e)));
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
        Ok(db)
    }

    async fn ensure_store_table(&self, store: &str) -> Result<String, PersistenceError> {
        if store.is_empty() {
            return Err(PersistenceError::new("store must be provided"));
        }
        let table = quote_ident(store);
        let doc_index = quote_ident(&format!("{}_doc_gin", store));
        let type_index = quote_ident(&format!("{}_type_idx", store));
        let client = self.client.lock().await;
        debug!("[pg] ensuring store table {}", table);
        let stmt = format!(
            r#"
            CREATE TABLE IF NOT EXISTS {table} (
                id TEXT PRIMARY KEY,
                bevy_type TEXT NOT NULL,
                bevy_persistence_version BIGINT NOT NULL,
                doc JSONB NOT NULL
            );
            CREATE INDEX IF NOT EXISTS {doc_idx} ON {table} USING GIN (doc jsonb_path_ops);
            CREATE INDEX IF NOT EXISTS {type_idx} ON {table} (bevy_type);
            "#,
            table = table,
            doc_idx = doc_index,
            type_idx = type_index
        );
        client
            .batch_execute(&stmt)
            .await
            .map_err(|e| PersistenceError::new(format!("pg ensure store table failed: {:?}", e)))?;
        Ok(table)
    }

    // Build WHERE and params for a spec
    fn build_where(spec: &PersistenceQuerySpecification) -> (String, Vec<SqlParam>) {
        let mut clauses: Vec<String> = Vec::new();
        let mut params: Vec<SqlParam> = Vec::new();

        // constrain bevy_type to the requested document kind
        params.push(SqlParam::Text(spec.kind.as_str().to_string()));
        clauses.push(format!("({} = ${})", BEVY_TYPE_FIELD, params.len()));

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
            let (sql, ps) = Self::translate_expr(expr, params.len());
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
    fn translate_expr(expr: &FilterExpression, offset: usize) -> (String, Vec<SqlParam>) {
        fn field_path(component: &str, field: &str) -> String {
            if field.is_empty() {
                // presence is handled specially; still return doc->'Comp' for casts
                format!("doc -> '{}'", component)
            } else {
                // text extract; cast in ops below
                format!("doc -> '{}' ->> '{}'", component, field)
            }
        }
        fn translate_rec(
            expr: &FilterExpression,
            args: &mut Vec<SqlParam>,
            offset: usize,
        ) -> String {
            match expr {
                FilterExpression::Literal(v) => {
                    let idx = offset + args.len() + 1;
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
                FilterExpression::Field {
                    component_name,
                    field_name,
                } => field_path(component_name, field_name),
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
                            let l = translate_rec(lhs, args, offset);
                            let r = translate_rec(rhs, args, offset);
                            format!("({} {} {})", l, op_str, r)
                        }
                        BinaryOperator::Eq | BinaryOperator::Ne => {
                            // Presence optimization for component-only fields against NULL
                            if let FilterExpression::Field {
                                component_name,
                                field_name,
                            } = &**lhs
                            {
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
                                let l = translate_rec(lhs, args, offset);
                                let r = translate_rec(rhs, args, offset);
                                return format!("({}) = ({}::text)", l, r);
                            }
                            if let FilterExpression::Field { field_name, .. } = &**lhs {
                                // determine RHS param type by peeking last pushed param index
                                let before = args.len();
                                let r = translate_rec(rhs, args, offset);
                                let rhs_is_bool =
                                    matches!(args.get(before), Some(SqlParam::Bool(_)));
                                let rhs_is_num = matches!(args.get(before), Some(SqlParam::F64(_)));
                                if field_name.is_empty() {
                                    // component object equality unsupported -> false
                                    "(FALSE)".to_string()
                                } else if rhs_is_bool {
                                    let l = translate_rec(lhs, &mut Vec::new(), offset); // rebuild path only
                                    format!("((({})::boolean) {} ({}::boolean))", l, op_str, r)
                                } else if rhs_is_num {
                                    let l = translate_rec(lhs, &mut Vec::new(), offset);
                                    format!(
                                        "((({})::double precision) {} ({}::double precision))",
                                        l, op_str, r
                                    )
                                } else {
                                    let l = translate_rec(lhs, &mut Vec::new(), offset);
                                    format!("(({}) {} ({}::text))", l, op_str, r)
                                }
                            } else {
                                let l = translate_rec(lhs, args, offset);
                                let r = translate_rec(rhs, args, offset);
                                format!("({}) = ({}::text)", l, r)
                            }
                        }
                        BinaryOperator::Gt
                        | BinaryOperator::Gte
                        | BinaryOperator::Lt
                        | BinaryOperator::Lte => {
                            // Force numeric compare where possible
                            let before = args.len();
                            let r = translate_rec(rhs, args, offset);
                            let rhs_is_num = matches!(args.get(before), Some(SqlParam::F64(_)));
                            let l = translate_rec(lhs, &mut Vec::new(), offset);
                            if rhs_is_num {
                                format!(
                                    "(({})::double precision {} ({}::double precision))",
                                    l, op_str, r
                                )
                            } else {
                                format!("(({})::text {} ({}::text))", l, op_str, r)
                            }
                        }
                        BinaryOperator::In => {
                            // Expect RHS to be an array literal; infer type
                            let (is_key, left_path, left_is_field, left_field_name) = match &**lhs {
                                FilterExpression::DocumentKey => {
                                    (true, KEY_COL.to_string(), false, "")
                                }
                                FilterExpression::Field {
                                    component_name,
                                    field_name,
                                } => (
                                    false,
                                    field_path(component_name, field_name),
                                    true,
                                    *field_name,
                                ),
                                _ => (false, translate_rec(lhs, args, offset), false, ""),
                            };

                            // Extract array from RHS
                            let mut arr_text: Option<Vec<String>> = None;
                            let mut arr_bool: Option<Vec<bool>> = None;
                            let mut arr_num: Option<Vec<f64>> = None;

                            if let FilterExpression::Literal(Value::Array(items)) = &**rhs {
                                // classify
                                if items.iter().all(|v| v.is_string()) {
                                    arr_text = Some(
                                        items
                                            .iter()
                                            .map(|v| v.as_str().unwrap().to_string())
                                            .collect(),
                                    );
                                } else if items.iter().all(|v| v.is_boolean()) {
                                    arr_bool =
                                        Some(items.iter().map(|v| v.as_bool().unwrap()).collect());
                                } else if items.iter().all(|v| v.is_number()) {
                                    arr_num = Some(
                                        items.iter().map(|v| v.as_f64().unwrap_or(0.0)).collect(),
                                    );
                                } else {
                                    // fallback: coerce to strings
                                    arr_text = Some(items.iter().map(|v| v.to_string()).collect());
                                }
                            } else {
                                // Non-array RHS is invalid for IN; generate FALSE
                                return "(FALSE)".to_string();
                            }

                            // Push typed array param and build ANY(...) expression
                            if is_key {
                                if let Some(v) = arr_text.take() {
                                    let idx = {
                                        args.push(SqlParam::TextArray(v));
                                        offset + args.len()
                                    };
                                    return format!("({} = ANY(${}::text[]))", KEY_COL, idx);
                                }
                                // keys are text; coerce others to text array
                                let idx = if let Some(v) = arr_num.take() {
                                    args.push(SqlParam::TextArray(
                                        v.into_iter().map(|n| n.to_string()).collect(),
                                    ));
                                    offset + args.len()
                                } else if let Some(v) = arr_bool.take() {
                                    args.push(SqlParam::TextArray(
                                        v.into_iter().map(|b| b.to_string()).collect(),
                                    ));
                                    offset + args.len()
                                } else {
                                    offset + args.len()
                                };
                                return format!("({} = ANY(${}::text[]))", KEY_COL, idx);
                            }

                            // Field-based IN
                            if left_is_field && left_field_name.is_empty() {
                                // Component object IN unsupported
                                return "(FALSE)".to_string();
                            }

                            if let Some(v) = arr_num {
                                let idx = {
                                    args.push(SqlParam::F64Array(v));
                                    offset + args.len()
                                };
                                format!(
                                    "(({})::double precision = ANY(${}::double precision[]))",
                                    left_path, idx
                                )
                            } else if let Some(v) = arr_bool {
                                let idx = {
                                    args.push(SqlParam::BoolArray(v));
                                    offset + args.len()
                                };
                                format!("(({})::boolean = ANY(${}::boolean[]))", left_path, idx)
                            } else if let Some(v) = arr_text {
                                let idx = {
                                    args.push(SqlParam::TextArray(v));
                                    offset + args.len()
                                };
                                format!("(({}) = ANY(${}::text[]))", left_path, idx)
                            } else {
                                "(FALSE)".to_string()
                            }
                        }
                    }
                }
            }
        }

        let mut params = Vec::new();
        let sql = translate_rec(expr, &mut params, offset);
        (sql, params)
    }

    // Fetch doc + version from a store table
    fn fetch_from_store(
        &self,
        store: String,
        key: String,
        kind: DocumentKind,
    ) -> BoxFuture<'static, Result<Option<(Value, u64)>, PersistenceError>> {
        let client = self.client.clone();
        let conn = self.clone();
        async move {
            let table = conn.ensure_store_table(&store).await?;
            let stmt = format!(
                "SELECT doc, bevy_persistence_version FROM {} WHERE {} = $1 AND {} = $2",
                table, KEY_COL, BEVY_TYPE_FIELD
            );
            let c = client.lock().await;
            let row_opt = c
                .query_opt(&stmt, &[&key, &kind.as_str()])
                .await
                .map_err(|e| PersistenceError::new(format!("pg fetch failed: {}", e)))?;
            if let Some(row) = row_opt {
                let doc: Value = row.get(0);
                let ver: i64 = row.get(1);
                Ok(Some((doc, ver as u64)))
            } else {
                Ok(None)
            }
        }
        .boxed()
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
        let client = self.client.clone();
        let conn = self.clone();
        let store = spec.store.clone();
        let table = quote_ident(&store);
        bevy::log::debug!(
            "[pg] execute_keys: table={} params_len={}",
            table,
            params.len()
        );
        async move {
            let table = conn.ensure_store_table(&store).await?;
            let sql = format!(
                "SELECT {k} FROM {t} WHERE {w}",
                k = KEY_COL,
                t = table,
                w = where_sql
            );
            let client = client.lock().await;
            let boxed: Vec<Box<dyn ToSql + Sync + Send>> =
                params.into_iter().map(|p| p.into_box()).collect();
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
        let spec = spec.clone();
        let (where_sql, params) = Self::build_where(&spec);
        let client = self.client.clone();
        let conn = self.clone();
        bevy::log::debug!(
            "[pg] execute_documents: store={} fetch_only={:?}; return_full_docs={}; params_len={}",
            spec.store,
            spec.fetch_only,
            spec.return_full_docs,
            params.len()
        );
        async move {
            let table = conn.ensure_store_table(&spec.store).await?;

            let sql = if spec.return_full_docs {
                format!(
                    "SELECT (doc || jsonb_build_object('{k}', {k}, '{ver}', bevy_persistence_version)) AS doc \
                     FROM {t} WHERE {w}",
                    k = KEY_COL,
                    ver = BEVY_PERSISTENCE_VERSION_FIELD,
                    t = table,
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
                proj.push_str(&format!(")) AS doc FROM {t} WHERE {w}", t = table, w = where_sql));
                proj
            } else {
                format!(
                    "SELECT jsonb_build_object('{k}', {k}, '{ver}', bevy_persistence_version) AS doc \
                     FROM {t} WHERE {w}",
                    k = KEY_COL,
                    ver = BEVY_PERSISTENCE_VERSION_FIELD,
                    t = table,
                    w = where_sql
                )
            };

            let client = client.lock().await;
            let boxed: Vec<Box<dyn ToSql + Sync + Send>> =
                params.into_iter().map(|p| p.into_box()).collect();
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

    fn count_documents(
        &self,
        spec: &PersistenceQuerySpecification,
    ) -> BoxFuture<'static, Result<usize, PersistenceError>> {
        let spec = spec.clone();
        let (where_sql, params) = Self::build_where(&spec);
        let client = self.client.clone();
        let conn = self.clone();
        bevy::log::debug!(
            "[pg] count_documents: store={} params_len={}",
            spec.store,
            params.len()
        );
        async move {
            let table = conn.ensure_store_table(&spec.store).await?;
            let sql = format!(
                "SELECT COUNT(*) FROM {t} WHERE {w}",
                t = table,
                w = where_sql
            );
            let client = client.lock().await;
            let boxed: Vec<Box<dyn ToSql + Sync + Send>> =
                params.into_iter().map(|p| p.into_box()).collect();
            let param_refs: Vec<&(dyn ToSql + Sync)> =
                boxed.iter().map(|b| &**b as &(dyn ToSql + Sync)).collect();
            let row = client
                .query_one(&sql, param_refs.as_slice())
                .await
                .map_err(|e| PersistenceError::new(format!("pg count_documents failed: {}", e)))?;
            let count: i64 = row.get(0);
            Ok(count as usize)
        }
        .boxed()
    }

    fn execute_transaction(
        &self,
        operations: Vec<TransactionOperation>,
    ) -> futures::future::BoxFuture<'static, Result<Vec<String>, PersistenceError>> {
        let client_arc = self.client.clone();
        let conn = self.clone();
        async move {
            let first = operations.get(0).ok_or_else(|| PersistenceError::new("execute_transaction requires at least one operation"))?;
            let store = first.store().to_string();
            if store.is_empty() {
                return Err(PersistenceError::new("store must be non-empty"));
            }
            if operations.iter().any(|op| op.store() != store) {
                return Err(PersistenceError::new("all operations in a transaction must target the same store"));
            }

            let table = conn.ensure_store_table(&store).await?;

            let mut client = client_arc.lock().await;
            let tx = client
                .transaction()
                .await
                .map_err(|e| PersistenceError::new(format!("pg START TRANSACTION failed: {}", e)))?;

            let groups = GroupedOperations::from_operations(operations, KEY_COL);
            let mut new_entity_ids: Vec<String> = Vec::new();

            async fn run_update(
                tx: &tokio_postgres::Transaction<'_>,
                values: &[serde_json::Value],
                kind: DocumentKind,
                table: &str,
                store: &str,
            ) -> Result<Vec<String>, PersistenceError> {
                if values.is_empty() {
                    return Ok(Vec::new());
                }
                let upd_sql = format!(
                    r#"
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
                        WHERE e.{k} = i.{k} AND e.bevy_type = $2 AND e.bevy_persistence_version = i.expected
                        RETURNING e.{k}
                    )
                    SELECT {k} FROM updated
                "#,
                    k = KEY_COL,
                    t = table
                );

                let rows = tx
                    .query(&upd_sql, &[&serde_json::Value::Array(values.to_vec()), &kind.as_str()])
                    .await
                    .map_err(|e| PersistenceError::new(format!("pg batch update ({}) failed: {}", store, e)))?;
                Ok(rows.into_iter().map(|r| r.get::<_, String>(0)).collect())
            }

            async fn run_delete(
                tx: &tokio_postgres::Transaction<'_>,
                values: &[serde_json::Value],
                kind: DocumentKind,
                table: &str,
                store: &str,
            ) -> Result<Vec<String>, PersistenceError> {
                if values.is_empty() {
                    return Ok(Vec::new());
                }
                let del_sql = format!(
                    r#"
                    WITH input AS (
                        SELECT (x->>'{k}')::text        AS {k},
                               (x->>'expected')::bigint AS expected
                        FROM jsonb_array_elements($1::jsonb) AS x
                    ),
                    deleted AS (
                        DELETE FROM {t} e
                        USING input i
                        WHERE e.{k} = i.{k} AND e.bevy_type = $2 AND e.bevy_persistence_version = i.expected
                        RETURNING e.{k}
                    )
                    SELECT {k} FROM deleted
                "#,
                    k = KEY_COL,
                    t = table
                );

                let rows = tx
                    .query(&del_sql, &[&serde_json::Value::Array(values.to_vec()), &kind.as_str()])
                    .await
                    .map_err(|e| PersistenceError::new(format!("pg batch delete ({}) failed: {}", store, e)))?;
                Ok(rows.into_iter().map(|r| r.get::<_, String>(0)).collect())
            }

            // Entity creates
            if !groups.entity_creates.is_empty() {
                use uuid::Uuid;
                let ids: Vec<String> = groups
                    .entity_creates
                    .iter()
                    .map(|_| Uuid::new_v4().to_string())
                    .collect();

                let ver_field = BEVY_PERSISTENCE_VERSION_FIELD;
                let input_docs: Vec<serde_json::Value> = groups
                    .entity_creates
                    .iter()
                    .cloned()
                    .zip(ids.iter())
                    .map(|(doc, id)| {
                        let ver = doc.get(ver_field).and_then(|v| v.as_i64()).unwrap_or(1);
                        serde_json::json!({
                            "id": id,
                            "ver": ver,
                            "kind": DocumentKind::Entity.as_str(),
                            "doc": doc
                        })
                    })
                    .collect();
                let input_json = serde_json::Value::Array(input_docs);

                let sql = format!(
                    r#"
                    WITH input AS (
                        SELECT (x->>'{k}')::text      AS {k},
                               (x->>'ver')::bigint    AS ver,
                               (x->>'kind')::text     AS kind,
                               (x->'doc')::jsonb      AS doc
                        FROM jsonb_array_elements($1::jsonb) AS x
                    ),
                    inserted AS (
                        INSERT INTO {t} ({k}, bevy_type, bevy_persistence_version, doc)
                        SELECT {k}, kind, ver, doc FROM input
                        RETURNING {k}
                    )
                    SELECT {k} FROM inserted
                "#,
                    k = KEY_COL,
                    t = table
                );

                tx.query(&sql, &[&input_json])
                    .await
                    .map_err(|e| PersistenceError::new(format!("pg batch insert ({}) failed: {}", store, e)))?;

                new_entity_ids = ids;
            }

            // Entity updates/deletes
            if !groups.entity_updates.is_empty() {
                let requested = groups.extract_keys(&groups.entity_updates, KEY_COL);
                let updated = run_update(&tx, &groups.entity_updates, DocumentKind::Entity, &table, &store).await?;
                if let Err(e) = check_operation_success(requested, updated, &OperationType::Update, &store) {
                    let _ = tx.rollback().await;
                    return Err(e);
                }
            }
            if !groups.entity_deletes.is_empty() {
                let requested = groups.extract_keys(&groups.entity_deletes, KEY_COL);
                let deleted = run_delete(&tx, &groups.entity_deletes, DocumentKind::Entity, &table, &store).await?;
                if let Err(e) = check_operation_success(requested, deleted, &OperationType::Delete, &store) {
                    let _ = tx.rollback().await;
                    return Err(e);
                }
            }

            // Resource creates
            if !groups.resource_creates.is_empty() {
                let ver_field = BEVY_PERSISTENCE_VERSION_FIELD;
                let input_docs: Vec<serde_json::Value> = groups
                    .resource_creates
                    .iter()
                    .cloned()
                    .map(|doc| {
                        let id = doc
                            .get("id")
                            .and_then(|v| v.as_str())
                            .ok_or_else(|| PersistenceError::new("Resource create missing id"))?
                            .to_string();
                        let ver = doc.get(ver_field).and_then(|v| v.as_i64()).unwrap_or(1);
                        Ok(serde_json::json!({
                            "id": id,
                            "ver": ver,
                            "kind": DocumentKind::Resource.as_str(),
                            "doc": doc
                        }))
                    })
                    .collect::<Result<_, PersistenceError>>()?;
                let input_json = serde_json::Value::Array(input_docs);

                let sql = format!(
                    r#"
                    WITH input AS (
                        SELECT (x->>'{k}')::text      AS {k},
                               (x->>'ver')::bigint    AS ver,
                               (x->>'kind')::text     AS kind,
                               (x->'doc')::jsonb      AS doc
                        FROM jsonb_array_elements($1::jsonb) AS x
                    ),
                    inserted AS (
                        INSERT INTO {t} ({k}, bevy_type, bevy_persistence_version, doc)
                        SELECT {k}, kind, ver, doc FROM input
                        RETURNING {k}
                    )
                    SELECT {k} FROM inserted
                "#,
                    k = KEY_COL,
                    t = table
                );

                tx.execute(&sql, &[&input_json])
                    .await
                    .map_err(|e| PersistenceError::new(format!("pg batch insert ({}) failed: {}", store, e)))?;
            }

            // Resource updates/deletes
            if !groups.resource_updates.is_empty() {
                let requested = groups.extract_keys(&groups.resource_updates, KEY_COL);
                let updated = run_update(&tx, &groups.resource_updates, DocumentKind::Resource, &table, &store).await?;
                if let Err(e) = check_operation_success(requested, updated, &OperationType::Update, &store) {
                    let _ = tx.rollback().await;
                    return Err(e);
                }
            }
            if !groups.resource_deletes.is_empty() {
                let requested = groups.extract_keys(&groups.resource_deletes, KEY_COL);
                let deleted = run_delete(&tx, &groups.resource_deletes, DocumentKind::Resource, &table, &store).await?;
                if let Err(e) = check_operation_success(requested, deleted, &OperationType::Delete, &store) {
                    let _ = tx.rollback().await;
                    return Err(e);
                }
            }

            tx.commit()
                .await
                .map_err(|e| PersistenceError::new(format!("pg COMMIT failed: {}", e)))?;
            Ok(new_entity_ids)
        }
        .boxed()
    }

    fn fetch_document(
        &self,
        store: &str,
        entity_key: &str,
    ) -> BoxFuture<'static, Result<Option<(Value, u64)>, PersistenceError>> {
        self.fetch_from_store(
            store.to_string(),
            entity_key.to_string(),
            DocumentKind::Entity,
        )
    }

    fn fetch_component(
        &self,
        store: &str,
        entity_key: &str,
        comp_name: &str,
    ) -> BoxFuture<'static, Result<Option<Value>, PersistenceError>> {
        let key = entity_key.to_string();
        let comp = comp_name.to_string();
        let store_name = store.to_string();
        let conn = self.clone();
        let client = self.client.clone();
        async move {
            let table = conn.ensure_store_table(&store_name).await?;
            debug!(
                "[pg] fetch_component store={} key={} comp={}",
                store_name, key, comp
            );
            let stmt = format!(
                "SELECT doc -> $2 FROM {t} WHERE {k} = $1 AND {type_col} = $3",
                t = table,
                k = KEY_COL,
                type_col = BEVY_TYPE_FIELD
            );
            let client = client.lock().await;
            let row_opt = client
                .query_opt(&stmt, &[&key, &comp, &DocumentKind::Entity.as_str()])
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
        store: &str,
        resource_name: &str,
    ) -> BoxFuture<'static, Result<Option<(Value, u64)>, PersistenceError>> {
        self.fetch_from_store(
            store.to_string(),
            resource_name.to_string(),
            DocumentKind::Resource,
        )
    }

    fn clear_store(
        &self,
        store: &str,
        kind: DocumentKind,
    ) -> BoxFuture<'static, Result<(), PersistenceError>> {
        let store_name = store.to_string();
        let conn = self.clone();
        let client = self.client.clone();
        async move {
            let table = conn.ensure_store_table(&store_name).await?;
            let stmt = format!(
                "DELETE FROM {t} WHERE {type_col} = $1",
                t = table,
                type_col = BEVY_TYPE_FIELD
            );
            let client = client.lock().await;
            client
                .execute(&stmt, &[&kind.as_str()])
                .await
                .map_err(|e| PersistenceError::new(format!("pg clear_store failed: {}", e)))?;
            Ok(())
        }
        .boxed()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::filter_expression::FilterExpression;
    use crate::query::persistence_query_specification::PersistenceQuerySpecification;

    /// Helper: run the private build_where and count params
    fn build_where(spec: PersistenceQuerySpecification) -> (String, usize) {
        let (sql, params) = PostgresDbConnection::build_where(&spec);
        (sql, params.len())
    }

    #[test]
    fn presence_only() {
        let mut spec = PersistenceQuerySpecification::default();
        spec.presence_with = vec!["Health"];
        let (sql, p) = build_where(spec);
        assert!(sql.contains("doc ? 'Health'"));
        assert_eq!(p, 1);
    }

    #[test]
    fn presence_without_only() {
        let mut spec = PersistenceQuerySpecification::default();
        spec.presence_without = vec!["Creature"];
        let (sql, p) = build_where(spec);
        assert!(sql.contains("NOT (doc ? 'Creature')"));
        assert_eq!(p, 1);
    }

    #[test]
    fn value_filter_generates_param_and_expr() {
        let mut spec = PersistenceQuerySpecification::default();
        spec.value_filters = Some(FilterExpression::field("Position", "x").lt(3.5));
        let (sql, p) = build_where(spec);
        assert!(sql.contains("<"));
        assert!(sql.contains("$1"));
        assert_eq!(p, 2);
    }

    #[test]
    fn or_value_filter_generates_two_params() {
        let mut spec = PersistenceQuerySpecification::default();
        let f1 = FilterExpression::DocumentKey.eq("a");
        let f2 = FilterExpression::DocumentKey.eq("b");
        spec.value_filters = Some(f1.or(f2));
        let (sql, p) = build_where(spec);
        assert!(sql.contains("OR"));
        assert_eq!(p, 3);
    }

    #[test]
    fn empty_spec_maps_to_true() {
        let spec = PersistenceQuerySpecification::default();
        let (sql, p) = build_where(spec);
        assert!(sql.contains(BEVY_TYPE_FIELD));
        assert_eq!(p, 1);
    }

    #[test]
    fn in_operator_generates_array_param_any_clause() {
        let mut spec = PersistenceQuerySpecification::default();
        // key IN ('a','b','c')
        spec.value_filters = Some(FilterExpression::DocumentKey.in_(vec!["a", "b", "c"]));
        let (sql, pcount) = build_where(spec);
        assert!(sql.contains("ANY("), "SQL should use ANY(...) for IN");
        assert_eq!(pcount, 2, "one bevy_type param plus array param expected");
    }
}
