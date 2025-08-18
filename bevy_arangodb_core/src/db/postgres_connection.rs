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
    ) -> futures::future::BoxFuture<'static, Result<Vec<String>, PersistenceError>> {
        let client_arc = self.client.clone();
        async move {
            // Acquire client and begin a transaction
            let mut client = client_arc.lock().await;
            let tx = client
                .transaction()
                .await
                .map_err(|e| PersistenceError::new(format!("pg START TRANSACTION failed: {}", e)))?;

            // Group ops
            let mut ent_creates: Vec<serde_json::Value> = Vec::new();
            let mut ent_updates: Vec<serde_json::Value> = Vec::new(); // { id, expected, patch }
            let mut ent_deletes: Vec<serde_json::Value> = Vec::new(); // { id, expected }

            let mut res_creates: Vec<serde_json::Value> = Vec::new();
            let mut res_updates: Vec<serde_json::Value> = Vec::new();
            let mut res_deletes: Vec<serde_json::Value> = Vec::new();

            for op in operations {
                match op {
                    TransactionOperation::CreateDocument { collection, data } => match collection {
                        Collection::Entities => ent_creates.push(data),
                        Collection::Resources => res_creates.push(data),
                    },
                    TransactionOperation::UpdateDocument {
                        collection,
                        key,
                        expected_current_version,
                        patch,
                    } => {
                        let rec = serde_json::json!({
                            "id": key,
                            "expected": expected_current_version,
                            "patch": patch
                        });
                        match collection {
                            Collection::Entities => ent_updates.push(rec),
                            Collection::Resources => res_updates.push(rec),
                        }
                    }
                    TransactionOperation::DeleteDocument {
                        collection,
                        key,
                        expected_current_version,
                    } => {
                        let rec = serde_json::json!({ "id": key, "expected": expected_current_version });
                        match collection {
                            Collection::Entities => ent_deletes.push(rec),
                            Collection::Resources => res_deletes.push(rec),
                        }
                    }
                }
            }

            let mut new_entity_ids: Vec<String> = Vec::new();

            // 1) Entity creates: generate IDs client-side, single INSERT FROM jsonb array
            if !ent_creates.is_empty() {
                use uuid::Uuid;
                let ids: Vec<String> = ent_creates
                    .iter()
                    .map(|_| Uuid::new_v4().to_string())
                    .collect();

                let ver_field = BEVY_PERSISTENCE_VERSION_FIELD;
                let input_docs: Vec<serde_json::Value> = ent_creates
                    .into_iter()
                    .zip(ids.iter())
                    .map(|(doc, id)| {
                        let ver = doc.get(ver_field).and_then(|v| v.as_i64()).unwrap_or(1);
                        serde_json::json!({ "id": id, "ver": ver, "doc": doc })
                    })
                    .collect();
                let input_json = serde_json::Value::Array(input_docs);

                let sql = r#"
                    WITH input AS (
                        SELECT (x->>'id')::text      AS id,
                               (x->>'ver')::bigint    AS ver,
                               (x->'doc')::jsonb      AS doc
                        FROM jsonb_array_elements($1::jsonb) AS x
                    )
                    INSERT INTO entities (id, bevy_persistence_version, doc)
                    SELECT id, ver, doc FROM input
                "#;

                tx.execute(sql, &[&input_json])
                    .await
                    .map_err(|e| PersistenceError::new(format!("pg batch insert (entities) failed: {}", e)))?;

                new_entity_ids = ids;
            }

            // 2) Entity updates: optimistic check, merge JSONB, return ids updated
            if !ent_updates.is_empty() {
                let requested: Vec<String> = ent_updates
                    .iter()
                    .filter_map(|v| v.get("id").and_then(|s| s.as_str()).map(|s| s.to_string()))
                    .collect();

                let upd_sql = r#"
                    WITH input AS (
                        SELECT (x->>'id')::text        AS id,
                               (x->>'expected')::bigint AS expected,
                               (x->'patch')::jsonb      AS patch
                        FROM jsonb_array_elements($1::jsonb) AS x
                    ),
                    updated AS (
                        UPDATE entities e
                        SET doc = e.doc || i.patch,
                            bevy_persistence_version = i.expected + 1
                        FROM input i
                        WHERE e.id = i.id AND e.bevy_persistence_version = i.expected
                        RETURNING e.id
                    )
                    SELECT id FROM updated
                "#;

                let rows = tx
                    .query(upd_sql, &[&serde_json::Value::Array(ent_updates.clone())])
                    .await
                    .map_err(|e| PersistenceError::new(format!("pg batch update (entities) failed: {}", e)))?;
                let updated: Vec<String> = rows.into_iter().map(|r| r.get::<_, String>(0)).collect();

                if updated.len() != requested.len() {
                    if let Some(conflict_key) = requested.into_iter().find(|k| !updated.iter().any(|u| u == k)) {
                        tx.rollback().await.ok();
                        return Err(PersistenceError::Conflict { key: conflict_key });
                    } else {
                        tx.rollback().await.ok();
                        return Err(PersistenceError::new("Update conflict (entities)"));
                    }
                }
            }

            // 3) Entity deletes: optimistic check
            if !ent_deletes.is_empty() {
                let requested: Vec<String> = ent_deletes
                    .iter()
                    .filter_map(|v| v.get("id").and_then(|s| s.as_str()).map(|s| s.to_string()))
                    .collect();

                let del_sql = r#"
                    WITH input AS (
                        SELECT (x->>'id')::text        AS id,
                               (x->>'expected')::bigint AS expected
                        FROM jsonb_array_elements($1::jsonb) AS x
                    ),
                    deleted AS (
                        DELETE FROM entities e
                        USING input i
                        WHERE e.id = i.id AND e.bevy_persistence_version = i.expected
                        RETURNING e.id
                    )
                    SELECT id FROM deleted
                "#;

                let rows = tx
                    .query(del_sql, &[&serde_json::Value::Array(ent_deletes.clone())])
                    .await
                    .map_err(|e| PersistenceError::new(format!("pg batch delete (entities) failed: {}", e)))?;
                let deleted: Vec<String> = rows.into_iter().map(|r| r.get::<_, String>(0)).collect();

                if deleted.len() != requested.len() {
                    if let Some(conflict_key) = requested.into_iter().find(|k| !deleted.iter().any(|u| u == k)) {
                        tx.rollback().await.ok();
                        return Err(PersistenceError::Conflict { key: conflict_key });
                    } else {
                        tx.rollback().await.ok();
                        return Err(PersistenceError::new("Delete conflict (entities)"));
                    }
                }
            }

            // 4) Resource creates
            if !res_creates.is_empty() {
                let ver_field = BEVY_PERSISTENCE_VERSION_FIELD;
                let input_docs: Vec<serde_json::Value> = res_creates
                    .into_iter()
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

                let sql = r#"
                    WITH input AS (
                        SELECT (x->>'id')::text      AS id,
                               (x->>'ver')::bigint    AS ver,
                               (x->'doc')::jsonb      AS doc
                        FROM jsonb_array_elements($1::jsonb) AS x
                    )
                    INSERT INTO resources (id, bevy_persistence_version, doc)
                    SELECT id, ver, doc FROM input
                "#;

                tx.execute(sql, &[&input_json])
                    .await
                    .map_err(|e| PersistenceError::new(format!("pg batch insert (resources) failed: {}", e)))?;
            }

            // 5) Resource updates
            if !res_updates.is_empty() {
                let requested: Vec<String> = res_updates
                    .iter()
                    .filter_map(|v| v.get("id").and_then(|s| s.as_str()).map(|s| s.to_string()))
                    .collect();

                let upd_sql = r#"
                    WITH input AS (
                        SELECT (x->>'id')::text        AS id,
                               (x->>'expected')::bigint AS expected,
                               (x->'patch')::jsonb      AS patch
                        FROM jsonb_array_elements($1::jsonb) AS x
                    ),
                    updated AS (
                        UPDATE resources r
                        SET doc = r.doc || i.patch,
                            bevy_persistence_version = i.expected + 1
                        FROM input i
                        WHERE r.id = i.id AND r.bevy_persistence_version = i.expected
                        RETURNING r.id
                    )
                    SELECT id FROM updated
                "#;

                let rows = tx
                    .query(upd_sql, &[&serde_json::Value::Array(res_updates.clone())])
                    .await
                    .map_err(|e| PersistenceError::new(format!("pg batch update (resources) failed: {}", e)))?;
                let updated: Vec<String> = rows.into_iter().map(|r| r.get::<_, String>(0)).collect();

                if updated.len() != requested.len() {
                    if let Some(conflict_key) = requested.into_iter().find(|k| !updated.iter().any(|u| u == k)) {
                        tx.rollback().await.ok();
                        return Err(PersistenceError::Conflict { key: conflict_key });
                    } else {
                        tx.rollback().await.ok();
                        return Err(PersistenceError::new("Update conflict (resources)"));
                    }
                }
            }

            // 6) Resource deletes
            if !res_deletes.is_empty() {
                let requested: Vec<String> = res_deletes
                    .iter()
                    .filter_map(|v| v.get("id").and_then(|s| s.as_str()).map(|s| s.to_string()))
                    .collect();

                let del_sql = r#"
                    WITH input AS (
                        SELECT (x->>'id')::text        AS id,
                               (x->>'expected')::bigint AS expected
                        FROM jsonb_array_elements($1::jsonb) AS x
                    ),
                    deleted AS (
                        DELETE FROM resources r
                        USING input i
                        WHERE r.id = i.id AND r.bevy_persistence_version = i.expected
                        RETURNING r.id
                    )
                    SELECT id FROM deleted
                "#;

                let rows = tx
                    .query(del_sql, &[&serde_json::Value::Array(res_deletes.clone())])
                    .await
                    .map_err(|e| PersistenceError::new(format!("pg batch delete (resources) failed: {}", e)))?;
                let deleted: Vec<String> = rows.into_iter().map(|r| r.get::<_, String>(0)).collect();

                if deleted.len() != requested.len() {
                    if let Some(conflict_key) = requested.into_iter().find(|k| !deleted.iter().any(|u| u == k)) {
                        tx.rollback().await.ok();
                        return Err(PersistenceError::Conflict { key: conflict_key });
                    } else {
                        tx.rollback().await.ok();
                        return Err(PersistenceError::new("Delete conflict (resources)"));
                    }
                }
            }

            // COMMIT and return entity ids (for Guid assignment)
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