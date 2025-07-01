//! A builder for loading entities from ArangoDB into an ArangoSession.
//!
//! You call `.with::<T>()` to request components, and `.filter("…")`
//! to inject raw AQL snippets. Later phases will construct full AQL
//! and fetch matching entity IDs and component data.

use std::collections::HashMap;
use std::sync::Arc;
use serde_json::Value;
use crate::DatabaseConnection;

/// Trait for types that can be queried as a component in AQL.
pub trait QueryComponent {
    /// The ArangoDB document field name for this component.
    fn name() -> &'static str;
}

/// AQL query builder: select which components and filters to apply.
pub struct ArangoQuery {
    db: Arc<dyn DatabaseConnection>,
    pub component_names: Vec<&'static str>,
    pub filters: Vec<String>,
}

impl ArangoQuery {
    /// Start a new query backed by a shared database connection.
    pub fn new(db: Arc<dyn DatabaseConnection>) -> Self {
        Self {
            db,
            component_names: Vec::new(),
            filters: Vec::new(),
        }
    }

    /// Request loading component `T`.
    pub fn with<T: QueryComponent>(mut self) -> Self {
        self.component_names.push(T::name());
        self
    }

    /// Inject a raw AQL filter clause (e.g. `doc.age > 10`).
    pub fn filter(mut self, clause: &str) -> Self {
        self.filters.push(clause.to_string());
        self
    }

    /// Construct the AQL and bind-variables tuple.
    fn build_aql(&self) -> (String, HashMap<String, Value>) {
        // base FOR clause
        let mut aql = "FOR doc IN entities".to_string();
        // component presence filters
        if !self.component_names.is_empty() {
            let presences = self.component_names
                .iter()
                .map(|name| format!("doc.{name} != null"))
                .collect::<Vec<_>>()
                .join(" && ");
            aql.push_str(&format!("\n  FILTER {presences}"));
        }
        // raw filters
        for clause in &self.filters {
            aql.push_str(&format!("\n  FILTER {clause}"));
        }
        aql.push_str("\n  RETURN doc._key");
        (aql, HashMap::new())
    }

    /// Run the AQL, fetch matching keys, and return them.
    pub fn fetch_ids(&self) -> Vec<String> {
        let (aql, bind_vars) = self.build_aql();
        let result = futures::executor::block_on(self.db.query_arango(aql, bind_vars))
            .expect("AQL query failed");
        result
    }

    /// Load matching entities into `session.local_world`.
    /// Returns all matching (old + new) entities.
    pub fn fetch_into(&self, session: &mut crate::ArangoSession) -> Vec<bevy::prelude::Entity> {
        // 1) run AQL to get keys
        let keys = self.fetch_ids();
        let mut result = Vec::new();
        for key in keys.iter() {
            // skip already loaded
            if session.loaded_entities.iter().any(|e| e.index().to_string() == *key) {
                continue;
            }
            // spawn in local cache
            let e = session.local_world.spawn(()).id();
            // for each requested component name, fetch and insert
            for &comp in &self.component_names {
                let jsonv = futures::executor::block_on(
                    self.db.fetch_component(key, comp)
                ).expect("fetch_component failed");
                if let Some(_val) = jsonv {
                    // deserialize into T and insert; omitted here
                    // TODO: deserialze & insert into `e`
                }
            }
            session.mark_loaded(e);
            result.push(e);
        }
        // return all loaded entities
        session.loaded_entities.iter().cloned().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ArangoSession, DatabaseConnection, MockDatabaseConnection};
    use serde_json::json;
    use std::sync::Arc;

    // Dummy components for skeleton tests
    struct A; struct B;
    impl QueryComponent for A { fn name() -> &'static str { "A" } }
    impl QueryComponent for B { fn name() -> &'static str { "B" } }

    #[test]
    fn build_query_skeleton() {
        let db = Arc::new(MockDatabaseConnection::new());
        let q = ArangoQuery::new(db.clone())
            .with::<A>()
            .with::<B>()
            .filter("doc.value > 5");

        assert_eq!(q.component_names, vec!["A", "B"]);
        assert_eq!(q.filters, vec!["doc.value > 5"]);
    }

    #[test]
    fn fetch_ids_invokes_query_arango() {
        let mut mock_db = MockDatabaseConnection::new();
        // Expect the correct AQL string
        mock_db
            .expect_query_arango()
            .withf(|aql, vars| {
                aql.contains("FOR doc IN entities")
                    && aql.contains("doc.A != null")
                    && aql.contains("doc.B != null")
                    && aql.contains("doc.value > 5")
                    && vars.is_empty()
            })
            .times(1)
            .returning(|_, _| {
                let keys = vec!["e1".to_string(), "e2".to_string()];
                Box::pin(async move { Ok(keys) })
            });

        let db_arc: Arc<dyn DatabaseConnection> = Arc::new(mock_db);
        let q = ArangoQuery::new(db_arc.clone())
            .with::<A>()
            .with::<B>()
            .filter("doc.value > 5");
        let ids = q.fetch_ids();
        assert_eq!(ids, vec!["e1".to_string(), "e2".to_string()]);
    }

    // Real component types for fetch_into
    #[allow(dead_code)]
    #[derive(bevy::prelude::Component, serde::Deserialize)]
    struct Health { value: i32 }
    #[allow(dead_code)]
    #[derive(bevy::prelude::Component, serde::Deserialize)]
    struct Position { x: f32, y: f32, z: f32 }

    impl QueryComponent for Health { fn name() -> &'static str { "Health" } }
    impl QueryComponent for Position { fn name() -> &'static str { "Position" } }

    #[test]
    fn fetch_into_loads_new_entities() {
        let mut mock_db = MockDatabaseConnection::new();
        mock_db.expect_query_arango()
            .returning(|_, _| Box::pin(async { Ok(vec!["k1".into(), "k2".into()]) }));
        mock_db.expect_fetch_component()
            .withf(|k, comp| (k == "k1" || k == "k2") && comp=="Health")
            .returning(|_, _| Box::pin(async { Ok(Some(json!({"value":10}))) }));
        mock_db.expect_fetch_component()
            .withf(|k, comp| (k == "k1" || k == "k2") && comp=="Position")
            .returning(|_, _| Box::pin(async { Ok(Some(json!({"x":1.0,"y":2.0,"z":3.0}))) }));

        let db_arc: Arc<dyn DatabaseConnection> = Arc::new(mock_db);
        let mut session = ArangoSession::new_mocked(db_arc.clone());
        let query = ArangoQuery::new(db_arc)
            .with::<Health>()
            .with::<Position>();
        let loaded = query.fetch_into(&mut session);

        assert_eq!(loaded.len(), 2);
        assert_eq!(session.loaded_entities.len(), 2);
    }
}
