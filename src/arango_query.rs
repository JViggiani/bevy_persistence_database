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
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use crate::MockDatabaseConnection;

    /// Dummy components for testing
    struct A; struct B;
    impl QueryComponent for A { fn name() -> &'static str { "A" }}
    impl QueryComponent for B { fn name() -> &'static str { "B" }}

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
        let q = ArangoQuery::new(db_arc)
            .with::<A>()
            .with::<B>()
            .filter("doc.value > 5");
        let ids = q.fetch_ids();
        assert_eq!(ids, vec!["e1".to_string(), "e2".to_string()]);
    }
}
