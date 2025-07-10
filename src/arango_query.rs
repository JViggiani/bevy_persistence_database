//! A builder for loading entities from ArangoDB into an ArangoSession.
//!
//! You call `.with::<T>()` to request components, and `.filter("…")`
//! to inject raw AQL snippets. Later phases will construct full AQL
//! and fetch matching entity IDs and component data.

use std::collections::HashMap;
use std::sync::Arc;
use serde_json::Value;
use crate::{DatabaseConnection, Guid, Collection, Persist};
use bevy::prelude::Component;

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
    pub fn with<T: Component + Persist>(mut self) -> Self {
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
        let mut aql = format!("FOR doc IN {}", Collection::Entities);
        if self.component_names.is_empty() && self.filters.is_empty() {
            // no components or filters: match all with a benign filter
            aql.push_str("\n  FILTER true");
        } else {
            if !self.component_names.is_empty() {
                let presences = self.component_names
                    .iter()
                    .map(|name| format!("doc.`{}` != null", name))
                    .collect::<Vec<_>>()
                    .join(" AND ");
                aql.push_str(&format!("\n  FILTER {}", presences));
            }
            // raw filters
            for clause in &self.filters {
                aql.push_str(&format!("\n  FILTER {}", clause));
            }
        }
        aql.push_str("\n  RETURN doc._key");
        (aql, HashMap::new())
    }

    /// Run the AQL, fetch matching keys, and return them.
    pub async fn fetch_ids(&self) -> Vec<String> {
        let (aql, bind_vars) = self.build_aql();
        let result = self.db.query(aql, bind_vars).await
            .expect("AQL query failed");
        result
    }

    /// Load matching entities into `session.local_world`.
    /// Returns all matching (old + new) entities.
    pub async fn fetch_into(&self, session: &mut crate::ArangoSession) -> Vec<bevy::prelude::Entity> {
        // 1) run AQL to get keys
        let keys = self.fetch_ids().await;
        let mut result = Vec::new();

        // Build a map of existing entities by Guid for quick lookup
        let mut existing_entities_by_guid: HashMap<String, bevy::prelude::Entity> = HashMap::new();
        let mut query = session.local_world.query::<(bevy::prelude::Entity, &Guid)>();
        for (entity, guid) in query.iter(&session.local_world) {
            existing_entities_by_guid.insert(guid.id().to_string(), entity);
        }

        for key in keys.iter() {
            let e = if let Some(existing_entity) = existing_entities_by_guid.get(key) {
                *existing_entity
            } else {
                let new_e = session.local_world.spawn(Guid::new(key.clone())).id();
                existing_entities_by_guid.insert(key.clone(), new_e);
                new_e
            };

            // for each requested component name, fetch and insert
            for &comp_name in &self.component_names {
                let jsonv = self.db.fetch_component(key, comp_name).await
                    .expect("fetch_component failed");
                if let Some(val) = jsonv {
                    if let Some(deserializer) = session.component_deserializers.get(comp_name) {
                        deserializer(&mut session.local_world, e, val).expect("deserialization failed");
                    }
                }
            }
            session.mark_loaded(e);
            result.push(e);
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ArangoSession, DatabaseConnection, ArangoError};
    use crate::arango_session::MockDatabaseConnection;
    use serde::{Deserialize, Serialize};
    use serde_json::json;
    use std::sync::Arc;

    // Dummy components for skeleton tests
    #[derive(Component, Serialize, Deserialize)]
    struct A;
    impl Persist for A {}
    #[derive(Component, Serialize, Deserialize)]
    struct B;
    impl Persist for B {}

    #[test]
    fn build_query_skeleton() {
        let db = Arc::new(MockDatabaseConnection::new());
        let q = ArangoQuery::new(db.clone())
            .with::<A>()
            .with::<B>()
            .filter("doc.value > 5");

        assert_eq!(q.component_names, vec![A::name(), B::name()]);
        assert_eq!(q.filters, vec!["doc.value > 5"]);
    }

    #[test]
    fn fetch_ids_invokes_query_arango() {
        let mut mock_db = MockDatabaseConnection::new();
        // Expect the correct AQL string
        mock_db
            .expect_query()
            .withf(|aql, vars| {
                aql.contains("FOR doc IN entities")
                    && aql.contains(&format!("doc.`{}` != null", A::name()))
                    && aql.contains(&format!("doc.`{}` != null", B::name()))
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
        let ids = futures::executor::block_on(q.fetch_ids());
        assert_eq!(ids, vec!["e1".to_string(), "e2".to_string()]);
    }

    // Real component types for fetch_into
    #[derive(bevy::prelude::Component, serde::Deserialize, Serialize)]
    struct Health { value: i32 }
    #[derive(bevy::prelude::Component, serde::Deserialize, Serialize)]
    struct Position { x: f32, y: f32, z: f32 }

    impl Persist for Health {}
    impl Persist for Position {}

    #[tokio::test]
    async fn fetch_into_loads_new_entities() {
        let mut mock_db = MockDatabaseConnection::new();
        mock_db.expect_query()
            .returning(|_, _| Box::pin(async { Ok(vec!["k1".into(), "k2".into()]) }));
        mock_db.expect_fetch_component()
            .withf(|k, comp| (k == "k1" || k == "k2") && comp==Health::name())
            .returning(|_, _| Box::pin(async { Ok(Some(json!({"value":10}))) }));
        mock_db.expect_fetch_component()
            .withf(|k, comp| (k == "k1" || k == "k2") && comp==Position::name())
            .returning(|_, _| Box::pin(async { Ok(Some(json!({"x":1.0,"y":2.0,"z":3.0}))) }));

        let db_arc: Arc<dyn DatabaseConnection> = Arc::new(mock_db);
        let mut session = ArangoSession::new_mocked(db_arc.clone());
        session.register_component::<Health>();
        session.register_component::<Position>();
        let query = ArangoQuery::new(db_arc)
            .with::<Health>()
            .with::<Position>();
        let loaded = futures::executor::block_on(query.fetch_into(&mut session));

        assert_eq!(loaded.len(), 2);
        assert_eq!(session.loaded_entities.len(), 2);
    }

    #[test]
    fn build_query_empty_filters() {
        let db = Arc::new(MockDatabaseConnection::new());
        let (aql, _) = ArangoQuery::new(db).build_aql();
        assert!(aql.contains("FILTER true"));
    }

    #[test]
    #[should_panic(expected = "AQL query failed")]
    fn fetch_ids_panics_on_error() {
        let mut mock_db = MockDatabaseConnection::new();
        mock_db
            .expect_query()
            .returning(|_, _| Box::pin(async { Err(ArangoError("fail".into())) }));
        let db = Arc::new(mock_db) as Arc<dyn DatabaseConnection>;
        futures::executor::block_on(ArangoQuery::new(db).fetch_ids());
    }

    // dummy components for mix tests
    #[derive(Component, Serialize, Deserialize)]
    struct H;
    impl Persist for H {}
    #[derive(Component, Serialize, Deserialize)]
    struct P;
    impl Persist for P {}

    #[test]
    fn build_query_single_and_multi() {
        let db = Arc::new(MockDatabaseConnection::new());
        let (a_single, _) = ArangoQuery::new(db.clone()).with::<H>().build_aql();
        assert!(a_single.contains(&format!("doc.`{}` != null", H::name())));

        let (a_multi, _) = ArangoQuery::new(db)
            .with::<H>()
            .with::<P>()
            .build_aql();
        assert!(a_multi.contains(&format!("doc.`{}` != null", H::name())));
        assert!(a_multi.contains(&format!("doc.`{}` != null", P::name())));
    }
}
