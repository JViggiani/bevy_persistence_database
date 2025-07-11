//! A builder for loading entities from ArangoDB into an ArangoSession.
//!
//! You call `.with::<T>()` to request components, and `.filter("…")`
//! to inject raw AQL snippets. Later phases will construct full AQL
//! and fetch matching entity IDs and component data.

use std::collections::HashMap;
use std::sync::Arc;
use serde_json::Value;
use crate::{DatabaseConnection, Guid, Collection, Persist, query_dsl};
use bevy::prelude::{Component, World, App};

/// AQL query builder: select which components and filters to apply.
pub struct ArangoQuery {
    db: Arc<dyn DatabaseConnection>,
    pub component_names: Vec<&'static str>,
    filter_expr: Option<query_dsl::Expression>,
}

impl ArangoQuery {
    /// Start a new query backed by a shared database connection.
    pub fn new(db: Arc<dyn DatabaseConnection>) -> Self {
        Self {
            db,
            component_names: Vec::new(),
            filter_expr: None,
        }
    }

    /// Request loading component `T`.
    pub fn with<T: Component + Persist>(mut self) -> Self {
        self.component_names.push(T::name());
        self
    }

    /// Sets the filter for the query using a `query_dsl::Expression`.
    pub fn filter(mut self, expression: query_dsl::Expression) -> Self {
        self.filter_expr = Some(expression);
        self
    }

    /// Construct the AQL and bind-variables tuple.
    fn build_aql(&self) -> (String, HashMap<String, Value>) {
        let mut bind_vars = HashMap::new();
        // base FOR clause
        let mut aql = format!("FOR doc IN {}", Collection::Entities);

        let mut filters = Vec::new();
        if !self.component_names.is_empty() {
            let presences = self.component_names
                .iter()
                .map(|name| format!("doc.`{}` != null", name))
                .collect::<Vec<_>>()
                .join(" AND ");
            filters.push(format!("({})", presences));
        }

        if let Some(expr) = &self.filter_expr {
            filters.push(query_dsl::translate_expression(expr, &mut bind_vars));
        }

        if filters.is_empty() {
             aql.push_str("\n  FILTER true");
        } else {
            aql.push_str("\n  FILTER ");
            aql.push_str(&filters.join(" AND "));
        }

        aql.push_str("\n  RETURN doc._key");
        (aql, bind_vars)
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
    pub async fn fetch_into(&self, session: &mut crate::ArangoSession, world: &mut World) -> Vec<bevy::prelude::Entity> {
        // 1) run AQL to get keys
        let keys = self.fetch_ids().await;
        let mut result = Vec::new();

        // Build a map of existing entities by Guid for quick lookup
        let mut existing_entities_by_guid: HashMap<String, bevy::prelude::Entity> = HashMap::new();
        let mut query = world.query::<(bevy::prelude::Entity, &Guid)>();
        for (entity, guid) in query.iter(world) {
            existing_entities_by_guid.insert(guid.id().to_string(), entity);
        }

        for key in keys.iter() {
            let e = if let Some(existing_entity) = existing_entities_by_guid.get(key) {
                *existing_entity
            } else {
                let new_e = world.spawn(Guid::new(key.clone())).id();
                existing_entities_by_guid.insert(key.clone(), new_e);
                new_e
            };

            // for each requested component name, fetch and insert
            for &comp_name in &self.component_names {
                let jsonv = self.db.fetch_component(key, comp_name).await
                    .expect("fetch_component failed");
                if let Some(val) = jsonv {
                    if let Some(deserializer) = session.component_deserializers.get(comp_name) {
                        deserializer(world, e, val).expect("deserialization failed");
                    }
                }
            }
            session.mark_loaded(e);
            result.push(e);
        }
        result
    }

    /// Load matching entities into the `App`'s `World`.
    ///
    /// This is a helper function that encapsulates the `async` complexity of
    /// removing the `ArangoSession` resource, performing the database
    /// operations, and then re-inserting it.
    pub async fn fetch_into_app(&self, app: &mut App) -> Vec<bevy::prelude::Entity> {
        let mut session = app.world.remove_resource::<crate::ArangoSession>().unwrap();
        let world_ptr = &mut app.world as *mut _;

        // SAFETY: We have removed the ArangoSession, so we can now safely get a mutable
        // reference to the world. The session resource is not accessed by any other
        // part of the app while it's removed.
        let world = unsafe { &mut *world_ptr };
        let result = self.fetch_into(&mut session, world).await;

        app.world.insert_resource(session);
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ArangoSession, DatabaseConnection, ArangoError, Persist};
    use crate::arango_session::MockDatabaseConnection;
    use bevy_arangodb_derive::Persist;
    use serde::{Deserialize, Serialize};
    use serde_json::json;
    use std::sync::Arc;
    use bevy::prelude::World;

    // Dummy components for skeleton tests
    #[derive(Component, Serialize, Deserialize, Persist)]
    struct A { value: i32 }
    #[derive(Component, Serialize, Deserialize, Persist)]
    struct B { name: String }

    #[test]
    fn build_query_with_dsl() {
        let db = Arc::new(MockDatabaseConnection::new());
        let q = ArangoQuery::new(db.clone())
            .with::<A>()
            .filter(A::value().gt(10).and(B::name().eq("test")));

        let (aql, bind_vars) = q.build_aql();

        assert!(aql.contains("FILTER (doc.`A` != null) AND ((doc.`A`.`value` > @bevy_arangodb_bind_0) AND (doc.`B`.`name` == @bevy_arangodb_bind_1))"));
        assert_eq!(bind_vars.get("bevy_arangodb_bind_0").unwrap(), &json!(10));
        assert_eq!(bind_vars.get("bevy_arangodb_bind_1").unwrap(), &json!("test"));
    }

    // Real component types for fetch_into
    #[derive(bevy::prelude::Component, serde::Deserialize, Serialize, Persist)]
    struct Health { value: i32 }
    #[derive(bevy::prelude::Component, serde::Deserialize, Serialize, Persist)]
    struct Position { x: f32, y: f32, z: f32 }

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
        let mut world = World::new();
        session.register_component::<Health>();
        session.register_component::<Position>();
        let query = ArangoQuery::new(db_arc)
            .with::<Health>()
            .with::<Position>();
        let loaded = futures::executor::block_on(query.fetch_into(&mut session, &mut world));

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
    #[derive(Component, Serialize, Deserialize, Persist)]
    struct H;
    #[derive(Component, Serialize, Deserialize, Persist)]
    struct P;

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
