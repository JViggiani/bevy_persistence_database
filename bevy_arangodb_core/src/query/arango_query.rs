//! A builder for loading entities from ArangoDB into an ArangoSession.
//!
//! You call `.with::<T>()` to request components, and `.filter("…")`
//! to inject raw AQL snippets. Later phases will construct full AQL
//! and fetch matching entity IDs and component data.

use std::collections::HashMap;
use std::sync::Arc;
use serde_json::Value;
use crate::{DatabaseConnection, Guid, Persist, ArangoSession};
use crate::Collection;
use crate::query_dsl::{Expression, translate_expression};
use bevy::prelude::{Component, World, App};

/// AQL query builder: select which components and filters to apply.
pub struct ArangoQuery {
    db: Arc<dyn DatabaseConnection>,
    pub component_names: Vec<&'static str>,
    filter_expr: Option<Expression>,
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
    pub fn filter(mut self, expression: Expression) -> Self {
        // Collect any component names referenced in the filter expression
        fn collect(expr: &Expression, names: &mut Vec<&'static str>) {
            match expr {
                Expression::Field { component_name, .. } => {
                    if !names.contains(component_name) {
                        names.push(component_name);
                    }
                }
                Expression::BinaryOp { lhs, rhs, .. } => {
                    collect(lhs, names);
                    collect(rhs, names);
                }
                _ => {}
            }
        }
        // Update component_names to include filter‐referenced types
        let mut names = self.component_names.clone();
        collect(&expression, &mut names);
        self.component_names = names;

        self.filter_expr = Some(expression);
        self
    }

    /// Construct the AQL and bind-variables tuple.
    fn build_aql(&self) -> (String, HashMap<String, Value>) {
        let mut bind_vars = HashMap::new();
        let mut aql = format!("FOR doc IN {}", Collection::Entities);

        let mut filters = Vec::new();

        if !self.component_names.is_empty() {
            let component_filter = self.component_names
                .iter()
                .map(|name| format!("doc.`{}` != null", name))
                .collect::<Vec<_>>()
                .join(" AND ");
            filters.push(format!("({})", component_filter));
        }

        if let Some(expr) = &self.filter_expr {
            filters.push(translate_expression(expr, &mut bind_vars));
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

    /// Load matching entities into the `App`'s `World`.
    ///
    /// Removes the `ArangoSession` resource, performs the database
    /// operations, and then re-inserts it.
    pub async fn fetch_into(&self, app: &mut App) -> Vec<bevy::prelude::Entity> {
        // remove the session resource
        let session = app.world.remove_resource::<ArangoSession>().unwrap();

        // SAFETY: With the session removed, it's safe to get a mutable World reference
        let world_ptr: *mut World = &mut app.world as *mut World;
        let world = unsafe { &mut *world_ptr };

        // 1) run AQL to get matching keys
        let keys = self.fetch_ids().await;
        let mut result = Vec::new();

        // 2) build map of existing entities by Guid
        let mut existing = std::collections::HashMap::new();
        for (e, guid) in world.query::<(bevy::prelude::Entity, &Guid)>().iter(world) {
            existing.insert(guid.id().to_string(), e);
        }

        // 3) for each key: spawn or reuse, then fetch & insert components
        for key in keys {
            let e = if let Some(&e) = existing.get(&key) {
                e
            } else {
                let new_e = world.spawn(Guid::new(key.clone())).id();
                existing.insert(key.clone(), new_e);
                new_e
            };
            // Attempt to load each requested component; panic on error
            session
                .fetch_and_insert_components(&*self.db, world, &key, e, &self.component_names)
                .await
                .expect("component deserialization failed");

            result.push(e);
        }

        // 4) Fetch all persisted resources back into the world
        session
            .fetch_and_insert_resources(&*self.db, world)
            .await
            .expect("resource deserialization failed");

        // 5) restore the session and return
        app.world.insert_resource(session);
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::connection::MockDatabaseConnection;
    use crate::{Persist, bevy_plugin::ArangoPlugin};
    use bevy_arangodb_derive::persist;
    use bevy::prelude::App;
    use serde_json::json;
    use std::sync::Arc;
    use futures::executor::block_on;

    // Dummy components for skeleton tests
    #[persist(component)]
    struct A { value: i32 }
    #[persist(component)]
    struct B { name: String }

    #[test]
    fn build_query_with_dsl() {
        let db = Arc::new(MockDatabaseConnection::new());
        let q = ArangoQuery::new(db.clone())
            .with::<A>()
            .filter(
                A::value().gt(10)
                .and(B::name().eq("test"))
            );

        let (aql, bind_vars) = q.build_aql();

        // Should require presence of both components by their Persist::name()
        assert!(aql.contains(&format!("doc.`{}` != null", <A as Persist>::name())));
        assert!(aql.contains(&format!("doc.`{}` != null", <B as Persist>::name())));

        // Should contain the filter expression using Persist::name()
        let expected_expr = format!(
            "((doc.`{}`.`value` > @bevy_arangodb_bind_0) AND (doc.`{}`.`name` == @bevy_arangodb_bind_1))",
            <A as Persist>::name(),
            <B as Persist>::name()
        );
        assert!(aql.contains(&expected_expr));

        assert_eq!(bind_vars.get("bevy_arangodb_bind_0").unwrap(), &json!(10));
        assert_eq!(bind_vars.get("bevy_arangodb_bind_1").unwrap(), &json!("test"));
    }

    // Real component types for fetch_into
    #[persist(component)]
    struct Health { value: i32 }
    #[persist(component)]
    struct Position { x: f32, y: f32, z: f32 }

    #[tokio::test]
    async fn fetch_into_loads_new_entities() {
        let mut mock_db = MockDatabaseConnection::new();
        mock_db.expect_query()
            .returning(|_, _| Box::pin(async { Ok(vec!["k1".into(), "k2".into()]) }));
        mock_db.expect_fetch_component()
            .withf(|k, comp| (k=="k1"||k=="k2") && comp==Health::name())
            .returning(|_, _| Box::pin(async { Ok(Some(json!({"value":10}))) }));
        mock_db.expect_fetch_component()
            .withf(|k, comp| (k=="k1"||k=="k2") && comp==Position::name())
            .returning(|_, _| Box::pin(async { Ok(Some(json!({"x":1.0,"y":2.0,"z":3.0}))) }));
        // Due to test pollution from other modules, other resource types might be registered.
        // We must expect `fetch_resource` to be called, and we can just return `None`.
        mock_db.expect_fetch_resource()
            .returning(|_| Box::pin(async { Ok(None) }));

        let db = Arc::new(mock_db) as Arc<dyn DatabaseConnection>;

        // build app + session
        let mut app = App::new();
        app.add_plugins(ArangoPlugin::new(db.clone()));
        let mut session = app.world.resource_mut::<ArangoSession>();
        session.register_component::<Health>();
        session.register_component::<Position>();

        let query = ArangoQuery::new(db)
            .with::<Health>()
            .with::<Position>();

        let loaded = block_on(query.fetch_into(&mut app));
        assert_eq!(loaded.len(), 2);
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
            .returning(|_, _| Box::pin(async { Err(crate::ArangoError("db error".into())) }));
        let db = Arc::new(mock_db);
        let query = ArangoQuery::new(db);
        block_on(query.fetch_ids());
    }

    // dummy components for mix tests
    #[persist(component)]
    struct H;
    #[persist(component)]
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
