//! A builder for loading entities from ArangoDB into an PersistenceSession.
//!
//! You call `.with::<T>()` to request components, and `.filter("…")`
//! to inject raw AQL snippets. Later phases will construct full AQL
//! and fetch matching entity IDs and component data.

use std::collections::HashMap;
use std::sync::Arc;
use serde_json::Value;
use crate::{DatabaseConnection, Guid, Persist, PersistenceSession};
use crate::db::connection::DocumentKey;
use crate::resources::persistence_session::EntityMetadata;
use crate::Collection;
use crate::dsl::{Expression, translate_expression};
use bevy::prelude::{Component, World};
use tracing::trace;

/// AQL query builder: select which components and filters to apply.
pub struct PersistenceQuery {
    db: Arc<dyn DatabaseConnection>,
    pub component_names: Vec<&'static str>,
    filter_expr: Option<Expression>,
}

impl PersistenceQuery {
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

    /// Sets the filter for the query using a `dsl::Expression`.
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
    pub async fn fetch_ids(&self) -> Vec<DocumentKey> {
        let (aql, bind_vars) = self.build_aql();
        let result = self.db.query(aql, bind_vars).await
            .expect("AQL query failed");
        result
    }

    /// Load matching entities into the World.
    ///
    /// Removes the `PersistenceSession` resource, performs the database
    /// operations, and then re-inserts it.
    pub async fn fetch_into(&self, world: &mut World) -> Vec<bevy::prelude::Entity> {
        // remove the session resource
        let mut session = world.remove_resource::<PersistenceSession>().unwrap();

        // 1) run AQL to get matching keys
        let keys = self.fetch_ids().await;
        let mut result_entities = Vec::new();

        // 2) map existing guid→entity
        let mut existing_entities_by_key: HashMap<DocumentKey, bevy::prelude::Entity> = world
            .query::<(bevy::prelude::Entity, &Guid)>()
            .iter(world)
            .map(|(e, guid)| (guid.id().to_string(), e))
            .collect();

        // Track which entities we fetched
        let mut fetched_entities = Vec::new();

        // 3) For each key, fetch the full document and deserialize components.
        //    If an entity with the Guid already exists, update it. Otherwise, spawn a new one.
        for key in keys {
            if let Some((doc, rev)) = self.db.fetch_document(&key).await.unwrap() {
                let entity = *existing_entities_by_key.entry(key.clone()).or_insert_with(|| {
                    // Entity does not exist in the world, spawn a new one.
                    world.spawn(Guid::new(key.clone())).id()
                });

                // Insert/update all components for this entity.
                for &comp_name in &self.component_names {
                    if let Some(val) = doc.get(comp_name) {
                        if let Some(deser) = session.component_deserializers.get(comp_name) {
                            deser(world, entity, val.clone()).expect("component deserialization failed");
                        }
                    }
                }

                // Update metadata for the entity. This is crucial for conflict resolution.
                trace!(
                    "Fetched entity {:?} (key: {}), updating metadata with rev: {}",
                    entity,
                    key,
                    rev
                );
                session.entity_meta.insert(entity, EntityMetadata { key: key.clone(), rev });
                fetched_entities.push(entity);
                result_entities.push(entity);
            }
        }

        // 4) insert resources
        session
            .fetch_and_insert_resources(&*self.db, world)
            .await
            .expect("resource deserialization failed");

        // 5) Clear our library's dirty flags for fetched entities BEFORE re-inserting session
        // This ensures the updated metadata is preserved
        for entity in &fetched_entities {
            session.dirty_entities.remove(entity);
            session.despawned_entities.remove(entity);
            session.component_changes.remove(entity);
        }
        
        // 6) Re-insert the session resource with updated metadata
        world.insert_resource(session);
        
        // 7) Clear Bevy's change trackers LAST.
        // This "ages" all the `Added` and `Changed` flags from the loading process,
        // so they won't be picked up by the auto-dirty systems in the next `app.update()`.
        // This is crucial for allowing a clean "reload-then-modify" workflow for conflict resolution.
        world.clear_trackers();
        
        result_entities
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::connection::MockDatabaseConnection;
    use crate::{Persist, persistence_plugin::PersistencePluginCore};
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
        let q = PersistenceQuery::new(db.clone())
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
    struct Position { x: f32, y: f32 }

    #[tokio::test]
    async fn fetch_into_loads_new_entities() {
        let mut mock_db = MockDatabaseConnection::new();
        mock_db
            .expect_query()
            .returning(|_, _| Box::pin(async { Ok(vec!["k1".into(), "k2".into()]) }));
        mock_db
            .expect_fetch_document()
            .withf(|k| k == "k1" || k == "k2")
            .returning(|_| {
                Box::pin(async {
                    Ok(Some((
                        json!({
                            "Health": {"value": 10},
                            "Position": {"x": 1.0, "y": 2.0}
                        }),
                        "rev1".to_string(),
                    )))
                })
            });
        // Due to test pollution from other modules, other resource types might be registered.
        // We must expect `fetch_resource` to be called, and we can just return `None`.
        mock_db
            .expect_fetch_resource()
            .returning(|_| Box::pin(async { Ok(None) }));

        let db = Arc::new(mock_db) as Arc<dyn DatabaseConnection>;

        // build app + session
        let mut app = App::new();
        app.add_plugins(PersistencePluginCore::new(db.clone()));
        
        let query = PersistenceQuery::new(db).with::<Health>().with::<Position>();

        // The #[persist] macro on Health and Position should auto-register them.
        // We can now call fetch_into directly.
        let loaded = query.fetch_into(app.world_mut()).await;
        assert_eq!(loaded.len(), 2);
    }

    #[test]
    fn build_query_empty_filters() {
        let db = Arc::new(MockDatabaseConnection::new());
        let (aql, _) = PersistenceQuery::new(db).build_aql();
        assert!(aql.contains("FILTER true"));
    }

    #[test]
    #[should_panic(expected = "AQL query failed")]
    fn fetch_ids_panics_on_error() {
        let mut mock_db = MockDatabaseConnection::new();
        mock_db.expect_query().returning(|_, _| {
            Box::pin(async { Err(crate::PersistenceError::Generic("db error".into())) })
        });
        let db = Arc::new(mock_db);
        let query = PersistenceQuery::new(db);
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
        let (a_single, _) = PersistenceQuery::new(db.clone()).with::<H>().build_aql();
        assert!(a_single.contains(&format!("doc.`{}` != null", H::name())));

        let (a_multi, _) = PersistenceQuery::new(db)
            .with::<H>()
            .with::<P>()
            .build_aql();
        assert!(a_multi.contains(&format!("doc.`{}` != null", H::name())));
        assert!(a_multi.contains(&format!("doc.`{}` != null", P::name())));
    }
}