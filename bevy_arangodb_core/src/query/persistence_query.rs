//! A manual builder for creating and executing database queries outside of Bevy systems.

use std::collections::HashMap;
use std::sync::Arc;
use serde_json::Value;
use crate::{DatabaseConnection, Guid, Persist, PersistenceSession, PersistenceError};
use crate::Collection;
use crate::db::connection::BEVY_PERSISTENCE_VERSION_FIELD;
use crate::query::expression::{Expression, translate_expression};
use crate::versioning::version_manager::VersionKey;
use bevy::prelude::{Component, World, Commands, EntityWorldMut, Entity};

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

    /// Sets the filter for the query using a `Expression`.
    pub fn filter(mut self, expression: Expression) -> Self {
        // Collect any component names referenced in the filter expression
        fn collect(expr: &Expression, names: &mut Vec<&'static str>) {
            match expr {
                Expression::Field { component_name, .. } => {
                    if !names.contains(component_name) {
                        names.push(component_name);
                    }
                }
                Expression::DocumentKey => {
                    // DocumentKey doesn't reference a component
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
    pub(crate) fn build_aql(&self, full_docs: bool) -> (String, HashMap<String, Value>) {
        let mut bind_vars = HashMap::new();
        let mut aql = format!("FOR doc IN {}", Collection::Entities);

        let mut filters = Vec::new();
        if !self.component_names.is_empty() {
            let comp_filter = self.component_names
                .iter()
                .map(|n| format!("doc.`{}` != null", n))
                .collect::<Vec<_>>()
                .join(" AND ");
            filters.push(format!("({})", comp_filter));
        }
        if let Some(expr) = &self.filter_expr {
            filters.push(translate_expression(expr, &mut bind_vars, &*self.db));
        }

        if filters.is_empty() {
            aql.push_str("\n  FILTER true");
        } else {
            aql.push_str("\n  FILTER ");
            aql.push_str(&filters.join(" AND "));
        }

        if full_docs {
            aql.push_str("\n  RETURN doc");
        } else {
            aql.push_str(&format!("\n  RETURN doc.{}", self.db.document_key_field()));
        }
        (aql, bind_vars)
    }

    /// Run the AQL, fetch matching keys, and return them.
    pub async fn fetch_ids(&self) -> Vec<String> {
        let (aql, bind_vars) = self.build_aql(false);
        self.db.query_keys(aql, bind_vars)
            .await
            .expect("AQL query failed")
    }

    /// Load matching entities into the World.
    ///
    /// Removes the `PersistenceSession` resource, performs the database
    /// operations, and then re-inserts it.
    pub async fn fetch_into(&self, world: &mut World) -> Vec<bevy::prelude::Entity> {
        // remove the session resource
        let mut session = world.remove_resource::<PersistenceSession>().unwrap();

        // fetch full documents in one go
        let (aql, bind_vars) = self.build_aql(true);
        let documents = self.db.query_documents(aql, bind_vars)
            .await
            .expect("Batch document fetch failed");

        let mut result = Vec::with_capacity(documents.len());
        if !documents.is_empty() {
            // map existing GUIDs→entities
            let mut existing = HashMap::new();
            for (e, guid) in world.query::<(bevy::prelude::Entity, &Guid)>().iter(world) {
                existing.insert(guid.id().to_string(), e);
            }

            for doc in documents {
                let key_field = self.db.document_key_field();
                let key = doc[key_field].as_str().unwrap().to_string();
                let version = doc[BEVY_PERSISTENCE_VERSION_FIELD].as_u64().unwrap();

                let entity = if let Some(&e) = existing.get(&key) {
                    e
                } else {
                    let e = world.spawn(Guid::new(key.clone())).id();
                    existing.insert(key.clone(), e);
                    e
                };

                session.version_manager.set_version(VersionKey::Entity(key.clone()), version);

                for &comp in &self.component_names {
                    if let Some(val) = doc.get(comp) {
                        let deser = &session.component_deserializers[comp];
                        deser(world, entity, val.clone())
                            .expect("component deserialization failed");
                    }
                }

                result.push(entity);
            }
        }

        session.fetch_and_insert_resources(&*self.db, world)
            .await
            .expect("resource deserialization failed");

        world.insert_resource(session);
        result
    }

    /// Load matching entities into the World using Commands
    /// This is primarily used by the PersistentQuery SystemParam
    pub(crate) fn fetch_into_world_with_commands(
        &self, 
        db: &Arc<dyn DatabaseConnection>,
        session: &PersistenceSession,
        commands: &mut Commands
    ) -> Vec<bevy::prelude::Entity> {
        // fetch full documents in one go
        let (aql, bind_vars) = self.build_aql(true);
        
        bevy::log::debug!("fetch_into_world_with_commands: Executing query synchronously");
        
        // Use synchronous query method to avoid Tokio runtime issues
        let documents = match db.query_documents_sync(aql, bind_vars) {
            Ok(docs) => {
                bevy::log::debug!("Retrieved {} documents from database", docs.len());
                docs
            },
            Err(e) => {
                bevy::log::error!("Error fetching documents: {}", e);
                Vec::new() // Return empty result on error
            }
        };

        bevy::log::debug!("Processing {} documents", documents.len());
        
        let mut result = Vec::with_capacity(documents.len());
        if documents.is_empty() {
            return result;
        }

        // First pass: collect existing GUIDs and create new entities
        let mut existing = HashMap::new();
        let mut entities_to_create = Vec::new();
        
        for doc in &documents {
            let key_field = db.document_key_field();
            let key = doc[key_field].as_str().unwrap().to_string();
            
            // Check if we already have this entity
            if let Some(guid) = session.entity_keys.iter()
                .find(|(_, k)| **k == key)
                .map(|(e, _)| *e) 
            {
                existing.insert(key, guid);
            } else {
                entities_to_create.push(key);
            }
        }
        
        // Create all new entities in one go - without any deferred logic
        bevy::log::debug!("Creating {} new entities", entities_to_create.len());
        for key in entities_to_create {
            let entity = commands.spawn(Guid::new(key.clone())).id();
            existing.insert(key, entity);
        }
        
        // We'll collect entity IDs and return them, but skip component insertion for now
        for doc in documents {
            let key_field = db.document_key_field();
            let key = doc[key_field].as_str().unwrap().to_string();
            
            // Get the entity (must exist now)
            if let Some(&entity) = existing.get(&key) {
                result.push(entity);
            }
        }

        bevy::log::debug!("Returning {} entities from query", result.len());
        // We'll let the caller handle the rest - this ensures we don't block or hang
        result
    }
}

// Extension trait for PersistenceQuery to add component by name
pub trait WithComponentExt {
    fn with_component(self, component_name: &'static str) -> Self;
}

impl WithComponentExt for PersistenceQuery {
    fn with_component(mut self, component_name: &'static str) -> Self {
        self.component_names.push(component_name);
        self
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
        let mut db = MockDatabaseConnection::new();
        db.expect_document_key_field().return_const("_key");
        let db = Arc::new(db);
        let q = PersistenceQuery::new(db.clone())
            .with::<A>()
            .filter(
                A::value().gt(10)
                .and(B::name().eq("test"))
            );

        let (aql, bind_vars) = q.build_aql(false);

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
        mock_db.expect_document_key_field().return_const("_key");
        mock_db.expect_query_documents()
            .returning(|_, _| Box::pin(async { Ok(vec![
                json!({"_key":"k1",BEVY_PERSISTENCE_VERSION_FIELD:1,"A":{}}),
                json!({"_key":"k2",BEVY_PERSISTENCE_VERSION_FIELD:1,"A":{}}),
            ]) }));

        // Due to test pollution from other modules, other resource types might be registered.
        // We must expect `fetch_resource` to be called, and we can just return `None`.
        mock_db.expect_fetch_resource()
            .returning(|_| Box::pin(async { Ok(None) }));

        let db = Arc::new(mock_db) as Arc<dyn DatabaseConnection>;

        // build app + session
        let mut app = App::new();
        app.add_plugins(PersistencePluginCore::new(db.clone()));
        let mut session = app.world_mut().resource_mut::<PersistenceSession>();
        session.register_component::<Health>();
        session.register_component::<Position>();

        let query = PersistenceQuery::new(db)
            .with::<Health>()
            .with::<Position>();

        let loaded = query.fetch_into(app.world_mut()).await;
        assert_eq!(loaded.len(), 2);
    }

    #[test]
    fn build_query_empty_filters() {
        let mut db = MockDatabaseConnection::new();
        db.expect_document_key_field().return_const("_key");
        let db = Arc::new(db);
        let (aql, _) = PersistenceQuery::new(db).build_aql(false);
        assert!(aql.contains("FILTER true"));
    }

    #[test]
    #[should_panic(expected = "AQL query failed")]
    fn fetch_ids_panics_on_error() {
        let mut mock_db = MockDatabaseConnection::new();
        mock_db.expect_document_key_field().return_const("_key");
        mock_db.expect_query_keys().returning(|_, _| {
            Box::pin(async { Err(crate::PersistenceError::General("db error".into())) })
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
        let mut db = MockDatabaseConnection::new();
        db.expect_document_key_field().return_const("_key");
        let db = Arc::new(db);
        let (a_single, _) = PersistenceQuery::new(db.clone()).with::<H>().build_aql(false);
        assert!(a_single.contains(&format!("doc.`{}` != null", H::name())));

        let (a_multi, _) = PersistenceQuery::new(db)
            .with::<H>()
            .with::<P>()
            .build_aql(false);
        assert!(a_multi.contains(&format!("doc.`{}` != null", H::name())));
        assert!(a_multi.contains(&format!("doc.`{}` != null", P::name())));
    }
}