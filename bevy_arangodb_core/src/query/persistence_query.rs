//! A manual builder for creating and executing database queries outside of Bevy systems.
//!
//! TODO(deprecation): Replace with backend-agnostic QuerySpec + DatabaseConnection::build_query (Step 4).

use std::collections::HashMap;
use std::sync::Arc;
use serde_json::Value;
use crate::{DatabaseConnection, Guid, Persist, PersistenceSession};
use crate::Collection;
use crate::db::connection::BEVY_PERSISTENCE_VERSION_FIELD;
use crate::query::expression::{Expression, translate_expression};
use crate::versioning::version_manager::VersionKey;
use bevy::prelude::{Component, World};

/// AQL query builder: select which components and filters to apply.
#[deprecated(
    since = "0.1.0",
    note = "Manual builder is deprecated; prefer the PersistentQuery SystemParam with type-driven filters."
)]
pub struct PersistenceQuery {
    db: Arc<dyn DatabaseConnection>,
    pub component_names: Vec<&'static str>,
    filter_expr: Option<Expression>,

    /// Track explicit absence filters for components
    pub(crate) without_component_names: Vec<&'static str>,

    /// Components to fetch/deserialize without gating presence in AQL
    pub(crate) fetch_only_component_names: Vec<&'static str>,
}

impl PersistenceQuery {
    /// Start a new query backed by a shared database connection.
    #[deprecated(since = "0.1.0", note = "Deprecated builder; prefer PersistentQuery SystemParam")]
    pub fn new(db: Arc<dyn DatabaseConnection>) -> Self {
        Self {
            db,
            component_names: Vec::new(),
            filter_expr: None,
            without_component_names: Vec::new(),
            fetch_only_component_names: Vec::new(),
        }
    }

    /// Request loading component `T`.
    #[deprecated(since = "0.1.0", note = "Deprecated builder; prefer type-driven Q in SystemParam")]
    pub fn with<T: Component + Persist>(mut self) -> Self {
        self.component_names.push(T::name());
        self
    }

    /// Request absence of component `T` (doc.`T` == null)
    #[deprecated(since = "0.1.0", note = "Deprecated builder; prefer type-driven F in SystemParam")]
    pub fn without<T: Component + Persist>(mut self) -> Self {
        self.without_component_names.push(T::name());
        self
    }

    /// Internal: request fetching component by name without presence gating.
    #[deprecated(since = "0.1.0", note = "Deprecated builder; prefer SystemParam and type-driven Q")]
    pub fn fetch_only_component(mut self, component_name: &'static str) -> Self {
        self.fetch_only_component_names.push(component_name);
        self
    }

    /// Combine current filter with OR.
    #[deprecated(since = "0.1.0", note = "Deprecated DSL; prefer type-driven filters")]
    pub fn or(mut self, expression: Expression) -> Self {
        self.filter_expr = Some(match self.filter_expr.take() {
            Some(existing) => existing.or(expression),
            None => expression,
        });
        self
    }

    /// Sets the filter for the query using an `Expression`.
    #[deprecated(since = "0.1.0", note = "Deprecated DSL; prefer type-driven filters")]
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
    #[deprecated(since = "0.1.0", note = "Will be replaced by DatabaseConnection::build_query")]
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
        // New: absence filters
        if !self.without_component_names.is_empty() {
            let without_filter = self.without_component_names
                .iter()
                .map(|n| format!("doc.`{}` == null", n))
                .collect::<Vec<_>>()
                .join(" AND ");
            filters.push(format!("({})", without_filter));
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
    #[deprecated(since = "0.1.0", note = "Deprecated builder; prefer SystemParam-based loading")]
    pub async fn fetch_ids(&self) -> Vec<String> {
        let (aql, bind_vars) = self.build_aql(false);
        self.db.query_keys(aql, bind_vars)
            .await
            .expect("AQL query failed")
    }

    /// Load matching entities into the World.
    #[deprecated(since = "0.1.0", note = "Deprecated builder; prefer SystemParam-based loading")]
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
                let version = doc[BEVY_PERSISTENCE_VERSION_FIELD].as_u64().unwrap_or(1);

                // Resolve entity (reuse if already present by Guid, otherwise spawn)
                let entity = if let Some(&e) = existing.get(&key) {
                    e
                } else {
                    let e = world.spawn(Guid::new(key.clone())).id();
                    existing.insert(key.clone(), e);
                    e
                };

                // Ensure the session knows about this entity<->key mapping for future operations
                session.entity_keys.insert(entity, key.clone());

                // Cache/refresh version for both new and existing entities
                session
                    .version_manager
                    .set_version(VersionKey::Entity(key.clone()), version);

                // For manual builder: overwrite requested components on existing entities,
                // and insert for new entities. Do not add unrequested components.
                // Union of presence-gated names and fetch-only names
                let mut to_deser = self.component_names.clone();
                to_deser.extend(self.fetch_only_component_names.iter().copied());
                to_deser.sort_unstable();
                to_deser.dedup();
                for &comp in &to_deser {
                    if let Some(val) = doc.get(comp) {
                        if let Some(deser) = session.component_deserializers.get(comp) {
                            deser(world, entity, val.clone())
                                .expect("component deserialization failed");
                        }
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
}

// Extension trait for PersistenceQuery to add component by name
#[deprecated(since = "0.1.0", note = "Deprecated builder; prefer SystemParam and type-driven filters")]
pub trait WithComponentExt {
    fn with_component(self, component_name: &'static str) -> Self;
    fn without_component(self, component_name: &'static str) -> Self;
    /// Fetch component without requiring presence in AQL filters
    fn fetch_only_component(self, component_name: &'static str) -> Self;
}

#[allow(deprecated)]
impl WithComponentExt for PersistenceQuery {
    fn with_component(mut self, component_name: &'static str) -> Self {
        self.component_names.push(component_name);
        self
    }
    fn without_component(mut self, component_name: &'static str) -> Self {
        self.without_component_names.push(component_name);
        self
    }
    fn fetch_only_component(mut self, component_name: &'static str) -> Self {
        self.fetch_only_component_names.push(component_name);
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

    #[test]
    fn build_query_with_or_combiner() {
        let mut db = MockDatabaseConnection::new();
        db.expect_document_key_field().return_const("_key");
        let db = Arc::new(db);

        // Start with a filter, then OR another
        let q = PersistenceQuery::new(db.clone())
            .filter(A::value().gt(10))
            .or(B::name().eq("foo"));

        let (aql, bind_vars) = q.build_aql(false);
        // Expect both branches joined with OR
        assert!(
            aql.contains("(doc.`A`.`value` > @bevy_arangodb_bind_0) OR (doc.`B`.`name` == @bevy_arangodb_bind_1)"),
            "AQL should contain OR-combined expression, got: {aql}"
        );
        assert_eq!(bind_vars.len(), 2);
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