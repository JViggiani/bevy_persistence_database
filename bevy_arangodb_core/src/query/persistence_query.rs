//! A manual builder for creating and executing database queries outside of Bevy systems.

use std::sync::Arc;
use crate::{DatabaseConnection, Guid, Persist, PersistenceSession};
use crate::db::connection::BEVY_PERSISTENCE_VERSION_FIELD;
use crate::query::persistence_query_specification::PersistenceQuerySpecification;
use crate::query::filter_expression::FilterExpression;
use crate::versioning::version_manager::VersionKey;
use bevy::prelude::{Component, World};

/// Query builder: select which components and filters to apply.
pub struct PersistenceQuery {
    db: Arc<dyn DatabaseConnection>,
    pub component_names: Vec<&'static str>,
    filter_expr: Option<FilterExpression>,

    /// Track explicit absence filters for components
    pub(crate) without_component_names: Vec<&'static str>,

    /// Components to fetch/deserialize without gating presence in backend
    pub(crate) fetch_only_component_names: Vec<&'static str>,
}

impl PersistenceQuery {
    /// Start a new query backed by a shared database connection.
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
    pub fn with<T: Component + Persist>(mut self) -> Self {
        self.component_names.push(T::name());
        self
    }

    /// Request absence of component `T`
    pub fn without<T: Component + Persist>(mut self) -> Self {
        self.without_component_names.push(T::name());
        self
    }

    /// Internal: request fetching component by name without presence gating.
    pub fn fetch_only_component(mut self, component_name: &'static str) -> Self {
        self.fetch_only_component_names.push(component_name);
        self
    }

    /// Combine current filter with OR.
    pub fn or(mut self, expression: FilterExpression) -> Self {
        self.filter_expr = Some(match self.filter_expr.take() {
            Some(existing) => existing.or(expression),
            None => expression,
        });
        self
    }

    /// Sets the filter for the query using a `FilterExpression`.
    pub fn filter(mut self, expression: FilterExpression) -> Self {
        // Collect any component names referenced in the filter expression
        fn collect(expr: &FilterExpression, names: &mut Vec<&'static str>) {
            match expr {
                FilterExpression::Field { component_name, .. } => {
                    if !names.contains(component_name) {
                        names.push(component_name);
                    }
                }
                FilterExpression::DocumentKey => { /* no component */ }
                FilterExpression::BinaryOperator { lhs, rhs, .. } => {
                    collect(lhs, names);
                    collect(rhs, names);
                }
                FilterExpression::Literal(_) => {}
            }
        }
        let mut names = self.component_names.clone();
        collect(&expression, &mut names);
        self.component_names = names;

        self.filter_expr = Some(expression);
        self
    }

    /// Build a backend-agnostic spec
    pub(crate) fn build_spec(&self, full_docs: bool) -> PersistenceQuerySpecification {
        let mut fetch_only = self.component_names.clone();
        fetch_only.extend(self.fetch_only_component_names.iter().copied());
        fetch_only.sort_unstable();
        fetch_only.dedup();

        let spec = PersistenceQuerySpecification {
            presence_with: self.component_names.clone(),
            presence_without: self.without_component_names.clone(),
            fetch_only,
            value_filters: self.filter_expr.clone(),
            return_full_docs: full_docs,
        };
        bevy::log::debug!(
            "[builder] build_spec full_docs={} presence_with={:?} without={:?} fetch_only={:?} filter={:?}",
            full_docs, spec.presence_with, spec.presence_without, spec.fetch_only, spec.value_filters
        );
        spec
    }

    /// Run the query for keys only.
    pub async fn fetch_ids(&self) -> Vec<String> {
        let spec = self.build_spec(false);
        self.db.execute_keys(&spec)
            .await
            .expect("query failed")
    }

    /// Load matching entities into the World.
    pub async fn fetch_into(&self, world: &mut World) -> Vec<bevy::prelude::Entity> {
         // remove the session resource
         let mut session = world.remove_resource::<PersistenceSession>().unwrap();

         // fetch full documents in one go
         let spec = self.build_spec(true);
         bevy::log::debug!("[builder] fetch_into issuing execute_documents");
         let documents = self.db.execute_documents(&spec)
             .await
             .expect("Batch document fetch failed");
         bevy::log::debug!("[builder] fetch_into: backend returned {} documents", documents.len());

         let mut result = Vec::with_capacity(documents.len());
         if !documents.is_empty() {
             // map existing GUIDs→entities
             let mut existing = std::collections::HashMap::new();
             for (e, guid) in world.query::<(bevy::prelude::Entity, &Guid)>().iter(world) {
                 existing.insert(guid.id().to_string(), e);
             }

             for doc in documents {
                 let key_field = self.db.document_key_field();
                 let key = doc[key_field].as_str().unwrap_or_default().to_string();
                 if key.is_empty() {
                     bevy::log::debug!("[builder] fetch_into: skipping doc missing key '{}'", key_field);
                     continue;
                 }
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

                 // Overwrite requested components on existing entities (manual builder policy)
                 let mut to_deser = self.component_names.clone();
                 to_deser.extend(self.fetch_only_component_names.iter().copied());
                 to_deser.sort_unstable();
                 to_deser.dedup();
                 bevy::log::trace!("[builder] deserializing {:?} for key={}", to_deser, key);
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
         bevy::log::debug!("[builder] fetch_into: inserted {} entities into world", result.len());
         result
    }
}

// Extension trait for PersistenceQuery to add component by name
pub trait WithComponentExt {
    fn with_component(self, component_name: &'static str) -> Self;
    fn without_component(self, component_name: &'static str) -> Self;
    /// Fetch component without requiring presence in backend filters
    fn fetch_only_component(self, component_name: &'static str) -> Self;
}

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
    fn build_spec_with_dsl() {
        let db = Arc::new(MockDatabaseConnection::new());
        let q = PersistenceQuery::new(db.clone())
            .with::<A>()
            .filter(
                A::value().gt(10)
                .and(B::name().eq("test"))
            );
        let spec = q.build_spec(false);

        // Should require presence of both components by their Persist::name()
        assert!(spec.presence_with.contains(&<A as Persist>::name()));
        assert!(spec.presence_with.contains(&<B as Persist>::name()));

        // Should contain some value filter
        assert!(spec.value_filters.is_some());
        assert!(!spec.return_full_docs);
    }

    #[test]
    fn build_spec_with_or_combiner() {
        let db = Arc::new(MockDatabaseConnection::new());

        // Start with a filter, then OR another
        let q = PersistenceQuery::new(db.clone())
            .filter(A::value().gt(10))
            .or(B::name().eq("foo"));

        let spec = q.build_spec(false);
        // Expect filter present and not full-docs
        assert!(spec.value_filters.is_some());
        assert!(!spec.return_full_docs);
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
        mock_db
            .expect_execute_documents()
            .returning(|spec| {
                assert!(spec.return_full_docs, "execute_documents must be full-docs");
                Box::pin(async { Ok(vec![
                    json!({"_key":"k1",BEVY_PERSISTENCE_VERSION_FIELD:1,"A":{}}),
                    json!({"_key":"k2",BEVY_PERSISTENCE_VERSION_FIELD:1,"A":{}}),
                ]) })
            });

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
    fn build_spec_empty_filters() {
        let db = Arc::new(MockDatabaseConnection::new());
        let spec = PersistenceQuery::new(db).build_spec(false);
        assert!(spec.value_filters.is_none());
        assert!(spec.presence_with.is_empty());
        assert!(spec.presence_without.is_empty());
        assert!(!spec.return_full_docs);
    }

    #[test]
    #[should_panic(expected = "query failed")]
    fn fetch_ids_panics_on_error() {
        let mut mock_db = MockDatabaseConnection::new();
        mock_db
            .expect_execute_keys()
            .returning(|_spec| {
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
    fn build_spec_single_and_multi() {
        let db = Arc::new(MockDatabaseConnection::new());
        let spec_single = PersistenceQuery::new(db.clone()).with::<H>().build_spec(false);
        assert!(spec_single.presence_with.contains(&H::name()));

        let spec_multi = PersistenceQuery::new(db)
            .with::<H>()
            .with::<P>()
            .build_spec(false);
        assert!(spec_multi.presence_with.contains(&H::name()));
        assert!(spec_multi.presence_with.contains(&P::name()));
    }
}