//! A manual builder for creating and executing database queries that load results into a Bevy `World`.

use crate::bevy::components::Guid;
use crate::core::db::connection::DocumentKind;
use crate::core::db::{DatabaseConnection, read_version};
use crate::core::persist::Persist;
use crate::core::query::{FilterExpression, PersistenceQuerySpecification};
use crate::core::session::PersistenceSession;
use crate::core::versioning::version_manager::VersionKey;
use bevy::prelude::{Component, World};
use std::sync::Arc;

/// Query builder: select which components and filters to apply.
pub struct PersistenceQuery {
    db: Arc<dyn DatabaseConnection>,
    store: String,
    pub component_names: Vec<&'static str>,
    filter_expr: Option<FilterExpression>,

    /// Track explicit absence filters for components.
    pub(crate) without_component_names: Vec<&'static str>,

    /// Components to fetch/deserialize without gating presence in backend.
    pub(crate) fetch_only_component_names: Vec<&'static str>,

    /// Whether to return full documents (internal use).
    force_full_docs: bool,
}

impl PersistenceQuery {
    /// Start a new query backed by a shared database connection.
    pub fn new(db: Arc<dyn DatabaseConnection>, store: impl Into<String>) -> Self {
        Self {
            db,
            store: store.into(),
            component_names: Vec::new(),
            filter_expr: None,
            without_component_names: Vec::new(),
            fetch_only_component_names: Vec::new(),
            force_full_docs: false,
        }
    }

    /// Override the store to query against.
    pub fn store(mut self, store: impl Into<String>) -> Self {
        self.store = store.into();
        self
    }

    /// Request loading component `T`.
    pub fn with<T: Component + Persist>(mut self) -> Self {
        self.component_names.push(T::name());
        self
    }

    /// Request absence of component `T`.
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
        fn collect(expr: &FilterExpression, names: &mut Vec<&'static str>) {
            match expr {
                FilterExpression::Field { component_name, .. } => {
                    if !names.contains(component_name) {
                        names.push(component_name);
                    }
                }
                FilterExpression::DocumentKey => {}
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

    /// For component tests - internal use.
    #[cfg(test)]
    pub fn for_component<T: Component + Persist>(mut self) -> Self {
        self.component_names.push(T::name());
        self
    }

    /// Build a backend-agnostic spec.
    pub fn build_spec(&self) -> PersistenceQuerySpecification {
        let mut fetch_only = self.component_names.clone();
        fetch_only.extend(self.fetch_only_component_names.iter().copied());
        fetch_only.sort_unstable();
        fetch_only.dedup();

        let presence_with = self.component_names.clone();
        let presence_without = self.without_component_names.clone();
        let value_filters = self.filter_expr.clone();
        let force_full_docs = self.force_full_docs;

        let spec = PersistenceQuerySpecification {
            store: self.store.clone(),
            kind: DocumentKind::Entity,
            presence_with: presence_with.clone(),
            presence_without: presence_without.clone(),
            fetch_only: fetch_only.clone(),
            value_filters: value_filters.clone(),
            return_full_docs: force_full_docs
                || (presence_with.is_empty() && presence_without.is_empty()),
            pagination: None,
        };

        bevy::log::debug!(
            "[builder] build_spec full_docs={} presence_with={:?} without={:?} fetch_only={:?} filter={:?}",
            force_full_docs,
            spec.presence_with,
            spec.presence_without,
            spec.fetch_only,
            spec.value_filters
        );

        spec
    }

    /// Run the query for keys only.
    pub async fn fetch_ids(&self) -> Vec<String> {
        let spec = self.build_spec();
        self.db.execute_keys(&spec).await.expect("query failed")
    }

    /// Load matching entities into the World.
    pub async fn fetch_into(&self, world: &mut World) -> Vec<bevy::prelude::Entity> {
        let mut session = world
            .remove_resource::<PersistenceSession>()
            .expect("PersistenceSession missing")
            ;

        let mut query_with_full_docs = self.clone();
        query_with_full_docs.force_full_docs = true;
        let spec = query_with_full_docs.build_spec();

        bevy::log::debug!("[builder] fetch_into issuing execute_documents");
        let documents = self
            .db
            .execute_documents(&spec)
            .await
            .expect("Batch document fetch failed");
        bevy::log::debug!(
            "[builder] fetch_into: backend returned {} documents",
            documents.len()
        );

        let mut result = Vec::with_capacity(documents.len());
        if !documents.is_empty() {
            let mut existing: bevy::platform::collections::HashMap<
                String,
                bevy::prelude::Entity,
            > = bevy::platform::collections::HashMap::default();

            for (e, guid) in world.query::<(bevy::prelude::Entity, &Guid)>().iter(world) {
                existing.insert(guid.id().to_string(), e);
            }

            for doc in documents {
                let key_field = self.db.document_key_field();
                let key = doc[key_field].as_str().unwrap_or_default().to_string();
                if key.is_empty() {
                    bevy::log::debug!(
                        "[builder] fetch_into: skipping doc missing key '{}'",
                        key_field
                    );
                    continue;
                }

                let version = read_version(&doc).unwrap_or(1);

                let entity = if let Some(&e) = existing.get(&key) {
                    e
                } else {
                    let e = world.spawn(Guid::new(key.clone())).id();
                    existing.insert(key.clone(), e);
                    e
                };

                session.insert_entity_key(entity, key.clone());
                session
                    .version_manager_mut()
                    .set_version(VersionKey::Entity(key.clone()), version);

                let mut to_deser = self.component_names.clone();
                to_deser.extend(self.fetch_only_component_names.iter().copied());
                to_deser.sort_unstable();
                to_deser.dedup();
                bevy::log::trace!(
                    "[builder] deserializing {:?} for key={}",
                    to_deser,
                    key
                );
                for &comp in &to_deser {
                    if let Some(val) = doc.get(comp) {
                        if let Some(deser) = session.component_deserializer(comp) {
                            deser(world, entity, val.clone())
                                .expect("component deserialization failed");
                        }
                    }
                }

                result.push(entity);
            }
        }

        session
            .fetch_and_insert_resources(&*self.db, &self.store, world)
            .await
            .expect("resource deserialization failed");

        world.insert_resource(session);
        bevy::log::debug!(
            "[builder] fetch_into: inserted {} entities into world",
            result.len()
        );
        result
    }
}

impl Clone for PersistenceQuery {
    fn clone(&self) -> Self {
        Self {
            db: self.db.clone(),
            store: self.store.clone(),
            component_names: self.component_names.clone(),
            filter_expr: self.filter_expr.clone(),
            without_component_names: self.without_component_names.clone(),
            fetch_only_component_names: self.fetch_only_component_names.clone(),
            force_full_docs: self.force_full_docs,
        }
    }
}

/// Extension trait for `PersistenceQuery` to add component by name.
pub trait WithComponentExt {
    fn with_component(self, component_name: &'static str) -> Self;
    fn without_component(self, component_name: &'static str) -> Self;
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
    use crate::bevy::plugins::persistence_plugin::PersistencePluginCore;
    use crate::core::db::connection::{
        BEVY_PERSISTENCE_DATABASE_BEVY_TYPE_FIELD, BEVY_PERSISTENCE_DATABASE_METADATA_FIELD,
        BEVY_PERSISTENCE_DATABASE_VERSION_FIELD, DocumentKind, MockDatabaseConnection,
        PersistenceError,
    };
    use crate::core::session::PersistenceSession;
    use bevy::MinimalPlugins;
    use bevy::prelude::App;
    use bevy_persistence_database_derive::persist;
    use futures::executor::block_on;
    use serde_json::json;
    use std::sync::Arc;

    const TEST_STORE: &str = "test_store";

    #[persist(component)]
    struct A {
        value: i32,
    }

    #[persist(component)]
    struct B {
        name: String,
    }

    #[test]
    fn build_spec_with_dsl() {
        let db = Arc::new(MockDatabaseConnection::new());
        let query = PersistenceQuery::new(db, TEST_STORE)
            .with::<A>()
            .filter(A::value().gt(10).and(B::name().eq("test")));

        let spec = query.build_spec();
        assert!(spec.presence_with.contains(&<A as Persist>::name()));
        assert!(spec.presence_with.contains(&<B as Persist>::name()));
        assert!(spec.value_filters.is_some());
        assert!(!spec.return_full_docs);
    }

    #[test]
    fn build_spec_with_or_combiner() {
        let db = Arc::new(MockDatabaseConnection::new());
        let query = PersistenceQuery::new(db, TEST_STORE)
            .filter(A::value().gt(10))
            .or(B::name().eq("foo"));
        let spec = query.build_spec();

        assert!(spec.value_filters.is_some());
        assert!(!spec.return_full_docs);
    }

    #[persist(component)]
    struct Health {
        value: i32,
    }

    #[persist(component)]
    struct Position {
        x: f32,
        y: f32,
    }

    #[tokio::test]
    async fn fetch_into_loads_new_entities() {
        let mut mock_db = MockDatabaseConnection::new();
        mock_db.expect_document_key_field().return_const("_key");
        mock_db.expect_execute_documents().returning(|spec| {
            assert!(spec.return_full_docs, "execute_documents must be full-docs");
            Box::pin(async {
                Ok(vec![
                    json!({
                        "_key":"k1",
                        BEVY_PERSISTENCE_DATABASE_METADATA_FIELD: {
                            BEVY_PERSISTENCE_DATABASE_VERSION_FIELD: 1,
                            BEVY_PERSISTENCE_DATABASE_BEVY_TYPE_FIELD: DocumentKind::Entity.as_str(),
                        },
                        "Health": {"value": 1},
                        "Position": {"x": 1.0, "y": 2.0},
                    }),
                    json!({
                        "_key":"k2",
                        BEVY_PERSISTENCE_DATABASE_METADATA_FIELD: {
                            BEVY_PERSISTENCE_DATABASE_VERSION_FIELD: 1,
                            BEVY_PERSISTENCE_DATABASE_BEVY_TYPE_FIELD: DocumentKind::Entity.as_str(),
                        },
                        "Health": {"value": 3},
                        "Position": {"x": 4.0, "y": 5.0},
                    }),
                ])
            })
        });
        mock_db
            .expect_fetch_resource()
            .returning(|_, _| Box::pin(async { Ok(None) }));

        let db = Arc::new(mock_db) as Arc<dyn DatabaseConnection>;

        let mut app = App::new();
        app.add_plugins(MinimalPlugins);
        app.add_plugins(PersistencePluginCore::new(db.clone()));

        {
            let mut session = app
                .world_mut()
                .resource_mut::<PersistenceSession>();
            session.register_component::<Health>();
            session.register_component::<Position>();
        }

        let query = PersistenceQuery::new(db, TEST_STORE)
            .with::<Health>()
            .with::<Position>();
        let loaded = query.fetch_into(app.world_mut()).await;

        assert_eq!(loaded.len(), 2);
    }

    #[test]
    fn build_spec_empty_filters() {
        #[persist(component)]
        struct Comp1;

        let db = Arc::new(MockDatabaseConnection::new());
        let query = PersistenceQuery::new(db, TEST_STORE).for_component::<Comp1>();
        let spec = query.build_spec();

        assert!(!spec.presence_with.is_empty());
        assert!(spec.presence_without.is_empty());
        assert!(!spec.return_full_docs);
        assert_eq!(spec.fetch_only, vec!["Comp1"]);
    }

    #[test]
    #[should_panic(expected = "query failed")]
    fn fetch_ids_panics_on_error() {
        let mut mock_db = MockDatabaseConnection::new();
        mock_db.expect_execute_keys().returning(|_spec| {
            Box::pin(async { Err(PersistenceError::General("db error".into())) })
        });
        let db = Arc::new(mock_db);
        let query = PersistenceQuery::new(db, TEST_STORE);
        block_on(query.fetch_ids());
    }
}
