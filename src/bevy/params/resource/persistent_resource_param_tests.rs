use std::sync::Arc;

use bevy::prelude::{App, MinimalPlugins, ResMut, Resource, Update};
use bevy_persistence_database_derive::persist;
use serde_json::json;

use crate::bevy::plugins::persistence_plugin::{PersistencePluginConfig, TokioRuntime};
use crate::core::db::{MockDatabaseConnection, connection::DatabaseConnectionResource};
use crate::core::persist::Persist;
use crate::core::session::PersistenceSession;

use super::persistent_resource_param::{PersistentRes, PersistentResMut};

#[derive(Clone)]
#[persist(resource)]
struct TestResource {
    value: String,
}

/// What the reading system saw on each frame.
#[derive(Resource, Default)]
struct Reads(Vec<Option<String>>);

fn app_with(db: MockDatabaseConnection) -> App {
    let mut app = App::new();
    app.add_plugins(MinimalPlugins);
    app.insert_resource(DatabaseConnectionResource {
        connection: Arc::new(db),
    });
    app.insert_resource(PersistencePluginConfig {
        default_store: "store".to_string(),
        ..Default::default()
    });
    app.insert_resource(TokioRuntime {
        runtime: Arc::new(tokio::runtime::Runtime::new().expect("runtime")),
    });
    let mut session = PersistenceSession::new();
    session.register_resource::<TestResource>();
    app.insert_resource(session);
    app.init_resource::<Reads>();
    app
}

fn record_reads(mut res: PersistentRes<TestResource>, mut reads: ResMut<Reads>) {
    let value = res.get().map(|r| r.value.clone());
    reads.0.push(value);
}

// GIVEN a resource stored in the database and absent from the world
// WHEN a system reads it through PersistentRes on two frames
// THEN the first read is None and queues one load from the default store
// AND the load applies after the system, so the second read sees the stored value
#[test]
fn absent_resource_loads_after_the_load_point() {
    let mut db = MockDatabaseConnection::new();
    db.expect_fetch_resource().times(1).returning(|store, name| {
        assert_eq!(store, "store");
        assert_eq!(name, TestResource::name());
        Box::pin(async { Ok(Some((json!({"value": "persisted"}), 3))) })
    });
    let mut app = app_with(db);
    app.add_systems(Update, record_reads);

    app.update();
    assert_eq!(
        app.world().resource::<TestResource>().value,
        "persisted",
        "load applies at the end of the requesting frame"
    );
    app.update();

    assert_eq!(
        app.world().resource::<Reads>().0,
        vec![None, Some("persisted".to_string())]
    );
}

// GIVEN a resource already in the world
// WHEN a system reads it through PersistentRes
// THEN the world value is returned and the database is never asked
#[test]
fn present_resource_skips_the_database() {
    let mut db = MockDatabaseConnection::new();
    db.expect_fetch_resource().never();
    let mut app = app_with(db);
    app.insert_resource(TestResource {
        value: "existing".to_string(),
    });
    app.add_systems(Update, record_reads);

    app.update();

    assert_eq!(
        app.world().resource::<Reads>().0,
        vec![Some("existing".to_string())]
    );
}

// GIVEN a resource absent from both the world and the database
// WHEN a system reads it through PersistentRes on three frames
// THEN the database is asked once, not every frame
#[test]
fn missing_resource_is_requested_once() {
    let mut db = MockDatabaseConnection::new();
    db.expect_fetch_resource()
        .times(1)
        .returning(|_, _| Box::pin(async { Ok(None) }));
    let mut app = app_with(db);
    app.add_systems(Update, record_reads);

    app.update();
    app.update();
    app.update();

    assert_eq!(app.world().resource::<Reads>().0, vec![None, None, None]);
}

// GIVEN a PersistentRes with a store override
// WHEN it requests the absent resource
// THEN the load asks that store
#[test]
fn store_override_is_used() {
    let mut db = MockDatabaseConnection::new();
    db.expect_fetch_resource().times(1).returning(|store, _| {
        assert_eq!(store, "alt-store");
        Box::pin(async { Ok(None) })
    });
    let mut app = app_with(db);
    app.add_systems(Update, |res: PersistentRes<TestResource>| {
        let _ = res.store("alt-store").get();
    });

    app.update();
}

// GIVEN a stored value whose shape does not match the resource
// WHEN PersistentRes requests it
// THEN the error is logged and no resource is inserted
#[test]
fn deserialize_error_inserts_nothing() {
    let mut db = MockDatabaseConnection::new();
    db.expect_fetch_resource()
        .returning(|_, _| Box::pin(async { Ok(Some((json!({ "wrong": 1 }), 2))) }));
    let mut app = app_with(db);
    app.add_systems(Update, record_reads);

    app.update();

    assert!(app.world().get_resource::<TestResource>().is_none());
}

// GIVEN a resource present in the world
// WHEN a system mutates it through PersistentResMut
// THEN the world holds the new value
#[test]
fn mutation_reaches_the_world() {
    let mut db = MockDatabaseConnection::new();
    db.expect_fetch_resource().never();
    let mut app = app_with(db);
    app.insert_resource(TestResource {
        value: "before".to_string(),
    });
    app.update();
    app.add_systems(Update, |mut res: PersistentResMut<TestResource>| {
        if let Some(mut r) = res.get_mut() {
            r.value = "after".to_string();
        }
    });

    app.update();

    assert_eq!(app.world().resource::<TestResource>().value, "after");
}
