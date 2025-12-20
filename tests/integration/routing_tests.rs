use bevy::prelude::*;
use bevy_persistence_database::{
    commit_sync, PersistenceSession, Guid, Persist, PersistenceError, CommitStatus,
    persistence_plugin::PersistencePlugins,
};
use bevy_persistence_database_derive::{persist, db_matrix_test};
use serde_json;
use crate::common::*;
use bevy_persistence_database::db::connection::MockDatabaseConnection;

#[persist(component)]
#[derive(Debug, Clone, PartialEq)]
struct RoutedComp { value: i32 }

#[db_matrix_test]
fn commit_uses_provided_connection() {
    let (db, _container) = setup();

    let mut app = App::new();
    app.add_plugins(PersistencePlugins::new(db.clone()));

    app.world_mut().resource_mut::<PersistenceSession>().register_component::<RoutedComp>();
    let entity = app.world_mut().spawn(RoutedComp { value: 1 }).id();
    app.update();

    commit_sync(&mut app, db.clone(), TEST_STORE).expect("commit should succeed with provided connection");

    let guid = app.world().get::<Guid>(entity).expect("Guid should be attached after commit");
    let stored = run_async(db.fetch_component(TEST_STORE, guid.id(), RoutedComp::name()))
        .expect("fetch should succeed")
        .expect("component should exist in database");
    let fetched: RoutedComp = serde_json::from_value(stored).expect("deserialize RoutedComp");
    assert_eq!(fetched.value, 1);
}

#[db_matrix_test]
fn commits_can_switch_connections_between_calls() {
    let (db_a, _guard_a) = setup();
    let (db_b, _guard_b) = setup();

    let mut app = App::new();
    app.add_plugins(PersistencePlugins::new(db_a.clone()));
    app.world_mut().resource_mut::<PersistenceSession>().register_component::<RoutedComp>();

    let e1 = app.world_mut().spawn(RoutedComp { value: 1 }).id();
    app.update();
    commit_sync(&mut app, db_a.clone(), TEST_STORE).expect("first commit should succeed on db_a");

    let guid1 = app.world().get::<Guid>(e1).expect("guid for first entity").id().to_string();
    let stored_a = run_async(db_a.fetch_component(TEST_STORE, &guid1, RoutedComp::name()))
        .expect("fetch from db_a should succeed")
        .expect("entity should exist in db_a");
    let fetched_a: RoutedComp = serde_json::from_value(stored_a).expect("deserialize from db_a");
    assert_eq!(fetched_a.value, 1);
    let missing_in_b = run_async(db_b.fetch_component(TEST_STORE, &guid1, RoutedComp::name()))
        .expect("fetch from db_b should succeed");
    assert!(missing_in_b.is_none(), "first commit should not write to db_b");

    let e2 = app.world_mut().spawn(RoutedComp { value: 2 }).id();
    app.update();
    commit_sync(&mut app, db_b.clone(), TEST_STORE).expect("second commit should succeed on db_b");

    let guid2 = app.world().get::<Guid>(e2).expect("guid for second entity").id().to_string();
    let stored_b = run_async(db_b.fetch_component(TEST_STORE, &guid2, RoutedComp::name()))
        .expect("fetch from db_b should succeed")
        .expect("second entity should exist in db_b");
    let fetched_b: RoutedComp = serde_json::from_value(stored_b).expect("deserialize from db_b");
    assert_eq!(fetched_b.value, 2);
    let missing_in_a = run_async(db_a.fetch_component(TEST_STORE, &guid2, RoutedComp::name()))
        .expect("fetch from db_a should succeed");
    assert!(missing_in_a.is_none(), "second commit should not write to db_a");
}

#[test]
fn commit_failure_propagates_error_and_resets_status() {
    let mut mock_db = MockDatabaseConnection::new();
    mock_db.expect_document_key_field().return_const("_key");
    mock_db
        .expect_execute_transaction()
        .times(1)
        .returning(|_| Box::pin(async { Err(PersistenceError::new("boom")) }));

    let db = std::sync::Arc::new(mock_db);
    let mut app = App::new();
    app.add_plugins(PersistencePlugins::new(db.clone()));
    app.world_mut().resource_mut::<PersistenceSession>().register_component::<RoutedComp>();

    app.world_mut().spawn(RoutedComp { value: 9 });
    app.update();

    let res = commit_sync(&mut app, db.clone(), TEST_STORE);
    assert!(res.is_err(), "commit should surface backend error");
    assert_eq!(*app.world().resource::<CommitStatus>(), CommitStatus::Idle, "status should return to Idle after failure");
}
