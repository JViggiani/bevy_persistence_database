use bevy::prelude::{App, Events, IntoScheduleConfigs};
use bevy_persistence_database::{
    CommitCompleted, CommitStatus, Guid, MockDatabaseConnection, persistence_plugin::PersistencePlugins, Persist,
    TriggerCommit,
    commit_sync, PersistentQuery, persistence_plugin::PersistenceSystemSet,
};
use crate::common::*;
use std::sync::Arc;
use bevy_persistence_database_derive::db_matrix_test;

#[test]
fn test_trigger_commit_clears_event_queue() {
    let mut db = MockDatabaseConnection::new();
    db.expect_document_key_field().return_const("_key");
    // Expect a transaction because sending events makes the world dirty,
    // which might trigger a commit if there are registered persistable types.
    // We allow it to be called any number of times.
    db.expect_execute_transaction()
        .returning(|_| Box::pin(async { Ok(vec![]) }));

    let db = Arc::new(db);
    let mut app = App::new();
    app.add_plugins(PersistencePlugins::new(db.clone()));

    // GIVEN multiple TriggerCommit events are sent
    let trigger = TriggerCommit { correlation_id: None, target_connection: db.clone() };
    app.world_mut().send_event(trigger.clone());
    app.world_mut().send_event(trigger);

    // WHEN the app updates
    let events_before_update = app.world().resource::<Events<TriggerCommit>>();
    assert_eq!(events_before_update.len(), 2);

    app.update();

    // THEN the event queue is cleared
    let events_after_update = app.world().resource::<Events<TriggerCommit>>();
    assert_eq!(events_after_update.len(), 0);
}

#[db_matrix_test]
fn test_event_triggers_commit_and_persists_data() {
    let (db, _container) = setup();
    let mut app = App::new();
    app.add_plugins(PersistencePlugins::new(db.clone()));

    // GIVEN an entity is spawned
    let entity_id = app.world_mut().spawn(Health { value: 100 }).id();
    app.update(); // Run change detection

    // WHEN a TriggerCommit event is sent
    app.world_mut().send_event(TriggerCommit { correlation_id: None, target_connection: db.clone() });

    // AND we manually drive the app loop until the commit is complete
    loop {
        app.update();
        if !app
            .world()
            .resource::<Events<CommitCompleted>>()
            .is_empty()
        {
            break;
        }
        std::thread::yield_now();
    }

    // THEN the commit should have completed successfully
    let mut events = app.world_mut().resource_mut::<Events<CommitCompleted>>();
    assert_eq!(events.len(), 1);
    let event = events.drain().next().unwrap();
    assert!(event.0.is_ok());

    let guid = app.world().get::<Guid>(entity_id).expect("Guid after commit");
    let health_json = run_async(db.fetch_component(guid.id(), Health::name()))
        .expect("DB fetch failed")
        .expect("Component not found in DB");
    let fetched_health: Health = serde_json::from_value(health_json).unwrap();
    assert_eq!(fetched_health.value, 100);
}

#[db_matrix_test]
fn test_queued_commit_persists_all_changes() {
    let (db, _container) = setup();
    let mut app = App::new();
    app.add_plugins(PersistencePlugins::new(db.clone()));

    // GIVEN an initial entity is created and a commit is triggered
    let entity_a = app.world_mut().spawn(Health { value: 100 }).id();
    app.update();
    app.world_mut().send_event(TriggerCommit { correlation_id: None, target_connection: db.clone() });
    app.update(); // Start the first commit

    // WHEN another entity is created and a second commit is triggered
    // before the first one has completed
    let entity_b = app.world_mut().spawn(Position { x: 50.0, y: 50.0 }).id();
    app.update();
    app.world_mut().send_event(TriggerCommit { correlation_id: None, target_connection: db.clone() });
    app.update(); // This should queue the second commit

    // THEN the status should be InProgressAndDirty
    assert_eq!(*app.world().resource::<CommitStatus>(), CommitStatus::InProgressAndDirty);

    // AND we drive the app loop until two commits have completed
    let mut completed_count = 0;
    for _ in 0..20 { // Loop with a timeout
        app.update();
        let mut events = app.world_mut().resource_mut::<Events<CommitCompleted>>();
        if !events.is_empty() {
            completed_count += events.len();
            events.clear();
        }
        if completed_count >= 2 {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(50));
    }
    assert_eq!(completed_count, 2, "Expected two commits to complete");
    
    // AND the final status is Idle
    assert_eq!(*app.world().resource::<CommitStatus>(), CommitStatus::Idle);

    // AND data from both commits exists in the database
    let guid_a = app.world().get::<Guid>(entity_a).unwrap();
    let health_json = run_async(db.fetch_component(guid_a.id(), Health::name()))
        .unwrap()
        .unwrap();
    assert_eq!(serde_json::from_value::<Health>(health_json).unwrap().value, 100);

    let guid_b = app.world().get::<Guid>(entity_b).unwrap();
    let pos_json = run_async(db.fetch_component(guid_b.id(), Position::name()))
        .unwrap()
        .unwrap();
    let pos: Position = serde_json::from_value(pos_json).unwrap();
    assert_eq!(pos.x, 50.0);
}

#[db_matrix_test]
fn test_postupdate_load_applies_next_frame() {
    let (db, _container) = setup();

    // GIVEN: one entity to load
    let mut app1 = App::new();
    app1.add_plugins(PersistencePlugins::new(db.clone()));
    app1.world_mut().spawn((Health { value: 7 }, Position { x: 1.0, y: 2.0 }));
    app1.update();
    commit_sync(&mut app1, db.clone()).expect("commit failed");

    // WHEN: load is triggered from PostUpdate
    let mut app2 = App::new();
    app2.add_plugins(PersistencePlugins::new(db.clone()));
    fn postupdate_load(mut pq: PersistentQuery<(&Health, &Position)>) {
        let _ = pq.ensure_loaded();
    }
    // Ensure the loader runs before PreCommit so ops are applied in the same frame.
    app2.add_systems(
        bevy::prelude::PostUpdate,
        postupdate_load.before(PersistenceSystemSet::PreCommit),
    );

    // First frame: load queued and applied within PostUpdate; data is visible immediately
    app2.update();
    let mut q0 = app2.world_mut().query::<&Health>();
    assert_eq!(q0.iter(&app2.world()).count(), 1, "PostUpdate load should be visible in the same frame");

    // Second frame: remains visible
    app2.update();
    let mut q1 = app2.world_mut().query::<(&Health, &Position)>();
    assert_eq!(q1.iter(&app2.world()).count(), 1, "Loaded entity should be present");
}