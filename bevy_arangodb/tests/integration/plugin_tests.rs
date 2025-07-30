use bevy::prelude::Events;
use bevy_arangodb::{
    CommitCompleted, CommitStatus, Guid, MockDatabaseConnection, Persist,
    TriggerCommit,
};
use crate::common::*;
use std::sync::Arc;
use std::time::Duration;

#[tokio::test]
async fn test_trigger_commit_clears_event_queue() {
    let _guard = DB_LOCK.lock().await;
    let mut db = MockDatabaseConnection::new();
    // Expect a transaction because sending events makes the world dirty,
    // which might trigger a commit if there are registered persistable types.
    // We allow it to be called any number of times.
    db.expect_execute_transaction()
        .returning(|_| Box::pin(async { Ok(Default::default()) }));

    let db = Arc::new(db);
    let mut app = new_app(db);

    // GIVEN multiple TriggerCommit events are sent
    app.world_mut().send_event(TriggerCommit::default());
    app.world_mut().send_event(TriggerCommit::default());

    // WHEN the app updates
    let events_before_update = app.world().resource::<Events<TriggerCommit>>();
    assert_eq!(events_before_update.len(), 2);

    // We expect the commit to do nothing but clear the events.
    // Using commit_and_wait ensures this test won't hang.
    diagnostic_commit(&mut app, Duration::from_secs(5)).await.unwrap();

    // THEN the event queue is cleared
    let events_after_update = app.world().resource::<Events<TriggerCommit>>();
    assert_eq!(events_after_update.len(), 0);
}

#[tokio::test]
async fn test_event_triggers_commit_and_persists_data() {
    let _guard = DB_LOCK.lock().await;
    let db = setup().await;
    let mut app = new_app(db.clone());

    // GIVEN an entity is spawned
    let entity_id = app.world_mut().spawn(Health { value: 100 }).id();
    
    // WHEN we commit
    diagnostic_commit(&mut app, Duration::from_secs(5)).await.unwrap();

    // THEN the commit should have completed successfully and the entity should have a Guid
    let guid = app
        .world()
        .get::<Guid>(entity_id)
        .expect("Entity should have a Guid after commit");

    // AND the data should be in the database
    let health_json = db
        .fetch_component(guid.id(), Health::name())
        .await
        .expect("DB fetch failed")
        .expect("Component not found in DB");
    let fetched_health: Health = serde_json::from_value(health_json).unwrap();
    assert_eq!(fetched_health.value, 100);
}

#[tokio::test]
async fn test_queued_commit_persists_all_changes() {
    let _guard = DB_LOCK.lock().await;
    let db = setup().await;
    let mut app = new_app(db.clone());

    // GIVEN an initial entity is created and a commit is triggered
    let entity_a = app.world_mut().spawn(Health { value: 100 }).id();
    app.update();
    app.world_mut().send_event(TriggerCommit::default());
    app.update(); // Start the first commit

    // WHEN another entity is created and a second commit is triggered
    // before the first one has completed
    let entity_b = app.world_mut().spawn(Position { x: 50.0, y: 50.0 }).id();
    app.update();
    app.world_mut().send_event(TriggerCommit::default());
    app.update(); // This should queue the second commit

    // THEN the status should be InProgressAndDirty
    let status = app.world().resource::<CommitStatus>();
    assert_eq!(*status, CommitStatus::InProgressAndDirty);

    // AND we drive the app loop until two commits have completed
    let mut completed_count = 0;
    for _ in 0..40 { // Loop with a timeout (increased iterations for safety)
        app.update();
        let mut events = app.world_mut().resource_mut::<Events<CommitCompleted>>();
        if !events.is_empty() {
            completed_count += events.len();
            events.clear();
        }
        if completed_count >= 2 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    assert_eq!(completed_count, 2, "Expected two commits to complete");

    // AND the final status is Idle
    let final_status = app.world().resource::<CommitStatus>();
    assert_eq!(*final_status, CommitStatus::Idle);

    // AND data from both commits exists in the database
    let guid_a = app.world().get::<Guid>(entity_a).unwrap();
    let health_json = db.fetch_component(guid_a.id(), Health::name()).await.unwrap().unwrap();
    assert_eq!(serde_json::from_value::<Health>(health_json).unwrap().value, 100);

    let guid_b = app.world().get::<Guid>(entity_b).unwrap();
    let pos_json = db.fetch_component(guid_b.id(), Position::name()).await.unwrap().unwrap();
    assert_eq!(serde_json::from_value::<Position>(pos_json).unwrap().x, 50.0);
}