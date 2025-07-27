use bevy::prelude::{App, Events};
use bevy_arangodb::{
    CommitCompleted, Guid, MockDatabaseConnection, PersistencePlugins, Persist,
    TriggerCommit,
};
use crate::common::*;
use std::sync::Arc;

#[tokio::test]
async fn test_trigger_commit_clears_event_queue() {
    let mut db = MockDatabaseConnection::new();
    // Expect a transaction because sending events makes the world dirty,
    // which might trigger a commit if there are registered persistable types.
    // We allow it to be called any number of times.
    db.expect_execute_transaction()
        .returning(|_| Box::pin(async { Ok(vec![]) }));

    let db = Arc::new(db);
    let mut app = App::new();
    app.add_plugins(PersistencePlugins(db));

    // GIVEN multiple TriggerCommit events are sent
    app.world_mut().send_event(TriggerCommit::default());
    app.world_mut().send_event(TriggerCommit::default());

    // WHEN the app updates
    let events_before_update = app.world().resource::<Events<TriggerCommit>>();
    assert_eq!(events_before_update.len(), 2);

    app.update();

    // THEN the event queue is cleared
    let events_after_update = app.world().resource::<Events<TriggerCommit>>();
    assert_eq!(events_after_update.len(), 0);
}

#[tokio::test]
async fn test_event_triggers_commit_and_persists_data() {
    let _guard = DB_LOCK.lock().await;
    let db = setup().await;
    let mut app = App::new();
    app.add_plugins(PersistencePlugins(db.clone()));

    // GIVEN an entity is spawned
    let entity_id = app.world_mut().spawn(Health { value: 100 }).id();
    app.update(); // Run change detection

    // WHEN a TriggerCommit event is sent
    app.world_mut().send_event(TriggerCommit::default());

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
        tokio::task::yield_now().await;
    }

    // THEN the commit should have completed successfully
    let mut events = app.world_mut().resource_mut::<Events<CommitCompleted>>();
    assert_eq!(events.len(), 1);
    let event = events.drain().next().unwrap();
    assert!(event.0.is_ok());

    // AND the entity should have a Guid
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