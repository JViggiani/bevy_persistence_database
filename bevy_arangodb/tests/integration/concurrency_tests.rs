use bevy::prelude::*;
use bevy_arangodb::{
    commit, Guid, Persist, PersistenceError, PersistencePlugins, PersistenceQuery,
};
use tracing;

use crate::common::*;

#[tokio::test]
async fn test_conflict_error_surfaces_correctly() {
    let _guard = DB_LOCK.lock().await;
    let db = setup().await;
    let mut app = App::new();
    app.add_plugins(PersistencePlugins(db.clone()));

    // 1. GIVEN a committed entity
    let entity_id = app.world_mut().spawn(Health { value: 100 }).id();
    app.update();
    commit(&mut app).await.expect("Initial commit failed");
    let guid = app.world().get::<Guid>(entity_id).unwrap().id().to_string();

    // 2. WHEN the document is modified directly in the database (simulating another user)
    // Let's do a direct update via AQL to be sure _rev changes.
    let aql = format!(
        "UPDATE \"{}\" WITH {{ `{}`: {{ \"value\": 200 }} }} IN entities",
        guid,
        Health::name()
    );
    db.query(aql, Default::default()).await.unwrap();

    // 3. AND the original app tries to modify and commit the same entity
    let mut health = app.world_mut().get_mut::<Health>(entity_id).unwrap();
    health.value = 50;
    app.update();

    // 4. THEN the commit fails with a Conflict error
    let result = commit(&mut app).await;
    assert!(result.is_err());
    match result.err().unwrap() {
        PersistenceError::Conflict { key } => assert_eq!(key, guid),
        e => panic!("Expected a Conflict error, but got {:?}", e),
    }
}

#[tokio::test]
async fn test_conflict_strategy_first_write_wins() {
    let _guard = DB_LOCK.lock().await;
    let db = setup().await;
    let mut app = App::new();
    app.add_plugins(PersistencePlugins(db.clone()));

    // GIVEN a committed entity
    let entity_id = app.world_mut().spawn(Health { value: 100 }).id();
    app.update();
    commit(&mut app).await.expect("Initial commit failed");
    let guid = app.world().get::<Guid>(entity_id).unwrap().id().to_string();

    // WHEN an external process writes a change (first write)
    let aql = format!(
        "UPDATE \"{}\" WITH {{ `{}`: {{ \"value\": 200 }} }} IN entities",
        guid,
        Health::name()
    );
    db.query(aql, Default::default()).await.unwrap();

    // AND our app tries to commit a different change
    let mut health = app.world_mut().get_mut::<Health>(entity_id).unwrap();
    health.value = 50;
    app.update();
    let result = commit(&mut app).await;

    // THEN the app's commit fails due to conflict
    assert!(matches!(result, Err(PersistenceError::Conflict { .. })));

    // AND the data in the database reflects the first write
    let health_json = db
        .fetch_component(&guid, Health::name())
        .await
        .unwrap()
        .unwrap();
    let final_health: Health = serde_json::from_value(health_json).unwrap();
    assert_eq!(final_health.value, 200);
}

#[tokio::test]
async fn test_conflict_strategy_last_write_wins() {
    let _guard = DB_LOCK.lock().await;
    let db = setup().await;
    let mut app = App::new();
    app.add_plugins(PersistencePlugins(db.clone()));

    // GIVEN a committed entity
    let entity_id = app.world_mut().spawn(Health { value: 100 }).id();
    app.update();
    commit(&mut app).await.expect("Initial commit failed");
    let guid = app.world().get::<Guid>(entity_id).unwrap().id().to_string();

    // WHEN an external process writes a change
    let aql = format!(
        "UPDATE \"{}\" WITH {{ `{}`: {{ \"value\": 200 }} }} IN entities",
        guid,
        Health::name()
    );
    db.query(aql, Default::default()).await.unwrap();

    // AND our app tries to commit, gets a conflict, and retries (last write wins)
    let mut health = app.world_mut().get_mut::<Health>(entity_id).unwrap();
    health.value = 50; // Our intended change
    app.update();

    if let Err(PersistenceError::Conflict { .. }) = commit(&mut app).await {
        // Reload the entity to get the latest _rev
        PersistenceQuery::new(db.clone())
            .with::<Health>()
            .fetch_into(app.world_mut())
            .await;

        // Re-apply our change
        let mut health = app.world_mut().get_mut::<Health>(entity_id).unwrap();
        health.value = 50;
        app.update();

        // Commit again
        commit(&mut app).await.expect("Retry commit failed");
    } else {
        panic!("Expected a conflict on the first commit attempt");
    }

    // THEN the final data in the database reflects our app's change
    let health_json = db
        .fetch_component(&guid, Health::name())
        .await
        .unwrap()
        .unwrap();
    let final_health: Health = serde_json::from_value(health_json).unwrap();
    assert_eq!(final_health.value, 50);
}

#[tokio::test]
async fn test_conflict_strategy_three_way_merge() {
    let _guard = DB_LOCK.lock().await;
    let db = setup().await;
    let mut app = App::new();
    app.add_plugins(PersistencePlugins(db.clone()));

    // GIVEN a committed entity with Health and Position
    let entity_id = app
        .world_mut()
        .spawn((Health { value: 100 }, Position { x: 10.0, y: 10.0 }))
        .id();
    app.update();
    commit(&mut app).await.expect("Initial commit failed");
    let guid = app.world().get::<Guid>(entity_id).unwrap().id().to_string();

    // WHEN an external process changes Health
    let aql = format!(
        "UPDATE \"{}\" WITH {{ `{}`: {{ \"value\": 200 }} }} IN entities RETURN NEW._rev",
        guid,
        Health::name()
    );
    db.query(aql, Default::default()).await.unwrap();

    // AND our app changes Position
    let mut pos = app.world_mut().get_mut::<Position>(entity_id).unwrap();
    pos.x = 50.0;
    app.update();

    // AND our app tries to commit, gets a conflict...
    if let Err(PersistenceError::Conflict { .. }) = commit(&mut app).await {
        tracing::info!("Successfully detected conflict, as expected.");

        // Reload to get the latest state from the DB (new Health, old Position)
        PersistenceQuery::new(db.clone())
            .with::<Health>()
            .with::<Position>()
            .fetch_into(app.world_mut())
            .await;

        // Verify that the external change was loaded
        assert_eq!(app.world().get::<Health>(entity_id).unwrap().value, 200);

        // Re-apply our local change
        let mut pos = app.world_mut().get_mut::<Position>(entity_id).unwrap();
        pos.x = 50.0;
        app.update();

        // Commit the merged state
        commit(&mut app).await.expect("Merge commit failed");
    } else {
        panic!("Expected a conflict on the first commit attempt");
    }

    // THEN the final document has both the external Health change and our Position change
    let final_doc = db.fetch_document(&guid).await.unwrap().unwrap().0;
    let final_health: Health = serde_json::from_value(final_doc[Health::name()].clone()).unwrap();
    let final_pos: Position =
        serde_json::from_value(final_doc[Position::name()].clone()).unwrap();

    assert_eq!(final_health.value, 200);
    assert_eq!(final_pos.x, 50.0);
}