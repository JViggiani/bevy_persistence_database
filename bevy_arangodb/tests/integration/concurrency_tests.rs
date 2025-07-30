use bevy::prelude::*;
use bevy_arangodb::{
    Guid, Persist, PersistenceError, PersistenceQuery,
};
use std::time::Duration;
use crate::common::diagnostic_commit;
use tracing::{info};
use crate::common::*;
use bevy_arangodb_core::arangors::{Connection, document::options::UpdateOptions};
use serde_json::json;

#[tokio::test]
async fn test_conflict_error_surfaces_correctly() {
    let _guard = DB_LOCK.lock().await;
    let context = setup().await;
    let db = context.db;

    // GIVEN two separate app "sessions" connected to the same DB
    let mut app1 = new_app(db.clone());
    let mut app2 = new_app(db.clone());

    // AND an entity is created and committed by app1
    let entity_id_app1 = app1.world_mut().spawn(Health { value: 100 }).id();
    diagnostic_commit(&mut app1, Duration::from_secs(5)).await.unwrap();
    let guid = app1.world().get::<Guid>(entity_id_app1).unwrap().id().to_string();

    // WHEN both apps load the same entity, caching its initial revision
    let loaded_entities_app1 = PersistenceQuery::new(db.clone()).filter(Health::value().eq(100)).fetch_into(app1.world_mut()).await;
    let entity_id_app1 = loaded_entities_app1[0];
    let loaded_entities_app2 = PersistenceQuery::new(db.clone()).filter(Health::value().eq(100)).fetch_into(app2.world_mut()).await;
    let entity_id_app2 = loaded_entities_app2[0];

    // AND app1 successfully modifies and commits the entity, updating its _rev in the DB
    app1.world_mut().get_mut::<Health>(entity_id_app1).unwrap().value = 90;
    diagnostic_commit(&mut app1, Duration::from_secs(5)).await.unwrap();

    // THEN an attempt by app2 to commit a change to the same entity fails with a conflict
    app2.world_mut().get_mut::<Health>(entity_id_app2).unwrap().value = 80;
    let result = diagnostic_commit(&mut app2, Duration::from_secs(5)).await;

    assert!(matches!(result, Err(PersistenceError::Conflict { key }) if key == guid));
}

#[tokio::test]
async fn test_conflict_strategy_first_write_wins() {
    let _guard = DB_LOCK.lock().await;
    let context = setup().await;
    let db = context.db;
    let mut app1 = new_app(db.clone());
    let mut app2 = new_app(db.clone());

    // GIVEN an entity is created
    let _entity_id_app1 = app1.world_mut().spawn(Health { value: 100 }).id();
    diagnostic_commit(&mut app1, Duration::from_secs(5)).await.unwrap();

    // WHEN both apps load it
    let loaded_entities_app1 = PersistenceQuery::new(db.clone()).with::<Health>().fetch_into(app1.world_mut()).await;
    let entity_id_app1 = loaded_entities_app1[0];
    PersistenceQuery::new(db.clone()).with::<Health>().fetch_into(app2.world_mut()).await;

    // AND app1 commits a change (first write)
    app1.world_mut().get_mut::<Health>(entity_id_app1).unwrap().value = 90;
    diagnostic_commit(&mut app1, Duration::from_secs(5)).await.unwrap();

    // AND app2 attempts to commit a change, which fails
    let entity_id_app2 = app2.world_mut().query_filtered::<Entity, With<Health>>().single(app2.world()).unwrap();
    app2.world_mut().get_mut::<Health>(entity_id_app2).unwrap().value = 80;
    let result = diagnostic_commit(&mut app2, Duration::from_secs(5)).await;
    assert!(matches!(result, Err(PersistenceError::Conflict { .. })));

    // THEN the value in the database reflects the first write
    let guid = app1.world().get::<Guid>(entity_id_app1).unwrap().id();
    let (final_doc, _rev) = db.fetch_document(guid).await.unwrap().unwrap();
    let health_val = final_doc.get(Health::name()).unwrap();
    let health: Health = serde_json::from_value(health_val.clone()).unwrap();
    assert_eq!(health.value, 90);
}

#[tokio::test]
async fn test_conflict_strategy_last_write_wins() {
    info!("Starting test_conflict_strategy_last_write_wins");
    let _guard = DB_LOCK.lock().await;
    let context = setup().await;
    let db = context.db;
    let mut app1 = new_app(db.clone());
    let mut app2 = new_app(db.clone());

    // GIVEN an entity is created and loaded by both apps
    info!("Creating initial entity");
    let _entity_id_app1 = app1.world_mut().spawn(Health { value: 100 }).id();
    diagnostic_commit(&mut app1, Duration::from_secs(5)).await.unwrap();
    
    info!("Loading entity into both apps");
    let loaded_entities_app1 = PersistenceQuery::new(db.clone()).with::<Health>().fetch_into(app1.world_mut()).await;
    let entity_id_app1 = loaded_entities_app1[0];
    PersistenceQuery::new(db.clone()).with::<Health>().fetch_into(app2.world_mut()).await;

    // AND app1 commits a change
    info!("App1 changing entity");
    app1.world_mut().get_mut::<Health>(entity_id_app1).unwrap().value = 90;
    diagnostic_commit(&mut app1, Duration::from_secs(5)).await.unwrap();

    // WHEN app2 attempts to commit and gets a conflict
    info!("App2 changing entity and expecting conflict");
    let entity_id_app2 = app2.world_mut().query_filtered::<Entity, With<Health>>().single(app2.world()).unwrap();
    app2.world_mut().get_mut::<Health>(entity_id_app2).unwrap().value = 80;
    
    let result = diagnostic_commit(&mut app2, Duration::from_secs(5)).await;
    assert!(matches!(result, Err(PersistenceError::Conflict { .. })));

    // THEN app2 can reload the entity, re-apply its change, and commit successfully
    info!("Reloading entity in app2");
    PersistenceQuery::new(db.clone()).with::<Health>().fetch_into(app2.world_mut()).await;

    // Re-query for the entity after reloading
    info!("Re-querying for entity and applying change");
    let entity_id_app2 = app2.world_mut().query_filtered::<Entity, With<Health>>().single(app2.world()).unwrap();
    app2.world_mut().get_mut::<Health>(entity_id_app2).unwrap().value = 80;
    
    // Run an update to allow Bevy's change detection to pick up the modification.
    app2.update();

    info!("Committing from app2 after reload");
    diagnostic_commit(&mut app2, Duration::from_secs(5)).await.unwrap();

    // AND the world should still only contain one entity
    assert_eq!(
        app2.world_mut().query::<&Guid>().iter(app2.world()).count(),
        1,
        "Reloading should not create duplicate entities"
    );

    // AND the final value in the database reflects the last write
    let guid = app1.world().get::<Guid>(entity_id_app1).unwrap().id();
    let (final_doc, _rev) = db
        .fetch_document(guid)
        .await
        .unwrap()
        .unwrap();
    let health_val = final_doc.get(Health::name()).unwrap();
    let health: Health = serde_json::from_value(health_val.clone()).unwrap();
    assert_eq!(health.value, 80);
}

#[tokio::test]
async fn test_conflict_strategy_three_way_merge() {
    info!("Starting test_conflict_strategy_three_way_merge");
    let _guard = DB_LOCK.lock().await;
    let context = setup().await;
    let db_arc = context.db;

    // --- Test Setup ---
    // 1. A single Bevy App
    let mut app = new_app(db_arc.clone());

    // 2. A direct DB client to simulate an external modification
    let conn = Connection::establish_jwt(&context.db_url, &context.db_user, &context.db_pass).await.unwrap();
    let direct_db = conn.db(&context.db_name).await.unwrap();
    let entities_collection = direct_db.collection("entities").await.unwrap();

    // --- GIVEN ---
    // An entity is created and persisted by our app.
    let e1 = app.world_mut().spawn((Health { value: 100 }, Position { x: 0.0, y: 0.0 })).id();
    diagnostic_commit(&mut app, Duration::from_secs(5)).await.unwrap();
    
    // The app now has the entity loaded with the initial revision.
    PersistenceQuery::new(db_arc.clone()).with::<Health>().with::<Position>().fetch_into(app.world_mut()).await;
    let guid = app.world().get::<Guid>(e1).unwrap().id().to_string();

    // --- WHEN ---
    // An external process modifies the Health component directly in the database.
    info!("Simulating external modification to Health component");
    let (mut doc, initial_rev) = db_arc.fetch_document(&guid).await.unwrap().unwrap();
    doc.as_object_mut().unwrap().insert(Health::name().to_string(), json!({"value": 90}));
    
    let update_options = UpdateOptions::builder().ignore_revs(false).build();
    let update_response = entities_collection.update_document(&guid, doc, update_options).await.unwrap();
    let new_rev_from_external_commit = update_response.header().unwrap()._rev.clone();
    assert_ne!(initial_rev, new_rev_from_external_commit);
    info!("External modification successful. New rev: {}", new_rev_from_external_commit);

    // The app, unaware of the change, tries to modify the Position component.
    // This MUST fail with a conflict because the app is using a stale `_rev`.
    info!("App attempting to commit with stale revision, expecting conflict");
    app.world_mut().get_mut::<Position>(e1).unwrap().x = 50.0;
    let result = diagnostic_commit(&mut app, Duration::from_secs(5)).await;
    assert!(matches!(result, Err(PersistenceError::Conflict { .. })), "Expected a conflict error, but got: {:?}", result);
    info!("App correctly received a conflict error.");

    // --- THEN ---
    // The app handles the conflict by reloading the entity, which fetches the latest state.
    info!("App reloading entity to resolve conflict");
    PersistenceQuery::new(db_arc.clone()).with::<Health>().with::<Position>().fetch_into(app.world_mut()).await;

    // The app re-applies its change to Position.
    app.world_mut().get_mut::<Position>(e1).unwrap().x = 50.0;
    
    // This second commit attempt MUST succeed.
    info!("App retrying commit after reload");
    diagnostic_commit(&mut app, Duration::from_secs(5)).await.unwrap();
    info!("App successfully committed after conflict resolution.");

    // --- Final Verification ---
    // The final document in the database should have BOTH changes.
    let (final_doc, _rev) = db_arc.fetch_document(&guid).await.unwrap().unwrap();
    let health: Health = serde_json::from_value(final_doc.get(Health::name()).unwrap().clone()).unwrap();
    let pos: Position = serde_json::from_value(final_doc.get(Position::name()).unwrap().clone()).unwrap();
    
    assert_eq!(health.value, 90, "Health change from external modification should be present");
    assert_eq!(pos.x, 50.0, "Position change from app's successful retry should be present");
    info!("Final document state is correct. Test passed.");
}