use bevy::app::App;
use bevy_arangodb::{commit, Guid, ArangoPlugin};
use bevy_arangodb::Persist;

mod common;
use common::*;

#[tokio::test]
async fn test_create_new_entity() {
    let _guard = DB_LOCK.lock().await;
    let db = setup().await;

    let mut app = App::new();
    app.add_plugins(ArangoPlugin::new(db.clone()));

    let health_val = Health { value: 100 };
    let pos_val = Position { x: 1.0, y: 2.0 };

    let entity_id = app.world.spawn((health_val, pos_val)).id();
    
    app.update(); // Run the schedule to trigger change detection

    commit(&mut app).await.unwrap();

    // 4. Verify the results
    // The entity should now have a Guid component assigned by the library.
    let guid = app.world.get::<Guid>(entity_id)
        .expect("Entity should have a Guid after commit");

    assert!(!guid.id().is_empty(), "Guid should not be empty");

    // To be absolutely sure, fetch the Health component directly from the DB
    // using the new Guid and verify its content.
    let health_json = db
        .fetch_component(guid.id(), Health::name())
        .await
        .expect("Failed to fetch component from DB")
        .expect("Component should exist in DB");

    let fetched_health: Health =
        serde_json::from_value(health_json).expect("Failed to deserialize Health component");

    assert_eq!(
        fetched_health.value, 100,
        "The health value in the database is incorrect"
    );
}

#[tokio::test]
async fn test_create_new_resource() {
    let _guard = DB_LOCK.lock().await;
    let db = setup().await;

    // 1. Create a session, add a resource, and commit it
    let mut app = App::new();
    app.add_plugins(ArangoPlugin::new(db.clone()));

    let settings = GameSettings {
        difficulty: 0.8,
        map_name: "level_1".into(),
    };
    app.insert_resource(settings);

    app.update(); // Run the schedule to trigger change detection

    commit(&mut app).await.expect("Commit failed");

    // 2. Verify the resource was saved correctly by fetching it directly
    let resource_name = GameSettings::name();
    let resource_json = db
        .fetch_resource(resource_name)
        .await
        .expect("Failed to fetch resource from DB")
        .expect("Resource should exist in DB");

    let fetched_settings: GameSettings =
        serde_json::from_value(resource_json).expect("Failed to deserialize resource");

    assert_eq!(fetched_settings.difficulty, 0.8);
    assert_eq!(fetched_settings.map_name, "level_1");
}

#[tokio::test]
async fn test_entity_delete() {
    let _guard = DB_LOCK.lock().await;
    let db = setup().await;

    let mut app = App::new();
    app.add_plugins(ArangoPlugin::new(db.clone()));

    // 1. Spawn and commit an entity.
    let entity_id = app.world.spawn(Health { value: 100 }).id();
    
    app.update();

    commit(&mut app).await.expect("Commit failed");

    // 2. Verify it exists in the database.
    let guid = app.world.get::<Guid>(entity_id).unwrap().id().to_string();
    let component = db
        .fetch_component(&guid, Health::name())
        .await
        .expect("Fetch should not fail")
        .expect("Component should exist after first commit");
    assert_eq!(component.get("value").unwrap().as_i64().unwrap(), 100);

    // 3. Despawn the entity and commit again.
    app.world.entity_mut(entity_id).despawn();
    app.update(); // This runs the despawn command and our auto-despawn-tracking system
    commit(&mut app).await.expect("Second commit failed");

    // 4. Verify it's gone from the database.
    let component_after_delete = db
        .fetch_component(&guid, Health::name())
        .await
        .expect("Fetch should not fail");

    assert!(
        component_after_delete.is_none(),
        "Component should be gone after delete commit"
    );
}
