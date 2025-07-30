use crate::common::*;
use bevy::prelude::*;
use bevy_arangodb::{Guid, Persist, PersistenceQuery, PersistenceSession};
use serde_json::json;
use std::time::Duration;

#[tokio::test]
async fn test_create_new_entity() {
    let context = setup().await;
    let db = context.db.clone();
    let mut app = new_app(db.clone());

    let health_val = Health { value: 100 };
    let pos_val = Position { x: 1.0, y: 2.0 };

    let entity_id = app.world_mut().spawn((health_val, pos_val)).id();

    app.update(); // Run the schedule to trigger change detection

    diagnostic_commit(&mut app, Duration::from_secs(5))
        .await
        .expect("Commit failed during test execution");

    // 4. Verify the results
    // The entity should now have a Guid component assigned by the library.
    let guid = app
        .world()
        .get::<Guid>(entity_id)
        .expect("Entity should have a Guid after commit");

    assert!(!guid.id().is_empty(), "Guid should not be empty");

    // To be absolutely sure, fetch the Health component directly from the DB
    // using the new Guid and verify its content.
    let health_json = db
        .fetch_component(guid.id(), Health::name())
        .await
        .unwrap()
        .unwrap();

    let fetched_health: Health =
        serde_json::from_value(health_json).expect("Failed to deserialize Health component");

    assert_eq!(
        fetched_health.value, 100,
        "The health value in the database is incorrect"
    );
}

#[tokio::test]
async fn test_create_new_resource() {
    let context = setup().await;
    let db = context.db.clone();
    let mut app = new_app(db.clone());

    // 1. Create a session, add a resource, and commit it
    let settings = GameSettings {
        difficulty: 0.8,
        map_name: "level_1".into(),
    };
    app.insert_resource(settings);

    app.update(); // Run the schedule to trigger change detection

    diagnostic_commit(&mut app, Duration::from_secs(5))
        .await
        .expect("Commit failed during test execution");

    // 2. Verify the resource was saved correctly by fetching it directly
    let resource_name = GameSettings::name();
    let resource_json = db
        .fetch_resource(resource_name)
        .await
        .unwrap()
        .unwrap();

    let fetched_settings: GameSettings =
        serde_json::from_value(resource_json).expect("Failed to deserialize resource");

    assert_eq!(fetched_settings.difficulty, 0.8);
    assert_eq!(fetched_settings.map_name, "level_1");
}

#[tokio::test]
async fn test_delete_persisted_resource() {
    let context = setup().await;
    let db = context.db.clone();
    let mut app = new_app(db.clone());

    // 1. GIVEN a committed GameSettings resource
    let settings = GameSettings {
        difficulty: 0.8,
        map_name: "level_1".into(),
    };
    app.insert_resource(settings);
    app.update();
    diagnostic_commit(&mut app, Duration::from_secs(5)).await.expect("Initial commit failed");

    // Verify it exists in the database
    let resource_json = db
        .fetch_resource(GameSettings::name())
        .await
        .expect("Fetch should not fail")
        .expect("Resource should exist after first commit");
    let fetched: GameSettings = serde_json::from_value(resource_json).unwrap();
    assert_eq!(fetched.difficulty, 0.8);

    // 2. WHEN the resource is removed and marked as despawned
    app.world_mut().remove_resource::<GameSettings>();
    app.world_mut()
        .resource_mut::<PersistenceSession>()
        .mark_resource_despawned::<GameSettings>(); // NOTE: no automatic way in bevy to detect deleted resource - must do this manually when deleting a resource for now... 
    app.update();
    diagnostic_commit(&mut app, Duration::from_secs(5)).await.expect("Delete commit failed");

    // 3. THEN it is gone from the database
    let resource_after_delete = db
        .fetch_resource(GameSettings::name())
        .await
        .unwrap();

    assert!(
        resource_after_delete.is_none(),
        "Resource should be gone after delete commit"
    );
}

#[tokio::test]
async fn test_update_existing_entity() {
    let context = setup().await;
    let db = context.db.clone();
    let mut app = new_app(db.clone());

    // 1. GIVEN a committed entity with a Health component of value 100
    let entity_id = app.world_mut().spawn(Health { value: 100 }).id();
    app.update();
    diagnostic_commit(&mut app, Duration::from_secs(5)).await.expect("Initial commit failed");

    // Get the Guid to use for direct DB verification later
    let guid = app
        .world()
        .get::<Guid>(entity_id)
        .unwrap()
        .id()
        .to_string();

    // Verify initial state in DB
    let health_json_before = db
        .fetch_component(&guid, Health::name())
        .await
        .unwrap()
        .unwrap();
    let fetched_health_before: Health =
        serde_json::from_value(health_json_before).expect("Deserialization before update failed");
    assert_eq!(fetched_health_before.value, 100);

    // 2. WHEN the entity's Health value is changed to 50
    let mut health = app
        .world_mut()
        .get_mut::<Health>(entity_id)
        .unwrap();
    health.value = 50;

    app.update(); // This will mark the component as Changed

    // 3. AND the app is committed again
    diagnostic_commit(&mut app, Duration::from_secs(5)).await.expect("Second commit failed");

    // 4. THEN the Health data in the database for that entity's Guid reflects the new value of 50.
    let health_json_after = db
        .fetch_component(&guid, Health::name())
        .await
        .unwrap()
        .unwrap();

    let fetched_health_after: Health =
        serde_json::from_value(health_json_after).expect("Failed to deserialize Health component");

    assert_eq!(
        fetched_health_after.value, 50,
        "The health value in the database was not updated correctly"
    );
}

#[tokio::test]
async fn test_update_existing_resource() {
    let context = setup().await;
    let db = context.db.clone();
    let mut app = new_app(db.clone());

    // 1. GIVEN a committed GameSettings resource
    let initial_settings = GameSettings {
        difficulty: 0.8,
        map_name: "level_1".into(),
    };
    app.insert_resource(initial_settings);
    app.update(); // Run schedule to trigger change detection
    diagnostic_commit(&mut app, Duration::from_secs(5)).await.expect("Initial commit failed");

    // 2. WHEN the GameSettings resource is modified in the app
    let mut settings = app.world_mut().resource_mut::<GameSettings>();
    settings.difficulty = 0.5;
    settings.map_name = "level_2".into();

    app.update(); // Run schedule to trigger change detection on the resource

    // 3. AND the app is committed again
    diagnostic_commit(&mut app, Duration::from_secs(5)).await.expect("Second commit failed");

    // 4. THEN the GameSettings data in the database reflects the new values.
    let resource_name = GameSettings::name();
    let resource_json_after = db
        .fetch_resource(resource_name)
        .await
        .unwrap()
        .unwrap();

    let fetched_settings_after: GameSettings =
        serde_json::from_value(resource_json_after).expect("Failed to deserialize resource");

    assert_eq!(fetched_settings_after.difficulty, 0.5);
    assert_eq!(fetched_settings_after.map_name, "level_2");
}

#[tokio::test]
async fn test_delete_persisted_entity() {
    let context = setup().await;
    let db = context.db.clone();
    let mut app = new_app(db.clone());

    // 1. Spawn and commit an entity.
    let entity_id = app.world_mut().spawn(Health { value: 100 }).id();

    app.update();

    diagnostic_commit(&mut app, Duration::from_secs(5)).await.expect("Initial commit failed");

    // 2. Verify it exists in the database.
    let guid = app
        .world()
        .get::<Guid>(entity_id)
        .unwrap()
        .id()
        .to_string();
    let component = db
        .fetch_component(&guid, Health::name())
        .await
        .expect("Fetch should not fail")
        .expect("Component should exist after first commit");
    assert_eq!(component.get("value").unwrap().as_i64().unwrap(), 100);

    // 3. Despawn the entity and commit again.
    app.world_mut().entity_mut(entity_id).despawn();
    app.update(); // This runs the despawn command and our auto-despawn-tracking system
    diagnostic_commit(&mut app, Duration::from_secs(5)).await.expect("Delete commit failed");

    // 4. Verify it's gone from the database.
    let component_after_delete = db
        .fetch_component(&guid, Health::name())
        .await
        .unwrap();

    assert!(
        component_after_delete.is_none(),
        "Component should be gone after delete commit"
    );
}

#[tokio::test]
async fn test_no_op_commit() {
    let context = setup().await;
    let db = context.db.clone();
    let mut app = new_app(db.clone());

    // GIVEN a committed app in a synchronized state with the database
    app.world_mut().spawn(Health { value: 100 });
    app.update();
    diagnostic_commit(&mut app, Duration::from_secs(5))
        .await
        .expect("Initial commit failed");

    // WHEN the app is committed again with no changes made to any entities or resources
    diagnostic_commit(&mut app, Duration::from_secs(5))
        .await
        .expect("Second commit with no changes failed");

    // THEN the commit operation succeeds without error
    // AND no database write operations are performed (this is handled by an early return in the commit function)
    let status = app.world().resource::<bevy_arangodb::CommitStatus>();
    assert_eq!(*status, bevy_arangodb::CommitStatus::Idle);
}

#[tokio::test]
async fn test_add_new_component_to_existing_entity() {
    let context = setup().await;
    let db = context.db.clone();
    let mut app = new_app(db.clone());

    // 1. GIVEN a committed entity with only a Health component
    let entity_id = app.world_mut().spawn(Health { value: 100 }).id();
    app.update();
    diagnostic_commit(&mut app, Duration::from_secs(5)).await.expect("Initial commit failed");

    let guid = app
        .world()
        .get::<Guid>(entity_id)
        .unwrap()
        .id()
        .to_string();

    // Verify initial state: Health exists, Position does not.
    let health_before = db
        .fetch_component(&guid, Health::name())
        .await
        .expect("Fetch before update failed");
    assert!(
        health_before.is_some(),
        "Health should exist after first commit"
    );
    let position_before = db
        .fetch_component(&guid, Position::name())
        .await
        .expect("Fetch before update failed");
    assert!(
        position_before.is_none(),
        "Position should not exist after first commit"
    );

    // 2. WHEN a Position component is added to that entity
    app
        .world_mut()
        .entity_mut(entity_id)
        .insert(Position { x: 10.0, y: 20.0 });
    app.update(); // This will mark the entity as dirty due to the added component

    // 3. AND the app is committed again
    diagnostic_commit(&mut app, Duration::from_secs(5)).await.expect("Second commit failed");

    // 4. THEN the document in the database is updated to include the new Position data
    //    while retaining the existing Health data.
    let health_after_json = db
        .fetch_component(&guid, Health::name())
        .await
        .expect("Fetch after update failed")
        .expect("Health component not found after update");
    let health_after: Health =
        serde_json::from_value(health_after_json).expect("Health deserialization failed");
    assert_eq!(health_after.value, 100, "Health data was not retained");

    let position_after_json = db
        .fetch_component(&guid, Position::name())
        .await
        .expect("Fetch after update failed")
        .expect("Position component not found after update");
    let position_after: Position =
        serde_json::from_value(position_after_json).expect("Position deserialization failed");
    assert_eq!(position_after.x, 10.0, "Position.x was not added correctly");
    assert_eq!(position_after.y, 20.0, "Position.y was not added correctly");
}

#[tokio::test]
async fn test_remove_component_from_existing_entity() {
    let context = setup().await;
    let db = context.db.clone();
    let mut app = new_app(db.clone());

    // 1. GIVEN a committed entity with a Health component and a Position component
    let entity_id = app
        .world_mut()
        .spawn((Health { value: 100 }, Position { x: 1.0, y: 2.0 }))
        .id();
    app.update();
    diagnostic_commit(&mut app, Duration::from_secs(5)).await.expect("Initial commit failed");

    let guid = app
        .world()
        .get::<Guid>(entity_id)
        .unwrap()
        .id()
        .to_string();

    // Verify initial state: both components exist in the DB
    let health_json_before = db
        .fetch_component(&guid, Health::name())
        .await
        .unwrap()
        .unwrap();
    let position_json_before = db
        .fetch_component(&guid, Position::name())
        .await
        .unwrap()
        .unwrap();

    let fetched_health_before: Health =
        serde_json::from_value(health_json_before).expect("Deserialization before update failed");
    let fetched_position_before: Position =
        serde_json::from_value(position_json_before).expect("Deserialization before update failed");

    assert_eq!(fetched_health_before.value, 100);
    assert_eq!(fetched_position_before.x, 1.0);
    assert_eq!(fetched_position_before.y, 2.0);

    // 2. WHEN the Position component is removed from that entity
    app.world_mut().entity_mut(entity_id).remove::<Position>();
    app.update(); // This will mark the entity as dirty due to the removed component

    // Manually mark dirty via PersistenceSession
    app.world_mut()
       .resource_mut::<PersistenceSession>()
       .dirty_entities.insert(entity_id);

    // 3. AND the app is committed again
    diagnostic_commit(&mut app, Duration::from_secs(5)).await.expect("Second commit failed");

    // 4. THEN the document in the database is updated to remove the Position data
    //    but the Health data remains unchanged.
    let health_json_after = db
        .fetch_component(&guid, Health::name())
        .await
        .unwrap()
        .unwrap();
    let position_json_after = db
        .fetch_component(&guid, Position::name())
        .await
        .unwrap();

    let fetched_health_after: Health =
        serde_json::from_value(health_json_after).expect("Failed to deserialize Health component");

    assert_eq!(
        fetched_health_after.value, 100,
        "The health value in the database was incorrectly modified"
    );

    assert!(
        position_json_after.is_none(),
        "Position component should be removed from the database"
    );
}

#[tokio::test]
async fn test_commit_with_no_changes() {
    let context = setup().await;
    let db = context.db.clone();
    let mut app = new_app(db.clone());

    // GIVEN a committed app in a synchronized state with the database
    app.world_mut().spawn(Health { value: 100 });
    app.update();
    diagnostic_commit(&mut app, Duration::from_secs(5))
        .await
        .expect("Initial commit failed");

    // WHEN the app is committed again with no changes made to any entities or resources
    diagnostic_commit(&mut app, Duration::from_secs(5))
        .await
        .expect("Second commit with no changes failed");

    // THEN the commit operation succeeds without error
    // AND no database write operations are performed (this is handled by an early return in the commit function)
    let status = app.world().resource::<bevy_arangodb::CommitStatus>();
    assert_eq!(*status, bevy_arangodb::CommitStatus::Idle);
}

#[tokio::test]
async fn test_persist_component_with_empty_vec() {
    let context = setup().await;
    let db = context.db.clone();
    let mut app = new_app(db.clone());

    // WHEN an entity is spawned with a component that contains an empty `Vec`
    let inventory = Inventory { items: vec![] };
    let entity_id = app.world_mut().spawn(inventory).id();

    app.update();

    // AND the app is committed
    diagnostic_commit(&mut app, Duration::from_secs(5)).await.expect("Commit failed");

    // THEN the commit succeeds and the data can be fetched and correctly deserialized
    // back into a component with an empty `Vec`.
    let guid = app
        .world()
        .get::<Guid>(entity_id)
        .expect("Entity should have a Guid after commit")
        .id();

    let inventory_json = db
        .fetch_component(guid, Inventory::name())
        .await
        .unwrap()
        .unwrap();

    let fetched_inventory: Inventory =
        serde_json::from_value(inventory_json).expect("Failed to deserialize Inventory component");

    assert!(
        fetched_inventory.items.is_empty(),
        "The fetched inventory should have an empty items vec"
    );
}

#[tokio::test]
async fn test_persist_component_with_option_none() {
    let context = setup().await;
    let db = context.db.clone();
    let mut app = new_app(db.clone());

    // WHEN an entity is spawned with a component that has an `Option<T>` field set to `None`
    let optional_data = OptionalData { data: None };
    let entity_id = app.world_mut().spawn(optional_data).id();

    app.update();

    // AND the app is committed
    diagnostic_commit(&mut app, Duration::from_secs(5))
        .await
        .expect("Commit failed");

    // THEN the commit succeeds and the data can be fetched and correctly deserialized.
    let guid = app
        .world()
        .get::<Guid>(entity_id)
        .expect("Entity should have a Guid after commit")
        .id();

    let data_json = db
        .fetch_component(guid, OptionalData::name())
        .await
        .unwrap()
        .unwrap();

    let fetched_data: OptionalData =
        serde_json::from_value(data_json).expect("Failed to deserialize OptionalData component");

    assert!(fetched_data.data.is_none(), "The fetched data should be None");

}
