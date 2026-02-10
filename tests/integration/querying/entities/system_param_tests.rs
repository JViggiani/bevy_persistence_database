use crate::common::*;
use bevy::prelude::*;
use bevy_persistence_database::bevy::components::Guid;
use bevy_persistence_database::bevy::params::query::PersistentQuery;
use bevy_persistence_database::core::session::commit_sync;
use bevy_persistence_database_derive::db_matrix_test;

fn test_persistent_query_system(mut query: PersistentQuery<(&Health, &Position)>) {
    // This will load entities from the database
    query.ensure_loaded();
    for (entity, (health, position)) in query.iter() {
        println!(
            "Entity {:?} has health {} and position ({}, {})",
            entity, health.value, position.x, position.y
        );
    }
}

#[db_matrix_test]
fn test_persistent_query_system_param() {
    // Use sync setup with a guard that drops inside a runtime
    let (db, _container) = setup();

    let mut app = setup_test_app(db.clone(), None);

    // 1. Create some test data
    let entity_high_health = app
        .world_mut()
        .spawn((Health { value: 150 }, Position { x: 10.0, y: 20.0 }))
        .id();

    let entity_low_health = app
        .world_mut()
        .spawn((Health { value: 50 }, Position { x: 5.0, y: 5.0 }))
        .id();

    app.update();
    // Commit synchronously using the plugin runtime
    commit_sync(&mut app, db.clone(), TEST_STORE).expect("Initial commit failed");

    // Get the GUIDs for verification
    let high_health_guid = app
        .world()
        .get::<Guid>(entity_high_health)
        .unwrap()
        .id()
        .to_string();
    let low_health_guid = app
        .world()
        .get::<Guid>(entity_low_health)
        .unwrap()
        .id()
        .to_string();

    // 2. Create a new app that will use the PersistentQuery
    let mut app2 = setup_test_app(db.clone(), None);

    // Add system that uses the PersistentQuery
    app2.add_systems(bevy::prelude::Update, test_persistent_query_system);

    // Run the app to execute the system (DB fetch + component insertion are done here)
    app2.update();

    // 3. Verify that entities were loaded
    let mut health_query = app2.world_mut().query::<&Health>();
    let health_count = health_query.iter(&app2.world()).count();
    assert_eq!(
        health_count, 2,
        "Should have loaded two entities with Health component"
    );

    // Verify the Position component was also loaded
    let mut position_query = app2.world_mut().query::<&Position>();
    let position_count = position_query.iter(&app2.world()).count();
    assert_eq!(
        position_count, 2,
        "Should have loaded two entities with Position component"
    );

    // Check that we have the right GUIDs
    let mut guid_query = app2.world_mut().query::<&Guid>();
    let guids: Vec<String> = guid_query
        .iter(&app2.world())
        .map(|guid| guid.id().to_string())
        .collect();
    assert!(
        guids.contains(&high_health_guid),
        "High health entity not loaded"
    );
    assert!(
        guids.contains(&low_health_guid),
        "Low health entity not loaded"
    );
}

#[db_matrix_test]
fn test_persistent_query_with_filter() {
    let (db, _container) = setup();
    let mut app = setup_test_app(db.clone(), None);

    // 1. Create some test data
    app.world_mut()
        .spawn((Health { value: 150 }, Position { x: 10.0, y: 20.0 }));
    app.world_mut()
        .spawn((Health { value: 50 }, Position { x: 5.0, y: 5.0 }));
    app.world_mut()
        .spawn((Health { value: 100 }, Position { x: 15.0, y: 15.0 }));
    app.update();
    commit_sync(&mut app, db.clone(), TEST_STORE).expect("Initial commit failed");

    // 2. Create a new app that will use the PersistentQuery with filter
    let mut app2 = setup_test_app(db.clone(), None);

    // Define a system that will test the filtered query
    fn filtered_query_system(mut query: PersistentQuery<(&Health, &Position)>) {
        // Add filter for Health > 100
        query = query.filter(Health::value().gt(100));

        // This will load entities from the database
        query.ensure_loaded();
        for (_entity, (health, position)) in query.iter() {
            println!(
                "Entity has health {} and position ({}, {})",
                health.value, position.x, position.y
            );
            // Assertion to verify filter works
            assert!(
                health.value > 100,
                "Filter should only return entities with health > 100"
            );
        }
    }

    // Add the system that tests filtering
    app2.add_systems(bevy::prelude::Update, filtered_query_system);

    // Run the app to execute the system
    app2.update();

    // Verify that only matching entities were loaded (Health > 100)
    let mut health_query = app2.world_mut().query::<&Health>();
    let entities_with_health: Vec<i32> = health_query
        .iter(&app2.world())
        .map(|health| health.value)
        .collect();

    assert_eq!(
        entities_with_health.len(),
        1,
        "Should have loaded only one entity"
    );
    assert!(
        entities_with_health.contains(&150),
        "Should have loaded only the high health entity"
    );
}
