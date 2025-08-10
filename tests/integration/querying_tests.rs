use bevy::prelude::App;
use bevy_arangodb_core::{
    commit, Guid, Persist, persistence_plugin::PersistencePlugins, PersistenceQuery, TransactionOperation, Collection,
    PersistentQuery, MockDatabaseConnection, BEVY_PERSISTENCE_VERSION_FIELD, DatabaseConnection,
    db::connection::DatabaseConnectionResource,
};
use std::sync::Arc;
use crate::common::*;

#[tokio::test]
async fn test_load_specific_entities_into_new_session() {
    let (db, _container) = setup().await;
    let mut app1 = App::new();
    app1.add_plugins(PersistencePlugins(db.clone()));

    // 1. Spawn two entities, one with Health+Position, one with only Health.
    let _entity_to_load = app1
        .world_mut()
        .spawn((
            Health { value: 150 },
            Position { x: 10.0, y: 20.0 },
        ))
        .id();
    let _entity_to_ignore = app1.world_mut().spawn(Health { value: 99 }).id();

    app1.update();

    commit(&mut app1)
        .await
        .expect("Initial commit failed");

    // 2. Create a new, clean session to load the data into.
    let mut app2 = App::new();
    app2.add_plugins(PersistencePlugins(db.clone()));

    // 3. Query for entities that have BOTH Health and Position, and Health > 100.
    let query = PersistenceQuery::new(db.clone())
        .with::<Health>()
        .with::<Position>()
        .filter(Health::value().gt(100));
    let loaded_entities = query.fetch_into(app2.world_mut()).await;

    // 4. Verify that only the correct entity was loaded and its data is correct.
    assert_eq!(
        loaded_entities.len(),
        1,
        "Should only load one entity with both components"
    );
    let loaded_entity = loaded_entities[0];

    let health = app2.world().get::<Health>(loaded_entity).unwrap();
    assert_eq!(health.value, 150);

    let position = app2.world().get::<Position>(loaded_entity).unwrap();
    assert_eq!(position.x, 10.0);
    assert_eq!(position.y, 20.0);
}

#[tokio::test]
async fn test_load_resources_alongside_entities() {
    let (db, _container) = setup().await;
    let mut app1 = App::new();
    app1.add_plugins(PersistencePlugins(db.clone()));

    // GIVEN a database containing a committed GameSettings resource
    let settings = GameSettings {
        difficulty: 0.42,
        map_name: "mystic".into(),
    };
    app1.insert_resource(settings.clone());
    app1.update();
    commit(&mut app1)
        .await
        .expect("Initial commit failed");

    // WHEN any query is fetched into a new app
    let mut app2 = App::new();
    app2.add_plugins(PersistencePlugins(db.clone()));
    let _ = PersistenceQuery::new(db.clone())
        .fetch_into(app2.world_mut())
        .await;

    // THEN the GameSettings resource is loaded
    let loaded: &GameSettings = app2.world().resource();
    assert_eq!(loaded.difficulty, 0.42);
    assert_eq!(loaded.map_name, "mystic");
}

#[tokio::test]
async fn test_load_into_world_with_existing_entities() {
    let (db, _container) = setup().await;

    // GIVEN entity A in DB, created by app1
    let mut app1 = App::new();
    app1.add_plugins(PersistencePlugins(db.clone()));
    let a = app1.world_mut().spawn(Health { value: 100 }).id();
    app1.update();
    commit(&mut app1)
        .await
        .expect("Commit for app1 failed");
    let key_a = app1.world().get::<Guid>(a).unwrap().id().to_string();

    // AND a fresh app2 with entity B already committed
    let mut app2 = App::new();
    app2.add_plugins(PersistencePlugins(db.clone()));
    let _b = app2.world_mut().spawn(Position { x: 1.0, y: 1.0 }).id();
    app2.update();
    commit(&mut app2)
        .await
        .expect("Commit for app2 failed");

    // WHEN we query for A and load it into app2
    let loaded = PersistenceQuery::new(db.clone())
        .filter(Health::value().eq(100))
        .fetch_into(app2.world_mut())
        .await;

    // THEN both A and B exist in app2, and A has correct components
    assert_eq!(loaded.len(), 1);
    let e = loaded[0];
    assert_eq!(app2.world().get::<Guid>(e).unwrap().id(), key_a);
    assert_eq!(app2.world().get::<Health>(e).unwrap().value, 100);
    assert_eq!(
        app2.world_mut()
            .query::<&Guid>()
            .iter(&app2.world())
            .count(),
        2
    );
}

#[tokio::test]
async fn test_dsl_filter_by_component_presence() {
    let (db, _container) = setup().await;
    let mut app = App::new();
    app.add_plugins(PersistencePlugins(db.clone()));

    // GIVEN some entities with/without Creature
    app
        .world_mut()
        .spawn(Creature { is_screaming: false });
    app.world_mut().spawn(Health { value: 100 });
    app.update();
    commit(&mut app)
        .await
        .expect("Initial commit failed");

    // WHEN we query .with::<Creature>()
    let mut app2 = App::new();
    app2.add_plugins(PersistencePlugins(db.clone()));
    let loaded = PersistenceQuery::new(db.clone())
        .with::<Creature>()
        .fetch_into(app2.world_mut())
        .await;

    // THEN only those with Creature load
    assert_eq!(loaded.len(), 1);
    assert!(app2.world().get::<Creature>(loaded[0]).is_some());
}

#[tokio::test]
async fn test_dsl_equality_operator() {
    let (db, _container) = setup().await;
    let mut app = App::new();
    app.add_plugins(PersistencePlugins(db.clone()));

    // GIVEN Health, Creature, PlayerName entities
    app.world_mut().spawn(Health { value: 100 });
    app.world_mut().spawn(Health { value: 99 });
    app
        .world_mut()
        .spawn(Creature { is_screaming: true });
    app
        .world_mut()
        .spawn(Creature { is_screaming: false });
    app
        .world_mut()
        .spawn(PlayerName { name: "Alice".into() });
    app
        .world_mut()
        .spawn(PlayerName { name: "Bob".into() });
    app.update();
    commit(&mut app)
        .await
        .expect("Initial commit failed");

    let mut app2 = App::new();
    app2.add_plugins(PersistencePlugins(db.clone()));

    // WHEN filtering Health == 100
    let h = PersistenceQuery::new(db.clone())
        .filter(Health::value().eq(100))
        .fetch_into(app2.world_mut())
        .await;
    assert_eq!(h.len(), 1);

    // WHEN filtering Creature.is_screaming == true
    let c = PersistenceQuery::new(db.clone())
        .filter(Creature::is_screaming().eq(true))
        .fetch_into(app2.world_mut())
        .await;
    assert_eq!(c.len(), 1);

    // WHEN filtering PlayerName == "Alice"
    let p = PersistenceQuery::new(db.clone())
        .filter(PlayerName::name().eq("Alice"))
        .fetch_into(app2.world_mut())
        .await;
    assert_eq!(p.len(), 1);
}

#[tokio::test]
async fn test_dsl_relational_operators() {
    let (db, _container) = setup().await;
    let mut app = App::new();
    app.add_plugins(PersistencePlugins(db.clone()));

    // GIVEN Health 99,100,101
    app.world_mut().spawn(Health { value: 99 });
    app.world_mut().spawn(Health { value: 100 });
    app.world_mut().spawn(Health { value: 101 });
    app.update();
    commit(&mut app)
        .await
        .expect("Initial commit failed");

    let mut app2 = App::new();
    app2.add_plugins(PersistencePlugins(db.clone()));

    assert_eq!(
        PersistenceQuery::new(db.clone())
            .filter(Health::value().gt(100))
            .fetch_into(app2.world_mut())
            .await
            .len(),
        1
    );
    assert_eq!(
        PersistenceQuery::new(db.clone())
            .filter(Health::value().gte(100))
            .fetch_into(app2.world_mut())
            .await
            .len(),
        2
    );
    assert_eq!(
        PersistenceQuery::new(db.clone())
            .filter(Health::value().lt(100))
            .fetch_into(app2.world_mut())
            .await
            .len(),
        1
    );
    assert_eq!(
        PersistenceQuery::new(db.clone())
            .filter(Health::value().lte(100))
            .fetch_into(app2.world_mut())
            .await
            .len(),
        2
    );
}

#[tokio::test]
async fn test_dsl_logical_combinations() {
    let (db, _container) = setup().await;
    let mut app = App::new();
    app.add_plugins(PersistencePlugins(db.clone()));

    // GIVEN entities for AND/OR
    app
        .world_mut()
        .spawn((Health { value: 150 }, Position { x: 50.0, y: 0.0 }));
    app
        .world_mut()
        .spawn((Health { value: 150 }, Position { x: 150.0, y: 0.0 }));
    app
        .world_mut()
        .spawn((Health { value: 50 }, Position { x: 50.0, y: 0.0 }));
    app
        .world_mut()
        .spawn((Health { value: 50 }, Position { x: 150.0, y: 0.0 }));
    app.update();
    commit(&mut app)
        .await
        .expect("Initial commit failed");

    let mut app2 = App::new();
    app2.add_plugins(PersistencePlugins(db.clone()));

    // AND case
    let and_loaded = PersistenceQuery::new(db.clone())
        .filter(Health::value().gt(100).and(Position::x().lt(100.0)))
        .fetch_into(app2.world_mut())
        .await;
    assert_eq!(and_loaded.len(), 1);

    // OR case
    let or_loaded = PersistenceQuery::new(db.clone())
        .filter(Health::value().gt(100).or(Position::x().lt(100.0)))
        .fetch_into(app2.world_mut())
        .await;
    assert_eq!(or_loaded.len(), 3);
}

#[tokio::test]
#[should_panic(expected = "component deserialization failed")]
async fn test_load_with_schema_mismatch() {
    let (db, _container) = setup().await;

    // GIVEN a bad Health document with required fields
    let bad_health_doc = serde_json::json!({
        "_key": "bad_doc",
        "bevy_persistence_version": 1,
        Health::name(): { "value": "a string, not a number" }
    });
    let _key = db
        .execute_transaction(vec![TransactionOperation::CreateDocument {
            collection: Collection::Entities,
            data: bad_health_doc,
        }])
        .await
        .expect("Transaction to create bad doc failed")
        .remove(0);

    // WHEN loading with .with::<Health>() – this should panic inside fetch_into
    let mut app2 = App::new();
    app2.add_plugins(PersistencePlugins(db.clone()));
    PersistenceQuery::new(db.clone())
        .with::<Health>()
        .fetch_into(app2.world_mut())
        .await;
}

#[tokio::test]
async fn test_fetch_ids_only() {
    let (db, _container) = setup().await;
    let mut app = App::new();
    app.add_plugins(PersistencePlugins(db.clone()));

    // Spawn entities with different components
    app.world_mut().spawn(Health { value: 100 });
    app.world_mut().spawn(Health { value: 50 });
    app.world_mut().spawn((Health { value: 200 }, Position { x: 10.0, y: 20.0 }));
    app.world_mut().spawn(Position { x: 5.0, y: 5.0 });
    app.update();
    commit(&mut app).await.expect("Initial commit failed");

    // Store Guids to verify them later
    let health_entities: Vec<String> = app.world_mut()
        .query::<(&Health, &Guid)>()
        .iter(&app.world())
        .map(|(_, guid)| guid.id().to_string())
        .collect();
    
    // Verify there are 3 entities with Health
    assert_eq!(health_entities.len(), 3);

    // Test fetch_ids with a Health value > 75 filter
    let keys = PersistenceQuery::new(db.clone())
        .with::<Health>()
        .filter(Health::value().gt(75))
        .fetch_ids()
        .await;
    
    // Should return 2 keys (Health 100 and 200)
    assert_eq!(keys.len(), 2);
    
    // All returned keys should be in our health_entities collection
    for key in &keys {
        assert!(health_entities.contains(key), "Returned key not found in expected set");
    }
    
    // Test a more specific query for Health AND Position
    let keys_with_position = PersistenceQuery::new(db.clone())
        .with::<Health>()
        .with::<Position>()
        .fetch_ids()
        .await;
    
    // Should find exactly 1 entity
    assert_eq!(keys_with_position.len(), 1);
}

// The test system that uses PersistentQuery
fn test_persistent_query_system(mut query: PersistentQuery<(&Health, &Position)>) {
    // This will load entities from the database
    for (entity, (health, position)) in query.iter_with_loading() {
        println!("Entity {:?} has health {} and position ({}, {})", 
            entity, health.value, position.x, position.y);
    }
}

// Replace the async tokio test with a sync test that owns its own runtime
#[test]
fn test_persistent_query_system_param() {
    // Build a dedicated runtime for the async setup/teardown calls used in this test
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("failed to build tokio runtime");

    rt.block_on(async {
        let (db, _container) = setup().await;
        let mut app = App::new();
        app.add_plugins(PersistencePlugins(db.clone()));

        // 1. Create some test data
        let entity_high_health = app
            .world_mut()
            .spawn((
                Health { value: 150 },
                Position { x: 10.0, y: 20.0 },
            ))
            .id();
        
        let entity_low_health = app
            .world_mut()
            .spawn((
                Health { value: 50 },
                Position { x: 5.0, y: 5.0 },
            ))
            .id();

        app.update();
        commit(&mut app).await.expect("Initial commit failed");

        // Get the GUIDs for verification
        let high_health_guid = app.world().get::<Guid>(entity_high_health).unwrap().id().to_string();
        let low_health_guid = app.world().get::<Guid>(entity_low_health).unwrap().id().to_string();

        // 2. Create a new app that will use the PersistentQuery
        let mut app2 = App::new();
        app2.add_plugins(PersistencePlugins(db.clone()));
        
        // Add system that uses the PersistentQuery
        app2.add_systems(bevy::prelude::Update, test_persistent_query_system);
        
        // Run the app to execute the system (DB fetch + component insertion are done here)
        app2.update();
        
        // 3. Verify that entities were loaded
        let mut health_query = app2.world_mut().query::<&Health>();
        let health_count = health_query.iter(&app2.world()).count();
        assert_eq!(health_count, 2, "Should have loaded two entities with Health component");
        
        // Verify the Position component was also loaded
        let mut position_query = app2.world_mut().query::<&Position>();
        let position_count = position_query.iter(&app2.world()).count();
        assert_eq!(position_count, 2, "Should have loaded two entities with Position component");
        
        // Check that we have the right GUIDs
        let mut guid_query = app2.world_mut().query::<&Guid>();
        let guids: Vec<String> = guid_query.iter(&app2.world())
            .map(|guid| guid.id().to_string())
            .collect();
        assert!(guids.contains(&high_health_guid), "High health entity not loaded");
        assert!(guids.contains(&low_health_guid), "Low health entity not loaded");
    });
}

#[tokio::test]
async fn test_persistent_query_with_filter() {
    let (db, _container) = setup().await;
    let mut app = App::new();
    app.add_plugins(PersistencePlugins(db.clone()));

    // 1. Create some test data
    app.world_mut().spawn((Health { value: 150 }, Position { x: 10.0, y: 20.0 }));
    app.world_mut().spawn((Health { value: 50 }, Position { x: 5.0, y: 5.0 }));
    app.world_mut().spawn((Health { value: 100 }, Position { x: 15.0, y: 15.0 }));
    app.update();
    commit(&mut app).await.expect("Initial commit failed");

    // 2. Create a new app that will use the PersistentQuery with filter
    let mut app2 = App::new();
    app2.add_plugins(PersistencePlugins(db.clone()));
    
    // Define a system that will test the filtered query
    fn filtered_query_system(mut query: PersistentQuery<(&Health, &Position)>) {
        // Add filter for Health > 100
        query = query.filter(Health::value().gt(100));
        
        // This will load entities from the database
        for (_entity, (health, position)) in query.iter_with_loading() {
            println!("Entity has health {} and position ({}, {})", 
                health.value, position.x, position.y);
            // Assertion to verify filter works
            assert!(health.value > 100, "Filter should only return entities with health > 100");
        }
    }
    
    // Add the system that tests filtering
    app2.add_systems(bevy::prelude::Update, filtered_query_system);
    
    // Run the app to execute the system
    app2.update();
    
    // Verify that only matching entities were loaded (Health > 100)
    let mut health_query = app2.world_mut().query::<&Health>();
    let entities_with_health: Vec<i32> = health_query.iter(&app2.world())
        .map(|health| health.value)
        .collect();
    
    assert_eq!(entities_with_health.len(), 1, "Should have loaded only one entity");
    assert!(entities_with_health.contains(&150), "Should have loaded only the high health entity");
}

// System that uses query twice to test caching
fn test_cached_query_system(mut query1: PersistentQuery<&Health>, mut query2: PersistentQuery<&Health>) {
    // First query execution - should hit database
    let _ = query1.iter_with_loading().count();
    
    // Second execution of equivalent query - should use cache
    let _ = query2.iter_with_loading().count();
}

// System that tests force refresh
fn test_force_refresh_system(mut query: PersistentQuery<&Health>) {
    // Use force refresh to bypass cache
    query = query.force_refresh();
    let _ = query.iter_with_loading().count();
}

#[tokio::test]
async fn test_persistent_query_caching() {
    let (db, _container) = setup().await;
    let mut app = App::new();
    app.add_plugins(PersistencePlugins(db.clone()));

    // 1. Create some test data
    app.world_mut().spawn(Health { value: 100 });
    app.update();
    commit(&mut app).await.expect("Initial commit failed");

    // 2. Create a new app that will use the PersistentQuery with cache tracking
    let mut app2 = App::new();
    app2.add_plugins(PersistencePlugins(db.clone()));
    
    // Override the database with a mock that counts calls
    let mut db_mock = MockDatabaseConnection::new();
    
    // Track query calls
    use std::sync::atomic::{AtomicUsize, Ordering};
    let query_count = Arc::new(AtomicUsize::new(0));
    let qc = query_count.clone();
    db_mock.expect_query_documents()
        .returning(move |_, _| {
            qc.fetch_add(1, Ordering::SeqCst);
            Box::pin(async {
                Ok(vec![
                    serde_json::json!({
                        "_key": "test_key",
                        BEVY_PERSISTENCE_VERSION_FIELD: 1,
                        "Health": { "value": 100 }
                    })
                ])
            })
        });

    // Setup other required methods with minimal implementations
    db_mock.expect_query_keys()
        .returning(|_, _| Box::pin(async { Ok(vec!["test_key".to_string()]) }));
    
    db_mock.expect_fetch_resource()
        .returning(|_| Box::pin(async { Ok(None) }));
    
    app2.insert_resource(DatabaseConnectionResource(Arc::new(db_mock) as Arc<dyn DatabaseConnection>));
    
    // Add system that uses the PersistentQuery twice
    app2.add_systems(bevy::prelude::Update, test_cached_query_system);
    
    // Run the app to execute the system
    app2.update();
    
    // Since our mock setup is counting query calls, we should see only 1 call
    // despite the system using the query twice (second time should use cache)
    assert_eq!(query_count.load(std::sync::atomic::Ordering::SeqCst), 1, "Database should only be queried once due to caching");
    
    // Run again with force refresh to verify it bypasses cache
    app2.add_systems(bevy::prelude::Update, test_force_refresh_system);
    app2.update();
    
    // Now we should see a second query
    assert_eq!(query_count.load(std::sync::atomic::Ordering::SeqCst), 2, "Force refresh should bypass cache and query again");
}