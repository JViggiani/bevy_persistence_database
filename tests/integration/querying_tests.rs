use bevy::prelude::App;
use bevy_arangodb_core::{
    commit_sync, Guid, persistence_plugin::PersistencePlugins, PersistentQuery, DatabaseConnection,
    db::connection::DatabaseConnectionResource,
};
use crate::common::*;
use crate::common::CountingDbConnection;
use bevy::prelude::{With, Without, IntoScheduleConfigs};
use bevy_arangodb_derive::db_matrix_test;

#[db_matrix_test]
fn test_load_specific_entities_into_new_session() {
    let (db, _container) = setup();

    // Session 1: create data
    let mut app1 = App::new();
    app1.add_plugins(PersistencePlugins(db.clone()));
    let _entity_to_load = app1.world_mut().spawn((Health { value: 150 }, Position { x: 10.0, y: 20.0 })).id();
    let _entity_to_ignore = app1.world_mut().spawn(Health { value: 99 }).id();
    app1.update();
    commit_sync(&mut app1).expect("Initial commit failed");

    // Session 2: load (Health AND Position) with Health > 100
    let mut app2 = App::new();
    app2.add_plugins(PersistencePlugins(db.clone()));
    fn sys(
        pq: PersistentQuery<(&Health, &Position), (With<Health>, With<Position>)>
    ) {
        let _ = pq.filter(Health::value().gt(100)).iter_with_loading().count();
    }
    app2.add_systems(bevy::prelude::Update, sys);
    app2.update();

    // Verify only one entity with both components
    let mut q = app2.world_mut().query::<(&Health, &Position)>();
    let results: Vec<_> = q.iter(&app2.world()).collect();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].0.value, 150);
    assert_eq!(results[0].1.x, 10.0);
    assert_eq!(results[0].1.y, 20.0);
}

#[db_matrix_test]
fn test_load_resources_alongside_entities() {
    let (db, _container) = setup();
    let mut app1 = App::new();
    app1.add_plugins(PersistencePlugins(db.clone()));

    // GIVEN a committed GameSettings resource
    let settings = GameSettings { difficulty: 0.42, map_name: "mystic".into() };
    app1.insert_resource(settings.clone());
    app1.update();
    commit_sync(&mut app1).expect("Initial commit failed");

    // WHEN any query is fetched into a new app
    let mut app2 = App::new();
    app2.add_plugins(PersistencePlugins(db.clone()));
    fn sys(mut pq: PersistentQuery<&Guid>) { let _ = pq.iter_with_loading().count(); }
    app2.add_systems(bevy::prelude::Update, sys);
    app2.update();

    // THEN the GameSettings resource is loaded
    let loaded: &GameSettings = app2.world().resource();
    assert_eq!(loaded.difficulty, 0.42);
    assert_eq!(loaded.map_name, "mystic");
}

#[db_matrix_test]
fn test_load_into_world_with_existing_entities() {
    let (db, _container) = setup();

    // GIVEN entity A in DB, created by app1
    let mut app1 = App::new();
    app1.add_plugins(PersistencePlugins(db.clone()));
    let a = app1.world_mut().spawn(Health { value: 100 }).id();
    app1.update();
    commit_sync(&mut app1).expect("Commit for app1 failed");
    let key_a = app1.world().get::<Guid>(a).unwrap().id().to_string();

    // AND a fresh app2 with entity B already committed
    let mut app2 = App::new();
    app2.add_plugins(PersistencePlugins(db.clone()));
    let _b = app2.world_mut().spawn(Position { x: 1.0, y: 1.0 }).id();
    app2.update();
    commit_sync(&mut app2).expect("Commit for app2 failed");

    // WHEN we query for A and load it into app2 via a system
    fn load_a(
        pq: PersistentQuery<&Health>
    ) {
        let _ = pq.filter(Health::value().eq(100)).iter_with_loading().count();
    }
    app2.add_systems(bevy::prelude::Update, load_a);
    app2.update();

    // THEN A is present with correct components and both A and B exist in app2
    // Find the entity with Health == 100 and verify Guid matches
    let mut q_hp = app2.world_mut().query::<(bevy::prelude::Entity, &Health, &Guid)>();
    let loaded: Vec<_> = q_hp.iter(&app2.world()).filter(|(_, h, _)| h.value == 100).collect();
    assert_eq!(loaded.len(), 1);
    let (_e, _h, g) = loaded[0];
    assert_eq!(g.id(), key_a);
    // Total entity count with Guid should be 2 (A loaded + B committed)
    let mut q_guid = app2.world_mut().query::<&Guid>();
    assert_eq!(q_guid.iter(&app2.world()).count(), 2);
}

#[db_matrix_test]
fn test_value_filters_equality_operator() {
    let (db, _container) = setup();
    let mut app = App::new();
    app.add_plugins(PersistencePlugins(db.clone()));

    // GIVEN Health, Creature, PlayerName entities
    app.world_mut().spawn(Health { value: 100 });
    app.world_mut().spawn(Health { value: 99 });
    app.world_mut().spawn(Creature { is_screaming: true });
    app.world_mut().spawn(Creature { is_screaming: false });
    app.world_mut().spawn(PlayerName { name: "Alice".into() });
    app.world_mut().spawn(PlayerName { name: "Bob".into() });
    app.update();
    commit_sync(&mut app).expect("Initial commit failed");

    // Health == 100
    let mut app2 = App::new();
    app2.add_plugins(PersistencePlugins(db.clone()));
    fn s1(
        pq: PersistentQuery<&Health>
    ) {
        let _ = pq.filter(Health::value().eq(100)).iter_with_loading().count();
    }
    app2.add_systems(bevy::prelude::Update, s1);
    app2.update();
    let count_h = app2.world_mut().query::<&Health>().iter(&app2.world()).count();
    assert_eq!(count_h, 1);

    // Creature.is_screaming == true
    app2.world_mut().clear_entities();
    fn s2(
        pq: PersistentQuery<&Creature>
    ) {
        let _ = pq.filter(Creature::is_screaming().eq(true)).iter_with_loading().count();
    }
    app2.add_systems(bevy::prelude::Update, s2);
    app2.update();
    let count_c = app2.world_mut().query::<&Creature>().iter(&app2.world()).count();
    assert_eq!(count_c, 1);

    // PlayerName == "Alice"
    app2.world_mut().clear_entities();
    fn s3(
        pq: PersistentQuery<&PlayerName>
    ) {
        let _ = pq.filter(PlayerName::name().eq("Alice")).iter_with_loading().count();
    }
    app2.add_systems(bevy::prelude::Update, s3);
    app2.update();
    let count_p = app2.world_mut().query::<&PlayerName>().iter(&app2.world()).count();
    assert_eq!(count_p, 1);
}

#[db_matrix_test]
fn test_value_filters_relational_operators() {
    let (db, _container) = setup();
    let mut app = App::new();
    app.add_plugins(PersistencePlugins(db.clone()));

    // GIVEN Health 99,100,101
    app.world_mut().spawn(Health { value: 99 });
    app.world_mut().spawn(Health { value: 100 });
    app.world_mut().spawn(Health { value: 101 });
    app.update();
    commit_sync(&mut app).expect("Initial commit failed");

    // gt(100) -> 1
    let mut app2 = App::new();
    app2.add_plugins(PersistencePlugins(db.clone()));
    fn gt(
        pq: PersistentQuery<&Health>
    ) { let _ = pq.filter(Health::value().gt(100)).iter_with_loading().count(); }
    app2.add_systems(bevy::prelude::Update, gt);
    app2.update();
    assert_eq!(app2.world_mut().query::<&Health>().iter(&app2.world()).count(), 1);

    // gte(100) -> 2
    app2.world_mut().clear_entities();
    fn gte(
        pq: PersistentQuery<&Health>
    ) { let _ = pq.filter(Health::value().gte(100)).iter_with_loading().count(); }
    app2.add_systems(bevy::prelude::Update, gte);
    app2.update();
    assert_eq!(app2.world_mut().query::<&Health>().iter(&app2.world()).count(), 2);

    // lt(100) -> 1
    app2.world_mut().clear_entities();
    fn lt(
        pq: PersistentQuery<&Health>
    ) { let _ = pq.filter(Health::value().lt(100)).iter_with_loading().count(); }
    app2.add_systems(bevy::prelude::Update, lt);
    app2.update();
    assert_eq!(app2.world_mut().query::<&Health>().iter(&app2.world()).count(), 1);

    // lte(100) -> 2
    app2.world_mut().clear_entities();
    fn lte(
        pq: PersistentQuery<&Health>
    ) { let _ = pq.filter(Health::value().lte(100)).iter_with_loading().count(); }
    app2.add_systems(bevy::prelude::Update, lte);
    app2.update();
    assert_eq!(app2.world_mut().query::<&Health>().iter(&app2.world()).count(), 2);
}

#[db_matrix_test]
fn test_value_filters_logical_combinations() {
    let (db, _container) = setup();
    let mut app = App::new();
    app.add_plugins(PersistencePlugins(db.clone()));

    // GIVEN entities for AND/OR
    app.world_mut().spawn((Health { value: 150 }, Position { x: 50.0, y: 0.0 }));
    app.world_mut().spawn((Health { value: 150 }, Position { x: 150.0, y: 0.0 }));
    app.world_mut().spawn((Health { value: 50 }, Position { x: 50.0, y: 0.0 }));
    app.world_mut().spawn((Health { value: 50 }, Position { x: 150.0, y: 0.0 }));
    app.update();
    commit_sync(&mut app).expect("Initial commit failed");

    let mut app2 = App::new();
    app2.add_plugins(PersistencePlugins(db.clone()));

    fn and_sys(
        pq: PersistentQuery<(&Health, &Position)>
    ) {
        let expr = Health::value().gt(100).and(Position::x().lt(100.0));
        let _ = pq.filter(expr).iter_with_loading().count();
    }
    fn or_sys(
        pq: PersistentQuery<(&Health, &Position)>
    ) {
        let expr = Health::value().gt(100).or(Position::x().lt(100.0));
        let _ = pq.filter(expr).iter_with_loading().count();
    }

    app2.add_systems(bevy::prelude::Update, and_sys);
    app2.update();
    assert_eq!(app2.world_mut().query::<(&Health, &Position)>().iter(&app2.world()).count(), 1);

    app2.world_mut().clear_entities();
    app2.add_systems(bevy::prelude::Update, or_sys);
    app2.update();
    assert_eq!(app2.world_mut().query::<(&Health, &Position)>().iter(&app2.world()).count(), 3);
}

#[db_matrix_test]
fn test_fetch_ids_only() {
    let (db, _container) = setup();
    let mut app = App::new();
    app.add_plugins(PersistencePlugins(db.clone()));

    // Spawn entities with different components
    app.world_mut().spawn(Health { value: 100 });
    app.world_mut().spawn(Health { value: 50 });
    app.world_mut().spawn((Health { value: 200 }, Position { x: 10.0, y: 20.0 }));
    app.world_mut().spawn(Position { x: 5.0, y: 5.0 });
    app.update();
    commit_sync(&mut app).expect("Initial commit failed");

    // Store Guids to verify them later
    let health_entities: Vec<String> = app.world_mut()
        .query::<(&Health, &Guid)>()
        .iter(&app.world())
        .map(|(_, guid)| guid.id().to_string())
        .collect();
    assert_eq!(health_entities.len(), 3);

    // Use a system to load Health where value > 75, then collect keys from world
    let mut app2 = App::new();
    app2.add_plugins(PersistencePlugins(db.clone()));
    fn load_health_gt_75(
        pq: PersistentQuery<&Health>
    ) {
        let _ = pq.filter(Health::value().gt(75)).iter_with_loading().count();
    }
    app2.add_systems(bevy::prelude::Update, load_health_gt_75);
    app2.update();
    let keys: Vec<String> = app2.world_mut()
        .query::<(&Health, &Guid)>()
        .iter(&app2.world())
        .map(|(_, g)| g.id().to_string())
        .collect();
    assert_eq!(keys.len(), 2);
    for key in &keys {
        assert!(health_entities.contains(key), "Returned key not found in expected set");
    }

    // Health AND Position: load via presence, then collect keys
    let mut app3 = App::new();
    app3.add_plugins(PersistencePlugins(db.clone()));
    fn load_h_and_p(mut pq: PersistentQuery<(&Health, &Position), (With<Health>, With<Position>)>) {
        let _ = pq.iter_with_loading().count();
    }
    app3.add_systems(bevy::prelude::Update, load_h_and_p);
    app3.update();
    let keys_with_position: Vec<String> = app3.world_mut()
        .query::<(&Health, &Position, &Guid)>()
        .iter(&app3.world())
        .map(|(_, _, g)| g.id().to_string())
        .collect();
    assert_eq!(keys_with_position.len(), 1);
}

fn test_persistent_query_system(mut query: PersistentQuery<(&Health, &Position)>) {
    // This will load entities from the database
    for (entity, (health, position)) in query.iter_with_loading() {
        println!("Entity {:?} has health {} and position ({}, {})", 
            entity, health.value, position.x, position.y);
    }
}

#[db_matrix_test]
fn test_persistent_query_system_param() {
    // Use sync setup with a guard that drops inside a runtime
    let (db, _container) = setup();

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
    // Commit synchronously using the plugin runtime
    commit_sync(&mut app).expect("Initial commit failed");

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
}

#[db_matrix_test]
fn test_persistent_query_with_filter() {
    let (db, _container) = setup();
    let mut app = App::new();
    app.add_plugins(PersistencePlugins(db.clone()));

    // 1. Create some test data
    app.world_mut().spawn((Health { value: 150 }, Position { x: 10.0, y: 20.0 }));
    app.world_mut().spawn((Health { value: 50 }, Position { x: 5.0, y: 5.0 }));
    app.world_mut().spawn((Health { value: 100 }, Position { x: 15.0, y: 15.0 }));
    app.update();
    commit_sync(&mut app).expect("Initial commit failed");

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

#[db_matrix_test]
fn test_persistent_query_caching() {
    let (db, _container) = setup();
    let mut app = App::new();
    app.add_plugins(PersistencePlugins(db.clone()));

    // 1. Create some test data
    app.world_mut().spawn(Health { value: 100 });
    app.update();
    commit_sync(&mut app).expect("Initial commit failed");

    // 2. Create a new app that will use the PersistentQuery with cache tracking
    let mut app2 = App::new();
    app2.add_plugins(PersistencePlugins(db.clone()));
    
    // Wrap the real DB so we can count execute_documents() calls
    use std::sync::{Arc, atomic::{AtomicUsize, Ordering}};
    let query_count = Arc::new(AtomicUsize::new(0));
    let counting = Arc::new(CountingDbConnection::new(db.clone(), query_count.clone())) as Arc<dyn DatabaseConnection>;
    app2.insert_resource(DatabaseConnectionResource(counting));
    
    // Add system that uses the PersistentQuery twice
    app2.add_systems(bevy::prelude::Update, test_cached_query_system);
    
    // Run the app to execute the system
    app2.update();
    
    // The second identical query should use cache, so only 1 DB call
    assert_eq!(query_count.load(Ordering::SeqCst), 1, "Database should only be queried once due to caching");
    
    // Run again with force refresh to verify it bypasses cache
    app2.add_systems(bevy::prelude::Update, test_force_refresh_system);
    app2.update();
    
    // Now we should see a second query
    assert_eq!(query_count.load(Ordering::SeqCst), 2, "Force refresh should bypass cache and query again");
}

// Add a small adapter that wraps a real DatabaseConnection and counts execute_documents calls.

#[db_matrix_test]
fn test_entity_not_overwritten_on_second_query_without_refresh() {
    // GIVEN an entity persisted with Health { value: 100 }
    let (db, _container) = setup();
    let mut app1 = App::new();
    app1.add_plugins(PersistencePlugins(db.clone()));
    let _e = app1.world_mut().spawn(Health { value: 100 }).id();
    app1.update();
    commit_sync(&mut app1).expect("commit failed");

    // WHEN we load it into a fresh world via systems
    let mut app2 = App::new();
    app2.add_plugins(PersistencePlugins(db.clone()));
    app2.insert_resource(TestState::default());

    // 1) Load and capture GUID
    app2.add_systems(bevy::prelude::Update, system_load_and_capture);
    app2.update();

    // 2) Mutate locally inside a system
    app2.add_systems(bevy::prelude::Update, system_mutate_once);
    app2.update();

    // 3) Run another query via system that would fetch the same entity again
    app2.add_systems(bevy::prelude::Update, system_second_load);
    app2.update();

    // THEN the local mutation is preserved (no overwrite by default)
    // Read back health using the captured entity
    let state = app2.world().resource::<TestState>();
    let entity = state.entity.expect("Entity should be captured");
    let health = app2.world().get::<Health>(entity).expect("Health not found");
    assert_eq!(health.value, 123, "Local mutation should be preserved");
}

#[derive(bevy::prelude::Resource, Default)]
struct TestState {
    entity: Option<bevy::prelude::Entity>,
    mutated: bool,
}

fn system_load_and_capture(
    mut pq: PersistentQuery<&Health>,
    mut state: bevy::prelude::ResMut<TestState>,
) {
    for (e, _) in pq.iter_with_loading() {
        if state.entity.is_none() {
            state.entity = Some(e);
            break;
        }
    }
}

// Mutate once by entity. Avoids scanning by guid string.
fn system_mutate_once(
    mut q: bevy::prelude::Query<&mut Health>,
    mut state: bevy::prelude::ResMut<TestState>,
) {
    if state.mutated {
        return;
    }
    if let Some(e) = state.entity {
        if let Ok(mut health) = q.get_mut(e) {
            health.value = 123;
            state.mutated = true;
        }
    }
}

// Second load: derive the key at runtime from the stored entity inside the system.
fn system_second_load(
    pq: PersistentQuery<&Health>,
    q_guid: bevy::prelude::Query<&Guid>,
    state: bevy::prelude::Res<TestState>,
) {
    if let Some(e) = state.entity {
        if let Ok(g) = q_guid.get(e) {
            let _ = pq
                .filter(Guid::key_field().eq(g.id()))
                .iter_with_loading()
                .count();
        }
    }
}

#[db_matrix_test]
fn test_force_refresh_overwrites() {
    // GIVEN an entity persisted with Health { value: 100 }
    let (db, _container) = setup();
    let mut app1 = App::new();
    app1.add_plugins(PersistencePlugins(db.clone()));
    let _e = app1.world_mut().spawn(Health { value: 100 }).id();
    app1.update();
    commit_sync(&mut app1).expect("commit failed");

    // Load into app2 via system, then mutate locally
    let mut app2 = App::new();
    app2.add_plugins(PersistencePlugins(db.clone()));
    fn load(mut pq: PersistentQuery<&Health, With<Health>>) { let _ = pq.iter_with_loading().count(); }
    app2.add_systems(bevy::prelude::Update, load);
    app2.update();

    // Find the loaded entity and its guid without using the manual builder
    let (e, g) = {
        let mut q = app2.world_mut().query::<(bevy::prelude::Entity, &Guid)>();
        let first = q.iter(&app2.world()).next().expect("no entity loaded");
        (first.0, first.1.id().to_string())
    };

    // Local mutation
    app2.world_mut().get_mut::<Health>(e).unwrap().value = 123;
    app2.update();

    // WHEN we run a system-param PersistentQuery with force_refresh
    app2.insert_resource(TestKey(g.clone()));
    app2.add_systems(bevy::prelude::Update, force_refresh_system);
    app2.update();

    // THEN the DB value overwrites the local change
    assert_eq!(app2.world().get::<Health>(e).unwrap().value, 100);
}

#[derive(bevy::prelude::Resource, Clone)]
struct TestKey(String);

fn force_refresh_system(query: PersistentQuery<&Health>, key: bevy::prelude::Res<TestKey>) {
    let _ = query
        .filter(Guid::key_field().eq(key.0.as_str()))
        .force_refresh()
        .iter_with_loading()
        .count();
}

fn system_without_creature(mut pq: PersistentQuery<&Guid, Without<Creature>>) {
    let _ = pq.iter_with_loading().count();
}

#[db_matrix_test]
fn test_presence_without_filter() {
    let (db, _container) = setup();

    // GIVEN: one entity with Creature, one with only Health
    let mut app = App::new();
    app.add_plugins(PersistencePlugins(db.clone()));
    app.world_mut().spawn(Creature { is_screaming: false });
    app.world_mut().spawn(Health { value: 42 });
    app.update();
    commit_sync(&mut app).expect("Initial commit failed");

    // WHEN: run a system that loads entities WITHOUT Creature
    let mut app2 = App::new();
    app2.add_plugins(PersistencePlugins(db.clone()));
    app2.add_systems(bevy::prelude::Update, system_without_creature);
    app2.update();

    // THEN: only the Health-only entity is present, and none have Creature
    let mut q_guid = app2.world_mut().query::<&Guid>();
    let count = q_guid.iter(&app2.world()).count();
    assert_eq!(count, 1, "Only one entity should be loaded (without Creature)");

    let mut q_creature = app2.world_mut().query::<&Creature>();
    assert_eq!(
        q_creature.iter(&app2.world()).count(),
        0,
        "No entities should have Creature component"
    );
}

fn system_type_driven_presence(mut pq: PersistentQuery<&Guid, (bevy::prelude::With<Health>, bevy::prelude::Without<Creature>)>) {
    // Load entities that have Health but do NOT have Creature
    let _ = pq.iter_with_loading().count();
}

#[db_matrix_test]
fn test_type_driven_presence_filters() {
    let (db, _container) = setup();

    // GIVEN: one entity with Creature+Health, one with only Health
    let mut app = App::new();
    app.add_plugins(PersistencePlugins(db.clone()));
    app.world_mut().spawn((Health { value: 1 }, Creature { is_screaming: false }));
    app.world_mut().spawn(Health { value: 2 });
    app.update();
    commit_sync(&mut app).expect("Initial commit failed");

    // WHEN: run a system that uses type-level With/Without in F
    let mut app2 = App::new();
    app2.add_plugins(PersistencePlugins(db.clone()));
    app2.add_systems(bevy::prelude::Update, system_type_driven_presence);
    app2.update();

    // THEN: only the Health-only entity is loaded (no Creature in world)
    let mut q_guid = app2.world_mut().query::<&Guid>();
    let count = q_guid.iter(&app2.world()).count();
    assert_eq!(count, 1, "Type-driven presence should exclude Creature entities");

    let mut q_creature = app2.world_mut().query::<&Creature>();
    assert_eq!(q_creature.iter(&app2.world()).count(), 0, "Loaded entities must not have Creature");
}

fn system_type_driven_or_presence(
    mut pq: PersistentQuery<
        &Guid,
        bevy::prelude::Or<(bevy::prelude::With<Health>, bevy::prelude::With<Creature>)>
    >,
) {
    // Load entities that have Health OR Creature
    let _ = pq.iter_with_loading().count();
}

#[db_matrix_test]
fn test_type_driven_or_presence_filters() {
    let (db, _container) = setup();

    // GIVEN: one with Health, one with Creature, one with neither
    let mut app = App::new();
    app.add_plugins(PersistencePlugins(db.clone()));
    app.world_mut().spawn(Health { value: 10 });
    app.world_mut().spawn(Creature { is_screaming: false });
    app.world_mut().spawn(Position { x: 0.0, y: 0.0 });
    app.update();
    commit_sync(&mut app).expect("Initial commit failed");

    // WHEN: run a system that uses OR(With<Health>, With<Creature>) in F
    let mut app2 = App::new();
    app2.add_plugins(PersistencePlugins(db.clone()));
    app2.add_systems(bevy::prelude::Update, system_type_driven_or_presence);
    app2.update();

    // THEN: exactly two entities are loaded (those with Health OR Creature)
    let mut q_guid = app2.world_mut().query::<&Guid>();
    let count = q_guid.iter(&app2.world()).count();
    assert_eq!(count, 2, "OR presence should include entities with Health or Creature");
}

fn system_optional_component_fetch(mut pq: PersistentQuery<(&Health, Option<&Position>)>) {
    // Request Health and optionally Position via Q; no explicit presence filter needed.
    let _ = pq.iter_with_loading().count();
}

#[db_matrix_test]
fn test_optional_component_in_q_is_fetched_if_present() {
    let (db, _container) = setup();

    // GIVEN: one entity with Health only, one with Health+Position
    let mut app = App::new();
    app.add_plugins(PersistencePlugins(db.clone()));
    app.world_mut().spawn(Health { value: 1 });
    app.world_mut().spawn((Health { value: 2 }, Position { x: 1.0, y: 2.0 }));
    app.update();
    commit_sync(&mut app).expect("Initial commit failed");

    // WHEN: run a system with Q = (&Health, Option<&Position>)
    let mut app2 = App::new();
    app2.add_plugins(PersistencePlugins(db.clone()));
    app2.add_systems(bevy::prelude::Update, system_optional_component_fetch);
    app2.update();

    // THEN: both entities with Health are loaded; Position is present for one
    let mut q_health = app2.world_mut().query::<&Health>();
    let health_count = q_health.iter(&app2.world()).count();
    assert_eq!(health_count, 2, "Both Health-bearing entities should be loaded");

    let mut q_position = app2.world_mut().query::<&Position>();
    let position_count = q_position.iter(&app2.world()).count();
    assert_eq!(position_count, 1, "Position should be loaded only where present");
}

// System: With<Health> AND (Without<Creature> AND With<Position>)
fn system_nested_tuple_presence(
    mut pq: PersistentQuery<
        &Guid,
        (
            bevy::prelude::With<Health>,
            (bevy::prelude::Without<Creature>, bevy::prelude::With<Position>),
        ),
    >,
) {
    let _ = pq.iter_with_loading().count();
}

#[db_matrix_test]
fn test_nested_tuple_presence_and() {
    let (db, _container) = setup();
    // GIVEN: H+P, H+C, P-only
    let mut app = App::new();
    app.add_plugins(PersistencePlugins(db.clone()));
    app.world_mut().spawn((Health { value: 1 }, Position { x: 0.0, y: 0.0 }));
    app.world_mut().spawn((Health { value: 2 }, Creature { is_screaming: false }));
    app.world_mut().spawn(Position { x: 1.0, y: 1.0 });
    app.update();
    commit_sync(&mut app).expect("Initial commit failed");

    // WHEN
    let mut app2 = App::new();
    app2.add_plugins(PersistencePlugins(db.clone()));
    app2.add_systems(bevy::prelude::Update, system_nested_tuple_presence);
    app2.update();

    // THEN: only H+P passes (Health AND Position AND NOT Creature)
    let mut q_guid = app2.world_mut().query::<&Guid>();
    assert_eq!(q_guid.iter(&app2.world()).count(), 1);
    let mut q_health = app2.world_mut().query::<&Health>();
    assert_eq!(q_health.iter(&app2.world()).count(), 1);
    let mut q_position = app2.world_mut().query::<&Position>();
    assert_eq!(q_position.iter(&app2.world()).count(), 1);
    let mut q_creature = app2.world_mut().query::<&Creature>();
    assert_eq!(q_creature.iter(&app2.world()).count(), 0);
}

// System: With<Health> AND (With<Creature> OR With<Position> OR With<PlayerName>) AND Without<PlayerName>
fn system_and_or_mix_with_without(
    mut pq: PersistentQuery<
        &Guid,
        (
            bevy::prelude::With<Health>,
            bevy::prelude::Or<(
                bevy::prelude::With<Creature>,
                bevy::prelude::With<Position>,
                bevy::prelude::With<PlayerName>,
            )>,
            bevy::prelude::Without<PlayerName>,
        ),
    >,
) {
    let _ = pq.iter_with_loading().count();
}

#[db_matrix_test]
fn test_and_or_mix_with_without_filters() {
    let (db, _container) = setup();

    // GIVEN: H+C, H+P, H+C+PlayerName, H-only
    let mut app = App::new();
    app.add_plugins(PersistencePlugins(db.clone()));
    app.world_mut().spawn((Health { value: 1 }, Creature { is_screaming: false }));
    app.world_mut().spawn((Health { value: 2 }, Position { x: 0.0, y: 0.0 }));
    app.world_mut().spawn((Health { value: 3 }, Creature { is_screaming: true }, PlayerName { name: "X".into() }));
    app.world_mut().spawn(Health { value: 4 });
    app.update();
    commit_sync(&mut app).expect("Initial commit failed");

    // WHEN
    let mut app2 = App::new();
    app2.add_plugins(PersistencePlugins(db.clone()));
    app2.add_systems(bevy::prelude::Update, system_and_or_mix_with_without);
    app2.update();

    // THEN: H+C and H+P match (2 entities). H+C+PlayerName excluded by Without<PlayerName>, H-only excluded by OR
    let mut q_guid = app2.world_mut().query::<&Guid>();
    assert_eq!(q_guid.iter(&app2.world()).count(), 2);
    let mut q_player = app2.world_mut().query::<&PlayerName>();
    assert_eq!(q_player.iter(&app2.world()).count(), 0);
}

// System: Optional in Q with type-driven Without in F
fn system_optional_q_with_without(
    mut pq: PersistentQuery<(Option<&Creature>, &Health), bevy::prelude::Without<Creature>>,
) {
    let _ = pq.iter_with_loading().count();
}

#[db_matrix_test]
fn test_optional_q_with_without_exclusion() {
    let (db, _container) = setup();

    // GIVEN: H-only, H+Creature
    let mut app = App::new();
    app.add_plugins(PersistencePlugins(db.clone()));
    app.world_mut().spawn(Health { value: 1 });
    app.world_mut().spawn((Health { value: 2 }, Creature { is_screaming: false }));
    app.update();
    commit_sync(&mut app).expect("Initial commit failed");

    // WHEN: Q asks for Option<&Creature> but F excludes Creature
    let mut app2 = App::new();
    app2.add_plugins(PersistencePlugins(db.clone()));
    app2.add_systems(bevy::prelude::Update, system_optional_q_with_without);
    app2.update();

    // THEN: only H-only entity is loaded; no Creature present after load
    let mut q_health = app2.world_mut().query::<&Health>();
    assert_eq!(q_health.iter(&app2.world()).count(), 1);
    let mut q_creature = app2.world_mut().query::<&Creature>();
    assert_eq!(q_creature.iter(&app2.world()).count(), 0);
}

// System: No presence filters; optional fetch-only for two components
fn system_no_presence_optional_fetch(mut pq: PersistentQuery<(Option<&Health>, Option<&Position>)>) {
    let _ = pq.iter_with_loading().count();
}

#[db_matrix_test]
fn test_no_presence_loads_all_docs_requested_by_optional_q() {
    let (db, _container) = setup();

    // GIVEN: one Health-only, one Position-only
    let mut app = App::new();
    app.add_plugins(PersistencePlugins(db.clone()));
    app.world_mut().spawn(Health { value: 10 });
    app.world_mut().spawn(Position { x: 3.0, y: 4.0 });
    app.update();
    commit_sync(&mut app).expect("Initial commit failed");

    // WHEN: Q has only Option<&Health> and Option<&Position>, no F presence filters
    let mut app2 = App::new();
    app2.add_plugins(PersistencePlugins(db.clone()));
    app2.add_systems(bevy::prelude::Update, system_no_presence_optional_fetch);
    app2.update();

    // THEN: both docs load; each component present where it existed
    let mut q_guid = app2.world_mut().query::<&Guid>();
    assert_eq!(q_guid.iter(&app2.world()).count(), 2);
    let mut q_h = app2.world_mut().query::<&Health>();
    assert_eq!(q_h.iter(&app2.world()).count(), 1);
    let mut q_p = app2.world_mut().query::<&Position>();
    assert_eq!(q_p.iter(&app2.world()).count(), 1);
}

// System: Duplicate component in Q should be deduped in fetch list
fn system_duplicate_q_components(mut pq: PersistentQuery<(&Health, Option<&Health>)>) {
    let _ = pq.iter_with_loading().count();
}

#[db_matrix_test]
fn test_duplicate_q_entries_are_deduped_and_work() {
    let (db, _container) = setup();

    // GIVEN: one Health-only entity
    let mut app = App::new();
    app.add_plugins(PersistencePlugins(db.clone()));
    app.world_mut().spawn(Health { value: 99 });
    app.update();
    commit_sync(&mut app).expect("Initial commit failed");

    // WHEN: Q requests &Health and Option<&Health> (duplicate)
    let mut app2 = App::new();
    app2.add_plugins(PersistencePlugins(db.clone()));
    app2.add_systems(bevy::prelude::Update, system_duplicate_q_components);
    app2.update();

    // THEN: loads once; Health present
    let mut q_guid = app2.world_mut().query::<&Guid>();
    assert_eq!(q_guid.iter(&app2.world()).count(), 1);
    let mut q_health = app2.world_mut().query::<&Health>();
    assert_eq!(q_health.iter(&app2.world()).count(), 1);
}

// System: OR with 3 arms
fn system_or_three_arms(
    mut pq: PersistentQuery<
        &Guid,
        bevy::prelude::Or<(bevy::prelude::With<Health>, bevy::prelude::With<Creature>, bevy::prelude::With<PlayerName>)>,
    >,
) {
    let _ = pq.iter_with_loading().count();
}

#[db_matrix_test]
fn test_or_presence_three_arms() {
    let (db, _container) = setup();

    // GIVEN: Health-only, Creature-only, PlayerName-only, Position-only
    let mut app = App::new();
    app.add_plugins(PersistencePlugins(db.clone()));
    app.world_mut().spawn(Health { value: 1 });
    app.world_mut().spawn(Creature { is_screaming: true });
    app.world_mut().spawn(PlayerName { name: "P".into() });
    app.world_mut().spawn(Position { x: 0.0, y: 0.0 });
    app.update();
    commit_sync(&mut app).expect("Initial commit failed");

    // WHEN
    let mut app2 = App::new();
    app2.add_plugins(PersistencePlugins(db.clone()));
    app2.add_systems(bevy::prelude::Update, system_or_three_arms);
    app2.update();

    // THEN: 3 entities loaded (H or C or Name)
    let mut q_guid = app2.world_mut().query::<&Guid>();
    assert_eq!(q_guid.iter(&app2.world()).count(), 3);
}

#[db_matrix_test]
fn test_persistent_query_pass_through_methods() {
    let (db, _container) = setup();

    // GIVEN: three entities with Health+Position, one also with PlayerName
    let mut app = App::new();
    app.add_plugins(PersistencePlugins(db.clone()));
    app.world_mut().spawn((Health { value: 10 }, Position { x: 1.0, y: 1.0 }, PlayerName { name: "only".into() }));
    app.world_mut().spawn((Health { value: 20 }, Position { x: 2.0, y: 2.0 }));
    app.world_mut().spawn((Health { value: 30 }, Position { x: 3.0, y: 3.0 }));
    app.update();
    commit_sync(&mut app).expect("Initial commit failed");

    // WHEN: run systems that first load, then use pass-through methods
    let mut app2 = App::new();
    app2.add_plugins(PersistencePlugins(db.clone()));
    app2.insert_resource(PassThroughState::default());

    // Enqueue load during Update
    app2.add_systems(bevy::prelude::Update, system_load_hp);
    // Run pass-through checks after deferred ops apply in PostUpdate
    app2.add_systems(
        bevy::prelude::PostUpdate,
        system_pass_through_core.after(bevy_arangodb_core::PersistenceSystemSet::PreCommit),
    );
    app2.add_systems(
        bevy::prelude::PostUpdate,
        system_pass_through_single.after(system_pass_through_core),
    );

    // Single frame: Update + PostUpdate
    app2.update();

    // THEN: pass-through results match in-world state
    let st = app2.world().resource::<PassThroughState>();
    // Loaded three entities
    assert_eq!(st.loaded_count, 3);

    // get_many sum of first two health values should be 10 + 20 = 30 (order-dependent but deterministic for first two)
    assert_eq!(st.got_sum_first_two, 30);

    // Combinations of 3 choose 2 = 3
    assert_eq!(st.combos_of_two, 3);

    // single (filtered by PlayerName) should be the one with health 10
    assert_eq!(st.single_health, 10);
    assert_eq!(st.single_health_via_single, 10);
}

#[db_matrix_test]
fn test_postupdate_load_applies_next_frame() {
    let (db, _container) = setup();

    // GIVEN: one entity to load
    let mut app1 = App::new();
    app1.add_plugins(PersistencePlugins(db.clone()));
    app1.world_mut().spawn((Health { value: 7 }, Position { x: 1.0, y: 2.0 }));
    app1.update();
    commit_sync(&mut app1).expect("commit failed");

    // WHEN: load is triggered from PostUpdate
    let mut app2 = App::new();
    app2.add_plugins(PersistencePlugins(db.clone()));
    fn postupdate_load(mut pq: PersistentQuery<(&Health, &Position)>) {
        let _ = pq.iter_with_loading().count();
    }
    // Ensure the loader runs before PreCommit so ops are applied in the same frame.
    app2.add_systems(
        bevy::prelude::PostUpdate,
        postupdate_load.before(bevy_arangodb_core::PersistenceSystemSet::PreCommit),
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

#[derive(bevy::prelude::Resource, Default)]
struct PassThroughState {
    loaded_count: usize,
    got_sum_first_two: i32,
    combos_of_two: usize,
    single_health: i32,
    single_health_via_single: i32,
}

// Enqueue a DB load during Update
fn system_load_hp(mut pq: PersistentQuery<(&Health, &Position)>) {
    let _ = pq.iter_with_loading().count();
}

// Validate get/get_many/iter_combinations after a load
fn system_pass_through_core(
    mut pq: PersistentQuery<(&Health, &Position)>,
    mut state: bevy::prelude::ResMut<PassThroughState>,
) {
    // Load from DB first
    let mut ids = Vec::new();
    for (e, (_h, _p)) in pq.iter_with_loading() {
        ids.push(e);
        state.loaded_count += 1;
    }
    // get for the first entity
    if let Some(&first) = ids.get(0) {
        let (_e, (h, _p)) = pq.get(first).expect("get() failed");
        assert!(h.value >= 0);
    }
    // get_many for two entities with smallest health
    if ids.len() >= 2 {
        let mut pairs: Vec<(bevy::prelude::Entity, i32)> = ids
            .iter()
            .map(|&e| {
                let (_e, (h, _)) = pq.get(e).expect("get() failed");
                (e, h.value)
            })
            .collect();
        pairs.sort_by_key(|(_, v)| *v);
        let a = pairs[0].0;
        let b = pairs[1].0;
        let arr = pq.get_many([a, b]).expect("get_many failed");
        state.got_sum_first_two = arr.iter().map(|(_, (h, _))| h.value).sum();
    }
    // combinations of 2
    state.combos_of_two = pq.iter_combinations::<2>().count();
}

// Validate single()/get_single() after a load with a presence filter
fn system_pass_through_single(
    pq: PersistentQuery<(&Health, &Position), With<PlayerName>>,
    mut state: bevy::prelude::ResMut<PassThroughState>,
) {
    let (_e, (h, _p)) = pq.single().expect("single failed");
    state.single_health = h.value;

    let (_e2, (h2, _p2)) = pq.single().expect("single failed");
    state.single_health_via_single = h2.value;
}