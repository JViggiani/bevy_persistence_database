use bevy::prelude::*;
use bevy_persistence_database_core::{
    commit_sync, Guid, persistence_plugin::PersistencePlugins, PersistentQuery,
    DatabaseConnection, db::connection::DatabaseConnectionResource,
};
use crate::common::*;
use crate::common::CountingDbConnection;
use bevy_persistence_database_derive::db_matrix_test;
use std::sync::{Arc, atomic::{AtomicUsize, Ordering}};

// System that uses query twice to test caching
fn test_cached_query_system(mut query1: PersistentQuery<&Health>, mut query2: PersistentQuery<&Health>) {
    // First query execution - should hit database
    let _ = query1.ensure_loaded();
    
    // Second execution of equivalent query - should use cache
    let _ = query2.ensure_loaded();
}

// System that tests force refresh
fn test_force_refresh_system(mut query: PersistentQuery<&Health>) {
    // Use force refresh to bypass cache
    query = query.force_refresh();
    let _ = query.ensure_loaded();
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

#[derive(bevy::prelude::Resource, Default)]
struct TestState {
    entity: Option<bevy::prelude::Entity>,
    mutated: bool,
}

fn system_load_and_capture(
    mut pq: PersistentQuery<&Health>,
    mut state: bevy::prelude::ResMut<TestState>,
) {
    pq.ensure_loaded();
    for (e, _) in pq.iter() {
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
                .ensure_loaded();
        }
    }
}

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

#[derive(bevy::prelude::Resource, Clone)]
struct TestKey(String);

fn force_refresh_system(query: PersistentQuery<&Health>, key: bevy::prelude::Res<TestKey>) {
    let _ = query
        .filter(Guid::key_field().eq(key.0.as_str()))
        .force_refresh()
        .ensure_loaded();
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
    fn load(mut pq: PersistentQuery<&Health, With<Health>>) { let _ = pq.ensure_loaded(); }
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
