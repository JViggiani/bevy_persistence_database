use bevy::prelude::App;
use bevy_persistence_database::{
    commit_sync, Guid, persistence_plugin::PersistencePlugins, PersistentQuery,
};
use crate::common::*;
use bevy::prelude::With;
use bevy_persistence_database_derive::db_matrix_test;

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
        let _ = pq.filter(Health::value().gt(100)).ensure_loaded();
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
        let _ = pq.filter(Health::value().eq(100)).ensure_loaded();
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
        let _ = pq.filter(Health::value().gt(75)).ensure_loaded();
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
        let _ = pq.ensure_loaded();
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

// System: Duplicate component in Q should be deduped in fetch list
fn system_duplicate_q_components(mut pq: PersistentQuery<(&Health, Option<&Health>)>) {
    let _ = pq.ensure_loaded();
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

// System: Optional in Q with type-driven Without in F
fn system_optional_component_fetch(mut pq: PersistentQuery<(&Health, Option<&Position>)>) {
    // Request Health and optionally Position via Q; no explicit presence filter needed.
    let _ = pq.ensure_loaded();
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

// System: No presence filters; optional fetch-only for two components
fn system_no_presence_optional_fetch(mut pq: PersistentQuery<(Option<&Health>, Option<&Position>)>) {
    let _ = pq.ensure_loaded();
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
