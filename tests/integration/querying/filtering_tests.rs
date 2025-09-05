use bevy::prelude::App;
use bevy_persistence_database::{
    commit_sync, Guid, persistence_plugin::PersistencePlugins, PersistentQuery,
};
use crate::common::*;
use bevy_persistence_database_derive::db_matrix_test;

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
        let _ = pq.filter(Health::value().eq(100)).ensure_loaded();
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
        let _ = pq.filter(Creature::is_screaming().eq(true)).ensure_loaded();
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
        let _ = pq.filter(PlayerName::name().eq("Alice")).ensure_loaded();
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
    ) { let _ = pq.filter(Health::value().gt(100)).ensure_loaded(); }
    app2.add_systems(bevy::prelude::Update, gt);
    app2.update();
    assert_eq!(app2.world_mut().query::<&Health>().iter(&app2.world()).count(), 1);

    // gte(100) -> 2
    app2.world_mut().clear_entities();
    fn gte(
        pq: PersistentQuery<&Health>
    ) { let _ = pq.filter(Health::value().gte(100)).ensure_loaded(); }
    app2.add_systems(bevy::prelude::Update, gte);
    app2.update();
    assert_eq!(app2.world_mut().query::<&Health>().iter(&app2.world()).count(), 2);

    // lt(100) -> 1
    app2.world_mut().clear_entities();
    fn lt(
        pq: PersistentQuery<&Health>
    ) { let _ = pq.filter(Health::value().lt(100)).ensure_loaded(); }
    app2.add_systems(bevy::prelude::Update, lt);
    app2.update();
    assert_eq!(app2.world_mut().query::<&Health>().iter(&app2.world()).count(), 1);

    // lte(100) -> 2
    app2.world_mut().clear_entities();
    fn lte(
        pq: PersistentQuery<&Health>
    ) { let _ = pq.filter(Health::value().lte(100)).ensure_loaded(); }
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
        let _ = pq.filter(expr).ensure_loaded();
    }
    fn or_sys(
        pq: PersistentQuery<(&Health, &Position)>
    ) {
        let expr = Health::value().gt(100).or(Position::x().lt(100.0));
        let _ = pq.filter(expr).ensure_loaded();
    }

    app2.add_systems(bevy::prelude::Update, and_sys);
    app2.update();
    assert_eq!(app2.world_mut().query::<(&Health, &Position)>().iter(&app2.world()).count(), 1);

    app2.world_mut().clear_entities();
    app2.add_systems(bevy::prelude::Update, or_sys);
    app2.update();
    assert_eq!(app2.world_mut().query::<(&Health, &Position)>().iter(&app2.world()).count(), 3);
}

// Presence OR tree combined with a value filter: ((Health AND Position) OR PlayerName) AND Health.value >= 100
#[db_matrix_test]
fn test_presence_value_combination_and_or() {
    let (db, _container) = setup();

    // Seed: only the first should match after value filter (Health.value >= 100)
    let mut app_seed = App::new();
    app_seed.add_plugins(PersistencePlugins(db.clone()));
    app_seed.world_mut().spawn((Health { value: 120 }, Position { x: 0.0, y: 0.0 })); // match
    app_seed.world_mut().spawn(PlayerName { name: "p".into() }); // present via OR, but filtered out by value predicate
    app_seed.world_mut().spawn(Health { value: 80 }); // Health present but under threshold
    app_seed.update();
    commit_sync(&mut app_seed).expect("seed commit failed");

    // App under test: load using presence OR + value filter
    let mut app = App::new();
    app.add_plugins(PersistencePlugins(db.clone()));

    fn sys(
        pq: PersistentQuery<
            &Guid,
            bevy::prelude::Or<(
                (bevy::prelude::With<Health>, bevy::prelude::With<Position>),
                bevy::prelude::With<PlayerName>
            )>,
        >
    ) {
        let _ = pq.filter(Health::value().gte(100)).ensure_loaded();
    }
    app.add_systems(bevy::prelude::Update, sys);
    app.update();

    // Only one entity should have been loaded
    let mut q = app.world_mut().query::<&Guid>();
    let count = q.iter(&app.world()).count();
    assert_eq!(count, 1, "expected exactly 1 entity to match presence OR + value filter");
}
