use bevy::prelude::*;
use bevy_arangodb_core::{
    PersistencePluginCore, PersistentQuery, commit_sync,
};
use crate::common::*;
use std::sync::{Arc, atomic::{AtomicUsize, Ordering}};
use bevy_arangodb_derive::db_matrix_test;
use crate::common::CountingDbConnection;

#[db_matrix_test]
fn test_ensure_loaded_then_pass_through() {
    let (real_db, _container) = setup();

    // Seed DB with one entity having Health+Position.
    let mut app_seed = App::new();
    app_seed.add_plugins(PersistencePluginCore::new(real_db.clone()));
    app_seed.world_mut().spawn((Health { value: 150 }, Position { x: 10.0, y: 20.0 }));
    app_seed.update();
    commit_sync(&mut app_seed).expect("seed commit failed");

    // Wrap the db with a counting adapter
    let counter = Arc::new(AtomicUsize::new(0));
    let db = Arc::new(CountingDbConnection::new(real_db.clone(), counter.clone()));

    // App under test
    let mut app = App::new();
    app.add_plugins(PersistencePluginCore::new(db.clone()));

    // 1) Update system: explicitly load once; call twice to validate cache later.
    fn sys_load(mut pq: PersistentQuery<(&Health, &Position), (With<Health>, With<Position>)>) {
        let _ = pq.ensure_loaded().ensure_loaded();
    }
    app.add_systems(Update, sys_load);
    app.update(); // triggers DB load and schedules world ops

    // 2) Next frame: pass-throughs should see data (no DB I/O).
    fn sys_verify(pq: PersistentQuery<(&Health, &Position), (With<Health>, With<Position>)>) {
        // Deref pass-throughs: iter, single, iter_combinations
        let all: Vec<_> = pq.iter().collect();
        assert_eq!(all.len(), 1);
        let (_e, (h, p)) = pq.single().unwrap();
        assert_eq!(h.value, 150);
        assert_eq!(p.x, 10.0);
        assert_eq!(p.y, 20.0);
        let combos = pq.iter_combinations::<1>().count();
        assert_eq!(combos, 1);
    }
    app.add_systems(Update, sys_verify);
    app.update();

    // Exactly one execute_documents call
    assert_eq!(counter.load(Ordering::SeqCst), 1, "expected exactly one execute_documents call");
}

#[db_matrix_test]
fn test_cache_prevents_duplicate_loads_in_same_frame() {
    let (real_db, _container) = setup();

    // Seed DB
    let mut app_seed = App::new();
    app_seed.add_plugins(PersistencePluginCore::new(real_db.clone()));
    app_seed.world_mut().spawn(Health { value: 42 });
    app_seed.update();
    commit_sync(&mut app_seed).unwrap();

    // Wrap DB to count execute_documents calls
    let counter = Arc::new(AtomicUsize::new(0));
    let db = Arc::new(CountingDbConnection::new(real_db.clone(), counter.clone()));

    // App under test
    let mut app = App::new();
    app.add_plugins(PersistencePluginCore::new(db.clone()));

    // Single system issuing two identical loads in the same frame
    fn sys_twice(mut pq: PersistentQuery<&Health, With<Health>>) {
        pq.ensure_loaded();
        pq.ensure_loaded(); // should hit cache
    }
    app.add_systems(Update, sys_twice);
    app.update();

    // No additional loads in later frame unless requested
    app.update();

    assert_eq!(counter.load(Ordering::SeqCst), 1, "cache should coalesce identical loads");
}

#[db_matrix_test]
fn test_deref_forwards_bevy_query_methods() {
    let (db_real, _container) = setup();

    // Seed one entity
    let mut app_seed = App::new();
    app_seed.add_plugins(PersistencePluginCore::new(db_real.clone()));
    let e = app_seed.world_mut().spawn((Health { value: 5 }, Position { x: 1.0, y: 2.0 })).id();
    app_seed.update();
    commit_sync(&mut app_seed).unwrap();

    // App under test
    let mut app = App::new();
    app.add_plugins(PersistencePluginCore::new(db_real.clone()));

    // Frame 1: load
    fn load(mut pq: PersistentQuery<(&Health, &Position)>) { let _ = pq.ensure_loaded(); }
    app.add_systems(Update, load);
    app.update();

    // Frame 2: use Bevy Query methods via Deref
    #[derive(Resource)]
    struct Ent(Entity);
    app.insert_resource(Ent(e));
    fn verify(
        pq: PersistentQuery<(&Health, &Position)>,
        ent: Res<Ent>,
    ) {
        // contains
        assert!(pq.contains(ent.0));
        // get
        let (_e, (h, p)) = pq.get(ent.0).unwrap();
        assert_eq!(h.value, 5);
        assert_eq!(p.x, 1.0);
        // iter and collect
        let v: Vec<_> = pq.iter().collect();
        assert_eq!(v.len(), 1);
        // combinations
        assert_eq!(pq.iter_combinations::<1>().count(), 1);
        // get_many on same entity twice just to hit the API shape
        let _ = pq.get_many([ent.0]).unwrap();
    }
    app.add_systems(Update, verify);
    app.update();
}
