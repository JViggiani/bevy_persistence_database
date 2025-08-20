use bevy::prelude::*;
use bevy_arangodb_core::{
    PersistencePluginCore, PersistentQuery, commit_sync,
};
use bevy_arangodb_core::persistence_plugin::PersistencePlugins;
use crate::common::*;
use std::sync::{Arc, atomic::{AtomicUsize, Ordering}};
use bevy_arangodb_derive::db_matrix_test;
use crate::common::CountingDbConnection;
use bevy::prelude::IntoScheduleConfigs;

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
    let _ = pq.ensure_loaded();
}

// Validate get/get_many/iter_combinations after a load
fn system_pass_through_core(
    mut pq: PersistentQuery<(&Health, &Position)>,
    mut state: bevy::prelude::ResMut<PassThroughState>,
) {
    // Load from DB first
    let mut ids = Vec::new();
    pq.ensure_loaded();
    for (e, (_h, _p)) in pq.iter() {
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
