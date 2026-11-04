use crate::common::CountingDbConnection;
use crate::common::*;
use bevy::prelude::IntoScheduleConfigs;
use bevy::prelude::*;
use bevy_persistence_database::bevy::params::query::PersistentQuery;
use bevy_persistence_database::core::session::commit_sync;
use bevy_persistence_database_derive::db_matrix_test;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

// GIVEN one committed entity with Health and Position
// WHEN a system requests the same load twice and a later frame reads through the query
// THEN the reads see the entity and the database was queried once
#[db_matrix_test]
fn test_load_then_pass_through() {
    let (real_db, _container) = setup();

    // Seed DB with one entity having Health+Position.
    let mut app_seed = setup_test_app(real_db.clone(), None);
    app_seed
        .world_mut()
        .spawn((Health { value: 150 }, Position { x: 10.0, y: 20.0 }));
    app_seed.update();
    commit_sync(&mut app_seed, real_db.clone(), TEST_STORE).expect("seed commit failed");

    // Wrap the db with a counting adapter
    let counter = Arc::new(AtomicUsize::new(0));
    let db = Arc::new(CountingDbConnection::new(real_db.clone(), counter.clone()));

    // App under test
    let mut app = setup_test_app(db.clone(), None);

    // 1) Update system: explicitly load once; call twice to validate cache later.
    fn sys_load(mut pq: PersistentQuery<(&Health, &Position), (With<Health>, With<Position>)>) {
        let _ = pq.load().load();
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
    assert_eq!(
        counter.load(Ordering::SeqCst),
        1,
        "expected exactly one execute_documents call"
    );
}

// GIVEN one committed entity with Health
// WHEN a system requests the same load twice in one frame, then another frame runs
// THEN the database is queried once
#[db_matrix_test]
fn test_cache_prevents_duplicate_loads_in_same_frame() {
    let (real_db, _container) = setup();

    // Seed DB
    let mut app_seed = setup_test_app(real_db.clone(), None);
    app_seed.world_mut().spawn(Health { value: 42 });
    app_seed.update();
    commit_sync(&mut app_seed, real_db.clone(), TEST_STORE).unwrap();

    // Wrap DB to count execute_documents calls
    let counter = Arc::new(AtomicUsize::new(0));
    let db = Arc::new(CountingDbConnection::new(real_db.clone(), counter.clone()));

    // App under test
    let mut app = setup_test_app(db.clone(), None);

    // Single system issuing two identical loads in the same frame
    fn sys_twice(mut pq: PersistentQuery<&Health, With<Health>>) {
        pq.load();
        pq.load(); // should hit cache
    }
    app.add_systems(Update, sys_twice);
    app.update();

    // No additional loads in later frame unless requested
    app.update();

    assert_eq!(
        counter.load(Ordering::SeqCst),
        1,
        "cache should coalesce identical loads"
    );
}

// GIVEN one committed entity with Health and Position
// WHEN a frame loads it and the next frame reads through the PersistentQuery
// THEN every Bevy Query method reached through Deref sees it
#[db_matrix_test]
fn test_deref_forwards_bevy_query_methods() {
    let (db_real, _container) = setup();

    let mut app_seed = setup_test_app(db_real.clone(), None);
    app_seed
        .world_mut()
        .spawn((Health { value: 5 }, Position { x: 1.0, y: 2.0 }));
    app_seed.update();
    commit_sync(&mut app_seed, db_real.clone(), TEST_STORE).unwrap();

    let mut app = setup_test_app(db_real.clone(), None);

    // Frame 1: load
    fn load(mut pq: PersistentQuery<(&Health, &Position)>) {
        let _ = pq.load();
    }
    app.add_systems(Update, load);
    app.update();

    // Frame 2: use Bevy Query methods via Deref
    #[derive(Resource, Default)]
    struct Checked {
        reads: bool,
        writes: bool,
    }
    app.init_resource::<Checked>();
    fn verify(pq: PersistentQuery<(&Health, &Position)>, mut checked: ResMut<Checked>) {
        let (entity, (h, p)) = pq.single().unwrap();
        assert_eq!(h.value, 5);
        assert_eq!(p.x, 1.0);
        assert!(pq.contains(entity));
        let (_e, (h, _p)) = pq.get(entity).unwrap();
        assert_eq!(h.value, 5);
        assert_eq!(pq.iter().count(), 1);
        assert_eq!(pq.iter_combinations::<1>().count(), 1);
        let [(_e, (h, _p))] = pq.get_many([entity]).unwrap();
        assert_eq!(h.value, 5);
        checked.reads = true;
    }
    fn verify_mut(mut pq: PersistentQuery<&mut Health>, mut checked: ResMut<Checked>) {
        let entity = pq.iter().next().map(|(e, _)| e).unwrap();
        let (_e, mut h) = pq.get_mut(entity).unwrap();
        h.value += 1;
        checked.writes = true;
    }
    app.add_systems(Update, (verify, verify_mut).chain());
    app.update();

    let checked = app.world().resource::<Checked>();
    assert!(checked.reads && checked.writes);
    let mut q = app.world_mut().query::<&Health>();
    assert_eq!(q.single(app.world()).unwrap().value, 6);
}

// GIVEN two committed entities with Health
// WHEN one system requests a load and a reader system is ordered after it
// THEN the reader sees both rows in the same frame
// AND the requesting system sees them on its next run
#[db_matrix_test]
fn test_reader_after_requester_sees_rows_same_frame() {
    let (db, _container) = setup();

    let mut app_seed = setup_test_app(db.clone(), None);
    app_seed.world_mut().spawn(Health { value: 10 });
    app_seed.world_mut().spawn(Health { value: 20 });
    app_seed.update();
    commit_sync(&mut app_seed, db.clone(), TEST_STORE).expect("seed commit failed");

    let mut app = setup_test_app(db.clone(), None);

    #[derive(Resource, Default)]
    struct Seen {
        requester: Vec<usize>,
        reader: Vec<usize>,
    }
    app.init_resource::<Seen>();

    fn request(mut pq: PersistentQuery<&Health>, mut seen: ResMut<Seen>) {
        pq.load();
        seen.requester.push(pq.iter().count());
    }
    fn read(q: Query<&Health>, mut seen: ResMut<Seen>) {
        seen.reader.push(q.iter().count());
    }
    app.add_systems(Update, (request, read).chain());
    app.update();
    app.update();

    let seen = app.world().resource::<Seen>();
    assert_eq!(seen.reader, vec![2, 2], "reader after the requester sees rows at once");
    assert_eq!(seen.requester, vec![0, 2], "requester sees rows on its next run");
}
