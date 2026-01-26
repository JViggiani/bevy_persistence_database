use crate::common::CountingDbConnection;
use crate::common::*;
use bevy::prelude::IntoScheduleConfigs;
use bevy::prelude::*;
use bevy_persistence_database::{
    Guid, PersistenceSystemSet, PersistentQuery, commit_sync,
};
use bevy_persistence_database_derive::db_matrix_test;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

#[db_matrix_test]
fn test_ensure_loaded_then_pass_through() {
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
    assert_eq!(
        counter.load(Ordering::SeqCst),
        1,
        "expected exactly one execute_documents call"
    );
}

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
        pq.ensure_loaded();
        pq.ensure_loaded(); // should hit cache
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

#[db_matrix_test]
fn test_deref_forwards_bevy_query_methods() {
    let (db_real, _container) = setup();

    // Seed one entity
    let mut app_seed = setup_test_app(db_real.clone(), None);
    let e = app_seed
        .world_mut()
        .spawn((Health { value: 5 }, Position { x: 1.0, y: 2.0 }))
        .id();
    app_seed.update();
    commit_sync(&mut app_seed, db_real.clone(), TEST_STORE).unwrap();

    // App under test
    let mut app = setup_test_app(db_real.clone(), None);

    // Frame 1: load
    fn load(mut pq: PersistentQuery<(&Health, &Position)>) {
        let _ = pq.ensure_loaded();
    }
    app.add_systems(Update, load);
    app.update();

    // Frame 2: use Bevy Query methods via Deref
    #[derive(Resource)]
    struct Ent(Entity);
    app.insert_resource(Ent(e));
    fn verify(pq: PersistentQuery<(&Health, &Position)>, ent: Res<Ent>) {
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

/// Test that verifies immediate pass-through works right after a load in the same system
#[db_matrix_test]
fn test_immediate_pass_through() {
    let (db, _container) = setup();

    // Seed DB with a few entities
    let mut app_seed = setup_test_app(db.clone(), None);
    app_seed.world_mut().spawn(Health { value: 10 });
    app_seed.world_mut().spawn(Health { value: 20 });
    app_seed.update();
    commit_sync(&mut app_seed, db.clone(), TEST_STORE).expect("seed commit failed");

    // App under test
    let mut app = setup_test_app(db.clone(), None);

    // Test resource to store results
    #[derive(Resource, Default)]
    struct TestResults {
        immediate_iter_count: usize,
        immediate_get_success: bool,
        immediate_contains: bool,
        first_entity: Option<Entity>,
    }
    app.insert_resource(TestResults::default());

    // System with load and immediate query in the same function
    fn test_immediate_system(mut pq: PersistentQuery<&Health>, mut results: ResMut<TestResults>) {
        // Load with immediate apply
        pq.ensure_loaded();

        // Test immediate iter
        let entities: Vec<Entity> = pq.iter().map(|(e, _)| e).collect();
        results.immediate_iter_count = entities.len();

        // Try get and contains on the first entity
        if let Some(&first) = entities.first() {
            results.first_entity = Some(first);
            results.immediate_get_success = pq.get(first).is_ok();
            results.immediate_contains = pq.contains(first);
        }
    }

    app.add_systems(
        PostUpdate,
        test_immediate_system.after(PersistenceSystemSet::PreCommit),
    );
    app.update();

    // Verify results
    let result = app.world().resource::<TestResults>();
    assert_eq!(
        result.immediate_iter_count, 2,
        "iter should return all loaded entities immediately"
    );
    assert!(
        result.immediate_get_success,
        "get should work immediately after load"
    );
    assert!(
        result.immediate_contains,
        "contains should work immediately after load"
    );

    // Verify entity is actually in the world
    if let Some(entity) = result.first_entity {
        let health = app.world().get::<Health>(entity);
        assert!(health.is_some(), "Entity should exist in the world");
    } else {
        panic!("Failed to capture entity during immediate apply");
    }
}

/// Test that verifies basic pass-through methods work after a load
#[db_matrix_test]
fn test_query_contains_method() {
    let (db, _container) = setup();

    // GIVEN a committed entity with Health
    let mut app_seed = setup_test_app(db.clone(), None);
    let entity = app_seed.world_mut().spawn(Health { value: 42 }).id();
    // Don't try to access Guid immediately - it gets added during commit
    app_seed.update();
    commit_sync(&mut app_seed, db.clone(), TEST_STORE).expect("seed commit failed");

    // Now we can safely get the Guid (after commit)
    let _guid = app_seed
        .world()
        .get::<Guid>(entity)
        .unwrap()
        .id()
        .to_string();

    // WHEN we load it into a new app
    let mut app = setup_test_app(db.clone(), None);

    // First system loads the entity
    #[derive(Resource, Default)]
    struct LoadedEntity(Option<Entity>);

    app.insert_resource(LoadedEntity::default());

    fn load_health(mut pq: PersistentQuery<&Health>, mut res: ResMut<LoadedEntity>) {
        // Load health entities
        pq.ensure_loaded();

        // Capture the first entity
        if let Some((e, _)) = pq.iter().next() {
            res.0 = Some(e);
        }
    }

    // Second system tests contains()
    #[derive(Resource, Default)]
    struct TestResult(bool);

    app.insert_resource(TestResult::default());

    fn check_contains(
        pq: PersistentQuery<&Health>,
        entity: Res<LoadedEntity>,
        mut result: ResMut<TestResult>,
    ) {
        if let Some(e) = entity.0 {
            result.0 = pq.contains(e);
        }
    }

    // Run the systems in sequence
    app.add_systems(Update, load_health.after(PersistenceSystemSet::PreCommit));
    app.update();

    app.add_systems(Update, check_contains);
    app.update();

    // Verify the result
    let contains_result = app.world().resource::<TestResult>().0;
    assert!(
        contains_result,
        "contains() should return true for a loaded entity"
    );
}

/// Test that verifies get() method works after a load
#[db_matrix_test]
fn test_query_get_method() {
    let (db, _container) = setup();

    // GIVEN a committed entity with Health
    let mut app_seed = setup_test_app(db.clone(), None);
    let _entity = app_seed.world_mut().spawn(Health { value: 42 }).id();
    app_seed.update();
    commit_sync(&mut app_seed, db.clone(), TEST_STORE).expect("seed commit failed");

    // WHEN we load it into a new app
    let mut app = setup_test_app(db.clone(), None);

    // First system loads the entity
    #[derive(Resource, Default)]
    struct LoadedEntity(Option<Entity>);

    app.insert_resource(LoadedEntity::default());

    fn load_health(mut pq: PersistentQuery<&Health>, mut res: ResMut<LoadedEntity>) {
        // Load health entities
        pq.ensure_loaded();

        // Capture the first entity
        if let Some((e, _)) = pq.iter().next() {
            res.0 = Some(e);
        }
    }

    // Second system tests get()
    #[derive(Resource, Default)]
    struct TestResult(bool);

    app.insert_resource(TestResult::default());

    fn check_get(
        pq: PersistentQuery<&Health>,
        entity: Res<LoadedEntity>,
        mut result: ResMut<TestResult>,
    ) {
        if let Some(e) = entity.0 {
            result.0 = pq.get(e).is_ok();
        }
    }

    // Run the systems in sequence
    app.add_systems(Update, load_health.after(PersistenceSystemSet::PreCommit));
    app.update();

    app.add_systems(Update, check_get);
    app.update();

    // Verify the result
    let get_result = app.world().resource::<TestResult>().0;
    assert!(get_result, "get() should succeed for a loaded entity");
}

/// Test that verifies get_mut() method works after a load
#[db_matrix_test]
fn test_query_get_mut_method() {
    let (db, _container) = setup();

    // GIVEN a committed entity with Health
    let mut app_seed = setup_test_app(db.clone(), None);
    let _entity = app_seed.world_mut().spawn(Health { value: 42 }).id();
    app_seed.update();
    commit_sync(&mut app_seed, db.clone(), TEST_STORE).expect("seed commit failed");

    // WHEN we load it into a new app
    let mut app = setup_test_app(db.clone(), None);

    // First system loads the entity
    #[derive(Resource, Default)]
    struct LoadedEntity(Option<Entity>);

    app.insert_resource(LoadedEntity::default());

    fn load_health(mut pq: PersistentQuery<&Health>, mut res: ResMut<LoadedEntity>) {
        // Load health entities
        pq.ensure_loaded();

        // Capture the first entity
        if let Some((e, _)) = pq.iter().next() {
            res.0 = Some(e);
        }
    }

    // Second system tests get_mut()
    #[derive(Resource, Default)]
    struct TestResult(bool);

    app.insert_resource(TestResult::default());

    fn check_get_mut(
        mut pq: PersistentQuery<&mut Health>,
        entity: Res<LoadedEntity>,
        mut result: ResMut<TestResult>,
    ) {
        if let Some(e) = entity.0 {
            result.0 = pq.get_mut(e).is_ok();
        }
    }

    // Run the systems in sequence
    app.add_systems(Update, load_health.after(PersistenceSystemSet::PreCommit));
    app.update();

    app.add_systems(Update, check_get_mut);
    app.update();

    // Verify the result
    let get_mut_result = app.world().resource::<TestResult>().0;
    assert!(
        get_mut_result,
        "get_mut() should succeed for a loaded entity"
    );
}

/// Test that verifies get_many() method works after a load
#[db_matrix_test]
fn test_query_get_many_method() {
    let (db, _container) = setup();

    // GIVEN multiple committed entities with Health
    let mut app_seed = setup_test_app(db.clone(), None);
    app_seed.world_mut().spawn(Health { value: 10 });
    app_seed.world_mut().spawn(Health { value: 20 });
    app_seed.update();
    commit_sync(&mut app_seed, db.clone(), TEST_STORE).expect("seed commit failed");

    // WHEN we load them into a new app
    let mut app = setup_test_app(db.clone(), None);

    // Resource to store entities
    #[derive(Resource, Default)]
    struct LoadedEntities(Vec<Entity>);

    app.insert_resource(LoadedEntities::default());

    fn load_health(mut pq: PersistentQuery<&Health>, mut res: ResMut<LoadedEntities>) {
        // Load health entities
        pq.ensure_loaded();

        // Capture entities
        for (e, _) in pq.iter() {
            res.0.push(e);
        }
    }

    // Second system tests get_many()
    #[derive(Resource, Default)]
    struct TestResult(bool);

    app.insert_resource(TestResult::default());

    fn check_get_many(
        pq: PersistentQuery<&Health>,
        entities: Res<LoadedEntities>,
        mut result: ResMut<TestResult>,
    ) {
        if entities.0.len() < 2 {
            return;
        }

        let get_result = pq.get_many([entities.0[0], entities.0[1]]);
        result.0 = get_result.is_ok();
    }

    // Run the systems in sequence
    app.add_systems(Update, load_health.after(PersistenceSystemSet::PreCommit));
    app.update();

    app.add_systems(Update, check_get_many);
    app.update();

    // Verify the result
    let get_many_result = app.world().resource::<TestResult>().0;
    assert!(
        get_many_result,
        "get_many() should succeed for loaded entities"
    );
}

/// Test that verifies single() method works after a load
#[db_matrix_test]
fn test_query_single_method() {
    let (db, _container) = setup();

    // GIVEN a committed entity with PlayerName (to ensure exactly one entity)
    let mut app_seed = setup_test_app(db.clone(), None);
    app_seed.world_mut().spawn((
        Health { value: 50 },
        PlayerName {
            name: "player".into(),
        },
    ));
    app_seed.update();
    commit_sync(&mut app_seed, db.clone(), TEST_STORE).expect("seed commit failed");

    // WHEN we load it into a new app with a filter for PlayerName
    let mut app = setup_test_app(db.clone(), None);

    // Resource to store result
    #[derive(Resource, Default)]
    struct SingleResult {
        health_value: Option<i32>,
        success: bool,
    }

    app.insert_resource(SingleResult::default());

    fn test_single(
        mut pq: PersistentQuery<&Health, With<PlayerName>>,
        mut result: ResMut<SingleResult>,
    ) {
        // Load and immediately use single
        pq.ensure_loaded();

        match pq.single() {
            Ok((_e, health)) => {
                result.health_value = Some(health.value);
                result.success = true;
            }
            Err(_) => {
                result.success = false;
            }
        }
    }

    app.add_systems(Update, test_single.after(PersistenceSystemSet::PreCommit));
    app.update();

    // THEN the single method should succeed
    let result = app.world().resource::<SingleResult>();
    assert!(
        result.success,
        "single() should succeed for a unique entity"
    );
    assert_eq!(
        result.health_value,
        Some(50),
        "single() should return the correct health value"
    );
}

/// Test that verifies iter_combinations() method works after a load
#[db_matrix_test]
fn test_query_iter_combinations_method() {
    let (db, _container) = setup();

    // GIVEN multiple committed entities with Health
    let mut app_seed = setup_test_app(db.clone(), None);
    app_seed.world_mut().spawn(Health { value: 10 });
    app_seed.world_mut().spawn(Health { value: 20 });
    app_seed.world_mut().spawn(Health { value: 30 });
    app_seed.update();
    commit_sync(&mut app_seed, db.clone(), TEST_STORE).expect("seed commit failed");

    // WHEN we load them into a new app
    let mut app = setup_test_app(db.clone(), None);

    // Resource to store result
    #[derive(Resource, Default)]
    struct CombinationsResult {
        count: usize,
    }

    app.insert_resource(CombinationsResult::default());

    fn test_combinations(mut pq: PersistentQuery<&Health>, mut result: ResMut<CombinationsResult>) {
        // Load and immediately use iter_combinations
        pq.ensure_loaded();

        // Count combinations of 2 entities
        result.count = pq.iter_combinations::<2>().count();
        bevy::log::info!("Found {} combinations of 2 entities", result.count);
    }

    app.add_systems(
        Update,
        test_combinations.after(PersistenceSystemSet::PreCommit),
    );
    app.update();

    // THEN there should be exactly 3 combinations (3 choose 2 = 3)
    let result = app.world().resource::<CombinationsResult>();
    assert_eq!(
        result.count, 3,
        "iter_combinations should return the correct number of combinations"
    );
}
