use bevy::prelude::*;
use bevy::prelude::IntoScheduleConfigs;
use bevy_persistence_database::{
    commit_sync,
    PersistentQuery,
    persistence_plugin::{PersistencePlugins, PersistenceSystemSet},
};
use bevy_persistence_database_derive::db_matrix_test;
use crate::common::*;

// Load via ensure_loaded, then transmute to a world-only view and verify.
#[db_matrix_test]
fn test_query_lens_transmutation_world_only() {
    let (db, _container) = setup();

    // Seed: E1 = H+P (match), E2 = H only (no), E3 = P only (no)
    let mut app_seed = App::new();
    app_seed.add_plugins(PersistencePlugins::new(db.clone()));
    app_seed.world_mut().spawn((Health { value: 10 }, Position { x: 1.0, y: 2.0 })); // match
    app_seed.world_mut().spawn(Health { value: 20 }); // excluded
    app_seed.world_mut().spawn(Position { x: 9.0, y: 9.0 }); // excluded
    app_seed.update();
    commit_sync(&mut app_seed, db.clone(), TEST_STORE).unwrap();

    // App under test
    let mut app = App::new();
    app.add_plugins(PersistencePlugins::new(db.clone()));

    #[derive(Resource, Default)]
    struct LensState {
        transmute_count: usize,
    }
    app.insert_resource(LensState::default());

    // Update: load original query set (&Health, &Position)
    fn sys_load(mut pq: PersistentQuery<(&Health, &Position)>) {
        let _ = pq.ensure_loaded();
    }
    app.add_systems(Update, sys_load);

    // PostUpdate: use transmute_lens to view as Query<&Health> and count matches
    fn sys_transmute(
        mut pq: PersistentQuery<(&Health, &Position)>,
        mut st: ResMut<LensState>,
    ) {
        let mut lens = pq.transmute_lens::<&Health>();
        st.transmute_count = lens.query().iter().count();
    }
    app.add_systems(PostUpdate, sys_transmute.after(PersistenceSystemSet::PreCommit));

    // Single frame (Update + PostUpdate)
    app.update();

    // Only E1 (H+P) is accessible through the lens derived from the original query.
    let st = app.world().resource::<LensState>();
    assert_eq!(st.transmute_count, 1);
}

// Like the Bevy example: build a lens and call another function with its Query<&Health>.
#[db_matrix_test]
fn test_query_lens_transmutation_calls_fn() {
    let (db, _container) = setup();

    // Seed: two H+P
    let mut app_seed = App::new();
    app_seed.add_plugins(PersistencePlugins::new(db.clone()));
    app_seed.world_mut().spawn((Health { value: 7 }, Position { x: 0.0, y: 0.0 }));
    app_seed.world_mut().spawn((Health { value: 8 }, Position { x: 1.0, y: 1.0 }));
    app_seed.update();
    commit_sync(&mut app_seed, db.clone(), TEST_STORE).unwrap();

    // App under test
    let mut app = App::new();
    app.add_plugins(PersistencePlugins::new(db.clone()));

    #[derive(Resource, Default)]
    struct CallState {
        total: i32,
        count: usize,
    }
    app.insert_resource(CallState::default());

    // Helper "callee" that takes a Query<&Health> and returns (count, sum)
    fn debug_healths(query: Query<&Health>) -> (usize, i32) {
        let mut total = 0;
        let mut count = 0;
        for h in query.iter() {
            total += h.value;
            count += 1;
        }
        (count, total)
    }

    // Update: load original set
    fn sys_load(mut pq: PersistentQuery<(&Health, &Position)>) {
        let _ = pq.ensure_loaded();
    }
    app.add_systems(Update, sys_load);

    // PostUpdate: transmute and call the helper
    fn sys_call(
        mut pq: PersistentQuery<(&Health, &Position)>,
        mut st: ResMut<CallState>,
    ) {
        let mut lens = pq.transmute_lens::<&Health>();
        let (count, total) = debug_healths(lens.query());
        st.count = count;
        st.total = total;
    }
    app.add_systems(PostUpdate, sys_call.after(PersistenceSystemSet::PreCommit));

    app.update();

    let st = app.world().resource::<CallState>();
    assert_eq!(st.count, 2);
    assert_eq!(st.total, 15);
}
