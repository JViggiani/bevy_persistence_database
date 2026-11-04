use std::sync::Arc;

use crate::common::*;
use bevy::prelude::*;
use bevy_persistence_database::bevy::params::query::PersistentQuery;
use bevy_persistence_database::bevy::params::resource::{PersistentRes, PersistentResMut};
use bevy_persistence_database::core::db::DatabaseConnection;
use bevy_persistence_database::core::session::{PersistenceSession, commit_sync};
use bevy_persistence_database_derive::db_matrix_test;

#[derive(Resource, Default)]
struct CaptureSettings {
    loaded: bool,
    settings: Option<GameSettings>,
}

#[derive(Resource, Default)]
struct DirtyFlag {
    dirty: bool,
}

#[derive(Resource, Default)]
struct StepState {
    step: u8,
}

#[derive(Resource, Default)]
struct ReadCapture {
    first: Option<String>,
    second: Option<String>,
}

fn seed_settings(db: &Arc<dyn DatabaseConnection>, store: &str, settings: GameSettings) {
    let mut seed = setup_test_app(db.clone(), None);
    seed.insert_resource(settings);
    seed.update();
    commit_sync(&mut seed, db.clone(), store).expect("seed commit failed");
}

fn capture(mut res: PersistentRes<GameSettings>, mut cap: ResMut<CaptureSettings>) {
    if let Some(s) = res.get() {
        cap.loaded = true;
        cap.settings = Some(s.clone());
    }
}

// GIVEN GameSettings committed to the store and absent from a fresh app
// WHEN a system reads it through PersistentRes on two frames
// THEN the first frame requests the load and the second reads the stored value
#[db_matrix_test]
fn loads_resource_after_first_request() {
    let (db, _container) = setup();
    seed_settings(
        &db,
        TEST_STORE,
        GameSettings {
            difficulty: 0.42,
            map_name: "mystic".into(),
        },
    );

    let mut app = setup_test_app(db.clone(), None);
    app.insert_resource(CaptureSettings::default());
    app.add_systems(Update, capture);
    app.update();
    app.update();

    let cap = app.world().resource::<CaptureSettings>();
    assert!(cap.loaded, "resource should load");
    let settings = cap.settings.as_ref().expect("captured settings");
    assert_eq!(settings.difficulty, 0.42);
    assert_eq!(settings.map_name, "mystic");
}

// GIVEN GameSettings in the store and a different local GameSettings in the world
// WHEN a system reads it through PersistentRes
// THEN the local value is returned and not overwritten
#[db_matrix_test]
fn respects_existing_resource() {
    let (db, _container) = setup();
    seed_settings(
        &db,
        TEST_STORE,
        GameSettings {
            difficulty: 0.1,
            map_name: "persisted".into(),
        },
    );

    let mut app = setup_test_app(db.clone(), None);
    app.insert_resource(CaptureSettings::default());
    app.insert_resource(GameSettings {
        difficulty: 0.9,
        map_name: "local".into(),
    });
    app.add_systems(Update, capture);
    app.update();
    app.update();

    let cap = app.world().resource::<CaptureSettings>();
    assert!(cap.loaded, "resource should be returned from world state");
    let settings = cap.settings.as_ref().expect("captured settings");
    assert_eq!(
        settings.map_name, "local",
        "existing world resource should win"
    );
    assert_eq!(settings.difficulty, 0.9);
}

// GIVEN GameSettings loaded from the store
// WHEN a system mutates it locally and another PersistentRes reader reads it after
// THEN the first read saw the stored value and the second sees the local mutation
#[db_matrix_test]
fn second_read_uses_local_after_mutation() {
    let (db, _container) = setup();
    seed_settings(
        &db,
        TEST_STORE,
        GameSettings {
            difficulty: 0.1,
            map_name: "persisted".into(),
        },
    );

    let mut app = setup_test_app(db.clone(), None);
    app.insert_resource(ReadCapture::default());

    fn first_load(mut res: PersistentRes<GameSettings>, mut cap: ResMut<ReadCapture>) {
        if cap.first.is_none() {
            if let Some(gs) = res.get() {
                cap.first = Some(gs.map_name.clone());
            }
        }
    }

    fn mutate_local(gs: Option<ResMut<GameSettings>>, cap: Res<ReadCapture>) {
        if let (Some(mut gs), Some(_)) = (gs, cap.first.as_ref()) {
            gs.map_name = "local".into();
        }
    }

    fn second_load(mut res: PersistentRes<GameSettings>, mut cap: ResMut<ReadCapture>) {
        if cap.first.is_some() && cap.second.is_none() {
            if let Some(gs) = res.get() {
                cap.second = Some(gs.map_name.clone());
            }
        }
    }

    app.add_systems(Update, (first_load, mutate_local, second_load).chain());
    app.update();
    app.update();

    let cap = app.world().resource::<ReadCapture>();
    assert_eq!(cap.first.as_deref(), Some("persisted"));
    assert_eq!(cap.second.as_deref(), Some("local"));
}

// GIVEN GameSettings committed only to an alternate store
// WHEN PersistentRes requests it with that store override
// THEN the value from the alternate store loads
#[db_matrix_test]
fn store_override_is_used() {
    let (db, _container) = setup();
    let alt_store = "alt_store";
    seed_settings(
        &db,
        alt_store,
        GameSettings {
            difficulty: 0.7,
            map_name: "alt".into(),
        },
    );

    let mut app = setup_test_app(db.clone(), None);
    app.insert_resource(CaptureSettings::default());

    fn sys(res: PersistentRes<GameSettings>, mut cap: ResMut<CaptureSettings>) {
        let mut res = res.store("alt_store");
        if let Some(s) = res.get() {
            cap.loaded = true;
            cap.settings = Some(s.clone());
        }
    }
    app.add_systems(Update, sys);
    app.update();
    app.update();

    let cap = app.world().resource::<CaptureSettings>();
    assert!(cap.loaded, "resource should load from overridden store");
    let settings = cap.settings.as_ref().expect("captured settings");
    assert_eq!(settings.map_name, "alt");
    assert_eq!(settings.difficulty, 0.7);
}

// GIVEN an empty store
// WHEN a system reads GameSettings through Option<PersistentRes> on two frames
// THEN nothing loads and the resource stays absent
#[db_matrix_test]
fn optional_res_none_when_absent() {
    let (db, _container) = setup();

    let mut app = setup_test_app(db.clone(), None);
    app.insert_resource(CaptureSettings::default());

    fn sys(res: Option<PersistentRes<GameSettings>>, mut cap: ResMut<CaptureSettings>) {
        cap.loaded = false;
        if let Some(mut res) = res {
            if let Some(s) = res.get() {
                cap.loaded = true;
                cap.settings = Some(s.clone());
            }
        }
    }
    app.add_systems(Update, sys);
    app.update();
    app.update();

    let cap = app.world().resource::<CaptureSettings>();
    assert!(
        !cap.loaded,
        "resource should remain absent when store empty"
    );
    assert!(cap.settings.is_none());
}

// GIVEN an empty store
// WHEN a system tries to mutate GameSettings through Option<PersistentResMut> on two frames
// THEN no mutation happens and the resource stays absent
#[db_matrix_test]
fn optional_resmut_none_when_absent() {
    let (db, _container) = setup();

    let mut app = setup_test_app(db.clone(), None);
    app.insert_resource(DirtyFlag::default());

    fn sys(res: Option<PersistentResMut<GameSettings>>, mut dirty: ResMut<DirtyFlag>) {
        if let Some(mut res) = res {
            if let Some(mut r) = res.get_mut() {
                r.map_name = "changed".into();
                dirty.dirty = true;
            }
        }
    }
    app.add_systems(Update, sys);
    app.update();
    app.update();

    let dirty = app.world().resource::<DirtyFlag>();
    assert!(!dirty.dirty, "no mutation when resource absent");
    assert!(app.world().get_resource::<GameSettings>().is_none());
}

// GIVEN GameSettings committed to the store
// WHEN an app loads it through PersistentResMut, mutates it and commits
// THEN a fresh app loads the mutated value (change detection marked it dirty)
#[db_matrix_test]
fn commit_persists_mutation() {
    let (db, _container) = setup();
    seed_settings(
        &db,
        TEST_STORE,
        GameSettings {
            difficulty: 0.5,
            map_name: "before".into(),
        },
    );

    let mut app = setup_test_app(db.clone(), None);
    fn mutate(mut res: PersistentResMut<GameSettings>) {
        if let Some(mut r) = res.get_mut() {
            r.map_name = "after".into();
            r.difficulty = 0.9;
        }
    }
    app.add_systems(Update, mutate);
    app.update();
    app.update();
    commit_sync(&mut app, db.clone(), TEST_STORE).expect("commit failed");

    let mut verify = setup_test_app(db.clone(), None);
    verify.insert_resource(CaptureSettings::default());
    verify.add_systems(Update, capture);
    verify.update();
    verify.update();

    let cap = verify.world().resource::<CaptureSettings>();
    assert!(cap.loaded, "resource should reload after mutation");
    let settings = cap.settings.as_ref().expect("captured settings");
    assert_eq!(settings.map_name, "after");
    assert_eq!(settings.difficulty, 0.9);
}

// GIVEN GameSettings committed once (version 1)
// WHEN PersistentRes requests it and the load applies
// THEN the session records version 1 for the resource
#[db_matrix_test]
fn sets_version_on_load() {
    let (db, _container) = setup();
    seed_settings(
        &db,
        TEST_STORE,
        GameSettings {
            difficulty: 0.11,
            map_name: "v1".into(),
        },
    );

    let mut app = setup_test_app(db.clone(), None);
    fn sys(mut res: PersistentRes<GameSettings>) {
        let _ = res.get();
    }
    app.add_systems(Update, sys);
    app.update();

    let session = app.world().resource::<PersistenceSession>();
    let version = session.resource_version::<GameSettings>();
    assert_eq!(version, Some(1));
}

// GIVEN an app holding a locally mutated GameSettings
// AND another app committing a new value to the store
// WHEN the first app reads through PersistentRes with force_refresh
// THEN the stored value overwrites the local one
#[db_matrix_test]
fn force_refresh_overwrites_local_state() {
    let (db, _container) = setup();
    seed_settings(
        &db,
        TEST_STORE,
        GameSettings {
            difficulty: 0.2,
            map_name: "initial".into(),
        },
    );

    let mut app = setup_test_app(db.clone(), None);
    app.insert_resource(StepState::default());
    app.insert_resource(CaptureSettings::default());

    fn mutate_local(mut res: PersistentResMut<GameSettings>, mut step: ResMut<StepState>) {
        if step.step == 0 {
            if let Some(mut r) = res.get_mut() {
                r.map_name = "local".into();
                step.step = 1;
            }
        }
    }
    app.add_systems(Update, mutate_local);
    app.update();
    app.update();
    assert_eq!(app.world().resource::<GameSettings>().map_name, "local");

    // Commit a new value to the store from a second app
    let mut updater = setup_test_app(db.clone(), None);
    fn load_and_mutate(mut res: PersistentResMut<GameSettings>) {
        if let Some(mut r) = res.get_mut() {
            if r.map_name != "remote" {
                r.map_name = "remote".into();
                r.difficulty = 0.8;
            }
        }
    }
    updater.add_systems(Update, load_and_mutate);
    updater.update();
    updater.update();
    commit_sync(&mut updater, db.clone(), TEST_STORE).expect("update commit failed");

    // Force refresh, then read once the reload has applied
    fn force_refresh(res: PersistentRes<GameSettings>) {
        let _ = res.force_refresh().get();
    }
    app.add_systems(Update, (force_refresh, capture).chain());
    app.update();

    let cap = app.world().resource::<CaptureSettings>();
    assert!(cap.loaded, "resource should load on force refresh");
    let settings = cap.settings.as_ref().expect("captured settings");
    assert_eq!(settings.map_name, "remote");
    assert_eq!(settings.difficulty, 0.8);
}

// GIVEN an entity and GameSettings committed to the store
// WHEN one system requests both through PersistentRes and PersistentQuery on two frames
// THEN the second frame sees the resource and the loaded entity
#[db_matrix_test]
fn mixed_query_and_resource_load() {
    let (db, _container) = setup();

    let mut seed = setup_test_app(db.clone(), None);
    seed.world_mut().spawn(Health { value: 7 });
    seed.insert_resource(GameSettings {
        difficulty: 0.33,
        map_name: "mixed".into(),
    });
    seed.update();
    commit_sync(&mut seed, db.clone(), TEST_STORE).expect("seed commit failed");

    #[derive(Resource, Default)]
    struct MixedState {
        loaded_resource: bool,
        loaded_entities: usize,
    }

    let mut app = setup_test_app(db.clone(), None);
    app.insert_resource(MixedState::default());

    fn sys(
        mut res: PersistentRes<GameSettings>,
        mut pq: PersistentQuery<&Health>,
        mut state: ResMut<MixedState>,
    ) {
        if res.get().is_some() {
            state.loaded_resource = true;
        }
        pq.load();
        state.loaded_entities = pq.iter().count();
    }
    app.add_systems(Update, sys);
    app.update();
    app.update();

    let state = app.world().resource::<MixedState>();
    assert!(
        state.loaded_resource,
        "resource should load alongside query"
    );
    assert_eq!(state.loaded_entities, 1);
}
