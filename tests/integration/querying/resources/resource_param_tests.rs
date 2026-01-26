use crate::common::*;
use bevy::prelude::*;
use bevy_persistence_database::{
    PersistenceSession, PersistentQuery, PersistentRes, PersistentResMut, commit_sync,
};
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

#[db_matrix_test]
fn loads_resource_on_first_access() {
    let (db, _container) = setup();

    // Seed: commit GameSettings to the store
    let mut seed = setup_test_app(db.clone(), None);
    seed.insert_resource(GameSettings {
        difficulty: 0.42,
        map_name: "mystic".into(),
    });
    seed.update();
    commit_sync(&mut seed, db.clone(), TEST_STORE).expect("seed commit failed");

    // App under test
    let mut app = setup_test_app(db.clone(), None);
    app.insert_resource(CaptureSettings::default());

    fn sys(mut res: PersistentRes<GameSettings>, mut cap: ResMut<CaptureSettings>) {
        if let Some(s) = res.get() {
            cap.loaded = true;
            cap.settings = Some(s.clone());
        }
    }
    app.add_systems(Update, sys);
    app.update();

    let cap = app.world().resource::<CaptureSettings>();
    assert!(cap.loaded, "resource should load");
    let settings = cap.settings.as_ref().expect("captured settings");
    assert_eq!(settings.difficulty, 0.42);
    assert_eq!(settings.map_name, "mystic");
}

#[db_matrix_test]
fn respects_existing_resource() {
    let (db, _container) = setup();

    // Persist a different value in the store.
    let mut seed = setup_test_app(db.clone(), None);
    seed.insert_resource(GameSettings {
        difficulty: 0.1,
        map_name: "persisted".into(),
    });
    seed.update();
    commit_sync(&mut seed, db.clone(), TEST_STORE).expect("seed commit failed");

    // App under test already has a local resource that should not be overwritten.
    let mut app = setup_test_app(db.clone(), None);
    app.insert_resource(CaptureSettings::default());
    app.insert_resource(GameSettings {
        difficulty: 0.9,
        map_name: "local".into(),
    });

    fn sys(mut res: PersistentRes<GameSettings>, mut cap: ResMut<CaptureSettings>) {
        if let Some(s) = res.get() {
            cap.loaded = true;
            cap.settings = Some(s.clone());
        }
    }
    app.add_systems(Update, sys);
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

#[db_matrix_test]
fn second_read_uses_local_after_mutation() {
    let (db, _container) = setup();

    // Persist initial value in the store
    let mut seed = setup_test_app(db.clone(), None);
    seed.insert_resource(GameSettings {
        difficulty: 0.1,
        map_name: "persisted".into(),
    });
    seed.update();
    commit_sync(&mut seed, db.clone(), TEST_STORE).expect("seed commit failed");

    // App under test
    let mut app = setup_test_app(db.clone(), None);
    app.insert_resource(ReadCapture::default());

    fn first_load(mut res: PersistentRes<GameSettings>, mut cap: ResMut<ReadCapture>) {
        if cap.first.is_none() {
            if let Some(gs) = res.get() {
                cap.first = Some(gs.map_name.clone());
            }
        }
    }

    fn mutate_local(mut gs: ResMut<GameSettings>) {
        gs.map_name = "local".into();
    }

    fn second_load(mut res: PersistentRes<GameSettings>, mut cap: ResMut<ReadCapture>) {
        if cap.second.is_none() {
            if let Some(gs) = res.get() {
                cap.second = Some(gs.map_name.clone());
            }
        }
    }

    app.add_systems(Update, (first_load, mutate_local, second_load).chain());
    app.update();

    let cap = app.world().resource::<ReadCapture>();
    assert_eq!(cap.first.as_deref(), Some("persisted"));
    assert_eq!(cap.second.as_deref(), Some("local"));
}

#[db_matrix_test]
fn store_override_is_used() {
    let (db, _container) = setup();
    let alt_store = "alt_store";

    // Seed alt store with a resource
    let mut seed = setup_test_app(db.clone(), None);
    seed.insert_resource(GameSettings {
        difficulty: 0.7,
        map_name: "alt".into(),
    });
    seed.update();
    commit_sync(&mut seed, db.clone(), alt_store).expect("seed commit failed");

    // App under test uses default store unless overridden
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

    let cap = app.world().resource::<CaptureSettings>();
    assert!(cap.loaded, "resource should load from overridden store");
    let settings = cap.settings.as_ref().expect("captured settings");
    assert_eq!(settings.map_name, "alt");
    assert_eq!(settings.difficulty, 0.7);
}

#[db_matrix_test]
fn optional_res_none_when_absent() {
    let (db, _container) = setup();

    let mut app = setup_test_app(db.clone(), None);
    app.insert_resource(CaptureSettings::default());

    fn sys(res: Option<PersistentRes<GameSettings>>, mut cap: ResMut<CaptureSettings>) {
        if let Some(mut res) = res {
            if let Some(s) = res.get() {
                cap.loaded = true;
                cap.settings = Some(s.clone());
            } else {
                cap.loaded = false;
            }
        } else {
            cap.loaded = false;
        }
    }
    app.add_systems(Update, sys);
    app.update();

    let cap = app.world().resource::<CaptureSettings>();
    assert!(
        !cap.loaded,
        "resource should remain absent when store empty"
    );
    assert!(cap.settings.is_none());
}

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

    let dirty = app.world().resource::<DirtyFlag>();
    assert!(!dirty.dirty, "no mutation when resource absent");
    assert!(app.world().get_resource::<GameSettings>().is_none());
}

#[db_matrix_test]
fn commit_persists_mutation() {
    let (db, _container) = setup();

    // Seed
    let mut seed = setup_test_app(db.clone(), None);
    seed.insert_resource(GameSettings {
        difficulty: 0.5,
        map_name: "before".into(),
    });
    seed.update();
    commit_sync(&mut seed, db.clone(), TEST_STORE).expect("seed commit failed");

    // Mutate via PersistentResMut and commit
    let mut app = setup_test_app(db.clone(), None);

    fn mutate(mut res: PersistentResMut<GameSettings>) {
        if let Some(mut r) = res.get_mut() {
            r.map_name = "after".into();
            r.difficulty = 0.9;
        }
    }
    app.add_systems(Update, mutate);
    app.update();
    commit_sync(&mut app, db.clone(), TEST_STORE).expect("commit failed");

    // Load in a fresh app to verify persistence
    let mut verify = setup_test_app(db.clone(), None);
    verify.insert_resource(CaptureSettings::default());

    fn load(mut res: PersistentRes<GameSettings>, mut cap: ResMut<CaptureSettings>) {
        if let Some(s) = res.get() {
            cap.loaded = true;
            cap.settings = Some(s.clone());
        }
    }
    verify.add_systems(Update, load);
    verify.update();

    let cap = verify.world().resource::<CaptureSettings>();
    assert!(cap.loaded, "resource should reload after mutation");
    let settings = cap.settings.as_ref().expect("captured settings");
    assert_eq!(settings.map_name, "after");
    assert_eq!(settings.difficulty, 0.9);
}

#[db_matrix_test]
fn sets_version_on_load() {
    let (db, _container) = setup();

    // Seed
    let mut seed = setup_test_app(db.clone(), None);
    seed.insert_resource(GameSettings {
        difficulty: 0.11,
        map_name: "v1".into(),
    });
    seed.update();
    commit_sync(&mut seed, db.clone(), TEST_STORE).expect("seed commit failed");

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

#[db_matrix_test]
fn force_refresh_overwrites_local_state() {
    let (db, _container) = setup();

    // Seed initial value in store
    let mut seed = setup_test_app(db.clone(), None);
    seed.insert_resource(GameSettings {
        difficulty: 0.2,
        map_name: "initial".into(),
    });
    seed.update();
    commit_sync(&mut seed, db.clone(), TEST_STORE).expect("seed commit failed");

    // App under test: mutate local copy without committing
    let mut app = setup_test_app(db.clone(), None);
    app.insert_resource(StepState::default());

    fn mutate_local(mut res: PersistentResMut<GameSettings>, mut step: ResMut<StepState>) {
        if step.step == 0 {
            if let Some(mut r) = res.get_mut() {
                r.map_name = "local".into();
            }
            step.step = 1;
        }
    }

    fn force_refresh(
        res: PersistentRes<GameSettings>,
        mut step: ResMut<StepState>,
        mut cap: ResMut<CaptureSettings>,
    ) {
        if step.step == 1 {
            let mut res = res.force_refresh();
            if let Some(s) = res.get() {
                cap.loaded = true;
                cap.settings = Some(s.clone());
            }
            step.step = 2;
        }
    }

    app.insert_resource(CaptureSettings::default());
    app.add_systems(Update, (mutate_local, force_refresh));

    // First update: mutate local copy
    app.update();

    // Update store externally to a new value via a second app
    #[derive(Resource, Default)]
    struct UpdaterState {
        done: bool,
    }

    let mut updater = setup_test_app(db.clone(), None);
    updater.insert_resource(UpdaterState::default());

    fn load_and_mutate(mut res: PersistentResMut<GameSettings>, mut state: ResMut<UpdaterState>) {
        if !state.done {
            if let Some(mut r) = res.get_mut() {
                r.map_name = "remote".into();
                r.difficulty = 0.8;
            }
            state.done = true;
        }
    }

    updater.add_systems(Update, load_and_mutate);
    updater.update();
    commit_sync(&mut updater, db.clone(), TEST_STORE).expect("update commit failed");

    // Second update: force refresh should pull remote value and overwrite local
    app.update();

    let cap = app.world().resource::<CaptureSettings>();
    assert!(cap.loaded, "resource should load on force refresh");
    let settings = cap.settings.as_ref().expect("captured settings");
    assert_eq!(settings.map_name, "remote");
    assert_eq!(settings.difficulty, 0.8);
}

#[db_matrix_test]
fn mixed_query_and_resource_load() {
    let (db, _container) = setup();

    // Seed entity + resource
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
        if let Some(_s) = res.get() {
            state.loaded_resource = true;
        }
        pq.ensure_loaded();
        state.loaded_entities = pq.iter().count();
    }
    app.add_systems(Update, sys);
    app.update();

    let state = app.world().resource::<MixedState>();
    assert!(
        state.loaded_resource,
        "resource should load alongside query"
    );
    assert_eq!(state.loaded_entities, 1);
}
