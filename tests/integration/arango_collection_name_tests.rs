use bevy::prelude::*;
use bevy_persistence_database::bevy::components::Guid;
use bevy_persistence_database::core::persist::Persist;
use bevy_persistence_database::core::session::commit_sync;

use crate::common::components::Health;
use crate::common::{TestBackend, run_async, setup_backend, setup_test_app};

#[test]
#[cfg(feature = "arango")]
fn arango_accepts_hyphenated_collection_names() {
    let (db, _guard) = setup_backend(TestBackend::Arango);
    let mut app = setup_test_app(db.clone(), None);

    let store = "world-test-with-dashes";
    let entity = app.world_mut().spawn(Health { value: 7 }).id();

    app.update();
    commit_sync(&mut app, db.clone(), store).expect("commit should succeed for hyphenated store");

    let guid = app
        .world()
        .get::<Guid>(entity)
        .expect("guid should be assigned")
        .id()
        .to_string();

    let raw = run_async(db.fetch_component(store, &guid, Health::name()))
        .expect("fetch should succeed")
        .expect("component should exist");
    let fetched: Health = serde_json::from_value(raw).expect("component should deserialize");

    assert_eq!(fetched.value, 7);
}
