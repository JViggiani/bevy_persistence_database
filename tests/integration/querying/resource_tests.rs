use bevy::prelude::App;
use bevy_persistence_database_core::{
    commit_sync, Guid, persistence_plugin::PersistencePlugins, PersistentQuery,
};
use crate::common::*;
use bevy_persistence_database_derive::db_matrix_test;

#[db_matrix_test]
fn test_load_resources_alongside_entities() {
    let (db, _container) = setup();
    let mut app1 = App::new();
    app1.add_plugins(PersistencePlugins(db.clone()));

    // GIVEN a committed GameSettings resource
    let settings = GameSettings { difficulty: 0.42, map_name: "mystic".into() };
    app1.insert_resource(settings.clone());
    app1.update();
    commit_sync(&mut app1).expect("Initial commit failed");

    // WHEN any query is fetched into a new app
    let mut app2 = App::new();
    app2.add_plugins(PersistencePlugins(db.clone()));
    fn sys(mut pq: PersistentQuery<&Guid>) { let _ = pq.ensure_loaded(); }
    app2.add_systems(bevy::prelude::Update, sys);
    app2.update();

    // THEN the GameSettings resource is loaded
    let loaded: &GameSettings = app2.world().resource();
    assert_eq!(loaded.difficulty, 0.42);
    assert_eq!(loaded.map_name, "mystic");
}
