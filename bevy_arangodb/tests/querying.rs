use bevy::app::App;
use bevy_arangodb::{commit, ArangoPlugin, ArangoQuery};

mod common;
use common::*;

#[tokio::test]
async fn test_load_specific_entities_into_new_session() {
    let _guard = DB_LOCK.lock().await;
    let db = setup().await;

    let mut app1 = App::new();
    app1.add_plugins(ArangoPlugin::new(db.clone()));

    // 1. Spawn two entities, one with Health+Position, one with only Health.
    let _entity_to_load = app1
        .world
        .spawn((
            Health { value: 150 },
            Position { x: 10.0, y: 20.0 },
        ))
        .id();
    let _entity_to_ignore = app1.world.spawn(Health { value: 99 }).id();
    
    app1.update();

    commit(&mut app1).await.expect("Initial commit failed");


    // 2. Create a new, clean session to load the data into.
    let mut app2 = App::new();
    app2.add_plugins(ArangoPlugin::new(db.clone()));
    
    // 3. Query for entities that have BOTH Health and Position, and Health > 100.
    let query = ArangoQuery::new(db.clone())
        .with::<Health>()
        .with::<Position>()
        .filter(Health::value().gt(100));
    let loaded_entities = query.fetch_into(&mut app2).await;

    // 4. Verify that only the correct entity was loaded and its data is correct.
    assert_eq!(
        loaded_entities.len(),
        1,
        "Should only load one entity with both components"
    );
    let loaded_entity = loaded_entities[0];

    let health = app2.world.get::<Health>(loaded_entity).unwrap();
    assert_eq!(health.value, 150);

    let position = app2.world.get::<Position>(loaded_entity).unwrap();
    assert_eq!(position.x, 10.0);
    assert_eq!(position.y, 20.0);
}

#[tokio::test]
async fn test_complex_query_with_dsl() {
    let _guard = DB_LOCK.lock().await;
    let db = setup().await;

    let mut app = App::new();
    app.add_plugins(ArangoPlugin::new(db.clone()));

    // 1. Spawn a few entities to represent a scenario where entities must move away from a screaming creature.
    // A screaming creature in the target zone
    app.world.spawn((
        Creature { is_screaming: true },
        Position { x: 50.0, y: 50.0 },
    ));
    // A quiet creature in the target zone
    app.world.spawn((
        Creature { is_screaming: false },
        Position { x: 75.0, y: 75.0 },
    ));
    // A screaming creature outside the target zone
    app.world.spawn((
        Creature { is_screaming: true },
        Position { x: 150.0, y: 150.0 },
    ));

    app.update();
    commit(&mut app).await.expect("Commit failed");

    // 2. Build a complex query with the DSL.
    // We want to find all creatures that are screaming AND are inside a specific area.
    let query = ArangoQuery::new(db.clone())
        .with::<Creature>()
        .with::<Position>()
        .filter(
            Creature::is_screaming().eq(true)
            .and(Position::x().gt(0.0))
            .and(Position::x().lt(100.0))
            .and(Position::y().gt(0.0))
            .and(Position::y().lt(100.0))
        );

    // 3. Fetch the results into a new app instance.
    let mut app2 = App::new();
    app2.add_plugins(ArangoPlugin::new(db.clone()));
    let loaded_entities = query.fetch_into(&mut app2).await;

    // 4. Verify that only the single matching entity was loaded.
    assert_eq!(loaded_entities.len(), 1, "Should only load one screaming creature in the zone");
    let loaded_entity = loaded_entities[0];

    let creature = app2.world.get::<Creature>(loaded_entity).unwrap();
    assert!(creature.is_screaming);

    let position = app2.world.get::<Position>(loaded_entity).unwrap();
    assert_eq!(position.x, 50.0);
}

#[tokio::test]
async fn test_query_fetches_resources() {
    let _guard = DB_LOCK.lock().await;
    let db = setup().await;

    // 1) Commit a resource in app1
    let mut app1 = App::new();
    app1.add_plugins(ArangoPlugin::new(db.clone()));
    let settings = GameSettings {
        difficulty: 0.42,
        map_name: "mystic".into(),
    };
    app1.insert_resource(settings.clone());
    app1.update();
    commit(&mut app1).await.expect("initial commit failed");

    // 2) Start a fresh session and run a query (no components requested)
    let mut app2 = App::new();
    app2.add_plugins(ArangoPlugin::new(db.clone()));
    // no `.with::<T>()` calls → fetch_ids returns all entities, but we only care about resources
    let query = ArangoQuery::new(db.clone());
    let _ = query.fetch_into(&mut app2).await;

    // 3) Verify the resource was reloaded into app2
    assert!(app2.world.get_resource::<GameSettings>().is_some(), "GameSettings resource should have been loaded");
    let loaded: &GameSettings = app2.world.resource::<GameSettings>();
    assert_eq!(loaded.difficulty, 0.42);
    assert_eq!(loaded.map_name, "mystic");
}