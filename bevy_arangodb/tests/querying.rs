use bevy::app::App;
use bevy_arangodb::{commit, ArangoPlugin, ArangoQuery, Guid, Persist, TransactionOperation};

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
async fn test_load_resources_alongside_entities() {
    let _guard = DB_LOCK.lock().await;
    let db = setup().await;

    // GIVEN a database containing a committed GameSettings resource
    let mut app1 = App::new();
    app1.add_plugins(ArangoPlugin::new(db.clone()));
    let settings = GameSettings {
        difficulty: 0.42,
        map_name: "mystic".into(),
    };
    app1.insert_resource(settings.clone());
    app1.update();
    commit(&mut app1).await.expect("initial commit failed");

    // WHEN any query is fetched into a new app
    let mut app2 = App::new();
    app2.add_plugins(ArangoPlugin::new(db.clone()));
    let _ = ArangoQuery::new(db.clone()).fetch_into(&mut app2).await;

    // THEN the GameSettings resource is loaded
    let loaded: &GameSettings = app2.world.resource();
    assert_eq!(loaded.difficulty, 0.42);
    assert_eq!(loaded.map_name, "mystic");
}

#[tokio::test]
async fn test_load_into_world_with_existing_entities() {
    let _guard = DB_LOCK.lock().await;
    let db = setup().await;

    // GIVEN entity A in DB
    let mut app1 = App::new();
    app1.add_plugins(ArangoPlugin::new(db.clone()));
    let a = app1.world.spawn(Health { value: 100 }).id();
    app1.update(); commit(&mut app1).await.unwrap();
    let key_a = app1.world.get::<Guid>(a).unwrap().id().to_string();

    // AND a fresh app2 with entity B already committed
    let mut app2 = App::new();
    app2.add_plugins(ArangoPlugin::new(db.clone()));
    let _b = app2.world.spawn(Position { x:1.0, y:1.0 }).id();
    app2.update(); commit(&mut app2).await.unwrap();

    // WHEN we query for A
    let loaded = ArangoQuery::new(db.clone())
        .filter(Health::value().eq(100))
        .fetch_into(&mut app2)
        .await;

    // THEN both A and B exist, and A has correct components
    assert_eq!(loaded.len(), 1);
    let e = loaded[0];
    assert_eq!(app2.world.get::<Guid>(e).unwrap().id(), key_a);
    assert_eq!(app2.world.get::<Health>(e).unwrap().value, 100);
    assert_eq!(app2.world.query::<&Guid>().iter(&app2.world).count(), 2);
}

#[tokio::test]
async fn test_dsl_filter_by_component_presence() {
    let _guard = DB_LOCK.lock().await;
    let db = setup().await;

    // GIVEN some entities with/without Creature
    let mut app1 = App::new();
    app1.add_plugins(ArangoPlugin::new(db.clone()));
    app1.world.spawn(Creature { is_screaming: false });
    app1.world.spawn(Health { value: 100 });
    app1.update(); commit(&mut app1).await.unwrap();

    // WHEN we query .with::<Creature>()
    let mut app2 = App::new();
    app2.add_plugins(ArangoPlugin::new(db.clone()));
    let loaded = ArangoQuery::new(db.clone())
        .with::<Creature>()
        .fetch_into(&mut app2)
        .await;

    // THEN only those with Creature load
    assert_eq!(loaded.len(), 1);
    assert!(app2.world.get::<Creature>(loaded[0]).is_some());
}

#[tokio::test]
async fn test_dsl_equality_operator() {
    let _guard = DB_LOCK.lock().await;
    let db = setup().await;

    // GIVEN Health, Creature, PlayerName entities
    let mut app1 = App::new();
    app1.add_plugins(ArangoPlugin::new(db.clone()));
    app1.world.spawn(Health { value: 100 });
    app1.world.spawn(Health { value: 99 });
    app1.world.spawn(Creature { is_screaming: true });
    app1.world.spawn(Creature { is_screaming: false });
    app1.world.spawn(PlayerName { name: "Alice".into() });
    app1.world.spawn(PlayerName { name: "Bob".into() });
    app1.update(); commit(&mut app1).await.unwrap();

    let mut app2 = App::new();
    app2.add_plugins(ArangoPlugin::new(db.clone()));

    // WHEN filtering Health == 100
    let h = ArangoQuery::new(db.clone())
        .filter(Health::value().eq(100))
        .fetch_into(&mut app2)
        .await;
    assert_eq!(h.len(), 1);

    // WHEN filtering Creature.is_screaming == true
    let c = ArangoQuery::new(db.clone())
        .filter(Creature::is_screaming().eq(true))
        .fetch_into(&mut app2)
        .await;
    assert_eq!(c.len(), 1);

    // WHEN filtering PlayerName == "Alice"
    let p = ArangoQuery::new(db.clone())
        .filter(PlayerName::name().eq("Alice"))
        .fetch_into(&mut app2)
        .await;
    assert_eq!(p.len(), 1);
}

#[tokio::test]
async fn test_dsl_relational_operators() {
    let _guard = DB_LOCK.lock().await;
    let db = setup().await;

    // GIVEN Health 99,100,101
    let mut app1 = App::new();
    app1.add_plugins(ArangoPlugin::new(db.clone()));
    app1.world.spawn(Health { value: 99 });
    app1.world.spawn(Health { value: 100 });
    app1.world.spawn(Health { value: 101 });
    app1.update(); commit(&mut app1).await.unwrap();

    let mut app2 = App::new();
    app2.add_plugins(ArangoPlugin::new(db.clone()));

    assert_eq!(
        ArangoQuery::new(db.clone()).filter(Health::value().gt(100))
            .fetch_into(&mut app2).await.len(), 1
    );
    assert_eq!(
        ArangoQuery::new(db.clone()).filter(Health::value().gte(100))
            .fetch_into(&mut app2).await.len(), 2
    );
    assert_eq!(
        ArangoQuery::new(db.clone()).filter(Health::value().lt(100))
            .fetch_into(&mut app2).await.len(), 1
    );
    assert_eq!(
        ArangoQuery::new(db.clone()).filter(Health::value().lte(100))
            .fetch_into(&mut app2).await.len(), 2
    );
}

#[tokio::test]
async fn test_dsl_logical_combinations() {
    let _guard = DB_LOCK.lock().await;
    let db = setup().await;

    // GIVEN entities for AND/OR
    let mut app1 = App::new();
    app1.add_plugins(ArangoPlugin::new(db.clone()));
    app1.world.spawn((Health { value:150 }, Position { x:50.0, y:0.0 }));
    app1.world.spawn((Health { value:150 }, Position { x:150.0, y:0.0 }));
    app1.world.spawn((Health { value:50  }, Position { x:50.0, y:0.0 }));
    app1.world.spawn((Health { value:50  }, Position { x:150.0, y:0.0 }));
    app1.update(); commit(&mut app1).await.unwrap();

    let mut app2 = App::new();
    app2.add_plugins(ArangoPlugin::new(db.clone()));

    // AND case
    let and_loaded = ArangoQuery::new(db.clone())
        .filter(Health::value().gt(100).and(Position::x().lt(100.0)))
        .fetch_into(&mut app2).await;
    assert_eq!(and_loaded.len(), 1);

    // OR case
    let or_loaded = ArangoQuery::new(db.clone())
        .filter(Health::value().gt(100).or(Position::x().lt(100.0)))
        .fetch_into(&mut app2).await;
    assert_eq!(or_loaded.len(), 3);
}

#[tokio::test]
#[should_panic(expected = "component deserialization failed")]
async fn test_load_with_schema_mismatch() {
    let _guard = DB_LOCK.lock().await;
    let db = setup().await;

    // GIVEN a bad Health document
    let bad_health_doc = serde_json::json!({
        Health::name(): { "value": "a string, not a number" }
    });
    let _key = db.execute_transaction(vec![
        TransactionOperation::CreateDocument(bad_health_doc)
    ])
    .await
    .unwrap()
    .remove(0);

    // WHEN loading with .with::<Health>() – this should panic inside fetch_into
    let mut app2 = App::new();
    app2.add_plugins(ArangoPlugin::new(db.clone()));
    ArangoQuery::new(db.clone())
        .with::<Health>()
        .fetch_into(&mut app2)
        .await;
}