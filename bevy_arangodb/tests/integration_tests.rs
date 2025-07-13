use bevy::app::App;
use bevy_arangodb::{commit, DatabaseConnection, Guid, ArangoPlugin, ArangoDbConnection};
use std::process::Command;
use std::sync::Arc;
use testcontainers::{core::WaitFor, runners::AsyncRunner, GenericImage, ImageExt, ContainerAsync};
use tokio::sync::{Mutex, OnceCell};
use serde_json;

// This will hold the container instance to keep it alive for the duration of the tests.
static DOCKER_CONTAINER: OnceCell<ContainerAsync<GenericImage>> = OnceCell::const_new();
// This will hold the database connection Arc, which is cheap to clone.
static DB_CONNECTION: OnceCell<Arc<dyn DatabaseConnection>> = OnceCell::const_new();

// A mutex to ensure that tests run serially, not in parallel.
static DB_LOCK: Mutex<()> = Mutex::const_new(());

/// This function will be executed when the test program exits.
#[ctor::dtor]
fn teardown() {
    if let Some(container) = DOCKER_CONTAINER.get() {
        let id = container.id();
        let status = Command::new("docker")
            .arg("rm")
            .arg("--force")
            .arg(id)
            .status()
            .expect("Failed to execute docker rm command");

        if !status.success() {
            eprintln!(
                "Error removing container '{}': command exited with status {}",
                id, status
            );
        }
    }
}

/// Lazily initializes and returns a shared DB connection.
/// On first call, starts the Docker container and connects to the database.
async fn get_db_connection() -> Arc<dyn DatabaseConnection> {
    if let Some(db) = DB_CONNECTION.get() {
        return db.clone();
    }

    let container = GenericImage::new("arangodb", "3.12.5")
        .with_wait_for(WaitFor::message_on_stdout("is ready for business"))
        .with_env_var("ARANGO_ROOT_PASSWORD", "password")
        .start()
        .await
        .expect("Failed to start ArangoDB container");

    let host_port = container.get_host_port_ipv4(8529).await.unwrap();
    let url = format!("http://127.0.0.1:{}", host_port);

    let db = Arc::new(
        ArangoDbConnection::connect(&url, "root", "password", "_system")
            .await
            .expect("Failed to connect to ArangoDB container"),
    );

    // Store the container to keep it alive.
    DOCKER_CONTAINER
        .set(container)
        .expect("Failed to set container");

    DB_CONNECTION
        .set(db.clone())
        .unwrap();
    db
}

/// Gets the shared test context and clears the database for a new test.
async fn setup() -> Arc<dyn DatabaseConnection> {
    let db = get_db_connection().await;
    db.clear_entities().await.unwrap();
    db.clear_resources().await.unwrap();
    db
}

#[bevy_arangodb::persist(component)]
struct Health {
    value: i32,
}

#[bevy_arangodb::persist(component)]
struct Position {
    x: f32,
    y: f32,
}

#[bevy_arangodb::persist(component)]
struct Creature {
    is_screaming: bool,
}

#[bevy_arangodb::persist(resource)]
struct GameSettings {
    difficulty: f32,
    map_name: String,
}

#[tokio::test]
async fn test_entity_commit_and_fetch() {
    let _guard = DB_LOCK.lock().await;
    let db = setup().await;

    let mut app = App::new();
    app.add_plugins(ArangoPlugin::new(db.clone()));

    let health_val = Health { value: 100 };
    let pos_val = Position { x: 1.0, y: 2.0 };

    let entity_id = app.world.spawn((health_val, pos_val)).id();
    
    app.update(); // Run the schedule to trigger change detection

    commit(&mut app).await.unwrap();

    // 4. Verify the results
    // The entity should now have a Guid component assigned by the library.
    let guid = app.world.get::<Guid>(entity_id)
        .expect("Entity should have a Guid after commit");

    assert!(!guid.id().is_empty(), "Guid should not be empty");

    // To be absolutely sure, let's fetch the Health component directly from the DB
    // using the new Guid and verify its content.
    let health_json = db
        .fetch_component(guid.id(), Health::name())
        .await
        .expect("Failed to fetch component from DB")
        .expect("Component should exist in DB");

    let fetched_health: Health =
        serde_json::from_value(health_json).expect("Failed to deserialize Health component");

    assert_eq!(
        fetched_health.value, 100,
        "The health value in the database is incorrect"
    );
}

// #[tokio::test]
// async fn test_resource_commit_and_fetch() {
//     let _guard = DB_LOCK.lock().await;
//     let db = setup().await;

//     // 2. Create a session, add a resource, and commit it
//     let mut app = App::new();
//     app.add_plugins(ArangoPlugin::new(db.clone()));

//     let settings = GameSettings {
//         difficulty: 0.8,
//         map_name: "level_1".into(),
//     };
//     app.insert_resource(settings);
    
//     app.world.resource_mut::<ArangoSession>().mark_resource_dirty::<GameSettings>();
//     commit(&mut app).await.expect("Commit failed");

//     // 3. Verify the resource was saved correctly by fetching it directly
//     let resource_name = GameSettings::name();
//     let resource_json = db
//         .fetch_resource(resource_name)
//         .await
//         .expect("Failed to fetch resource from DB")
//         .expect("Resource should exist in DB");

//     let fetched_settings: GameSettings =
//         serde_json::from_value(resource_json).expect("Failed to deserialize resource");

//     assert_eq!(fetched_settings.difficulty, 0.8);
//     assert_eq!(fetched_settings.map_name, "level_1");
// }

// #[tokio::test]
// async fn test_entity_load_into_new_session() {
//     let _guard = DB_LOCK.lock().await;
//     let db = setup().await;

//     let mut app1 = App::new();
//     app1.add_plugins(ArangoPlugin::new(db.clone()));

//     // 2. Spawn two entities, one with Health+Position, one with only Health.
//     let _entity_to_load = app1
//         .world
//         .spawn((
//             Health { value: 150 },
//             Position { x: 10.0, y: 20.0 },
//         ))
//         .id();
//     let _entity_to_ignore = app1.world.spawn(Health { value: 99 }).id();
    
//     app1.update();

//     commit(&mut app1).await.expect("Initial commit failed");


//     // 3. Create a new, clean session to load the data into.
//     let mut app2 = App::new();
//     app2.add_plugins(ArangoPlugin::new(db.clone()));
    
//     // 4. Query for entities that have BOTH Health and Position, and Health > 100.
//     let query = ArangoQuery::new(db.clone())
//         .with::<Health>()
//         .with::<Position>()
//         .filter(Health::value().gt(100));
//     let loaded_entities = query.fetch_into_app(&mut app2).await;

//     // 5. Verify that only the correct entity was loaded and its data is correct.
//     assert_eq!(
//         loaded_entities.len(),
//         1,
//         "Should only load one entity with both components"
//     );
//     let loaded_entity = loaded_entities[0];

//     let health = app2.world.get::<Health>(loaded_entity).unwrap();
//     assert_eq!(health.value, 150);

//     let position = app2.world.get::<Position>(loaded_entity).unwrap();
//     assert_eq!(position.x, 10.0);
//     assert_eq!(position.y, 20.0);
// }

// #[tokio::test]
// async fn test_complex_query_with_dsl() {
//     let _guard = DB_LOCK.lock().await;
//     let db = setup().await;

//     let mut app = App::new();
//     app.add_plugins(ArangoPlugin::new(db.clone()));

//     // 2. Spawn a few entities to represent our "scream" scenario.
//     // A screaming creature in the target zone
//     app.world.spawn((
//         Creature { is_screaming: true },
//         Position { x: 50.0, y: 50.0 },
//     ));
//     // A quiet creature in the target zone
//     app.world.spawn((
//         Creature { is_screaming: false },
//         Position { x: 75.0, y: 75.0 },
//     ));
//     // A screaming creature outside the target zone
//     app.world.spawn((
//         Creature { is_screaming: true },
//         Position { x: 150.0, y: 150.0 },
//     ));

//     app.update();
//     commit(&mut app).await.expect("Commit failed");

//     // 3. Build a complex query with the DSL.
//     // We want to find all creatures that are screaming AND are inside a specific area.
//     let query = ArangoQuery::new(db.clone())
//         .with::<Creature>()
//         .with::<Position>()
//         .filter(
//             Creature::is_screaming().eq(true)
//             .and(Position::x().gt(0.0))
//             .and(Position::x().lt(100.0))
//             .and(Position::y().gt(0.0))
//             .and(Position::y().lt(100.0))
//         );

//     // 4. Fetch the results into a new app instance.
//     let mut app2 = App::new();
//     app2.add_plugins(ArangoPlugin::new(db.clone()));
//     let loaded_entities = query.fetch_into_app(&mut app2).await;

//     // 5. Verify that only the single matching entity was loaded.
//     assert_eq!(loaded_entities.len(), 1, "Should only load one screaming creature in the zone");
//     let loaded_entity = loaded_entities[0];

//     let creature = app2.world.get::<Creature>(loaded_entity).unwrap();
//     assert!(creature.is_screaming);

//     let position = app2.world.get::<Position>(loaded_entity).unwrap();
//     assert_eq!(position.x, 50.0);
// }

// #[tokio::test]
// async fn test_entity_delete() {
//     let _guard = DB_LOCK.lock().await;
//     let db = setup().await;

//     let mut app = App::new();
//     app.add_plugins(ArangoPlugin::new(db.clone()));

//     // 2. Spawn and commit an entity.
//     let entity_id = app.world.spawn(Health { value: 100 }).id();
    
//     app.update();

//     commit(&mut app).await.expect("Commit failed");

//     // 3. Verify it exists in the database.
//     let guid = app.world.get::<Guid>(entity_id).unwrap().id().to_string();
//     let component = db
//         .fetch_component(&guid, Health::name())
//         .await
//         .expect("Fetch should not fail")
//         .expect("Component should exist after first commit");
//     assert_eq!(component.get("value").unwrap().as_i64().unwrap(), 100);

//     // 4. Despawn the entity and commit again.
//     app.world.entity_mut(entity_id).despawn();
//     app.update(); // This runs the despawn command and our auto-despawn-tracking system
//     commit(&mut app).await.expect("Second commit failed");

//     // 5. Verify it's gone from the database.
//     let component_after_delete = db
//         .fetch_component(&guid, Health::name())
//         .await
//         .expect("Fetch should not fail");

//     assert!(
//         component_after_delete.is_none(),
//         "Component should be gone after delete commit"
//     );
// }
