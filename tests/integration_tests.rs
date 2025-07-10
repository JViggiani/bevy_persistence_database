use bevy::prelude::{Component, Resource};
use bevy_arangodb::{
    ArangoDbConnection, ArangoQuery, ArangoSession, DatabaseConnection, Guid, Persist,
};
use ctor::dtor;
use serde::{Deserialize, Serialize};
use std::process::Command;
use std::sync::Arc;
use testcontainers::{
    core::WaitFor, runners::AsyncRunner, GenericImage, ImageExt, ContainerAsync
};
use tokio::sync::{Mutex, OnceCell};

// This will hold the container instance to keep it alive for the duration of the tests.
static DOCKER_CONTAINER: OnceCell<ContainerAsync<GenericImage>> = OnceCell::const_new();
// This will hold the database connection Arc, which is cheap to clone.
static DB_CONNECTION: OnceCell<Arc<ArangoDbConnection>> = OnceCell::const_new();

// A mutex to ensure that tests run serially, not in parallel.
static DB_LOCK: Mutex<()> = Mutex::const_new(());

/// This function will be executed when the test program exits.
#[dtor]
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
async fn get_db_connection() -> Arc<ArangoDbConnection> {
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
        .expect("Failed to set DB connection");
    db
}

/// Gets the shared test context and clears the database for a new test.
async fn setup() -> Arc<ArangoDbConnection> {
    let db = get_db_connection().await;
    db.clear_entities().await.unwrap();
    db.clear_resources().await.unwrap();
    db
}

// Define components for testing purposes, similar to the example in main.rs
#[derive(Component, Serialize, Deserialize, Debug, PartialEq)]
struct Health {
    value: i32,
}
impl Persist for Health {}

#[derive(Component, Serialize, Deserialize, Debug, PartialEq)]
struct Position {
    x: f32,
    y: f32,
}
impl Persist for Position {}

#[derive(Resource, Serialize, Deserialize, Debug, PartialEq)]
struct GameSettings {
    difficulty: f32,
    map_name: String,
}
impl Persist for GameSettings {}

#[tokio::test]
async fn test_entity_commit_and_fetch() {
    let _guard = DB_LOCK.lock().await;
    let db = setup().await;

    // 2. Create a session and register components
    let mut session = ArangoSession::new(db.clone());
    session.register_component::<Health>();
    session.register_component::<Position>();

    // 3. Spawn an entity and commit it
    let health_val = Health { value: 100 };
    let pos_val = Position { x: 1.0, y: 2.0 };

    let entity_id = session.local_world.spawn((health_val, pos_val)).id();
    session.mark_dirty(entity_id);

    session.commit().await.expect("Commit failed");

    // 4. Verify the results
    // The entity should now have a Guid component assigned by the library.
    let guid = session
        .local_world
        .get::<Guid>(entity_id)
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

#[tokio::test]
async fn test_resource_commit_and_fetch() {
    let _guard = DB_LOCK.lock().await;
    let db = setup().await;

    // 2. Create a session, add a resource, and commit it
    let mut session = ArangoSession::new(db.clone());
    session.register_resource::<GameSettings>();

    let settings = GameSettings {
        difficulty: 0.8,
        map_name: "level_1".into(),
    };
    session.local_world.insert_resource(settings);
    session.mark_resource_dirty::<GameSettings>();

    session.commit().await.expect("Commit failed");

    // 3. Verify the resource was saved correctly by fetching it directly
    let resource_name = GameSettings::name();
    let resource_json = db
        .fetch_resource(resource_name)
        .await
        .expect("Failed to fetch resource from DB")
        .expect("Resource should exist in DB");

    let fetched_settings: GameSettings =
        serde_json::from_value(resource_json).expect("Failed to deserialize resource");

    assert_eq!(fetched_settings.difficulty, 0.8);
    assert_eq!(fetched_settings.map_name, "level_1");
}

#[tokio::test]
async fn test_entity_load_into_new_session() {
    let _guard = DB_LOCK.lock().await;
    let db = setup().await;

    let mut session1 = ArangoSession::new(db.clone());
    session1.register_component::<Health>();
    session1.register_component::<Position>();

    // 2. Spawn two entities, one with Health+Position, one with only Health.
    let entity_to_load = session1
        .local_world
        .spawn((
            Health { value: 150 },
            Position { x: 10.0, y: 20.0 },
        ))
        .id();
    let entity_to_ignore = session1.local_world.spawn(Health { value: 99 }).id();
    session1.mark_dirty(entity_to_load);
    session1.mark_dirty(entity_to_ignore);
    session1.commit().await.expect("Initial commit failed");

    // 3. Create a new, clean session to load the data into.
    let mut session2 = ArangoSession::new(db.clone());
    session2.register_component::<Health>();
    session2.register_component::<Position>();

    // 4. Query for entities that have BOTH Health and Position.
    let loaded_entities = ArangoQuery::new(db.clone())
        .with::<Health>()
        .with::<Position>()
        .fetch_into(&mut session2)
        .await;

    // 5. Verify that only the correct entity was loaded and its data is correct.
    assert_eq!(
        loaded_entities.len(),
        1,
        "Should only load one entity with both components"
    );
    let loaded_entity = loaded_entities[0];

    let health = session2.local_world.get::<Health>(loaded_entity).unwrap();
    assert_eq!(health.value, 150);

    let position = session2.local_world.get::<Position>(loaded_entity).unwrap();
    assert_eq!(position.x, 10.0);
    assert_eq!(position.y, 20.0);
}

#[tokio::test]
async fn test_entity_delete() {
    let _guard = DB_LOCK.lock().await;
    let db = setup().await;

    let mut session = ArangoSession::new(db.clone());
    session.register_component::<Health>();

    // 2. Spawn and commit an entity.
    let entity_id = session.local_world.spawn(Health { value: 100 }).id();
    session.mark_dirty(entity_id);
    session.commit().await.expect("Commit failed");

    // 3. Verify it exists in the database.
    let guid = session.local_world.get::<Guid>(entity_id).unwrap().id().to_string();
    let component = db
        .fetch_component(&guid, Health::name())
        .await
        .expect("Fetch should not fail")
        .expect("Component should exist after first commit");
    assert_eq!(component.get("value").unwrap().as_i64().unwrap(), 100);

    // 4. Despawn the entity and commit again.
    session.mark_despawned(entity_id);
    session.commit().await.expect("Second commit failed");

    // 5. Verify it's gone from the database.
    let component_after_delete = db
        .fetch_component(&guid, Health::name())
        .await
        .expect("Fetch should not fail");

    assert!(
        component_after_delete.is_none(),
        "Component should be gone after delete commit"
    );
}
