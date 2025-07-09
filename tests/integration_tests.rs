use bevy::prelude::{Component, Resource};
use bevy_arangodb::{
    ArangoDbConnection, ArangoSession, DatabaseConnection, Guid, QueryComponent,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use testcontainers::{
    core::WaitFor, runners::AsyncRunner, ContainerAsync, ContainerRequest, GenericImage, ImageExt,
};
use tokio::sync::OnceCell;

// holds the running arango container, initialized only once.
static DOCKER: OnceCell<ContainerAsync<GenericImage>> = OnceCell::const_new();

async fn get_docker_container() -> &'static ContainerAsync<GenericImage> {
    DOCKER
        .get_or_init(|| async {
            arangodb_image()
                .start()
                .await
                .expect("Failed to start ArangoDB container")
        })
        .await
}

// Define components for testing purposes, similar to the example in main.rs
#[derive(Component, Serialize, Deserialize, Debug, PartialEq)]
struct Health {
    value: i32,
}
impl QueryComponent for Health {
    fn name() -> &'static str {
        std::any::type_name::<Health>()
    }
}

#[derive(Component, Serialize, Deserialize, Debug, PartialEq)]
struct Position {
    x: f32,
    y: f32,
}
impl QueryComponent for Position {
    fn name() -> &'static str {
        std::any::type_name::<Position>()
    }
}

#[derive(Resource, Serialize, Deserialize, Debug, PartialEq)]
struct GameSettings {
    difficulty: f32,
    map_name: String,
}

// Helper function to define our ArangoDB container.
fn arangodb_image() -> ContainerRequest<GenericImage> {
    GenericImage::new("arangodb", "3.12.5")
        .with_wait_for(WaitFor::message_on_stdout("is ready for business"))
        .with_env_var("ARANGO_ROOT_PASSWORD", "password")
}

/// Test fixture to set up a clean database connection for each test.
async fn setup_test_db() -> Arc<ArangoDbConnection> {
    let container = get_docker_container().await;
    let host_port = container.get_host_port_ipv4(8529).await.unwrap();
    let url = format!("http://127.0.0.1:{}", host_port);

    let db = Arc::new(
        ArangoDbConnection::connect(&url, "root", "password", "_system")
            .await
            .expect("Failed to connect to ArangoDB container"),
    );

    // Clear all collections to ensure test isolation.
    db.clear_entities()
        .await
        .expect("Failed to clear 'entities' collection");
    db.clear_resources()
        .await
        .expect("Failed to clear 'resources' collection");

    db
}

#[tokio::test]
async fn test_spawn_and_commit() {
    // 1. Set up the database using our test fixture.
    let db = setup_test_db().await;

    // 2. Create a session and register components
    let mut session = ArangoSession::new(db.clone());
    session.register_serializer::<Health>();
    session.register_serializer::<Position>();

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
    // 1. Set up the database using our test fixture.
    let db = setup_test_db().await;

    // 2. Create a session, add a resource, and commit it
    let mut session = ArangoSession::new(db.clone());
    session.register_resource_serializer::<GameSettings>();

    let settings = GameSettings {
        difficulty: 0.8,
        map_name: "level_1".into(),
    };
    session.local_world.insert_resource(settings);
    session.mark_resource_dirty::<GameSettings>();

    session.commit().await.expect("Commit failed");

    // 3. Verify the resource was saved correctly by fetching it directly
    let resource_name = std::any::type_name::<GameSettings>();
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
