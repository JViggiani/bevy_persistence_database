use bevy::prelude::{Component, Resource};
use bevy_arangodb::{
    ArangoDbConnection, ArangoQuery, ArangoSession, DatabaseConnection, Guid, QueryComponent,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use testcontainers::{
    core::WaitFor,
    runners::AsyncRunner,
    ContainerAsync,
    ContainerRequest,
    GenericImage,
    ImageExt,
};
use tokio::sync::{Mutex, OnceCell};

// holds the running arango container, initialized only once.
static DOCKER: OnceCell<ContainerAsync<GenericImage>> = OnceCell::const_new();

// A mutex to ensure that tests run serially, not in parallel.
static DB_LOCK: Mutex<()> = Mutex::const_new(());

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
async fn test_entity_commit_and_fetch() {
    let _guard = DB_LOCK.lock().await;
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
    let _guard = DB_LOCK.lock().await;
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

#[tokio::test]
async fn test_entity_load_into_new_session() {
    let _guard = DB_LOCK.lock().await;
    // 1. Set up the database and a session to commit initial data.
    let db = setup_test_db().await;
    let mut session1 = ArangoSession::new(db.clone());
    session1.register_serializer::<Health>();
    session1.register_serializer::<Position>();

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
    session2.register_deserializer::<Health>();
    session2.register_deserializer::<Position>();

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
    // 1. Set up the database and a session.
    let db = setup_test_db().await;
    let mut session = ArangoSession::new(db.clone());
    session.register_serializer::<Health>();

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
