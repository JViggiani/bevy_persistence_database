use bevy::prelude::{App};
use bevy_arangodb::{
    ArangoDbConnection, DatabaseConnection, PersistencePlugins,
};
use std::process::Command;
use std::sync::Arc;
use testcontainers::{core::WaitFor, runners::AsyncRunner, ContainerAsync, GenericImage, ImageExt};
use tokio::sync::{Mutex, OnceCell};
use tracing_subscriber::EnvFilter;

// This will hold the container instance to keep it alive for the duration of the tests.
static DOCKER_CONTAINER: OnceCell<ContainerAsync<GenericImage>> = OnceCell::const_new();
// This will hold the test context, including the DB connection and connection details.
static TEST_CONTEXT: OnceCell<TestContext> = OnceCell::const_new();

// A mutex to ensure that tests run serially, not in parallel.
pub static DB_LOCK: Mutex<()> = Mutex::const_new(());

/// A struct to hold all necessary information for a test run.
#[derive(Clone, Debug)]
pub struct TestContext {
    pub db: Arc<dyn DatabaseConnection>,
    pub db_url: String,
    pub db_user: String,
    pub db_pass: String,
    pub db_name: String,
}

/// This function will be executed once when the test binary starts.
#[ctor::ctor]
fn initialize_logging() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .with_test_writer()
        .try_init();
}

/// This function will be executed when the test program exits.
#[ctor::dtor]
fn teardown() {
    if let Some(container) = DOCKER_CONTAINER.get() {
        let id = container.id();
        
        let status = Command::new("docker")
            .arg("stop")
            .arg(id)
            .status()
            .expect("Failed to execute docker stop command");

        if !status.success() {
            eprintln!(
                "Error stopping container '{}': command exited with status {}",
                id, status
            );
        }

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

/// Lazily initializes and returns a shared test context.
/// On first call, starts the Docker container and connects to the database.
async fn get_test_context() -> TestContext {
    if let Some(context) = TEST_CONTEXT.get() {
        return context.clone();
    }

    let container = GenericImage::new("arangodb", "3.12.5")
        .with_wait_for(WaitFor::message_on_stdout("is ready for business"))
        .with_env_var("ARANGO_ROOT_PASSWORD", "password")
        .start()
        .await
        .expect("Failed to start ArangoDB container");

    let host_port = container.get_host_port_ipv4(8529).await.unwrap();
    let url = format!("http://127.0.0.1:{}", host_port);
    let user = "root".to_string();
    let pass = "password".to_string();
    let db_name = "_system".to_string();

    let db = Arc::new(
        ArangoDbConnection::connect(&url, &user, &pass, &db_name)
            .await
            .expect("Failed to connect to ArangoDB container"),
    );

    // Store the container to keep it alive.
    DOCKER_CONTAINER
        .set(container)
        .expect("Failed to set container");

    let context = TestContext {
        db: db.clone(),
        db_url: url,
        db_user: user,
        db_pass: pass,
        db_name,
    };

    TEST_CONTEXT
        .set(context.clone())
        .unwrap();
    context
}

/// Gets the shared test context and clears the database for a new test.
pub async fn setup() -> TestContext {
    let context = get_test_context().await;
    context.db.clear_entities().await.unwrap();
    context.db.clear_resources().await.unwrap();
    context
}

/// Helper to create a new app with the persistence plugin.
pub fn new_app(db: Arc<dyn DatabaseConnection>) -> App {
    let mut app = App::new();
    app.add_plugins(PersistencePlugins(db));
    app
}
