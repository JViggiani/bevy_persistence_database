use bevy::prelude::App;
use bevy_arangodb_core::PersistencePluginCore;
use bevy_arangodb_core::persistence_plugin::PersistencePluginConfig;
use bevy_arangodb_core::{
    ArangoDbConnection, DatabaseConnection, 
};
use std::sync::Arc;
use testcontainers::{core::WaitFor, runners::AsyncRunner, ContainerAsync, GenericImage, ImageExt};

/// This function will be executed once when the test binary starts.
#[ctor::ctor]
fn initialize_logging() {
    // Set the RUST_LOG environment variable if it's not already set
    if std::env::var("RUST_LOG").is_err() {
        std::env::set_var("RUST_LOG", "debug,bevy_arangodb_core=trace");
    }
    
    // Initialize simple logger
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_test_writer()
        .init();
}

/// Starts a new ArangoDB container and returns a connection and the container handle.
/// The container will be stopped automatically when the handle is dropped.
pub async fn setup() -> (Arc<dyn DatabaseConnection>, ContainerAsync<GenericImage>) {
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

    (db, container)
}

/// Creates a new App with the PersistencePlugin configured with batching enabled.
pub fn make_app(db: Arc<dyn DatabaseConnection>, batch_size: usize) -> App {
    let config = PersistencePluginConfig {
        batching_enabled: true,
        commit_batch_size: batch_size,
        thread_count: 4,
    };
    let plugin = PersistencePluginCore::new(db.clone()).with_config(config);
    let mut app = App::new();
    app.add_plugins(plugin);
    app
}
