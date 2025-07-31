use bevy_arangodb::{
    ArangoDbConnection, DatabaseConnection, 
};
use std::sync::Arc;
use testcontainers::{core::WaitFor, runners::AsyncRunner, ContainerAsync, GenericImage, ImageExt};

/// This function will be executed once when the test binary starts.
#[ctor::ctor]
fn initialize_logging() {
    // The `try_init` call will fail if the logger is already set, which is fine.
    // This ensures that logging is initialized exactly once.
    let _ = tracing_subscriber::fmt().with_test_writer().try_init();
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
