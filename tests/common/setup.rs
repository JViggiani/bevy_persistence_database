use bevy::prelude::App;
use bevy_arangodb_core::PersistencePluginCore;
use bevy_arangodb_core::persistence_plugin::PersistencePluginConfig;
use bevy_arangodb_core::{
    ArangoDbConnection, DatabaseConnection, 
};
use std::sync::Arc;
use testcontainers::{core::WaitFor, runners::AsyncRunner, ContainerAsync, GenericImage, ImageExt};
use std::sync::{OnceLock, atomic::{AtomicUsize, Ordering}, Mutex};
use tokio::runtime::Runtime;

static TEST_RT: OnceLock<Arc<Runtime>> = OnceLock::new();

struct GlobalContainerState {
    rt: Arc<Runtime>,
    // Store the container and its ID; use Mutex<Option<..>> for safe take() in dtor/drop.
    container: Mutex<Option<ContainerAsync<GenericImage>>>,
    container_id: String,
    base_url: String,
}

impl Drop for GlobalContainerState {
    fn drop(&mut self) {
        // Keep container running if flag is set
        let keep = std::env::var("BEVY_ARANGODB_KEEP_CONTAINER")
            .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
            .unwrap_or(false);

        if keep {
            if self.container.lock().unwrap().is_some() {
                eprintln!("[bevy_arangodb tests] BEVY_ARANGODB_KEEP_CONTAINER=1 set; leaving ArangoDB container running at {}", self.base_url);
            }
            return;
        }

        // Force-remove the container using docker CLI (blocking, no Tokio runtime required)
        if self.container.lock().unwrap().is_some() {
            let _ = std::process::Command::new("docker")
                .args(["rm", "-f", "-v", &self.container_id])
                .status();
            // Prevent async Drop from running (which would require a Tokio context)
            if let Some(c) = self.container.lock().unwrap().take() {
                std::mem::forget(c);
            }
        }
    }
}

static GLOBAL: OnceLock<GlobalContainerState> = OnceLock::new();
static DB_COUNTER: AtomicUsize = AtomicUsize::new(0);

// Start a single async container once for all tests.
async fn start_container() -> (ContainerAsync<GenericImage>, String, String) {
    let container = GenericImage::new("arangodb", "3.12.5")
        .with_wait_for(WaitFor::message_on_stdout("is ready for business"))
        .with_env_var("ARANGO_ROOT_PASSWORD", "password")
        .start()
        .await
        .expect("Failed to start ArangoDB container");

    let host_port = container.get_host_port_ipv4(8529).await.unwrap();
    let url = format!("http://127.0.0.1:{}", host_port);
    let id = container.id().to_string();
    (container, url, id)
}

fn ensure_global() -> &'static GlobalContainerState {
    GLOBAL.get_or_init(|| {
        let rt = TEST_RT
            .get_or_init(|| {
                Arc::new(
                    tokio::runtime::Builder::new_multi_thread()
                        .enable_all()
                        .build()
                        .expect("failed to build test tokio runtime"),
                )
            })
            .clone();
        let (container, base_url, container_id) = rt.block_on(start_container());
        GlobalContainerState {
            rt,
            container: Mutex::new(Some(container)),
            container_id,
            base_url,
        }
    })
}

/// This function will be executed once when the test binary starts.
#[ctor::ctor]
fn initialize_logging() {
    // Default to warn to avoid noisy logs; allow override via RUST_LOG.
    if std::env::var("RUST_LOG").is_err() {
        std::env::set_var("RUST_LOG", "warn");
    }

    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_test_writer()
        .init();

    // Also ensure the global container is started once for all tests
    let _ = ensure_global();
}

// Run once after all tests complete to ensure the container is stopped unless explicitly kept.
#[ctor::dtor]
fn teardown_container() {
    if let Some(state) = GLOBAL.get() {
        // Keep container running if flag is set
        let keep = std::env::var("BEVY_ARANGODB_KEEP_CONTAINER")
            .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
            .unwrap_or(false);

        if keep {
            if state.container.lock().unwrap().is_some() {
                eprintln!("[bevy_arangodb tests] BEVY_ARANGODB_KEEP_CONTAINER=1 set; leaving ArangoDB container running at {}", state.base_url);
            }
            return;
        }

        // Stop/remove the container explicitly here (blocking) and skip async Drop
        if state.container.lock().unwrap().is_some() {
            let _ = std::process::Command::new("docker")
                .args(["rm", "-f", "-v", &state.container_id])
                .status();
            if let Some(c) = state.container.lock().unwrap().take() {
                std::mem::forget(c);
            }
        }
    }
}

// Guard that ensures correct drop within a runtime (no-op per test now)
pub struct ContainerGuard {
    inner: Option<ContainerAsync<GenericImage>>,
}

impl Drop for ContainerGuard {
    fn drop(&mut self) {
        if let Some(inner) = self.inner.take() {
            // Avoid async Drop at test shutdown; forget here too.
            std::mem::forget(inner);
        }
    }
}

/// Synchronous variant that creates a unique database per test using the shared container.
pub fn setup_sync() -> (Arc<dyn DatabaseConnection>, ContainerGuard) {
    let state = ensure_global();
    let db_name = format!("test_db_{}", DB_COUNTER.fetch_add(1, Ordering::Relaxed));

    // Ensure DB exists and connect
    state.rt.block_on(async {
        ArangoDbConnection::ensure_database(&state.base_url, "root", "password", &db_name)
            .await
            .expect("Failed to create database");
    });

    // Only print DB info if explicitly enabled
    let verbose = std::env::var("BEVY_ARANGODB_TEST_LOG")
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false);
    if verbose {
        eprintln!("Created/ensured test database '{}' at {}", db_name, state.base_url);
    }

    let db = state.rt.block_on(ArangoDbConnection::connect(&state.base_url, "root", "password", &db_name))
        .expect("Failed to connect to per-test database");

    let guard = ContainerGuard { inner: None };
    (Arc::new(db), guard)
}

/// Run any async future on the shared test runtime.
pub fn run_async<F: std::future::Future>(fut: F) -> F::Output {
    let rt = TEST_RT
        .get_or_init(|| {
            Arc::new(
                tokio::runtime::Builder::new_multi_thread()
                    .enable_all()
                    .build()
                    .expect("failed to build test tokio runtime"),
            )
        })
        .clone();
    rt.block_on(fut)
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