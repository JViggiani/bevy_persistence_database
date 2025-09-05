// modules
pub mod components;
pub mod db;
pub mod plugins;
pub mod query;
pub mod resources;
pub mod registration;
pub mod versioning;

mod persist;

#[doc(hidden)]
pub mod prelude {
    pub use bevy::prelude::Component;
}

// Re-export for macro use
#[doc(hidden)]
pub use once_cell;

// Public API
pub use plugins::persistence_plugin;

pub use components::Guid;
pub use db::{
    DatabaseConnection,
    Collection,
    PersistenceError,
    TransactionOperation,
    BEVY_PERSISTENCE_VERSION_FIELD,
};
#[cfg(feature = "arango")]
pub use db::ArangoDbConnection;
#[cfg(feature = "postgres")]
pub use db::PostgresDbConnection;
pub use db::connection::DatabaseConnectionResource;
pub use db::MockDatabaseConnection;
pub use plugins::{
    CommitStatus, PersistencePluginCore, TriggerCommit, CommitCompleted,
    persistence_plugin::PersistenceSystemSet,
};
pub use query::{PersistentQuery, PersistenceQueryCache, CachePolicy};

pub use resources::{PersistenceSession, commit, commit_sync};
pub use persist::Persist;

pub use versioning::VersionManager;
