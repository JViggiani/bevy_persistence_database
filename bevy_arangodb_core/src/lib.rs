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
    ArangoDbConnection,
    Collection,
    PersistenceError,
    TransactionOperation,
    BEVY_PERSISTENCE_VERSION_FIELD,
};
pub use db::connection::DatabaseConnectionResource;
pub use db::MockDatabaseConnection;
pub use plugins::{
    CommitStatus, PersistencePluginCore, TriggerCommit, CommitCompleted,
    persistence_plugin::PersistenceSystemSet,
};
pub use query::{PersistenceQuery, PersistentQuery, PersistenceQueryCache, CachePolicy};

pub use query::expression::{Expression, BinaryOperator};
pub use resources::{PersistenceSession, commit, commit_sync};
pub use persist::Persist;

pub use versioning::VersionManager;
