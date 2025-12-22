//! Database persistence layer for Bevy ECS

// Re-export the derive macro
pub use bevy_persistence_database_derive::persist;

// modules
pub mod components;
pub mod db;
pub mod plugins;
pub mod query;
pub mod registration;
pub mod resources;
pub mod versioning;

mod persist;

// Public API
pub use plugins::persistence_plugin;

pub use components::Guid;
#[cfg(feature = "arango")]
pub use db::ArangoDbConnection;
pub use db::MockDatabaseConnection;
#[cfg(feature = "postgres")]
pub use db::PostgresDbConnection;
pub use db::connection::DatabaseConnectionResource;
pub use db::{
    BEVY_PERSISTENCE_VERSION_FIELD, BEVY_TYPE_FIELD, DatabaseConnection, DocumentKind,
    PersistenceError, TransactionOperation,
};
pub use persist::Persist;
pub use plugins::{
    CommitCompleted, CommitStatus, PersistencePluginCore, TriggerCommit,
    persistence_plugin::PersistenceSystemSet,
};
pub use query::{CachePolicy, PersistenceQueryCache, PersistentQuery};
pub use resources::{PersistenceSession, commit, commit_sync};

pub use versioning::VersionManager;

// Re-export for macro use
#[doc(hidden)]
pub use once_cell;

// Convenient prelude for users
pub mod prelude {
    pub use crate::{
        DatabaseConnection, Guid, Persist, PersistenceError, PersistenceSession, persist,
        plugins::{PersistencePluginCore, PersistencePlugins, TriggerCommit},
        query::{FilterExpression, PersistentQuery},
    };
    pub use bevy::prelude::Component;

    #[cfg(feature = "arango")]
    pub use crate::ArangoDbConnection;

    #[cfg(feature = "postgres")]
    pub use crate::PostgresDbConnection;
}
