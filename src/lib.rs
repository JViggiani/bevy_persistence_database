//! Database persistence layer for Bevy ECS

// Re-export the derive macro
pub use bevy_persistence_database_derive::persist;

// modules
pub mod components;
pub mod db;
pub mod plugins;
pub mod query;
pub mod resources;
pub mod registration;
pub mod versioning;

mod persist;

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

// Re-export for macro use
#[doc(hidden)]
pub use once_cell;

// Convenient prelude for users
pub mod prelude {
    pub use bevy::prelude::Component;
    pub use crate::{
        persist,
        Persist,
        Guid,
        PersistenceSession,
        DatabaseConnection,
        PersistenceError,
        plugins::{PersistencePlugins, PersistencePluginCore, TriggerCommit},
        query::{PersistentQuery, FilterExpression},
    };
    
    #[cfg(feature = "arango")]
    pub use crate::ArangoDbConnection;
    
    #[cfg(feature = "postgres")]
    pub use crate::PostgresDbConnection;
}