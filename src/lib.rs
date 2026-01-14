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
pub use db::MockDatabaseConnection;
#[cfg(feature = "postgres")]
pub use db::PostgresDbConnection;
pub use db::connection::DatabaseConnectionResource;
#[cfg(feature = "arango")]
pub use db::{ArangoAuthMode, ArangoAuthRefresh, ArangoConnectionConfig, ArangoDbConnection};
pub use db::{
    BEVY_PERSISTENCE_DATABASE_METADATA_FIELD, BEVY_PERSISTENCE_DATABASE_VERSION_FIELD, BEVY_PERSISTENCE_DATABASE_BEVY_TYPE_FIELD, DatabaseConnection,
    DocumentKind, PersistenceError, TransactionOperation, read_kind, read_version,
};
pub use persist::Persist;
pub use plugins::{
    CommitCompleted, CommitStatus, PersistencePluginCore, TriggerCommit,
    persistence_plugin::PersistenceSystemSet,
};
pub use query::{CachePolicy, PersistenceQueryCache, PersistentQuery};
pub use resources::{PersistenceSession, PersistentRes, PersistentResMut, commit, commit_sync};

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
        resources::{PersistentRes, PersistentResMut},
    };
    pub use bevy::prelude::Component;

    #[cfg(feature = "arango")]
    pub use crate::{
        ArangoAuthMode, ArangoAuthRefresh, ArangoConnectionConfig, ArangoDbConnection,
    };

    #[cfg(feature = "postgres")]
    pub use crate::PostgresDbConnection;
}
