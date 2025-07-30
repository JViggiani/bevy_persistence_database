// modules
pub mod components;
pub mod db;
pub mod plugins;
pub mod query;
pub mod resources;
pub mod registration;
pub mod debug_utils;

mod persist;

#[doc(hidden)]
pub mod prelude {
    pub use bevy::prelude::Component;
}

// Re-export for macro use
#[doc(hidden)]
pub use once_cell;

// Re-export for tests
#[doc(hidden)]
pub use arangors;

// Public API
pub use plugins::persistence_plugin;

pub use components::Guid;
pub use db::{
    DatabaseConnection,
    ArangoDbConnection,
    Collection,
    PersistenceError,
    TransactionOperation,
    DocumentId,
    TransactionResult,
};
pub use db::MockDatabaseConnection;
pub use plugins::{
    CommitStatus, PersistencePluginCore, TriggerCommit, CommitCompleted,
    persistence_plugin::PersistenceSystemSet,
};
pub use query::{PersistenceQuery, dsl};
pub use resources::{PersistenceSession, commit_and_wait as commit};
pub use persist::Persist;
pub use debug_utils::{dump_persistence_state};