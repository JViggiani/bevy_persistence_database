//! Database persistence layer for Bevy ECS
//!
//! This crate provides a way to persist Bevy ECS components and resources to a database.

// Re-export the derive macro
pub use bevy_persistence_database_derive::persist;

// Re-export core types from the core crate
pub use bevy_persistence_database_core::{
    // Core traits and marker
    Persist,
    
    // Components
    Guid,
    
    // Resources and session
    PersistenceSession,
    
    // Connections
    DatabaseConnection,
    DatabaseConnectionResource,
    PersistenceError,
    TransactionOperation,
    BEVY_PERSISTENCE_VERSION_FIELD,
    Collection,
};

// Conditionally re-export database implementations
#[cfg(feature = "arango")]
pub use bevy_persistence_database_core::ArangoDbConnection;

#[cfg(feature = "postgres")]
pub use bevy_persistence_database_core::PostgresDbConnection;

// Re-export plugins
pub use bevy_persistence_database_core::plugins::{
    PersistencePluginCore,
    PersistencePlugins,
    PersistencePluginConfig,
    CommitCompleted,
    CommitStatus,
    TriggerCommit,
};

// Re-export query functionality
pub use bevy_persistence_database_core::query::{
    FilterExpression,
    BinaryOperator,
    PersistentQuery,
    PersistenceQuerySpecification,
    PersistenceQueryCache,
    CachePolicy,
};

// Re-export common utility functions
pub use bevy_persistence_database_core::resources::commit;
pub use bevy_persistence_database_core::resources::commit_sync;

/// Public prelude module for commonly used imports
pub mod prelude {
    pub use crate::persist;
    pub use bevy_persistence_database_core::{
        Persist,
        Guid,
        PersistenceSession,
        DatabaseConnection,
        PersistenceError,
        plugins::{PersistencePlugins, PersistencePluginCore, TriggerCommit},
        query::{PersistentQuery, FilterExpression},
    };
    
    #[cfg(feature = "arango")]
    pub use bevy_persistence_database_core::ArangoDbConnection;
    
    #[cfg(feature = "postgres")]
    pub use bevy_persistence_database_core::PostgresDbConnection;
}