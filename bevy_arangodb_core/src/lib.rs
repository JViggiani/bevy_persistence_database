/// Note: Components and resources you wish to persist **must** derive
/// `serde::Serialize` and `serde::Deserialize`.
mod arango_session;
mod arango_query;
mod arango_connection;
mod guid;
mod persist;
pub mod bevy_plugin;
pub mod query_dsl;
pub mod registration;

#[doc(hidden)]
pub mod prelude {
    pub use bevy::prelude::Component;
}

// Re-export for macro use
#[doc(hidden)]
pub use once_cell;

pub use arango_session::{ArangoSession, DatabaseConnection, ArangoError, commit_app as commit};
pub use arango_query::ArangoQuery;
pub use arango_connection::{ArangoDbConnection, Collection};
pub use guid::Guid;
pub use persist::Persist;
pub use bevy_plugin::ArangoPlugin;