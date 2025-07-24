// modules
pub mod components;
pub mod db;
pub mod plugins;
pub mod query;
pub mod resources;
pub mod registration;

mod persist;

#[doc(hidden)]
pub mod prelude {
    pub use bevy::prelude::Component;
}

// Re-export for macro use
#[doc(hidden)]
pub use once_cell;

// Public API 
pub use plugins::bevy_plugin;

pub use components::Guid;
pub use db::{
    DatabaseConnection,
    ArangoDbConnection,
    Collection,
    ArangoError,
    TransactionOperation,
};
pub use resources::commit_app as commit;
pub use plugins::ArangoPlugin;
pub use query::{ArangoQuery, query_dsl};
pub use resources::ArangoSession;
pub use persist::Persist;