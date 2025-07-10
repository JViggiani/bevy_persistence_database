/// Note: Components and resources you wish to persist **must** derive
/// `serde::Serialize` and `serde::Deserialize`.
mod arango_session;
mod arango_query;
mod arango_connection;
mod guid;
mod persist;
pub enum Collection {
    /// The collection where all Bevy entities are stored as documents.
    Entities,
    /// The special document key for storing Bevy resources.
    Resources,
}

impl std::fmt::Display for Collection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Collection::Entities => write!(f, "entities"),
            Collection::Resources => write!(f, "resources"),
        }
    }
}

pub use arango_session::{ArangoSession, DatabaseConnection, ArangoError};
pub use arango_query::ArangoQuery;
pub use arango_connection::ArangoDbConnection;
pub use guid::Guid;
pub use persist::Persist;