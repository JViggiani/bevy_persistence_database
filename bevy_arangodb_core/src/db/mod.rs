mod arango_connection;
pub use arango_connection::{ArangoDbConnection, Collection};

// bring the trait into `db` namespace to satisfy lib.rs
pub use crate::resources::DatabaseConnection;
