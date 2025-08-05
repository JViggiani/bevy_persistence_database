pub mod connection;
mod arango_connection;

pub use connection::{DatabaseConnection, PersistenceError, TransactionOperation, Collection, BEVY_PERSISTENCE_VERSION_FIELD};
pub use arango_connection::{ArangoDbConnection};

pub use connection::MockDatabaseConnection;
