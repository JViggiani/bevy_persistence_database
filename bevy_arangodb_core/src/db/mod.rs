pub mod connection;
mod arango_connection;

pub use connection::{DatabaseConnection, PersistenceError, TransactionOperation, Collection};
pub use arango_connection::{ArangoDbConnection};

pub use connection::MockDatabaseConnection;
