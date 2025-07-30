pub mod connection;
mod arango_connection;

pub use connection::{
    DatabaseConnection, DocumentId, PersistenceError, TransactionOperation, Collection,
    TransactionResult,
};
pub use arango_connection::{ArangoDbConnection};

pub use connection::MockDatabaseConnection;
