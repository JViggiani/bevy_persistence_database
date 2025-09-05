pub mod connection;
mod shared;
#[cfg(feature = "arango")]
mod arango_connection;
#[cfg(feature = "postgres")]
mod postgres_connection;

pub use connection::{DatabaseConnection, PersistenceError, TransactionOperation, Collection, BEVY_PERSISTENCE_VERSION_FIELD};

#[cfg(feature = "arango")]
pub use arango_connection::ArangoDbConnection;
#[cfg(feature = "postgres")]
pub use postgres_connection::PostgresDbConnection;

pub use connection::MockDatabaseConnection;
