#[cfg(feature = "arango")]
mod arango_connection;
pub mod connection;
#[cfg(feature = "postgres")]
mod postgres_connection;
mod shared;

pub use connection::{
    BEVY_PERSISTENCE_VERSION_FIELD, BEVY_TYPE_FIELD, DatabaseConnection, DocumentKind,
    PersistenceError, TransactionOperation,
};

#[cfg(feature = "arango")]
pub use arango_connection::ArangoDbConnection;
#[cfg(feature = "postgres")]
pub use postgres_connection::PostgresDbConnection;

pub use connection::MockDatabaseConnection;
