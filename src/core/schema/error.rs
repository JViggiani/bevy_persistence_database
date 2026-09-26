//! Failures from schema migration and the schema lock.

use std::fmt;

use crate::core::db::connection::PersistenceError;

/// Why a migration or schema-lock check stopped.
#[derive(Debug)]
pub enum MigrationError {
    Persistence(PersistenceError),
    NewerThanBinary { stored: u32, current: u32 },
    BehindRequiresExplicit { stored: u32, current: u32 },
    UnversionedStore,
    BelowMinimum { stored: u32, minimum: u32 },
    Chain(String),
    Step { id: &'static str, message: String },
    Validation(String),
    Lock(String),
    Store(String),
}

impl MigrationError {
    pub fn step(id: &'static str, message: impl Into<String>) -> Self {
        Self::Step {
            id,
            message: message.into(),
        }
    }
}

impl fmt::Display for MigrationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Persistence(error) => write!(f, "{error}"),
            Self::NewerThanBinary { stored, current } => write!(
                f,
                "store schema version {stored} is newer than this binary ({current})"
            ),
            Self::BehindRequiresExplicit { stored, current } => write!(
                f,
                "store schema version {stored} is behind {current}; run the explicit migration"
            ),
            Self::UnversionedStore => write!(
                f,
                "store has data and no schema document; add a migration step from version 0"
            ),
            Self::BelowMinimum { stored, minimum } => write!(
                f,
                "store schema version {stored} is older than the oldest supported version {minimum}"
            ),
            Self::Chain(message) | Self::Validation(message) | Self::Lock(message) | Self::Store(message) => {
                f.write_str(message)
            }
            Self::Step { id, message } => write!(f, "migration step `{id}` failed: {message}"),
        }
    }
}

impl std::error::Error for MigrationError {}

impl From<PersistenceError> for MigrationError {
    fn from(error: PersistenceError) -> Self {
        Self::Persistence(error)
    }
}
