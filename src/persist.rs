//! A marker trait for components and resources that should be persisted.

use serde::{Serialize, de::DeserializeOwned};

/// A marker trait for components and resources that should be persisted.
///
/// This trait requires `Serialize` and `DeserializeOwned` for data conversion,
/// and `Send + Sync + 'static` to be safely used as a Bevy component/resource.
/// The `name()` method provides a stable, unique name for the type, used as
/// a key in the ArangoDB document.
pub trait Persist: Serialize + DeserializeOwned + Send + Sync + 'static {
    /// A unique, stable name for this type, used as a key in the database.
    /// Typically, this is just the struct's name.
    fn name() -> &'static str;
}
