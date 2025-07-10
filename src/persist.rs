use serde::{de::DeserializeOwned, Serialize};

/// A trait for types that can be persisted to ArangoDB.
///
/// Implementing this trait on a `Component` or `Resource` marks it as
/// persistable.
pub trait Persist: Serialize + DeserializeOwned + 'static {
    /// The name of the type in the database.
    ///
    /// For a `Component`, this is the field name within the entity document.
    /// For a `Resource`, this is the `_key` of the resource document.
    ///
    /// This defaults to the type's full name. It should only be overridden for
    /// special components like `Guid` which maps to `_key`.
    fn name() -> &'static str {
        std::any::type_name::<Self>()
    }
}
