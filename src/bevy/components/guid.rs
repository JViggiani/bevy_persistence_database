//! A component for storing a globally-unique ID for an entity.

use bevy::prelude::Component;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// A globally-unique identifier for an entity, used to link the Bevy `Entity`
/// to its corresponding document in the database. This is typically the document key.
/// The inner value is private to prevent manual modification.
#[derive(Component, Debug, Clone, PartialEq, Eq, Hash)]
pub struct Guid {
    value: String,
}

impl Serialize for Guid {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.value)
    }
}

impl<'de> Deserialize<'de> for Guid {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        String::deserialize(deserializer).map(Guid::new)
    }
}

impl Guid {
    /// Creates a new `Guid`. This is intended for modification in the internal library, but is available to read externally.
    pub fn new(id: String) -> Self {
        Self { value: id }
    }

    /// Returns the underlying global ID
    pub fn id(&self) -> &str {
        &self.value
    }

    /// Creates a value-expression for the document key field
    pub fn key_field() -> crate::core::query::FilterExpression {
        crate::core::query::FilterExpression::DocumentKey
    }
}
