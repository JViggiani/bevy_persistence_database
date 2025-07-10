//! A component for storing a globally-unique ID for an entity.

use bevy::prelude::Component;
use serde::{Deserialize, Serialize};

use crate::Persist;

/// A globally-unique identifier for an entity, used to link the Bevy `Entity`
/// to its corresponding document in ArangoDB. This is typically the `_key`.
/// The inner value is private to prevent manual modification.
#[derive(Component, Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Hash)]
pub struct Guid(String);

impl Guid {
    /// Creates a new `Guid`. This is intended for modification in the internal library, but is available to read externally.
    pub(crate) fn new(id: String) -> Self {
        Self(id)
    }

    /// Returns the underlying global ID
    pub fn id(&self) -> &str {
        &self.0
    }
}

impl Persist for Guid {
    fn name() -> &'static str {
        "_key"
    }
}
