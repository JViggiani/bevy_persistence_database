//! Database persistence layer for Bevy ECS

// Re-export the derive macro
pub use bevy_persistence_database_derive::persist;

// Re-export commonly used types at the crate root
pub use crate::bevy::registration::{register_persist_component, register_persist_resource, apply_registered_persist_types};
#[cfg(not(feature = "bevy_many_relationship_edges"))]
pub use crate::bevy::registration::register_persist_bevy_relationship;
#[cfg(feature = "bevy_many_relationship_edges")]
pub use crate::bevy::registration::register_persist_many_relationship;

pub mod bevy;
pub mod core;




