//! A marker trait for components and resources that should be persisted.

use serde::{de::DeserializeOwned, Serialize};
use bevy::prelude::{Entity, World};

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

    /// Inserts an instance of this component into an entity, bypassing change detection.
    /// This is necessary when loading data from the database to prevent feedback loops.
    fn insert_bypassing_change_detection(world: &mut World, entity: Entity, component: Self);
}