use bevy::ecs::world::EntityWorldMut;
use bevy::prelude::{Bundle, Commands, EntityCommands, World};

use crate::bevy::components::Guid;

/// Ergonomic helpers for spawning persisted entities via `Commands`.
///
/// These helpers always attach a `Guid` component so call sites do not need to
/// remember `(Guid, Bundle)` tuples.
pub trait PersistSpawnCommandsExt {
    fn spawn_persisted<T: Bundle>(&mut self, bundle: T) -> EntityCommands<'_>;

    fn spawn_persisted_with_id<T: Bundle>(
        &mut self,
        id: impl Into<String>,
        bundle: T,
    ) -> EntityCommands<'_>;
}

impl<'w, 's> PersistSpawnCommandsExt for Commands<'w, 's> {
    fn spawn_persisted<T: Bundle>(&mut self, bundle: T) -> EntityCommands<'_> {
        self.spawn((Guid::generate(), bundle))
    }

    fn spawn_persisted_with_id<T: Bundle>(
        &mut self,
        id: impl Into<String>,
        bundle: T,
    ) -> EntityCommands<'_> {
        self.spawn((Guid::new(id.into()), bundle))
    }
}

/// Ergonomic helpers for spawning persisted entities directly on a `World`.
///
/// Useful for exclusive systems and tests that work with `&mut World`.
pub trait PersistSpawnWorldExt {
    fn spawn_persisted<T: Bundle>(&mut self, bundle: T) -> EntityWorldMut<'_>;

    fn spawn_persisted_with_id<T: Bundle>(
        &mut self,
        id: impl Into<String>,
        bundle: T,
    ) -> EntityWorldMut<'_>;
}

impl PersistSpawnWorldExt for World {
    fn spawn_persisted<T: Bundle>(&mut self, bundle: T) -> EntityWorldMut<'_> {
        self.spawn((Guid::generate(), bundle))
    }

    fn spawn_persisted_with_id<T: Bundle>(
        &mut self,
        id: impl Into<String>,
        bundle: T,
    ) -> EntityWorldMut<'_> {
        self.spawn((Guid::new(id.into()), bundle))
    }
}
