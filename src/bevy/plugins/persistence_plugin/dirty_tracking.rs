use crate::bevy::components::Guid;
use crate::core::persist::Persist;
use crate::core::session::PersistenceSession;
use bevy::prelude::*;
use std::any::TypeId;

/// Automatically marks entities with added/changed components as dirty.
pub fn auto_dirty_tracking_entity_system<T: Component + Persist>(
    mut session: ResMut<PersistenceSession>,
    query: Query<Entity, Or<(Added<T>, Changed<T>)>>,
) {
    for entity in query.iter() {
        bevy::log::debug!(
            "Marking entity {:?} as dirty due to component {}",
            entity,
            std::any::type_name::<T>()
        );
        session.mark_entity_component_dirty(entity, TypeId::of::<T>());
    }
}

/// Automatically marks changed resources as dirty.
pub fn auto_dirty_tracking_resource_system<T: Resource + Persist>(
    mut session: ResMut<PersistenceSession>,
    resource: Option<Res<T>>,
) {
    if let Some(resource) = resource {
        if resource.is_changed() {
            session.mark_resource_dirty::<T>();
        }
    }
}

/// Detects removal of persisted resources and marks them for deletion.
pub(crate) fn auto_despawn_tracking_resource_system(ecs: &mut World) {
    let presence_snapshot = {
        let session = ecs.resource::<PersistenceSession>();
        session.resource_presence_snapshot(ecs)
    };

    let mut session = ecs.resource_mut::<PersistenceSession>();
    for (type_id, is_present) in presence_snapshot {
        let was_present = session.update_resource_presence(type_id, is_present);
        if was_present && !is_present {
            session.mark_resource_despawned_type_id(type_id);
        }
    }
}

/// Automatically marks despawned entities as needing deletion.
pub(crate) fn auto_despawn_tracking_system(
    mut session: ResMut<PersistenceSession>,
    mut removed: RemovedComponents<Guid>,
) {
    for entity in removed.read() {
        session.mark_despawned(entity);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};

    #[derive(Component, Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct TestHealth {
        value: i32,
    }

    impl Persist for TestHealth {
        fn name() -> &'static str {
            "TestHealth"
        }
    }

    #[test]
    fn read_only_access_does_not_mark_dirty() {
        let mut app = App::new();

        let session = PersistenceSession::new();
        app.insert_resource(session);

        app.add_systems(Update, auto_dirty_tracking_entity_system::<TestHealth>);

        let entity = app.world_mut().spawn(TestHealth { value: 100 }).id();

        // First update will mark it as dirty because it was just added
        app.update();

        // Clear dirty state
        {
            let mut session = app.world_mut().resource_mut::<PersistenceSession>();
            session.clear_dirty_entity_components();
        }

        // Read the component without modifying it
        {
            let health = app.world().get::<TestHealth>(entity).unwrap();
            assert_eq!(health.value, 100);
        }

        // Update again - tracking system runs
        app.update();

        // Verify the entity wasn't marked dirty after read-only access
        {
            let session = app.world().resource::<PersistenceSession>();
            assert!(
                !session.is_entity_dirty(entity),
                "Entity was incorrectly marked dirty after read-only access"
            );
        }

        // Now mutate the component
        {
            let mut health = app.world_mut().get_mut::<TestHealth>(entity).unwrap();
            health.value = 200;
        }

        // Update again - should mark as dirty
        app.update();

        {
            let session = app.world().resource::<PersistenceSession>();
            assert!(
                session.is_entity_dirty(entity),
                "Entity should be marked dirty after modification"
            );
        }
    }
}
