use std::any::TypeId;

use bevy::prelude::*;

use crate::core::session::PersistenceSession;

use super::despawn_tracking::mark_resource_removed;

#[derive(Resource)]
struct Score;

/// Whether `session` holds `R` for deletion at the next commit.
fn marked_removed<R: Resource>(session: &mut PersistenceSession) -> bool {
    let state = session.take_dirty_state();
    let marked = state.despawned_resources.contains(&TypeId::of::<R>());
    session.restore_dirty_state(state);
    marked
}

// GIVEN an observer of `Remove` for a resource type (resources are components
//     since Bevy 0.19)
// WHEN the resource is inserted, replaced, then removed
// THEN the observer runs once, on the removal: what removal tracking relies on
#[test]
fn removing_a_resource_triggers_its_remove_observer() {
    #[derive(Resource, Default)]
    struct Removals(u32);

    let mut world = World::new();
    world.init_resource::<Removals>();
    world.add_observer(|_: On<Remove<Score>>, mut removals: ResMut<Removals>| removals.0 += 1);
    world.insert_resource(Score);
    world.insert_resource(Score);
    assert_eq!(world.resource::<Removals>().0, 0);
    world.remove_resource::<Score>();
    assert_eq!(world.resource::<Removals>().0, 1);
}

// GIVEN a persisted resource's removal observer
// WHEN the resource is inserted and replaced, then removed
// THEN it is marked for deletion only once removed
#[test]
fn a_removed_persisted_resource_is_marked_for_deletion() {
    let mut world = World::new();
    world.insert_resource(PersistenceSession::new());
    world.add_observer(mark_resource_removed::<Score>);
    world.insert_resource(Score);
    world.insert_resource(Score);
    assert!(!marked_removed::<Score>(&mut world.resource_mut::<PersistenceSession>()));
    world.remove_resource::<Score>();
    assert!(marked_removed::<Score>(&mut world.resource_mut::<PersistenceSession>()));
}
