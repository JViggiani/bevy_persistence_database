//! Despawn / removal tracking for persisted entities and resources.

use bevy::prelude::*;

use crate::{bevy::components::Guid, core::session::PersistenceSession};

/// Marks a persisted resource for deletion when it leaves the world (an
/// observer of `Remove`: resources are components since Bevy 0.19).
///
/// A resource removed before it was ever committed has no stored document;
/// the commit skips it.
pub(crate) fn mark_resource_removed<R: Resource>(
    _: On<Remove<R>>,
    session: Option<ResMut<PersistenceSession>>,
) {
    if let Some(mut session) = session {
        session.mark_resource_despawned::<R>();
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
