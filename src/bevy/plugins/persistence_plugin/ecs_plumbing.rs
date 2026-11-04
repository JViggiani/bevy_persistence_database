use bevy::prelude::ResMut;

use crate::core::session::PersistenceSession;

/// Close hydration scopes opened during load, after dirty tracking has run.
///
/// Hydration scopes are opened automatically by [`PersistenceSession::materialize_entity_document`],
/// [`PersistenceSession::materialize_resource`], and related load APIs. This runs in
/// `PersistenceSystemSet::FinishHydration` so change detection in `TrackChanges` still sees
/// an active hydration scope.
pub(crate) fn finish_hydration(mut session: ResMut<PersistenceSession>) {
    if !session.is_hydrating() {
        return;
    }
    session.finish_all_hydration();
    bevy::log::debug!("finished hydration scopes; dirty tracking baseline is now gameplay state");
}
