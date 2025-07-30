//! Utilities for debugging persistence issues

use bevy::prelude::*;
use crate::{CommitStatus, PersistenceSession};

/// Dumps the current state of the persistence system to logs.
/// Useful for debugging deadlocks and other issues.
pub fn dump_persistence_state(app: &mut App) {
    let status = app.world().resource::<CommitStatus>();
    let session = app.world().resource::<PersistenceSession>();
    
    info!("===== PERSISTENCE STATE DUMP =====");
    info!("CommitStatus: {:?}", *status);
    info!("Dirty entities: {}", session.dirty_entities.len());
    info!("Despawned entities: {}", session.despawned_entities.len());
    info!("Entity metadata count: {}", session.entity_meta.len());
    info!("Dirty resources: {}", session.dirty_resources.len());
    
    if let Some(listeners) = app.world().get_resource::<crate::plugins::persistence_plugin::CommitEventListeners>() {
        info!("Active commit listeners: {}", listeners.0.len());
        for id in listeners.0.keys() {
            info!("  Listener waiting for correlation ID: {}", id);
        }
    }
    
    // Check for active commit tasks - without accessing the private TriggerID
    let task_count = app.world_mut()
        .query_filtered::<Entity, With<crate::plugins::persistence_plugin::CommitTask>>()
        .iter(app.world())
        .count();
    info!("Active commit tasks: {}", task_count);
    info!("=================================");
}
