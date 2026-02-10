use super::commit::CommitCompleted;
use bevy::prelude::{MessageReader, Resource, World};
use std::collections::HashMap;
use tokio::sync::oneshot;

use crate::core::db::PersistenceError;

#[derive(Resource, Default)]
pub(super) struct CommitEventListeners {
    listeners: HashMap<u64, oneshot::Sender<Result<(), PersistenceError>>>,
}

pub(super) fn init_commit_listeners(ecs: &mut World) {
    if ecs.get_resource::<CommitEventListeners>().is_none() {
        ecs.init_resource::<CommitEventListeners>();
    }
}

/// Register a commit listener for the given correlation ID.
pub fn register_commit_listener(
    ecs: &mut World,
    correlation_id: u64,
    sender: oneshot::Sender<Result<(), PersistenceError>>,
) {
    ecs.resource_mut::<CommitEventListeners>()
        .listeners
        .insert(correlation_id, sender);
}

/// Remove and return a registered commit listener by correlation ID, if present.
pub fn take_commit_listener(
    ecs: &mut World,
    correlation_id: u64,
) -> Option<oneshot::Sender<Result<(), PersistenceError>>> {
    ecs.resource_mut::<CommitEventListeners>()
        .listeners
        .remove(&correlation_id)
}

pub(super) fn commit_event_listener(
    mut events: MessageReader<CommitCompleted>,
    mut listeners: bevy::prelude::ResMut<CommitEventListeners>,
) {
    for event in events.read() {
        if let Some(id) = event.correlation_id {
            if let Some(sender) = listeners.listeners.remove(&id) {
                bevy::log::info!("Found listener for commit {}. Sending result.", id);
                let result = match &event.result {
                    Ok(_) => Ok(()),
                    Err(e) => Err(e.clone()),
                };
                let _ = sender.send(result);
            } else {
                bevy::log::info!("Commit listener missing for correlation_id={}", id);
            }
        } else {
            bevy::log::trace!("CommitCompleted event without correlation id consumed");
        }
    }
}
