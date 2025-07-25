use bevy::prelude::{Event, Plugin, App};

/// An event that users fire to trigger a commit.
#[derive(Event)]
pub struct TriggerCommit;

/// A small plugin that registers the `TriggerCommit` event.
pub struct CommitPlugin;

impl Plugin for CommitPlugin {
    fn build(&self, app: &mut App) {
        app.add_event::<TriggerCommit>();
    }
}
