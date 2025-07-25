use bevy::prelude::{App, EventReader, Events, Plugin};
use bevy_arangodb::{CommitPlugin, TriggerCommit};

#[test]
fn trigger_commit_event_received() {
    let mut app = App::new();
    app.add_plugin(CommitPlugin);

    // fire the event
    app.send_event(TriggerCommit);
    app.update();

    // collect events
    let events = app.world.resource::<Events<TriggerCommit>>();
    let mut reader = EventReader::<TriggerCommit>::default();
    let collected = reader.iter(events).collect::<Vec<_>>();
    assert_eq!(collected.len(), 1, "Should receive exactly one TriggerCommit event");
}
