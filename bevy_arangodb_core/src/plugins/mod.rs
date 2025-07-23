pub mod bevy_plugin;

pub use bevy_plugin::ArangoPlugin;
// re-export commit_app so `plugins::commit_app` exists
pub use crate::resources::commit_app;
