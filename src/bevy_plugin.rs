//! A Bevy `Plugin` for integrating `bevy_arangodb`.
//!
//! This plugin simplifies the setup process by managing the `ArangoSession`
//! as a resource and automatically adding systems for change detection.

use crate::{ArangoSession, DatabaseConnection, Persist};
use bevy::app::{App, Plugin};
use bevy::prelude::{Changed, Component, Entity, Query, ResMut, Resource, Update};
use std::sync::Arc;

type Registration = Box<dyn Fn(&mut App) + Send + Sync>;

/// A system that automatically marks entities with changed components as dirty.
fn auto_dirty_tracking_system<T: Component + Persist>(
    mut session: ResMut<ArangoSession>,
    query: Query<Entity, Changed<T>>,
) {
    for entity in query.iter() {
        session.dirty_entities.insert(entity);
    }
}

/// A Bevy `Plugin` that sets up `bevy_arangodb`.
pub struct ArangoPlugin {
    db: Arc<dyn DatabaseConnection>,
    registrations: Vec<Registration>,
}

impl ArangoPlugin {
    /// Creates a new `ArangoPlugin` with the given database connection.
    pub fn new(db: Arc<dyn DatabaseConnection>) -> Self {
        Self {
            db,
            registrations: Vec::new(),
        }
    }

    /// Registers a component type for persistence and automatic change tracking.
    pub fn with_component<T: Component + Persist>(mut self) -> Self {
        self.registrations.push(Box::new(|app| {
            app.world_mut()
                .resource_mut::<ArangoSession>()
                .register_component::<T>();
            app.add_systems(Update, auto_dirty_tracking_system::<T>);
        }));
        self
    }

    /// Registers a resource type for persistence.
    pub fn with_resource<R: Resource + Persist>(mut self) -> Self {
        self.registrations.push(Box::new(|app| {
            app.world_mut()
                .resource_mut::<ArangoSession>()
                .register_resource::<R>();
        }));
        self
    }
}

impl Plugin for ArangoPlugin {
    fn build(&self, app: &mut App) {
        let session = ArangoSession::new(self.db.clone());
        app.insert_resource(session);

        for register in &self.registrations {
            register(app);
        }
    }
}
