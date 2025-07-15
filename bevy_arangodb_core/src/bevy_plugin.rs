//! A Bevy `Plugin` for integrating `bevy_arangodb`.
//!
//! This plugin simplifies the setup process by managing the `ArangoSession`
//! as a resource and automatically adding systems for change detection.

use crate::{registration, ArangoSession, DatabaseConnection, Guid, Persist};
use bevy::{
    app::{App, Plugin, PostUpdate},
    ecs::{
        change_detection::DetectChanges,
        component::Component,
        entity::Entity,
        query::Changed,
        removal_detection::RemovedComponents,
        system::{Query, Res, ResMut, Resource},
    },
};
use std::sync::Arc;

/// A system that automatically marks entities with changed components as dirty.
#[allow(dead_code)]
pub fn auto_dirty_tracking_entity_system<T: Component + Persist>(
    mut session: ResMut<ArangoSession>,
    query: Query<Entity, Changed<T>>,
) {
    for entity in query.iter() {
        session.dirty_entities.insert(entity);
    }
}

/// A system that automatically marks changed resources as dirty.
#[allow(dead_code)]
pub fn auto_dirty_tracking_resource_system<T: Resource + Persist>(
    mut session: ResMut<ArangoSession>,
    resource: Option<Res<T>>,
) {
    if let Some(resource) = resource {
        if resource.is_changed() {
            session.mark_resource_dirty::<T>();
        }
    }
}

/// A system that automatically marks despawned entities as needing deletion.
fn auto_despawn_tracking_system(
    mut session: ResMut<ArangoSession>,
    mut removed: RemovedComponents<Guid>,
) {
    for r in removed.read() {
        session.mark_despawned(r);
    }
}

/// A Bevy `Plugin` that sets up `bevy_arangodb`.
pub struct ArangoPlugin {
    db: Arc<dyn DatabaseConnection>,
}

impl ArangoPlugin {
    /// Creates a new `ArangoPlugin` with the given database connection.
    pub fn new(db: Arc<dyn DatabaseConnection>) -> Self {
        Self { db }
    }
}

impl Plugin for ArangoPlugin {
    fn build(&self, app: &mut App) {
        let mut session = ArangoSession::new(self.db.clone());
        // Manually register the Guid component, as it's part of the library itself.
        session.register_component::<Guid>();
        app.insert_resource(session);

        // Drain the runtime registry and call each registration fn
        let mut registry = registration::COMPONENT_REGISTRY.lock().unwrap();
        for reg in registry.drain(..) {
            reg(app);
        }

        // Add the despawn tracking system.
        app.add_systems(PostUpdate, auto_despawn_tracking_system);
    }
}
