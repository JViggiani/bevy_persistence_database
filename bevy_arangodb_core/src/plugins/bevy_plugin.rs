//! A Bevy `Plugin` for integrating `bevy_arangodb`.
//!
//! This plugin simplifies the setup process by managing the `ArangoSession`
//! as a resource and automatically adding systems for change detection.

use crate::registration::COMPONENT_REGISTRY;
use crate::{ArangoSession, DatabaseConnection, Guid, Persist};
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
use std::any::TypeId;
use std::collections::HashSet;
use std::sync::Arc;

/// A resource used to track which `Persist` types have been registered with an `App`.
/// This prevents duplicate systems from being added.
#[derive(Resource, Default)]
pub struct RegisteredPersistTypes(pub HashSet<TypeId>);

/// A system that automatically marks entities with changed components as dirty.
pub fn auto_dirty_tracking_entity_system<T: Component + Persist>(
    mut session: ResMut<ArangoSession>,
    query: Query<Entity, Changed<T>>,
) {
    for entity in query.iter() {
        session.dirty_entities.insert(entity);
    }
}

/// A system that automatically marks changed resources as dirty.
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
    for entity in removed.read() {
        session.mark_despawned(entity);
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
        let session = ArangoSession::new(self.db.clone());
        app.insert_resource(session);
        app.init_resource::<RegisteredPersistTypes>();

        // Drain the registration functions from the global registry to ensure they are only
        // processed once per plugin instance.
        let registry = COMPONENT_REGISTRY.lock().unwrap();
        // Clone the registry so that each App/Plugin instance gets all registrations.
        let registrations = registry.clone();
        // Drop the lock before iterating
        drop(registry);

        // Call each registration fn
        for reg in registrations {
            reg(app);
        }

        // Add the despawn tracking system.
        app.add_systems(PostUpdate, auto_despawn_tracking_system);
    }
}
