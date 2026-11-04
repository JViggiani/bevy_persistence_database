//! Requests persisted relationship edges for entities already in the world.
//!
//! Each call queues a load; the edges (and any endpoint entity not yet loaded) arrive when
//! the system's commands apply. A repeat request for the same type, sources and depth is a
//! cache hit.

use std::any::TypeId;

use bevy::ecs::query::{QueryData, QueryFilter};
use bevy::ecs::system::SystemParam;
use bevy::prelude::{Commands, Entity, Query, Res, World};

use crate::bevy::plugins::persistence_plugin::PersistencePluginConfig;

use super::query::loader::{RelationshipLoad, RelationshipTypes};

#[derive(SystemParam)]
pub struct PersistenceRelationshipHydrator<'w, 's> {
    commands: Commands<'w, 's>,
    config: Res<'w, PersistencePluginConfig>,
}

impl<'w, 's> PersistenceRelationshipHydrator<'w, 's> {
    pub fn load<R: Send + Sync + 'static>(&mut self, entity: Entity, depth: usize) {
        self.load_many::<R>(&[entity], depth);
    }

    pub fn load_all(&mut self, entity: Entity, depth: usize) {
        self.load_all_many(&[entity], depth);
    }

    pub fn load_many<R: Send + Sync + 'static>(&mut self, entities: &[Entity], depth: usize) {
        self.request(RelationshipTypes::One(TypeId::of::<R>()), entities, depth);
    }

    pub fn load_all_many(&mut self, entities: &[Entity], depth: usize) {
        self.request(RelationshipTypes::AllRegistered, entities, depth);
    }

    pub fn load_for_typed<R: Send + Sync + 'static, Q: QueryData, F: QueryFilter>(
        &mut self,
        query: &Query<(Entity, Q), F>,
        depth: usize,
    ) {
        let entities: Vec<Entity> = query.iter().map(|(entity, _)| entity).collect();
        self.load_many::<R>(&entities, depth);
    }

    pub fn load_for<Q: QueryData, F: QueryFilter>(
        &mut self,
        query: &Query<(Entity, Q), F>,
        depth: usize,
    ) {
        let entities: Vec<Entity> = query.iter().map(|(entity, _)| entity).collect();
        self.load_all_many(&entities, depth);
    }

    fn request(&mut self, types: RelationshipTypes, entities: &[Entity], depth: usize) {
        if depth == 0 || entities.is_empty() {
            return;
        }
        let load = RelationshipLoad {
            types,
            entities: entities.to_vec(),
            depth,
            store: self.config.default_store.clone(),
        };
        self.commands.queue(move |world: &mut World| load.run(world));
    }
}
