use bevy::prelude::*;
use bevy::reflect::Reflect;
use bevy_persistence_database_derive::persist;

// Example component which derives additional traits - not required for the tests, but shows how to use the derive macro.
#[derive(Debug, Reflect, Clone)]
#[persist(component)]
pub struct Health {
    pub value: i32,
}

#[derive(Clone)]
#[persist(component)]
pub struct Position {
    pub x: f32,
    pub y: f32,
}

#[persist(component)]
pub struct Creature {
    pub is_screaming: bool,
}

#[persist(component)]
pub struct Inventory {
    pub items: Vec<String>,
}

#[persist(component)]
pub struct OptionalData {
    pub data: Option<String>,
}

#[persist(component)]
pub struct PlayerName {
    pub name: String,
}

#[cfg(feature = "bevy_many_relationship_edges")]
#[persist(relationship)]
pub struct Friendship {
    pub strength: f32,
}

#[cfg(feature = "bevy_many_relationship_edges")]
#[persist(relationship)]
pub struct Ownership {
    pub weight: i32,
}

/// A native Bevy relationship used in non-feature-mode tests.
/// `MemberOf` lives on the source entity and points to the target entity.
#[cfg(not(feature = "bevy_many_relationship_edges"))]
#[persist(relationship)]
#[derive(Component)]
#[relationship(relationship_target = TeamMembers)]
pub struct MemberOf(pub Entity);

#[cfg(not(feature = "bevy_many_relationship_edges"))]
impl From<bevy::prelude::Entity> for MemberOf {
    fn from(entity: bevy::prelude::Entity) -> Self {
        MemberOf(entity)
    }
}

/// The relationship-target counterpart required by Bevy's relationship system.
#[cfg(not(feature = "bevy_many_relationship_edges"))]
#[derive(Component, Default)]
#[relationship_target(relationship = MemberOf)]
pub struct TeamMembers(Vec<Entity>);
