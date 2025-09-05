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
