use bevy::reflect::Reflect;

// Example component which derives additional traits - not required for the tests, but shows how to use the derive macro.
#[derive(Clone, Debug, Reflect)]
#[bevy_arangodb::persist(component)]
pub struct Health {
    pub value: i32,
}

#[derive(Clone)]
#[bevy_arangodb::persist(component)]
pub struct Position {
    pub x: f32,
    pub y: f32,
}

#[bevy_arangodb::persist(component)]
pub struct Creature {
    pub is_screaming: bool,
}

#[bevy_arangodb::persist(component)]
pub struct Inventory {
    pub items: Vec<String>,
}

#[bevy_arangodb::persist(component)]
pub struct OptionalData {
    pub data: Option<String>,
}

#[bevy_arangodb::persist(component)]
pub struct PlayerName {
    pub name: String,
}
