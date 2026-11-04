use bevy_persistence_database_derive::persist;

#[derive(Clone)]
#[persist(resource)]
pub struct GameSettings {
    pub difficulty: f32,
    pub map_name: String,
}

/// The 64-bit integer edges a store must keep exactly.
#[derive(Clone, Debug, PartialEq)]
#[persist(resource)]
pub struct IntegerEdges {
    pub unsigned: Vec<u64>,
    pub signed: Vec<i64>,
}
