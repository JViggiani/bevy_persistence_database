use bevy_persistence_database_derive::persist;

#[derive(Clone)]
#[persist(resource)]
pub struct GameSettings {
    pub difficulty: f32,
    pub map_name: String,
}
