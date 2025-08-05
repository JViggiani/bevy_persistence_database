#[derive(Clone)]
#[bevy_arangodb::persist(resource)]
pub struct GameSettings {
    pub difficulty: f32,
    pub map_name: String,
}
