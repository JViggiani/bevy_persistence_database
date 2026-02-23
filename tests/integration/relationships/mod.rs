/// Integration tests for relationship persistence, loading, and raw DB-layer edge operations.
///
/// - `persistence_tests`  — dirty tracking and commit behaviour for both relationship
///   backends (`bevy_many_relationships` and Bevy-native), side by side.
/// - `loading_tests`      — loading persisted relationships back into ECS; covers
///   `bevy_many_relationships` (hydrator, `PersistentQuery`, `schedule_load`) and
///   Bevy-native (`with_relationship_depth` + registered loader) in two `#[cfg]` modules.
/// - `db_tests`           — raw database-layer edge CRUD and query operations,
///   independent of Bevy ECS and the persistence plugin.
pub mod db_tests;
pub mod loading_tests;
pub mod persistence_tests;
