//! Bevy entry points for schema migration.

use std::path::Path;

use bevy::prelude::{App, World};

use crate::{
    bevy::plugins::persistence_plugin::{PersistencePluginConfig, TokioRuntime},
    core::{
        db::connection::DatabaseConnectionResource,
        schema::{
            error::MigrationError,
            history::SchemaHistory,
            lock::{file::prepare_lock_file, replay::replay},
            runner::{MigrateOptions, MigrationMode, MigrationReport, migrate_store},
        },
        session::PersistenceSession,
    },
};

/// Migrate the plugin's default store before hydration.
///
/// Checks the embedded lock, then applies [`MigrationMode`]. A missing
/// persistence plugin is an error so startup can abort the same way a failed
/// load does.
pub fn migrate_world(
    world: &mut World,
    history: &SchemaHistory,
    mode: MigrationMode,
) -> Result<MigrationReport, MigrationError> {
    let store = world
        .get_resource::<PersistencePluginConfig>()
        .ok_or_else(|| MigrationError::Store("PersistencePluginConfig is not installed".into()))?
        .default_store
        .clone();
    let runtime = world
        .get_resource::<TokioRuntime>()
        .ok_or_else(|| MigrationError::Store("TokioRuntime is not installed".into()))?
        .runtime
        .clone();
    let db = world
        .get_resource::<DatabaseConnectionResource>()
        .ok_or_else(|| {
            MigrationError::Store("DatabaseConnectionResource is not installed".into())
        })?
        .connection
        .clone();
    let session = world
        .get_resource::<PersistenceSession>()
        .ok_or_else(|| MigrationError::Store("PersistenceSession is not installed".into()))?;
    runtime.block_on(migrate_store(
        db.as_ref(),
        &store,
        history,
        session,
        MigrateOptions {
            mode,
            dry_run: false,
        },
    ))
}

/// Compare compiled types to the lock file, honor `BEVY_PERSISTENCE_SCHEMA_LOCK`,
/// and replay every released section.
pub fn check_schema_lock(
    path: &str,
    app: &App,
    history: &SchemaHistory,
) -> Result<(), MigrationError> {
    let session = app
        .world()
        .get_resource::<PersistenceSession>()
        .ok_or_else(|| MigrationError::Store("PersistenceSession is not installed".into()))?;
    let file = prepare_lock_file(Path::new(path), history, session)?;
    replay(history, session, &file)
}
