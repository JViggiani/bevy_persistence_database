//! Run a [`SchemaHistory`] against one store.

use crate::core::{
    db::connection::DatabaseConnection,
    schema::metadata::SchemaMetadata,
    session::PersistenceSession,
};

use super::{
    error::MigrationError,
    history::{SchemaHistory, apply_pending},
    lock::file::ensure_embedded_lock,
    snapshot::StoreSnapshot,
    validate::validate_snapshot,
};

/// What to do when the stored schema is older than this binary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MigrationMode {
    /// Apply every pending step, then write the result in one transaction.
    OnOpen,
    /// Report that the store is behind. Write nothing.
    RequireCurrent,
}

/// Options for [`migrate_store`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MigrateOptions {
    pub mode: MigrationMode,
    /// Run steps and validation, then write nothing.
    pub dry_run: bool,
}

impl MigrateOptions {
    pub fn on_open() -> Self {
        Self {
            mode: MigrationMode::OnOpen,
            dry_run: false,
        }
    }

    pub fn require_current() -> Self {
        Self {
            mode: MigrationMode::RequireCurrent,
            dry_run: false,
        }
    }
}

/// What a migration did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MigrationReport {
    pub from_version: u32,
    pub to_version: u32,
    pub applied: Vec<String>,
    pub dry_run: bool,
}

/// Migrate `store` to [`SchemaHistory::current_version`].
///
/// Needs no `World`. An admin batch builds a headless app, registers the same
/// types, and calls this. The embedded lock is checked before anything is read.
pub async fn migrate_store(
    db: &dyn DatabaseConnection,
    store: &str,
    history: &SchemaHistory,
    session: &PersistenceSession,
    options: MigrateOptions,
) -> Result<MigrationReport, MigrationError> {
    history.validate_chain()?;
    ensure_embedded_lock(history, session)?;
    let contents = db.read_store(store).await?;
    let loaded = StoreSnapshot::load(contents, session.compact_threshold_bytes())?;
    let current = history.current_version();

    if loaded.schema.is_none() && loaded.is_empty() {
        return stamp(db, store, &loaded.snapshot, current, options).await;
    }

    if loaded.schema.is_none() && history.steps().iter().all(|step| step.from_version() != 0) {
        return Err(MigrationError::UnversionedStore);
    }

    let stored = loaded
        .schema
        .as_ref()
        .map(|meta| meta.schema_version)
        .unwrap_or(0);

    if stored > current {
        return Err(MigrationError::NewerThanBinary { stored, current });
    }
    if stored < history.starting_version() {
        return Err(MigrationError::BelowMinimum {
            stored,
            minimum: history.starting_version(),
        });
    }
    if stored == current {
        return Ok(MigrationReport {
            from_version: stored,
            to_version: current,
            applied: Vec::new(),
            dry_run: options.dry_run,
        });
    }
    if options.mode == MigrationMode::RequireCurrent {
        return Err(MigrationError::BehindRequiresExplicit { stored, current });
    }

    let mut snapshot = loaded.snapshot;
    let applied = apply_pending(history, &mut snapshot, stored)?;
    validate_snapshot(&snapshot, session)?;
    let last_id = applied.last().copied().map(str::to_string);
    if !options.dry_run {
        write_snapshot(db, store, &snapshot, current, last_id.clone()).await?;
    }
    Ok(MigrationReport {
        from_version: stored,
        to_version: current,
        applied: applied.into_iter().map(str::to_string).collect(),
        dry_run: options.dry_run,
    })
}

async fn stamp(
    db: &dyn DatabaseConnection,
    store: &str,
    snapshot: &StoreSnapshot,
    current: u32,
    options: MigrateOptions,
) -> Result<MigrationReport, MigrationError> {
    if !options.dry_run {
        write_snapshot(db, store, snapshot, current, None).await?;
    }
    Ok(MigrationReport {
        from_version: 0,
        to_version: current,
        applied: Vec::new(),
        dry_run: options.dry_run,
    })
}

async fn write_snapshot(
    db: &dyn DatabaseConnection,
    store: &str,
    snapshot: &StoreSnapshot,
    version: u32,
    last_migration_id: Option<String>,
) -> Result<(), MigrationError> {
    let metadata = SchemaMetadata::stamped(version, last_migration_id);
    let operations =
        snapshot.to_operations(store, db.document_key_field(), &metadata, true)?;
    if operations.is_empty() {
        return Ok(());
    }
    db.execute_transaction(operations).await?;
    Ok(())
}
