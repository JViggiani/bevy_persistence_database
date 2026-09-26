//! Schema migration and storage-parity checks against both backends.

use std::{collections::BTreeMap, sync::Arc};

use bevy::prelude::Component;
use bevy_persistence_database::{
    MigrationError, MigrationStep, MigrateOptions, SchemaHistory, StoreSnapshot, migrate_store,
    core::{
        db::{
            connection::{
                BEVY_PERSISTENCE_DATABASE_BEVY_TYPE_FIELD, BEVY_PERSISTENCE_DATABASE_METADATA_FIELD,
                BEVY_PERSISTENCE_DATABASE_VERSION_FIELD, DocumentKind, StoreContents,
                TransactionOperation,
            },
            DatabaseConnection,
        },
        schema::{
            lock::{
                file::{LockStatus, SchemaLockFile, SchemaLockVersion},
                trace::SchemaFormat,
            },
            metadata::SCHEMA_DOCUMENT_KEY,
            runner::MigrationMode,
        },
        session::PersistenceSession,
    },
};
use bevy_persistence_database_derive::db_matrix_test;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};

use crate::common::{TEST_STORE, run_async};

#[derive(Component, Serialize, Deserialize)]
struct Health {
    value: i32,
}

struct SetHealth;

impl MigrationStep for SetHealth {
    fn from_version(&self) -> u32 {
        1
    }
    fn id(&self) -> &'static str {
        "set-health"
    }
    fn apply(&self, store: &mut StoreSnapshot) -> Result<(), MigrationError> {
        let mut entity = store.entity_mut("hero").expect("hero");
        entity.insert("Health", json!({"value": 2}));
        Ok(())
    }
}

struct AddHealthNote;

impl MigrationStep for AddHealthNote {
    fn from_version(&self) -> u32 {
        2
    }
    fn id(&self) -> &'static str {
        "add-note"
    }
    fn apply(&self, store: &mut StoreSnapshot) -> Result<(), MigrationError> {
        let mut health = store
            .entity("hero")
            .and_then(|components| components.get("Health"))
            .cloned()
            .unwrap_or_else(|| json!({}));
        health["note"] = json!("kept");
        store
            .entity_mut("hero")
            .expect("hero")
            .insert("Health", health);
        Ok(())
    }
}

struct InsertGhost;

impl MigrationStep for InsertGhost {
    fn from_version(&self) -> u32 {
        1
    }
    fn id(&self) -> &'static str {
        "insert-ghost"
    }
    fn apply(&self, store: &mut StoreSnapshot) -> Result<(), MigrationError> {
        let mut entity = store.entity_mut("hero").expect("hero");
        entity.insert("Health", json!({"value": 7}));
        entity.insert("Ghost", json!({"value": 1}));
        Ok(())
    }
}

struct DropExtra;

impl MigrationStep for DropExtra {
    fn from_version(&self) -> u32 {
        1
    }
    fn id(&self) -> &'static str {
        "drop-extra"
    }
    fn apply(&self, store: &mut StoreSnapshot) -> Result<(), MigrationError> {
        store
            .entity_mut("hero")
            .expect("hero")
            .remove("Extra");
        Ok(())
    }
}

struct ConcurrentBump {
    db: Arc<dyn DatabaseConnection>,
}

impl MigrationStep for ConcurrentBump {
    fn from_version(&self) -> u32 {
        1
    }
    fn id(&self) -> &'static str {
        "concurrent-bump"
    }
    fn apply(&self, store: &mut StoreSnapshot) -> Result<(), MigrationError> {
        let db = Arc::clone(&self.db);
        let joined = std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("runtime");
            runtime.block_on(async move {
                db.execute_transaction(vec![TransactionOperation::UpdateDocument {
                    store: TEST_STORE.to_string(),
                    kind: DocumentKind::Entity,
                    key: "hero".to_string(),
                    expected_current_version: 1,
                    patch: json!({
                        "Health": {"value": 99},
                        BEVY_PERSISTENCE_DATABASE_METADATA_FIELD: {
                            BEVY_PERSISTENCE_DATABASE_VERSION_FIELD: 2,
                            BEVY_PERSISTENCE_DATABASE_BEVY_TYPE_FIELD: DocumentKind::Entity.as_ref(),
                        }
                    }),
                }])
                .await
            })
        })
        .join()
        .expect("bump thread");
        joined?;
        store
            .entity_mut("hero")
            .expect("hero")
            .insert("Health", json!({"value": 2}));
        Ok(())
    }
}

fn health_components() -> BTreeMap<String, SchemaFormat> {
    let mut components = BTreeMap::new();
    components.insert(
        "Health".to_string(),
        SchemaFormat::Struct {
            name: "Health".to_string(),
            fields: vec![("value".to_string(), SchemaFormat::I32)],
        },
    );
    components
}

fn lock_source(start: u32, current: u32, components: BTreeMap<String, SchemaFormat>) -> String {
    let versions = (start..=current)
        .map(|version| SchemaLockVersion {
            version,
            status: if version == current {
                LockStatus::Open
            } else {
                LockStatus::Released
            },
            components: components.clone(),
            resources: BTreeMap::new(),
            relationships: BTreeMap::new(),
        })
        .collect();
    SchemaLockFile { versions }
        .to_pretty()
        .expect("lock")
}

fn session_with_health() -> PersistenceSession {
    let mut session = PersistenceSession::new();
    session.register_component_named::<Health>("Health");
    session
}

fn commit(db: &dyn DatabaseConnection, operations: Vec<TransactionOperation>) {
    let result = run_async(db.execute_transaction(operations));
    assert!(result.is_ok(), "{result:?}");
}

fn read(db: &dyn DatabaseConnection) -> StoreContents {
    run_async(db.read_store(TEST_STORE)).expect("read store")
}

fn document(key_field: &str, key: &str, kind: DocumentKind, occ: u64, mut body: Map<String, Value>) -> Value {
    body.insert(key_field.to_string(), Value::String(key.to_string()));
    body.insert(
        BEVY_PERSISTENCE_DATABASE_METADATA_FIELD.to_string(),
        json!({
            BEVY_PERSISTENCE_DATABASE_VERSION_FIELD: occ,
            BEVY_PERSISTENCE_DATABASE_BEVY_TYPE_FIELD: kind.as_ref(),
        }),
    );
    Value::Object(body)
}

fn schema_body(version: u32) -> Map<String, Value> {
    let mut body = Map::new();
    body.insert("schema_version".to_string(), json!(version));
    body
}

fn create(
    db: &dyn DatabaseConnection,
    kind: DocumentKind,
    key: &str,
    occ: u64,
    body: Map<String, Value>,
) -> TransactionOperation {
    TransactionOperation::CreateDocument {
        store: TEST_STORE.to_string(),
        kind,
        data: document(db.document_key_field(), key, kind, occ, body),
    }
}

fn schema_version(contents: &StoreContents) -> Option<(u32, u64)> {
    contents.documents.iter().find_map(|document| {
        if document.kind != DocumentKind::Schema {
            return None;
        }
        let version = document.body.get("schema_version")?.as_u64()? as u32;
        Some((version, document.version))
    })
}

fn entity<'a>(contents: &'a StoreContents, key: &str) -> Option<&'a Value> {
    contents
        .documents
        .iter()
        .find(|document| document.kind == DocumentKind::Entity && document.key == key)
        .map(|document| &document.body)
}

// GIVEN an empty store and a history at version 1
// WHEN migrate_store runs
// THEN the store is stamped at the current version
#[db_matrix_test]
fn empty_store_is_stamped() {
    let (db, _container) = setup();
    let session = PersistenceSession::new();
    let history = SchemaHistory::starting_at(1).lock(lock_source(1, 1, BTreeMap::new()));
    let report = run_async(migrate_store(
        db.as_ref(),
        TEST_STORE,
        &history,
        &session,
        MigrateOptions::on_open(),
    ))
    .expect("stamp");
    assert_eq!(report.to_version, 1);
    assert!(report.applied.is_empty());
    assert_eq!(schema_version(&read(db.as_ref())), Some((1, 1)));
}

// GIVEN a store already at the current version
// WHEN migrate_store runs
// THEN nothing is written
#[db_matrix_test]
fn equal_version_is_a_no_op() {
    let (db, _container) = setup();
    commit(
        db.as_ref(),
        vec![create(
            db.as_ref(),
            DocumentKind::Schema,
            SCHEMA_DOCUMENT_KEY,
            4,
            schema_body(1),
        )],
    );
    let session = PersistenceSession::new();
    let history = SchemaHistory::starting_at(1).lock(lock_source(1, 1, BTreeMap::new()));
    let report = run_async(migrate_store(
        db.as_ref(),
        TEST_STORE,
        &history,
        &session,
        MigrateOptions::on_open(),
    ))
    .expect("noop");
    assert!(report.applied.is_empty());
    assert_eq!(schema_version(&read(db.as_ref())), Some((1, 4)));
}

// GIVEN a store newer than this binary
// WHEN migrate_store runs
// THEN it fails and leaves the schema document alone
#[db_matrix_test]
fn newer_store_fails() {
    let (db, _container) = setup();
    commit(
        db.as_ref(),
        vec![create(
            db.as_ref(),
            DocumentKind::Schema,
            SCHEMA_DOCUMENT_KEY,
            1,
            schema_body(9),
        )],
    );
    let session = PersistenceSession::new();
    let history = SchemaHistory::starting_at(1).lock(lock_source(1, 1, BTreeMap::new()));
    let error = run_async(migrate_store(
        db.as_ref(),
        TEST_STORE,
        &history,
        &session,
        MigrateOptions::on_open(),
    ))
    .expect_err("newer");
    assert!(error.to_string().contains("newer"), "{error}");
    assert_eq!(schema_version(&read(db.as_ref())), Some((9, 1)));
}

// GIVEN a store behind the binary and RequireCurrent
// WHEN migrate_store runs
// THEN it fails and writes nothing
#[db_matrix_test]
fn behind_store_requires_an_explicit_migration() {
    let (db, _container) = setup();
    commit(
        db.as_ref(),
        vec![create(
            db.as_ref(),
            DocumentKind::Schema,
            SCHEMA_DOCUMENT_KEY,
            1,
            schema_body(1),
        )],
    );
    let session = PersistenceSession::new();
    let history = SchemaHistory::starting_at(1)
        .step(SetHealth)
        .lock(lock_source(1, 2, BTreeMap::new()));
    let error = run_async(migrate_store(
        db.as_ref(),
        TEST_STORE,
        &history,
        &session,
        MigrateOptions::require_current(),
    ))
    .expect_err("behind");
    assert!(error.to_string().contains("explicit"), "{error}");
    assert_eq!(schema_version(&read(db.as_ref())), Some((1, 1)));
}

// GIVEN a non-empty store with no schema document and no step from version 0
// WHEN migrate_store runs
// THEN it fails and the entity remains
#[db_matrix_test]
fn unversioned_store_without_a_baseline_step_fails() {
    let (db, _container) = setup();
    let mut body = Map::new();
    body.insert("Health".to_string(), json!({"value": 1}));
    commit(
        db.as_ref(),
        vec![create(db.as_ref(), DocumentKind::Entity, "hero", 1, body)],
    );
    let session = PersistenceSession::new();
    let history = SchemaHistory::starting_at(1).lock(lock_source(1, 1, BTreeMap::new()));
    let error = run_async(migrate_store(
        db.as_ref(),
        TEST_STORE,
        &history,
        &session,
        MigrateOptions::on_open(),
    ))
    .expect_err("unversioned");
    assert!(error.to_string().contains("version 0"), "{error}");
    assert!(entity(&read(db.as_ref()), "hero").is_some());
    assert!(schema_version(&read(db.as_ref())).is_none());
}

// GIVEN two pending steps
// WHEN migrate_store runs
// THEN both changes and the new schema version commit together
#[db_matrix_test]
fn chained_steps_commit_together() {
    let (db, _container) = setup();
    let mut body = Map::new();
    body.insert("Health".to_string(), json!({"value": 1}));
    commit(
        db.as_ref(),
        vec![
            create(db.as_ref(), DocumentKind::Entity, "hero", 1, body),
            create(
                db.as_ref(),
                DocumentKind::Schema,
                SCHEMA_DOCUMENT_KEY,
                1,
                schema_body(1),
            ),
        ],
    );
    let session = session_with_health();
    let history = SchemaHistory::starting_at(1)
        .step(SetHealth)
        .step(AddHealthNote)
        .lock(lock_source(1, 3, health_components()));
    let report = run_async(migrate_store(
        db.as_ref(),
        TEST_STORE,
        &history,
        &session,
        MigrateOptions::on_open(),
    ))
    .expect("chain");
    assert_eq!(report.applied, vec!["set-health".to_string(), "add-note".to_string()]);
    let contents = read(db.as_ref());
    assert_eq!(schema_version(&contents), Some((3, 2)));
    let hero = entity(&contents, "hero").expect("hero");
    assert_eq!(hero["Health"]["value"], 2);
    assert_eq!(hero["Health"]["note"], "kept");
}

// GIVEN a step that leaves an unregistered component
// WHEN migrate_store runs
// THEN validation fails and the store is unchanged
#[db_matrix_test]
fn failed_validation_writes_nothing() {
    let (db, _container) = setup();
    let mut body = Map::new();
    body.insert("Health".to_string(), json!({"value": 1}));
    commit(
        db.as_ref(),
        vec![
            create(db.as_ref(), DocumentKind::Entity, "hero", 1, body),
            create(
                db.as_ref(),
                DocumentKind::Schema,
                SCHEMA_DOCUMENT_KEY,
                1,
                schema_body(1),
            ),
        ],
    );
    let session = session_with_health();
    let history = SchemaHistory::starting_at(1)
        .step(InsertGhost)
        .lock(lock_source(1, 2, health_components()));
    let error = run_async(migrate_store(
        db.as_ref(),
        TEST_STORE,
        &history,
        &session,
        MigrateOptions::on_open(),
    ))
    .expect_err("ghost");
    assert!(error.to_string().contains("Ghost"), "{error}");
    let contents = read(db.as_ref());
    assert_eq!(schema_version(&contents), Some((1, 1)));
    assert_eq!(entity(&contents, "hero").expect("hero")["Health"]["value"], 1);
}

// GIVEN a writer that updates the entity after the migration has read it
// WHEN the migration commits
// THEN optimistic concurrency aborts and the schema version stays put
#[db_matrix_test]
fn concurrent_write_aborts_the_migration() {
    let (db, _container) = setup();
    let mut body = Map::new();
    body.insert("Health".to_string(), json!({"value": 1}));
    commit(
        db.as_ref(),
        vec![
            create(db.as_ref(), DocumentKind::Entity, "hero", 1, body),
            create(
                db.as_ref(),
                DocumentKind::Schema,
                SCHEMA_DOCUMENT_KEY,
                1,
                schema_body(1),
            ),
        ],
    );
    let session = session_with_health();
    let history = SchemaHistory::starting_at(1)
        .step(ConcurrentBump { db: db.clone() })
        .lock(lock_source(1, 2, health_components()));
    let error = run_async(migrate_store(
        db.as_ref(),
        TEST_STORE,
        &history,
        &session,
        MigrateOptions::on_open(),
    ))
    .expect_err("conflict");
    assert!(error.to_string().contains("conflict"), "{error}");
    let contents = read(db.as_ref());
    assert_eq!(schema_version(&contents), Some((1, 1)));
    assert_eq!(entity(&contents, "hero").expect("hero")["Health"]["value"], 99);
}

// GIVEN a step that removes a component key
// WHEN the migration replaces the document
// THEN the key is gone on both backends
#[db_matrix_test]
fn replace_removes_keys() {
    let (db, _container) = setup();
    let mut body = Map::new();
    body.insert("Health".to_string(), json!({"value": 1}));
    body.insert("Extra".to_string(), json!({"value": true}));
    commit(
        db.as_ref(),
        vec![
            create(db.as_ref(), DocumentKind::Entity, "hero", 1, body),
            create(
                db.as_ref(),
                DocumentKind::Schema,
                SCHEMA_DOCUMENT_KEY,
                1,
                schema_body(1),
            ),
        ],
    );
    let session = session_with_health();
    let history = SchemaHistory::starting_at(1)
        .step(DropExtra)
        .lock(lock_source(1, 2, health_components()));
    run_async(migrate_store(
        db.as_ref(),
        TEST_STORE,
        &history,
        &session,
        MigrateOptions::on_open(),
    ))
    .expect("replace");
    let contents = read(db.as_ref());
    let hero = entity(&contents, "hero").expect("hero");
    assert!(hero.get("Extra").is_none(), "{hero}");
    assert_eq!(hero["Health"]["value"], 1);
}

// GIVEN an update patch that replaces one component object
// WHEN it is committed
// THEN nested keys inside that component disappear and sibling components stay
#[db_matrix_test]
fn update_replaces_nested_component_keys() {
    let (db, _container) = setup();
    let mut body = Map::new();
    body.insert("Health".to_string(), json!({"value": 1, "extra": true}));
    body.insert("Marker".to_string(), json!({"present": true}));
    commit(
        db.as_ref(),
        vec![create(db.as_ref(), DocumentKind::Entity, "hero", 1, body)],
    );
    commit(
        db.as_ref(),
        vec![TransactionOperation::UpdateDocument {
            store: TEST_STORE.to_string(),
            kind: DocumentKind::Entity,
            key: "hero".to_string(),
            expected_current_version: 1,
            patch: json!({
                "Health": {"value": 2},
                BEVY_PERSISTENCE_DATABASE_METADATA_FIELD: {
                    BEVY_PERSISTENCE_DATABASE_VERSION_FIELD: 2,
                    BEVY_PERSISTENCE_DATABASE_BEVY_TYPE_FIELD: DocumentKind::Entity.as_ref(),
                }
            }),
        }],
    );
    let contents = read(db.as_ref());
    let hero = entity(&contents, "hero").expect("hero");
    assert_eq!(hero["Health"], json!({"value": 2}), "{hero}");
    assert_eq!(hero["Marker"]["present"], true);
}

// GIVEN entity, resource, and schema documents in one store
// WHEN clear_store removes entities
// THEN the resource and the schema document remain
#[db_matrix_test]
fn clear_store_filters_by_kind() {
    let (db, _container) = setup();
    let mut entity_body = Map::new();
    entity_body.insert("Health".to_string(), json!({"value": 1}));
    let mut resource_body = Map::new();
    resource_body.insert("value".to_string(), json!(3));
    commit(
        db.as_ref(),
        vec![
            create(db.as_ref(), DocumentKind::Entity, "hero", 1, entity_body),
            create(db.as_ref(), DocumentKind::Resource, "Budget", 1, resource_body),
            create(
                db.as_ref(),
                DocumentKind::Schema,
                SCHEMA_DOCUMENT_KEY,
                1,
                schema_body(1),
            ),
        ],
    );
    run_async(db.clear_store(TEST_STORE, DocumentKind::Entity)).expect("clear");
    let contents = read(db.as_ref());
    assert!(entity(&contents, "hero").is_none());
    assert!(contents.documents.iter().any(|document| {
        document.kind == DocumentKind::Resource && document.key == "Budget"
    }));
    assert_eq!(schema_version(&contents), Some((1, 1)));
}

// GIVEN a pending step and dry_run
// WHEN migrate_store runs
// THEN the report lists the step and the store is unchanged
#[db_matrix_test]
fn dry_run_writes_nothing() {
    let (db, _container) = setup();
    let mut body = Map::new();
    body.insert("Health".to_string(), json!({"value": 1}));
    commit(
        db.as_ref(),
        vec![
            create(db.as_ref(), DocumentKind::Entity, "hero", 1, body),
            create(
                db.as_ref(),
                DocumentKind::Schema,
                SCHEMA_DOCUMENT_KEY,
                1,
                schema_body(1),
            ),
        ],
    );
    let session = session_with_health();
    let history = SchemaHistory::starting_at(1)
        .step(SetHealth)
        .lock(lock_source(1, 2, health_components()));
    let report = run_async(migrate_store(
        db.as_ref(),
        TEST_STORE,
        &history,
        &session,
        MigrateOptions {
            mode: MigrationMode::OnOpen,
            dry_run: true,
        },
    ))
    .expect("dry run");
    assert!(report.dry_run);
    assert_eq!(report.applied, vec!["set-health".to_string()]);
    let contents = read(db.as_ref());
    assert_eq!(schema_version(&contents), Some((1, 1)));
    assert_eq!(entity(&contents, "hero").expect("hero")["Health"]["value"], 1);
}
