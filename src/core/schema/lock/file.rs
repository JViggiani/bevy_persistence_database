//! The committed schema lock: one section per schema version.

use std::{
    collections::{BTreeMap, HashSet},
    fs,
    path::Path,
};

use serde::{Deserialize, Serialize};

use crate::core::{
    schema::{error::MigrationError, history::SchemaHistory},
    session::PersistenceSession,
};

use super::trace::SchemaFormat;

/// Environment variable that rewrites the lock file.
///
/// `update` regenerates the open section for the current version.
/// `release` marks that section released.
pub const SCHEMA_LOCK_ENV: &str = "BEVY_PERSISTENCE_SCHEMA_LOCK";

/// Whether a lock section has shipped.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum LockStatus {
    Released,
    Open,
}

/// Payload shape of one relationship, when the relationship carries one.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RelationshipSchema {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub payload: Option<SchemaFormat>,
}

/// One schema version in the lock file.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SchemaLockVersion {
    pub version: u32,
    pub status: LockStatus,
    pub components: BTreeMap<String, SchemaFormat>,
    pub resources: BTreeMap<String, SchemaFormat>,
    pub relationships: BTreeMap<String, RelationshipSchema>,
}

/// Pretty JSON lock file.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SchemaLockFile {
    pub versions: Vec<SchemaLockVersion>,
}

impl SchemaLockFile {
    pub fn to_pretty(&self) -> Result<String, MigrationError> {
        let mut text = serde_json::to_string_pretty(self)
            .map_err(|error| MigrationError::Lock(format!("schema lock: {error}")))?;
        text.push('\n');
        Ok(text)
    }
}

pub(crate) fn compiled_lock_version(
    session: &PersistenceSession,
    version: u32,
    status: LockStatus,
) -> Result<SchemaLockVersion, MigrationError> {
    let payloads = session
        .traced_relationship_payloads()
        .map_err(MigrationError::Lock)?;
    let mut relationships = BTreeMap::new();
    for (name, payload) in payloads {
        relationships.insert(name, RelationshipSchema { payload });
    }
    Ok(SchemaLockVersion {
        version,
        status,
        components: session.traced_components().map_err(MigrationError::Lock)?,
        resources: session.traced_resources().map_err(MigrationError::Lock)?,
        relationships,
    })
}

pub(crate) fn parse_lock(source: &str) -> Result<SchemaLockFile, MigrationError> {
    let file: SchemaLockFile = serde_json::from_str(source)
        .map_err(|error| MigrationError::Lock(format!("schema lock: {error}")))?;
    normalize(file)
}

pub(crate) fn ensure_embedded_lock(
    history: &SchemaHistory,
    session: &PersistenceSession,
) -> Result<(), MigrationError> {
    let source = history.lock_source().ok_or_else(|| {
        MigrationError::Lock(
            "SchemaHistory has no schema lock; call .lock(include_str!(...))".to_string(),
        )
    })?;
    let file = parse_lock(source)?;
    let compiled = compiled_lock_version(session, history.current_version(), LockStatus::Open)?;
    verify_lock(&file, &compiled, history)
}

/// Load `path`, apply [`SCHEMA_LOCK_ENV`] when it is set, and return the file
/// that matches `history`.
pub(crate) fn prepare_lock_file(
    path: &Path,
    history: &SchemaHistory,
    session: &PersistenceSession,
) -> Result<SchemaLockFile, MigrationError> {
    history.validate_chain()?;
    let compiled = compiled_lock_version(session, history.current_version(), LockStatus::Open)?;
    let file = match std::env::var(SCHEMA_LOCK_ENV) {
        Ok(command) => apply_command(path, command.trim(), &compiled, history)?,
        Err(_) => {
            if !path.exists() {
                return Err(MigrationError::Lock(format!(
                    "schema lock {} is missing; run with {SCHEMA_LOCK_ENV}=update",
                    path.display()
                )));
            }
            parse_lock(&fs::read_to_string(path).map_err(|error| {
                MigrationError::Lock(format!("schema lock {}: {error}", path.display()))
            })?)?
        }
    };
    verify_lock(&file, &compiled, history)?;
    Ok(file)
}

fn apply_command(
    path: &Path,
    command: &str,
    compiled: &SchemaLockVersion,
    history: &SchemaHistory,
) -> Result<SchemaLockFile, MigrationError> {
    let mut file = if path.exists() {
        parse_lock(&fs::read_to_string(path).map_err(|error| {
            MigrationError::Lock(format!("schema lock {}: {error}", path.display()))
        })?)?
    } else {
        SchemaLockFile { versions: Vec::new() }
    };
    match command {
        "update" => update_open(&mut file, compiled, history)?,
        "release" => release_current(&mut file, compiled, history)?,
        other => {
            return Err(MigrationError::Lock(format!(
                "{SCHEMA_LOCK_ENV}={other:?} is not update or release"
            )));
        }
    }
    fs::write(path, file.to_pretty()?).map_err(|error| {
        MigrationError::Lock(format!("schema lock {}: {error}", path.display()))
    })?;
    Ok(file)
}

fn update_open(
    file: &mut SchemaLockFile,
    compiled: &SchemaLockVersion,
    history: &SchemaHistory,
) -> Result<(), MigrationError> {
    let current = history.current_version();
    if let Some(existing) = file.versions.iter().find(|section| section.version == current) {
        if existing.status == LockStatus::Released && !same_shape(existing, compiled) {
            return Err(shipped(current, existing, compiled));
        }
        if existing.status == LockStatus::Released {
            return Ok(());
        }
    } else {
        for version in history.starting_version()..current {
            let Some(section) = file.versions.iter().find(|section| section.version == version) else {
                return Err(MigrationError::Lock(format!(
                    "schema lock is missing released version {version}"
                )));
            };
            if section.status != LockStatus::Released {
                return Err(MigrationError::Lock(format!(
                    "version {version} is still open; release it before adding a step"
                )));
            }
        }
    }
    let section = SchemaLockVersion {
        version: current,
        status: LockStatus::Open,
        components: compiled.components.clone(),
        resources: compiled.resources.clone(),
        relationships: compiled.relationships.clone(),
    };
    if let Some(existing) = file.versions.iter_mut().find(|item| item.version == current) {
        *existing = section;
    } else {
        file.versions.push(section);
    }
    file.versions.sort_by_key(|section| section.version);
    Ok(())
}

fn release_current(
    file: &mut SchemaLockFile,
    compiled: &SchemaLockVersion,
    history: &SchemaHistory,
) -> Result<(), MigrationError> {
    let current = history.current_version();
    let Some(section) = file.versions.iter_mut().find(|section| section.version == current) else {
        return Err(MigrationError::Lock(format!(
            "schema lock has no version {current}; run with {SCHEMA_LOCK_ENV}=update"
        )));
    };
    if !same_shape(section, compiled) {
        return Err(if section.status == LockStatus::Released {
            shipped(current, section, compiled)
        } else {
            MigrationError::Lock(format!(
                "schema lock is stale; run with {SCHEMA_LOCK_ENV}=update\n{}",
                shape_diff(section, compiled)
            ))
        });
    }
    section.status = LockStatus::Released;
    Ok(())
}

pub(crate) fn verify_lock(
    file: &SchemaLockFile,
    compiled: &SchemaLockVersion,
    history: &SchemaHistory,
) -> Result<(), MigrationError> {
    let expected: Vec<u32> = (history.starting_version()..=history.current_version()).collect();
    let got: Vec<u32> = file.versions.iter().map(|section| section.version).collect();
    if got != expected {
        return Err(MigrationError::Lock(format!(
            "schema lock versions {got:?} do not match the history range {expected:?}"
        )));
    }
    let open: Vec<u32> = file
        .versions
        .iter()
        .filter(|section| section.status == LockStatus::Open)
        .map(|section| section.version)
        .collect();
    if open.len() > 1 {
        return Err(MigrationError::Lock(format!(
            "schema lock has more than one open version: {open:?}"
        )));
    }
    if let Some(open_version) = open.first() {
        if *open_version != history.current_version() {
            return Err(MigrationError::Lock(format!(
                "open schema lock version {open_version} is not the current version {}",
                history.current_version()
            )));
        }
    }
    let current = file
        .versions
        .iter()
        .find(|section| section.version == history.current_version())
        .expect("current version is in the checked range");
    if same_shape(current, compiled) {
        return Ok(());
    }
    if current.status == LockStatus::Released {
        return Err(shipped(current.version, current, compiled));
    }
    Err(MigrationError::Lock(format!(
        "schema lock is stale; run with {SCHEMA_LOCK_ENV}=update\n{}",
        shape_diff(current, compiled)
    )))
}

fn shipped(version: u32, locked: &SchemaLockVersion, compiled: &SchemaLockVersion) -> MigrationError {
    MigrationError::Lock(format!(
        "version {version} shipped: add a step from {version}\n{}",
        shape_diff(locked, compiled)
    ))
}

fn same_shape(locked: &SchemaLockVersion, compiled: &SchemaLockVersion) -> bool {
    locked.components == compiled.components
        && locked.resources == compiled.resources
        && locked.relationships == compiled.relationships
}

fn shape_diff(locked: &SchemaLockVersion, compiled: &SchemaLockVersion) -> String {
    let mut lines = Vec::new();
    diff_map("component", &locked.components, &compiled.components, &mut lines);
    diff_map("resource", &locked.resources, &compiled.resources, &mut lines);
    diff_rel(
        &locked.relationships,
        &compiled.relationships,
        &mut lines,
    );
    if lines.is_empty() {
        lines.push("shapes match".to_string());
    }
    lines.join("\n")
}

fn diff_map(
    kind: &str,
    locked: &BTreeMap<String, SchemaFormat>,
    compiled: &BTreeMap<String, SchemaFormat>,
    lines: &mut Vec<String>,
) {
    for (name, format) in locked {
        match compiled.get(name) {
            None => lines.push(format!("{kind} `{name}` was removed")),
            Some(compiled_format) if compiled_format != format => lines.push(format!(
                "{kind} `{name}` changed\n  lock: {}\n  compiled: {}",
                format_json(format),
                format_json(compiled_format)
            )),
            Some(_) => {}
        }
    }
    for name in compiled.keys() {
        if !locked.contains_key(name) {
            lines.push(format!("{kind} `{name}` was added"));
        }
    }
}

fn diff_rel(
    locked: &BTreeMap<String, RelationshipSchema>,
    compiled: &BTreeMap<String, RelationshipSchema>,
    lines: &mut Vec<String>,
) {
    for (name, format) in locked {
        match compiled.get(name) {
            None => lines.push(format!("relationship `{name}` was removed")),
            Some(compiled_format) if compiled_format != format => {
                lines.push(format!("relationship `{name}` changed"))
            }
            Some(_) => {}
        }
    }
    for name in compiled.keys() {
        if !locked.contains_key(name) {
            lines.push(format!("relationship `{name}` was added"));
        }
    }
}

fn format_json(format: &SchemaFormat) -> String {
    serde_json::to_string(format).unwrap_or_else(|_| "<unprintable>".to_string())
}

fn normalize(mut file: SchemaLockFile) -> Result<SchemaLockFile, MigrationError> {
    file.versions.sort_by_key(|section| section.version);
    let mut seen = HashSet::new();
    for section in &file.versions {
        if !seen.insert(section.version) {
            return Err(MigrationError::Lock(format!(
                "duplicate lock version {}",
                section.version
            )));
        }
    }
    Ok(file)
}

#[cfg(test)]
mod tests {
    use bevy::prelude::Component;
    use serde::{Deserialize, Serialize};

    use crate::core::session::PersistenceSession;

    use super::*;

    fn section(version: u32, status: LockStatus, component: &str) -> SchemaLockVersion {
        let mut components = BTreeMap::new();
        components.insert(
            component.to_string(),
            SchemaFormat::Struct {
                name: component.to_string(),
                fields: vec![("value".to_string(), SchemaFormat::I32)],
            },
        );
        SchemaLockVersion {
            version,
            status,
            components,
            resources: BTreeMap::new(),
            relationships: BTreeMap::new(),
        }
    }

    // GIVEN a released lock section whose component set differs from the compiled schema
    // WHEN the lock is verified
    // THEN the error tells the caller to add a step from that version
    #[test]
    fn released_drift_asks_for_a_step() {
        let history = SchemaHistory::starting_at(1);
        let file = SchemaLockFile {
            versions: vec![section(1, LockStatus::Released, "Speed")],
        };
        let compiled = section(1, LockStatus::Open, "WalkSpeed");
        let error = verify_lock(&file, &compiled, &history).unwrap_err();
        let message = error.to_string();
        assert!(message.contains("add a step from 1"), "{message}");
        assert!(message.contains("Speed"), "{message}");
        assert!(message.contains("WalkSpeed"), "{message}");
    }

    #[derive(Component, Serialize, Deserialize)]
    #[allow(dead_code)]
    struct Health {
        value: i32,
    }

    // GIVEN a history at version 1 and no lock section yet
    // WHEN the open section is written and then released
    // THEN a later shape change is refused until a step is added
    #[test]
    fn update_then_release_then_refuse_a_shipped_change() {
        let mut session = PersistenceSession::new();
        session.register_component_named::<Health>("Health");
        let history = SchemaHistory::starting_at(1);
        let compiled = compiled_lock_version(&session, history.current_version(), LockStatus::Open)
            .expect("trace");
        let mut file = SchemaLockFile { versions: Vec::new() };

        update_open(&mut file, &compiled, &history).expect("update");
        assert_eq!(file.versions[0].status, LockStatus::Open);
        assert!(file.versions[0].components.contains_key("Health"));

        release_current(&mut file, &compiled, &history).expect("release");
        assert_eq!(file.versions[0].status, LockStatus::Released);
        verify_lock(&file, &compiled, &history).expect("matches");

        let drifted = section(1, LockStatus::Open, "Other");
        let error = update_open(&mut file, &drifted, &history).expect_err("shipped");
        assert!(error.to_string().contains("add a step from 1"), "{error}");
    }
}
