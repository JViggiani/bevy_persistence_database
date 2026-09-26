//! Replay released lock sections through the real migration steps.

use serde_json::Map;

use crate::core::{
    db::connection::EdgeDocument,
    schema::{
        error::MigrationError,
        history::{SchemaHistory, apply_pending},
        snapshot::StoreSnapshot,
        validate::validate_snapshot,
    },
    session::PersistenceSession,
};

use super::{
    file::{LockStatus, SchemaLockFile, SchemaLockVersion},
    sample::{synthesize, variant_span},
};

pub(crate) fn replay(
    history: &SchemaHistory,
    session: &PersistenceSession,
    file: &SchemaLockFile,
) -> Result<(), MigrationError> {
    for section in file
        .versions
        .iter()
        .filter(|section| section.status == LockStatus::Released)
    {
        let mut snapshot = snapshot_from_section(section);
        apply_pending(history, &mut snapshot, section.version)?;
        validate_snapshot(&snapshot, session).map_err(|error| {
            MigrationError::Lock(format!(
                "replaying released version {} failed: {error}",
                section.version
            ))
        })?;
    }
    Ok(())
}

fn snapshot_from_section(section: &SchemaLockVersion) -> StoreSnapshot {
    let mut span = 1usize;
    for format in section.components.values().chain(section.resources.values()) {
        span = span.max(variant_span(format));
    }
    for relationship in section.relationships.values() {
        if let Some(payload) = &relationship.payload {
            span = span.max(variant_span(payload));
        }
    }
    let mut entities = Vec::new();
    for index in 0..span {
        let mut fields = Map::new();
        for (name, format) in &section.components {
            fields.insert(name.clone(), synthesize(format, index));
        }
        entities.push((format!("replay-{index}"), fields));
    }
    let resources = section
        .resources
        .iter()
        .map(|(name, format)| (name.clone(), synthesize(format, 0)))
        .collect();
    let source = "replay-0".to_string();
    let edges = section
        .relationships
        .iter()
        .map(|(name, relationship)| EdgeDocument {
            key: EdgeDocument::make_key(name, &source, &source),
            relationship_type: name.clone(),
            from_guid: source.clone(),
            to_guid: source.clone(),
            payload: relationship
                .payload
                .as_ref()
                .map(|format| synthesize(format, 0)),
        })
        .collect();
    StoreSnapshot::from_synthesized(entities, resources, edges)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use bevy::prelude::Component;
    use serde::{Deserialize, Serialize};

    use crate::core::{
        schema::{
            error::MigrationError,
            history::{MigrationStep, SchemaHistory},
            snapshot::StoreSnapshot,
        },
        session::PersistenceSession,
    };

    use super::{
        super::{
            file::{LockStatus, RelationshipSchema, SchemaLockFile, SchemaLockVersion},
            trace::trace_type,
        },
        replay,
    };

    #[derive(Serialize, Deserialize)]
    struct Speed {
        value: i32,
    }

    #[derive(Component, Serialize, Deserialize)]
    struct WalkSpeed {
        value: i32,
    }

    struct Forgot;

    impl MigrationStep for Forgot {
        fn from_version(&self) -> u32 {
            1
        }
        fn id(&self) -> &'static str {
            "forgot-rename"
        }
        fn apply(&self, _store: &mut StoreSnapshot) -> Result<(), MigrationError> {
            Ok(())
        }
    }

    struct RenameSpeed;

    impl MigrationStep for RenameSpeed {
        fn from_version(&self) -> u32 {
            1
        }
        fn id(&self) -> &'static str {
            "rename-speed"
        }
        fn apply(&self, store: &mut StoreSnapshot) -> Result<(), MigrationError> {
            for key in store.entity_keys() {
                let Some(speed) = store
                    .entity(&key)
                    .and_then(|components| components.get("Speed"))
                    .cloned()
                else {
                    continue;
                };
                let mut entity = store.entity_mut(&key).expect("entity");
                entity.remove("Speed");
                entity.insert("WalkSpeed", speed);
            }
            Ok(())
        }
    }

    fn lock_with(step_registered: bool) -> (SchemaHistory, PersistenceSession, SchemaLockFile) {
        let mut session = PersistenceSession::new();
        session.register_component_named::<WalkSpeed>("WalkSpeed");
        let old = trace_type::<Speed>().expect("trace speed");
        let mut old_components = BTreeMap::new();
        old_components.insert("Speed".to_string(), old);
        let mut current_components = BTreeMap::new();
        current_components.insert(
            "WalkSpeed".to_string(),
            trace_type::<WalkSpeed>().expect("trace walk"),
        );
        let file = SchemaLockFile {
            versions: vec![
                SchemaLockVersion {
                    version: 1,
                    status: LockStatus::Released,
                    components: old_components,
                    resources: BTreeMap::new(),
                    relationships: BTreeMap::new(),
                },
                SchemaLockVersion {
                    version: 2,
                    status: LockStatus::Open,
                    components: current_components,
                    resources: BTreeMap::new(),
                    relationships: BTreeMap::<String, RelationshipSchema>::new(),
                },
            ],
        };
        let history = if step_registered {
            SchemaHistory::starting_at(1).step(RenameSpeed)
        } else {
            SchemaHistory::starting_at(1).step(Forgot)
        };
        (history, session, file)
    }

    // GIVEN a released section that still names Speed
    // WHEN replay runs a step that never renames it
    // THEN validation fails and names Speed
    #[test]
    fn replay_catches_a_forgotten_rename() {
        let (history, session, file) = lock_with(false);
        let error = replay(&history, &session, &file).unwrap_err();
        let message = error.to_string();
        assert!(message.contains("Speed"), "{message}");
    }

    // GIVEN a released section that names Speed
    // WHEN the step renames Speed to the registered WalkSpeed
    // THEN replay succeeds
    #[test]
    fn replay_accepts_a_rename_step() {
        let (history, session, file) = lock_with(true);
        replay(&history, &session, &file).expect("replay");
    }
}
