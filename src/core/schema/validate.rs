//! Check a migrated snapshot against the types registered in this binary.

use crate::core::session::PersistenceSession;

use super::{error::MigrationError, snapshot::StoreSnapshot};

pub(crate) fn validate_snapshot(
    snapshot: &StoreSnapshot,
    session: &PersistenceSession,
) -> Result<(), MigrationError> {
    for key in snapshot.entity_keys() {
        let Some(components) = snapshot.entity(&key) else {
            continue;
        };
        for (name, value) in components {
            let Some(validator) = session.component_validator(name) else {
                return Err(MigrationError::Validation(format!(
                    "entity `{key}` has component `{name}` which is not registered"
                )));
            };
            validator(value).map_err(|error| {
                MigrationError::Validation(format!(
                    "entity `{key}` component `{name}` does not match its type: {error}"
                ))
            })?;
        }
    }
    for name in snapshot.resource_names() {
        let Some(value) = snapshot.resource(&name) else {
            continue;
        };
        let Some(validator) = session.resource_validator(&name) else {
            return Err(MigrationError::Validation(format!(
                "resource `{name}` is not registered"
            )));
        };
        validator(value).map_err(|error| {
            MigrationError::Validation(format!(
                "resource `{name}` does not match its type: {error}"
            ))
        })?;
    }
    let names: Vec<&str> = session.relationship_names().collect();
    for edge in snapshot.edges() {
        if !names.contains(&edge.relationship_type.as_str()) {
            return Err(MigrationError::Validation(format!(
                "edge `{}` has relationship `{}` which is not registered",
                edge.key, edge.relationship_type
            )));
        }
        match (
            session.relationship_payload_validator(&edge.relationship_type),
            &edge.payload,
        ) {
            (Some(validator), Some(payload)) => validator(payload).map_err(|error| {
                MigrationError::Validation(format!(
                    "edge `{}` payload does not match `{}`: {error}",
                    edge.key, edge.relationship_type
                ))
            })?,
            (Some(_), None) => {
                return Err(MigrationError::Validation(format!(
                    "edge `{}` is missing a payload for `{}`",
                    edge.key, edge.relationship_type
                )));
            }
            (None, Some(_)) => {
                return Err(MigrationError::Validation(format!(
                    "edge `{}` has a payload but `{}` does not",
                    edge.key, edge.relationship_type
                )));
            }
            (None, None) => {}
        }
    }
    Ok(())
}
