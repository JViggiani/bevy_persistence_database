//! The library-owned schema document stored in each store.

use serde::{Deserialize, Serialize};

use crate::core::db::connection::{
    BEVY_PERSISTENCE_DATABASE_BEVY_TYPE_FIELD, BEVY_PERSISTENCE_DATABASE_METADATA_FIELD,
    BEVY_PERSISTENCE_DATABASE_VERSION_FIELD, DocumentKind,
};
use serde_json::{Map, Value};

/// Document key of the schema record. Registration refuses to hand this name out.
pub const SCHEMA_DOCUMENT_KEY: &str = "__bevy_persistence_schema";

/// Schema version and the last step that advanced it.
///
/// `schema_version` is the save schema. It is not the per-document
/// optimistic-concurrency version.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchemaMetadata {
    pub schema_version: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_migration_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_migrated_at_unix_secs: Option<u64>,
}

impl SchemaMetadata {
    pub fn stamped(schema_version: u32, migration_id: Option<String>) -> Self {
        let last_migrated_at_unix_secs = migration_id.as_ref().map(|_| unix_secs());
        Self {
            schema_version,
            last_migration_id: migration_id,
            last_migrated_at_unix_secs,
        }
    }
}

pub(crate) fn unix_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| elapsed.as_secs())
        .unwrap_or(0)
}

/// Full document written for a schema record, including persistence metadata.
pub(crate) fn schema_document(
    key_field: &str,
    occ_version: u64,
    metadata: &SchemaMetadata,
) -> Result<Value, serde_json::Error> {
    let mut body = serde_json::to_value(metadata)?;
    let Some(obj) = body.as_object_mut() else {
        return Ok(body);
    };
    obj.insert(
        key_field.to_string(),
        Value::String(SCHEMA_DOCUMENT_KEY.to_string()),
    );
    obj.insert(
        BEVY_PERSISTENCE_DATABASE_METADATA_FIELD.to_string(),
        Value::Object(metadata_object(DocumentKind::Schema, occ_version)),
    );
    Ok(body)
}

pub(crate) fn metadata_object(kind: DocumentKind, occ_version: u64) -> Map<String, Value> {
    let mut meta = Map::new();
    meta.insert(
        BEVY_PERSISTENCE_DATABASE_VERSION_FIELD.to_string(),
        Value::from(occ_version),
    );
    meta.insert(
        BEVY_PERSISTENCE_DATABASE_BEVY_TYPE_FIELD.to_string(),
        Value::String(kind.as_ref().to_string()),
    );
    meta
}

/// Attach persistence metadata and the document key to a component or resource body.
pub(crate) fn document_with_meta(
    kind: DocumentKind,
    key: &str,
    key_field: &str,
    occ_version: u64,
    mut body: Value,
) -> Value {
    let Some(obj) = body.as_object_mut() else {
        return body;
    };
    obj.insert(key_field.to_string(), Value::String(key.to_string()));
    obj.insert(
        BEVY_PERSISTENCE_DATABASE_METADATA_FIELD.to_string(),
        Value::Object(metadata_object(kind, occ_version)),
    );
    body
}
