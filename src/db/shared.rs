//! Shared utilities for database connection implementations

use crate::db::connection::{DocumentKind, PersistenceError, TransactionOperation};
use serde_json::Value;

/// Groups transaction operations by type and collection
pub struct GroupedOperations {
    pub entity_creates: Vec<Value>,
    pub entity_updates: Vec<Value>,  // { key/id, expected, patch }
    pub entity_deletes: Vec<Value>,  // { key/id, expected }
    pub resource_creates: Vec<Value>,
    pub resource_updates: Vec<Value>,
    pub resource_deletes: Vec<Value>,
}

impl GroupedOperations {
    pub fn from_operations(operations: Vec<TransactionOperation>, key_field: &str) -> Self {
        let mut result = Self {
            entity_creates: Vec::new(),
            entity_updates: Vec::new(),
            entity_deletes: Vec::new(),
            resource_creates: Vec::new(),
            resource_updates: Vec::new(),
            resource_deletes: Vec::new(),
        };

        for op in operations {
            match op {
                TransactionOperation::CreateDocument { kind, data, .. } => {
                    match kind {
                        DocumentKind::Entity => result.entity_creates.push(data),
                        DocumentKind::Resource => result.resource_creates.push(data),
                    }
                }
                TransactionOperation::UpdateDocument {
                    kind,
                    key,
                    expected_current_version,
                    patch,
                    ..
                } => {
                    let obj = serde_json::json!({
                        key_field: key,
                        "expected": expected_current_version,
                        "patch": patch
                    });
                    match kind {
                        DocumentKind::Entity => result.entity_updates.push(obj),
                        DocumentKind::Resource => result.resource_updates.push(obj),
                    }
                }
                TransactionOperation::DeleteDocument {
                    kind,
                    key,
                    expected_current_version,
                    ..
                } => {
                    let obj = serde_json::json!({
                        key_field: key,
                        "expected": expected_current_version,
                    });
                    match kind {
                        DocumentKind::Entity => result.entity_deletes.push(obj),
                        DocumentKind::Resource => result.resource_deletes.push(obj),
                    }
                }
            }
        }

        result
    }

    pub fn extract_keys(&self, values: &[Value], key_field: &str) -> Vec<String> {
        values
            .iter()
            .filter_map(|v| v.get(key_field).and_then(|k| k.as_str()).map(|s| s.to_string()))
            .collect()
    }
}

pub enum OperationType {
    Update,
    Delete,
}

impl OperationType {
    pub fn as_str(&self) -> &'static str {
        match self {
            OperationType::Update => "Update",
            OperationType::Delete => "Delete",
        }
    }
}

/// Checks if all requested operations succeeded and returns appropriate error
pub fn check_operation_success(
    requested: Vec<String>,
    processed: Vec<String>,
    operation_type: &OperationType,
    collection: &str,
) -> Result<(), PersistenceError> {
    if processed.len() != requested.len() {
        if let Some(conflict_key) = requested
            .into_iter()
            .find(|k| !processed.iter().any(|p| p == k))
        {
            return Err(PersistenceError::Conflict { key: conflict_key });
        } else {
            return Err(PersistenceError::new(format!(
                "{} conflict ({})",
                operation_type.as_str(), collection
            )));
        }
    }
    Ok(())
}
