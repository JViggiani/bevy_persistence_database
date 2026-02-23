//! Shared utilities for database connection implementations

use crate::core::db::connection::{DocumentKind, EdgeDocument, PersistenceError, TransactionOperation};
use serde_json::Value;

/// Document CRUD operations for a single document kind (entity or resource).
pub struct DocumentOperations {
    pub creates: Vec<Value>,
    /// `{ key_field: key, "expected": version, "patch": patch }`
    pub updates: Vec<Value>,
    /// `{ key_field: key, "expected": version }`
    pub deletes: Vec<Value>,
}

/// Edge upserts and deletes extracted from a transaction's operations.
pub struct EdgeOperations {
    pub upserts: Vec<EdgeDocument>,
    pub deletes: Vec<String>,
}

/// Transaction operations grouped by kind so each database backend can process
/// them in whatever order / batching strategy it needs.
pub struct GroupedOperations {
    pub entities: DocumentOperations,
    pub resources: DocumentOperations,
    pub edges: EdgeOperations,
}

impl GroupedOperations {
    pub fn from_operations(operations: Vec<TransactionOperation>, key_field: &str) -> Self {
        let mut entities = DocumentOperations {
            creates: Vec::new(),
            updates: Vec::new(),
            deletes: Vec::new(),
        };
        let mut resources = DocumentOperations {
            creates: Vec::new(),
            updates: Vec::new(),
            deletes: Vec::new(),
        };
        let mut edge_ops = EdgeOperations {
            upserts: Vec::new(),
            deletes: Vec::new(),
        };

        for op in operations {
            match op {
                TransactionOperation::CreateDocument { kind, data, .. } => match kind {
                    DocumentKind::Entity => entities.creates.push(data),
                    DocumentKind::Resource => resources.creates.push(data),
                },
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
                        DocumentKind::Entity => entities.updates.push(obj),
                        DocumentKind::Resource => resources.updates.push(obj),
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
                        DocumentKind::Entity => entities.deletes.push(obj),
                        DocumentKind::Resource => resources.deletes.push(obj),
                    }
                }
                TransactionOperation::UpsertEdges { edges, .. } => {
                    edge_ops.upserts.extend(edges);
                }
                TransactionOperation::DeleteEdges { keys, .. } => {
                    edge_ops.deletes.extend(keys);
                }
            }
        }

        Self {
            entities,
            resources,
            edges: edge_ops,
        }
    }
}

/// Extracts the value of `key_field` as a String from each JSON object in `values`.
pub fn extract_keys(values: &[Value], key_field: &str) -> Vec<String> {
    values
        .iter()
        .filter_map(|v| {
            v.get(key_field)
                .and_then(|k| k.as_str())
                .map(|s| s.to_string())
        })
        .collect()
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
                operation_type.as_str(),
                collection
            )));
        }
    }
    Ok(())
}
