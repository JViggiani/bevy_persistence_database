//! In-memory copy of one store. Migration steps edit this, then the runner
//! diffs it back into a transaction.

use std::collections::HashMap;

use serde_json::{Map, Value};

use crate::core::{
    compact::{expand_envelope, to_persist_value},
    db::connection::{
        DocumentKind, EdgeDocument, StoreContents, StoredDocument, TransactionOperation,
    },
    schema::metadata::{
        SCHEMA_DOCUMENT_KEY, SchemaMetadata, document_with_meta, schema_document,
    },
};

use super::error::MigrationError;

struct TrackedDoc {
    occ_version: u64,
    /// Expanded body as it was read. `None` for a document this snapshot created.
    original: Option<Value>,
    body: Value,
    removed: bool,
}

impl TrackedDoc {
    fn fresh() -> Self {
        Self {
            occ_version: 0,
            original: None,
            body: Value::Object(Map::new()),
            removed: false,
        }
    }
}

/// One entity's component map. Component values are plain JSON.
pub struct EntityView<'a> {
    key: String,
    fields: &'a mut Map<String, Value>,
}

impl EntityView<'_> {
    pub fn key(&self) -> &str {
        &self.key
    }

    pub fn get(&self, component: &str) -> Option<&Value> {
        self.fields.get(component)
    }

    pub fn get_mut(&mut self, component: &str) -> Option<&mut Value> {
        self.fields.get_mut(component)
    }

    pub fn insert(&mut self, component: &str, value: Value) {
        self.fields.insert(component.to_string(), value);
    }

    pub fn remove(&mut self, component: &str) -> Option<Value> {
        self.fields.remove(component)
    }

    pub fn components(&self) -> &Map<String, Value> {
        self.fields
    }
}

/// Documents and edges of one store, with compact envelopes already expanded.
pub struct StoreSnapshot {
    entities: HashMap<String, TrackedDoc>,
    resources: HashMap<String, TrackedDoc>,
    edges: Vec<EdgeDocument>,
    original_edges: Vec<EdgeDocument>,
    schema_occ: Option<u64>,
    compact_threshold: usize,
}

/// A store read from the database, split into the schema document and the snapshot steps edit.
pub(crate) struct LoadedStore {
    pub snapshot: StoreSnapshot,
    pub schema: Option<SchemaMetadata>,
    pub non_schema_documents: usize,
}

impl LoadedStore {
    pub(crate) fn is_empty(&self) -> bool {
        self.non_schema_documents == 0 && self.snapshot.edges.is_empty()
    }
}

impl StoreSnapshot {
    pub(crate) fn load(
        contents: StoreContents,
        compact_threshold: usize,
    ) -> Result<LoadedStore, MigrationError> {
        let mut entities = HashMap::new();
        let mut resources = HashMap::new();
        let mut schema = None;
        let mut schema_occ = None;
        let mut non_schema_documents = 0usize;
        for document in contents.documents {
            match document.kind {
                DocumentKind::Schema => {
                    if schema.is_some() {
                        return Err(MigrationError::Store(
                            "store has more than one schema document".to_string(),
                        ));
                    }
                    if document.key != SCHEMA_DOCUMENT_KEY {
                        return Err(MigrationError::Store(format!(
                            "schema document key is `{}`, expected `{SCHEMA_DOCUMENT_KEY}`",
                            document.key
                        )));
                    }
                    schema = Some(parse_schema(&document)?);
                    schema_occ = Some(document.version);
                }
                DocumentKind::Entity => {
                    non_schema_documents += 1;
                    let key = document.key.clone();
                    let body = expand_entity(document.body, &key)?;
                    insert_unique(&mut entities, key, body, document.version)?;
                }
                DocumentKind::Resource => {
                    non_schema_documents += 1;
                    let key = document.key.clone();
                    let body = expand_envelope(document.body).map_err(|error| {
                        MigrationError::Store(format!("resource `{key}`: {error}"))
                    })?;
                    insert_unique(&mut resources, key, body, document.version)?;
                }
            }
        }
        Ok(LoadedStore {
            snapshot: StoreSnapshot {
                entities,
                resources,
                edges: contents.edges.clone(),
                original_edges: contents.edges,
                schema_occ,
                compact_threshold,
            },
            schema,
            non_schema_documents,
        })
    }

    pub(crate) fn from_synthesized(
        entities: Vec<(String, Map<String, Value>)>,
        resources: Vec<(String, Value)>,
        edges: Vec<EdgeDocument>,
    ) -> Self {
        let entities = entities
            .into_iter()
            .map(|(key, fields)| {
                let body = Value::Object(fields);
                (
                    key,
                    TrackedDoc {
                        occ_version: 1,
                        original: Some(body.clone()),
                        body,
                        removed: false,
                    },
                )
            })
            .collect();
        let resources = resources
            .into_iter()
            .map(|(key, body)| {
                (
                    key,
                    TrackedDoc {
                        occ_version: 1,
                        original: Some(body.clone()),
                        body,
                        removed: false,
                    },
                )
            })
            .collect();
        Self {
            entities,
            resources,
            edges: edges.clone(),
            original_edges: edges,
            schema_occ: None,
            compact_threshold: 0,
        }
    }

    pub fn entity_keys(&self) -> Vec<String> {
        let mut keys: Vec<String> = self
            .entities
            .iter()
            .filter(|(_, doc)| !doc.removed)
            .map(|(key, _)| key.clone())
            .collect();
        keys.sort();
        keys
    }

    pub fn entity(&self, key: &str) -> Option<&Map<String, Value>> {
        let doc = self.entities.get(key)?;
        if doc.removed {
            return None;
        }
        doc.body.as_object()
    }

    pub fn entity_mut(&mut self, key: &str) -> Option<EntityView<'_>> {
        let doc = self.entities.get_mut(key)?;
        if doc.removed {
            return None;
        }
        let fields = doc.body.as_object_mut()?;
        Some(EntityView {
            key: key.to_string(),
            fields,
        })
    }

    /// Return the entity, creating it when the key is new.
    ///
    /// A key removed earlier in this step is restored with the body it had.
    pub fn insert_entity(&mut self, key: impl Into<String>) -> EntityView<'_> {
        let key = key.into();
        let doc = self.entities.entry(key.clone()).or_insert_with(TrackedDoc::fresh);
        doc.removed = false;
        if doc.body.as_object().is_none() {
            doc.body = Value::Object(Map::new());
        }
        let fields = doc
            .body
            .as_object_mut()
            .expect("entity body is a JSON object");
        EntityView { key, fields }
    }

    pub fn remove_entity(&mut self, key: &str) {
        let Some(doc) = self.entities.get_mut(key) else {
            return;
        };
        if doc.original.is_none() {
            self.entities.remove(key);
        } else {
            doc.removed = true;
        }
    }

    pub fn resource_names(&self) -> Vec<String> {
        let mut names: Vec<String> = self
            .resources
            .iter()
            .filter(|(_, doc)| !doc.removed)
            .map(|(name, _)| name.clone())
            .collect();
        names.sort();
        names
    }

    pub fn resource(&self, name: &str) -> Option<&Value> {
        let doc = self.resources.get(name)?;
        if doc.removed { None } else { Some(&doc.body) }
    }

    pub fn resource_mut(&mut self, name: &str) -> Option<&mut Value> {
        let doc = self.resources.get_mut(name)?;
        if doc.removed { None } else { Some(&mut doc.body) }
    }

    pub fn insert_resource(&mut self, name: impl Into<String>, value: Value) {
        let name = name.into();
        let doc = self
            .resources
            .entry(name)
            .or_insert_with(TrackedDoc::fresh);
        doc.removed = false;
        doc.body = value;
    }

    pub fn remove_resource(&mut self, name: &str) {
        let Some(doc) = self.resources.get_mut(name) else {
            return;
        };
        if doc.original.is_none() {
            self.resources.remove(name);
        } else {
            doc.removed = true;
        }
    }

    pub fn edges(&self) -> &[EdgeDocument] {
        &self.edges
    }

    pub fn edges_mut(&mut self) -> &mut Vec<EdgeDocument> {
        &mut self.edges
    }

    pub(crate) fn to_operations(
        &self,
        store: &str,
        key_field: &str,
        schema: &SchemaMetadata,
        write_schema: bool,
    ) -> Result<Vec<TransactionOperation>, MigrationError> {
        let mut operations = Vec::new();
        diff_docs(
            &self.entities,
            DocumentKind::Entity,
            store,
            key_field,
            self.compact_threshold,
            true,
            &mut operations,
        )?;
        diff_docs(
            &self.resources,
            DocumentKind::Resource,
            store,
            key_field,
            self.compact_threshold,
            false,
            &mut operations,
        )?;
        if write_schema {
            let next_occ = self.schema_occ.map(|version| version + 1).unwrap_or(1);
            let document = schema_document(key_field, next_occ, schema)
                .map_err(|error| MigrationError::Store(format!("schema document: {error}")))?;
            operations.push(match self.schema_occ {
                Some(expected) => TransactionOperation::ReplaceDocument {
                    store: store.to_string(),
                    kind: DocumentKind::Schema,
                    key: SCHEMA_DOCUMENT_KEY.to_string(),
                    expected_current_version: expected,
                    document,
                },
                None => TransactionOperation::CreateDocument {
                    store: store.to_string(),
                    kind: DocumentKind::Schema,
                    data: document,
                },
            });
        }
        diff_edges(store, &self.original_edges, &self.edges, &mut operations);
        Ok(operations)
    }
}

fn parse_schema(document: &StoredDocument) -> Result<SchemaMetadata, MigrationError> {
    serde_json::from_value(document.body.clone())
        .map_err(|error| MigrationError::Store(format!("schema document `{}`: {error}", document.key)))
}

fn insert_unique(
    docs: &mut HashMap<String, TrackedDoc>,
    key: String,
    body: Value,
    occ_version: u64,
) -> Result<(), MigrationError> {
    if docs.contains_key(&key) {
        return Err(MigrationError::Store(format!(
            "duplicate document key `{key}`"
        )));
    }
    docs.insert(
        key,
        TrackedDoc {
            occ_version,
            original: Some(body.clone()),
            body,
            removed: false,
        },
    );
    Ok(())
}

fn expand_entity(body: Value, key: &str) -> Result<Value, MigrationError> {
    let Some(obj) = body.as_object().cloned() else {
        return Err(MigrationError::Store(format!(
            "entity `{key}` is not a JSON object"
        )));
    };
    let mut expanded = Map::new();
    for (name, value) in obj {
        let value = expand_envelope(value).map_err(|error| {
            MigrationError::Store(format!("entity `{key}` component `{name}`: {error}"))
        })?;
        expanded.insert(name, value);
    }
    Ok(Value::Object(expanded))
}

fn diff_docs(
    docs: &HashMap<String, TrackedDoc>,
    kind: DocumentKind,
    store: &str,
    key_field: &str,
    threshold: usize,
    per_field: bool,
    operations: &mut Vec<TransactionOperation>,
) -> Result<(), MigrationError> {
    let mut keys: Vec<&String> = docs.keys().collect();
    keys.sort();
    for key in keys {
        let doc = &docs[key];
        if doc.removed {
            if doc.original.is_some() {
                operations.push(TransactionOperation::DeleteDocument {
                    store: store.to_string(),
                    kind,
                    key: key.clone(),
                    expected_current_version: doc.occ_version,
                });
            }
            continue;
        }
        let changed = match &doc.original {
            None => true,
            Some(original) => original != &doc.body,
        };
        if !changed {
            continue;
        }
        let packed = pack_body(&doc.body, threshold, per_field, key)?;
        let next_occ = if doc.original.is_none() {
            1
        } else {
            doc.occ_version + 1
        };
        let document = document_with_meta(kind, key, key_field, next_occ, packed);
        if doc.original.is_none() {
            operations.push(TransactionOperation::CreateDocument {
                store: store.to_string(),
                kind,
                data: document,
            });
        } else {
            operations.push(TransactionOperation::ReplaceDocument {
                store: store.to_string(),
                kind,
                key: key.clone(),
                expected_current_version: doc.occ_version,
                document,
            });
        }
    }
    Ok(())
}

fn pack_body(body: &Value, threshold: usize, per_field: bool, key: &str) -> Result<Value, MigrationError> {
    if !per_field {
        return to_persist_value(body, threshold)
            .map_err(|error| MigrationError::Store(format!("resource `{key}`: {error}")));
    }
    let Some(obj) = body.as_object() else {
        return Err(MigrationError::Store(format!(
            "entity `{key}` is not a JSON object"
        )));
    };
    let mut packed = Map::new();
    for (name, value) in obj {
        packed.insert(
            name.clone(),
            to_persist_value(value, threshold).map_err(|error| {
                MigrationError::Store(format!("entity `{key}` component `{name}`: {error}"))
            })?,
        );
    }
    Ok(Value::Object(packed))
}

fn edge_same(left: &EdgeDocument, right: &EdgeDocument) -> bool {
    left.key == right.key
        && left.relationship_type == right.relationship_type
        && left.from_guid == right.from_guid
        && left.to_guid == right.to_guid
        && left.payload == right.payload
}

fn diff_edges(
    store: &str,
    original: &[EdgeDocument],
    current: &[EdgeDocument],
    operations: &mut Vec<TransactionOperation>,
) {
    let original_by_key: HashMap<&str, &EdgeDocument> =
        original.iter().map(|edge| (edge.key.as_str(), edge)).collect();
    let current_by_key: HashMap<&str, &EdgeDocument> =
        current.iter().map(|edge| (edge.key.as_str(), edge)).collect();
    let mut upserts = Vec::new();
    for (key, edge) in &current_by_key {
        match original_by_key.get(key) {
            Some(previous) if edge_same(previous, edge) => {}
            _ => upserts.push((*edge).clone()),
        }
    }
    let deletes: Vec<String> = original_by_key
        .keys()
        .filter(|key| !current_by_key.contains_key(*key))
        .map(|key| (*key).to_string())
        .collect();
    if !upserts.is_empty() {
        operations.push(TransactionOperation::UpsertEdges {
            store: store.to_string(),
            edges: upserts,
        });
    }
    if !deletes.is_empty() {
        operations.push(TransactionOperation::DeleteEdges {
            store: store.to_string(),
            keys: deletes,
        });
    }
}
