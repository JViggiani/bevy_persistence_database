//! Component / resource / relationship serializer registries and registration.

use std::{
    any::TypeId,
    collections::{BTreeMap, HashMap, HashSet},
};

use bevy::prelude::{Component, Entity, Resource, World};
use serde::{Serialize, de::DeserializeOwned};
use serde_json::Value;

use crate::core::compact::{from_persist_value, to_persist_value};
use crate::core::db::connection::{EdgeDocument, PersistenceError};
use crate::core::persist::Persist;
use crate::core::schema::lock::trace::{SchemaFormat, trace_type};
use crate::core::schema::metadata::SCHEMA_DOCUMENT_KEY;
use crate::core::versioning::version_manager::VersionKey;

pub(crate) type SchemaTracer =
    Box<dyn Fn() -> Result<SchemaFormat, String> + Send + Sync>;
pub(crate) type SchemaValidator = Box<dyn Fn(&Value) -> Result<(), String> + Send + Sync>;

use super::PersistenceSession;

pub(super) type ComponentSerializer =
    Box<dyn Fn(Entity, &World) -> Result<Option<(String, Value)>, PersistenceError> + Send + Sync>;
pub(super) type ComponentDeserializer =
    Box<dyn Fn(&mut World, Entity, Value) -> Result<(), PersistenceError> + Send + Sync>;
pub(super) type ResourceSerializer = Box<
    dyn Fn(&World, &PersistenceSession) -> Result<Option<(String, Value)>, PersistenceError>
        + Send
        + Sync,
>;
pub(super) type ResourceDeserializer =
    Box<dyn Fn(&mut World, Value) -> Result<(), PersistenceError> + Send + Sync>;
pub(super) type ResourceRemover = Box<dyn Fn(&mut World) + Send + Sync>;

/// Extracts all edge documents for a given relationship type from the world.
/// The third parameter is a map of client-side preassigned keys for entities that are new in the
/// current commit (not yet in the session's entity-key cache); it enables entity + relationship
/// to be persisted in a single commit.
pub(super) type RelationshipSerializer = Box<
    dyn Fn(
            &World,
            &PersistenceSession,
            &HashMap<Entity, String>,
            &HashSet<Entity>,
        ) -> Result<Vec<EdgeDocument>, PersistenceError>
        + Send
        + Sync,
>;

pub(super) type RelationshipDeserializer = Box<
    dyn Fn(&mut World, Entity, Vec<(Entity, Option<Value>)>) -> Result<(), PersistenceError>
        + Send
        + Sync,
>;

#[derive(Default)]
pub(super) struct ComponentRegistry {
    pub(super) serializers: HashMap<TypeId, ComponentSerializer>,
    pub(super) deserializers: HashMap<String, ComponentDeserializer>,
    pub(super) type_id_to_name: HashMap<TypeId, &'static str>,
    pub(super) name_to_type_id: HashMap<String, TypeId>,
    name_owners: HashMap<String, (TypeId, &'static str)>,
    pub(crate) tracers: HashMap<String, SchemaTracer>,
    pub(crate) validators: HashMap<String, SchemaValidator>,
    pub(super) presence: HashMap<String, Box<dyn Fn(&World, Entity) -> bool + Send + Sync>>,
}

#[derive(Default)]
pub(super) struct ResourceRegistry {
    pub(super) serializers: HashMap<TypeId, ResourceSerializer>,
    pub(super) deserializers: HashMap<String, ResourceDeserializer>,
    pub(super) name_to_type_id: HashMap<String, TypeId>,
    pub(super) type_id_to_name: HashMap<TypeId, &'static str>,
    name_owners: HashMap<String, (TypeId, &'static str)>,
    pub(crate) tracers: HashMap<String, SchemaTracer>,
    pub(crate) validators: HashMap<String, SchemaValidator>,
    pub(super) removers: HashMap<TypeId, ResourceRemover>,
    pub(super) presence: HashMap<TypeId, Box<dyn Fn(&World) -> bool + Send + Sync>>,
    pub(super) last_seen_present: HashMap<TypeId, bool>,
}

#[derive(Default)]
pub(super) struct RelationshipRegistry {
    /// One serializer per relationship TypeId. Extracts edge documents.
    pub(super) serializers: HashMap<TypeId, RelationshipSerializer>,
    /// One deserializer per relationship TypeId. Applies edge payloads to ECS state.
    pub(super) deserializers: HashMap<TypeId, RelationshipDeserializer>,
    /// Maps relationship type name → TypeId.
    pub(super) name_to_type_id: HashMap<String, TypeId>,
    /// Maps TypeId → relationship type name.
    pub(super) type_id_to_name: HashMap<TypeId, &'static str>,
    name_owners: HashMap<String, (TypeId, &'static str)>,
    pub(crate) payload_tracers: HashMap<String, SchemaTracer>,
    pub(crate) payload_validators: HashMap<String, SchemaValidator>,
}

fn claim_storage_name(
    owners: &mut HashMap<String, (TypeId, &'static str)>,
    type_id_to_name: &mut HashMap<TypeId, &'static str>,
    name_to_type_id: &mut HashMap<String, TypeId>,
    type_id: TypeId,
    storage_name: &'static str,
    rust_name: &'static str,
) {
    if storage_name == SCHEMA_DOCUMENT_KEY {
        panic!(
            "storage name `{storage_name}` is reserved by bevy_persistence_database"
        );
    }
    if let Some((owner_id, owner_name)) = owners.get(storage_name) {
        if *owner_id != type_id {
            panic!(
                "storage name `{storage_name}` is already registered for `{owner_name}`; `{rust_name}` cannot use it"
            );
        }
    }
    owners.insert(storage_name.to_string(), (type_id, rust_name));
    type_id_to_name.insert(type_id, storage_name);
    name_to_type_id.insert(storage_name.to_string(), type_id);
}

impl ComponentRegistry {
    fn insert_name_maps(&mut self, type_id: TypeId, name: &'static str, rust_name: &'static str) {
        claim_storage_name(
            &mut self.name_owners,
            &mut self.type_id_to_name,
            &mut self.name_to_type_id,
            type_id,
            name,
            rust_name,
        );
    }
}

impl ResourceRegistry {
    fn insert_name_maps(&mut self, type_id: TypeId, name: &'static str, rust_name: &'static str) {
        claim_storage_name(
            &mut self.name_owners,
            &mut self.type_id_to_name,
            &mut self.name_to_type_id,
            type_id,
            name,
            rust_name,
        );
    }
}

impl RelationshipRegistry {
    fn insert_name_maps(&mut self, type_id: TypeId, name: &'static str, rust_name: &'static str) {
        claim_storage_name(
            &mut self.name_owners,
            &mut self.type_id_to_name,
            &mut self.name_to_type_id,
            type_id,
            name,
            rust_name,
        );
    }
}

impl PersistenceSession {
    pub fn register_component<T: Component + Persist>(&mut self) {
        self.register_component_named::<T>(T::name());
    }

    /// Registers a component type for persistence using an explicit collection name.
    ///
    /// Unlike `register_component`, this does not require `T: Persist`.
    /// The `name` parameter is used as the collection/field key in the database.
    pub fn register_component_named<
        T: Component + Serialize + DeserializeOwned + Send + Sync + 'static,
    >(
        &mut self,
        name: &'static str,
    ) {
        let ser_key = name;
        let type_id = TypeId::of::<T>();
        self.components
            .insert_name_maps(type_id, ser_key, std::any::type_name::<T>());
        self.components.presence.insert(
            ser_key.to_string(),
            Box::new(|world: &World, entity: Entity| world.entity(entity).contains::<T>()),
        );
        let threshold = self.compact_threshold_bytes;
        self.components.serializers.insert(
            type_id,
            Box::new(
                move |entity, world| -> Result<Option<(String, Value)>, PersistenceError> {
                    if let Some(c) = world.get::<T>(entity) {
                        let v = to_persist_value(c, threshold)
                            .map_err(|e| PersistenceError::new(e.to_string()))?;
                        Ok(Some((ser_key.to_string(), v)))
                    } else {
                        Ok(None)
                    }
                },
            ),
        );

        let de_key = name;
        self.components.deserializers.insert(
            de_key.to_string(),
            Box::new(|world, entity, json_val| {
                let comp: T = from_persist_value(json_val)
                    .map_err(|e| PersistenceError::new(e.to_string()))?;
                world.entity_mut(entity).insert(comp);
                Ok(())
            }),
        );
        self.components.tracers.insert(
            name.to_string(),
            Box::new(|| trace_type::<T>()),
        );
        self.components.validators.insert(
            name.to_string(),
            Box::new(|value| {
                from_persist_value::<T>(value.clone())
                    .map(|_| ())
                    .map_err(|error| error.to_string())
            }),
        );
    }

    /// Registers a resource type for persistence.
    ///
    /// This method sets up both serialization and deserialization for any
    /// resource that implements the `Persist` marker trait.
    pub fn register_resource<R: Resource + Persist>(&mut self) {
        self.register_resource_named::<R>(R::name());
    }

    /// Registers a resource type for persistence using an explicit collection name.
    ///
    /// Unlike `register_resource`, this does not require `R: Persist`.
    /// The `name` parameter is used as the key in the database.
    pub fn register_resource_named<
        R: Resource + Serialize + DeserializeOwned + Send + Sync + 'static,
    >(
        &mut self,
        name: &'static str,
    ) {
        let ser_key = name;
        let type_id = std::any::TypeId::of::<R>();
        self.resources
            .insert_name_maps(type_id, ser_key, std::any::type_name::<R>());
        self.resources.presence.insert(
            type_id,
            Box::new(|world: &World| world.get_resource::<R>().is_some()),
        );
        // Insert serializer into map keyed by TypeId
        self.resources.serializers.insert(
            type_id,
            Box::new(move |world, session| {
                // Fetch and serialize the resource
                if let Some(r) = world.get_resource::<R>() {
                    let v = to_persist_value(r, session.compact_threshold_bytes())
                        .map_err(|e| PersistenceError::new(e.to_string()))?;
                    Ok(Some((ser_key.to_string(), v)))
                } else {
                    Ok(None)
                }
            }),
        );

        let de_key = name;
        self.resources.deserializers.insert(
            de_key.to_string(),
            Box::new(|world, json_val| {
                let res: R = from_persist_value(json_val)
                    .map_err(|e| PersistenceError::new(e.to_string()))?;
                world.insert_resource(res);
                Ok(())
            }),
        );
        // Register remover function
        self.resources.removers.insert(
            type_id,
            Box::new(|world| {
                world.remove_resource::<R>();
            }),
        );
        self.resources
            .tracers
            .insert(name.to_string(), Box::new(|| trace_type::<R>()));
        self.resources.validators.insert(
            name.to_string(),
            Box::new(|value| {
                from_persist_value::<R>(value.clone())
                    .map(|_| ())
                    .map_err(|error| error.to_string())
            }),
        );
    }

    /// Register a relationship type for edge persistence.
    /// The serializer closure extracts all edge documents for this relationship type.
    pub fn register_relationship(
        &mut self,
        type_id: TypeId,
        name: &'static str,
        rust_name: &'static str,
        serializer: RelationshipSerializer,
    ) {
        self.relationships
            .insert_name_maps(type_id, name, rust_name);
        self.relationships.serializers.insert(type_id, serializer);
    }

    /// Register a built-in Bevy `Relationship` component for edge persistence.
    ///
    /// This iterates all entities that have `R` and a cached GUID, extracting
    /// each relationship target to build `EdgeDocument`s. Disabled when the
    /// `bevy_many_relationship_edges` feature is enabled.
    #[cfg(not(feature = "bevy_many_relationship_edges"))]
    pub fn register_bevy_relationship<R: Component + bevy::ecs::relationship::Relationship>(
        &mut self,
        name: &'static str,
    ) {
        let type_id = TypeId::of::<R>();
        self.register_relationship(
            type_id,
            name,
            std::any::type_name::<R>(),
            Box::new(
                move |world,
                      session,
                      preassigned: &HashMap<Entity, String>,
                      scan_sources: &HashSet<Entity>| {
                    let mut edges = Vec::new();
                    for &from_entity in scan_sources {
                        let Ok(entity_ref) = world.get_entity(from_entity) else {
                            continue;
                        };
                        if let Some(rel) = entity_ref.get::<R>() {
                            let target = rel.get();
                            let from_guid = session
                                .entity_key(from_entity)
                                .cloned()
                                .or_else(|| preassigned.get(&from_entity).cloned());
                            let to_guid = session
                                .entity_key(target)
                                .cloned()
                                .or_else(|| preassigned.get(&target).cloned());
                            if let (Some(from_guid), Some(to_guid)) = (from_guid, to_guid) {
                                edges.push(EdgeDocument {
                                    key: EdgeDocument::make_key(name, &from_guid, &to_guid),
                                    relationship_type: name.to_string(),
                                    from_guid,
                                    to_guid,
                                    payload: None,
                                });
                            }
                        }
                    }
                    Ok(edges)
                },
            ),
        );
    }

    /// Register a built-in Bevy `Relationship` component for edge persistence AND loading.
    ///
    /// Unlike `register_bevy_relationship`, this also registers a deserializer so that
    /// `PersistentQuery::with_relationship_depth` and the hydrator can reconstruct the
    /// relationship component on entities loaded from the database.
    ///
    /// Requires `R: From<Entity>` — all standard single-field tuple-struct relationships
    /// (e.g. `struct MemberOf(Entity)`) satisfy this trivially.
    #[cfg(not(feature = "bevy_many_relationship_edges"))]
    pub fn register_bevy_relationship_loader<
        R: Component + bevy::ecs::relationship::Relationship + From<Entity> + 'static,
    >(
        &mut self,
        _name: &'static str,
    ) {
        let type_id = TypeId::of::<R>();
        self.relationships.deserializers.insert(
            type_id,
            Box::new(|world, source_entity, targets| {
                for (target, _payload) in targets {
                    world
                        .entity_mut(source_entity)
                        .insert(<R as From<Entity>>::from(target));
                }
                Ok(())
            }),
        );
    }

    /// Register a `bevy_many_relationships` relationship type for edge persistence.
    ///
    /// This iterates all entities that have `OutgoingRelationships<R>` and a
    /// cached GUID, extracting each outgoing edge (with optional serialised
    /// payload) to build `EdgeDocument`s. Only available when the
    /// `bevy_many_relationship_edges` feature is enabled.
    #[cfg(feature = "bevy_many_relationship_edges")]
    pub fn register_many_relationship<
        R: serde::Serialize + DeserializeOwned + Send + Sync + 'static,
    >(
        &mut self,
        name: &'static str,
    ) {
        let type_id = TypeId::of::<R>();
        self.register_relationship(
            type_id,
            name,
            std::any::type_name::<R>(),
            Box::new(
                move |world,
                      session,
                      preassigned: &HashMap<Entity, String>,
                      scan_sources: &HashSet<Entity>| {
                    let mut edges = Vec::new();
                    for &from_entity in scan_sources {
                        let Ok(entity_ref) = world.get_entity(from_entity) else {
                            continue;
                        };
                        if let Some(outgoing) =
                            entity_ref.get::<bevy_many_relationships::OutgoingRelationships<R>>()
                        {
                            let Some(from_guid) = session
                                .entity_key(from_entity)
                                .cloned()
                                .or_else(|| preassigned.get(&from_entity).cloned())
                            else {
                                continue;
                            };
                            for (target, payload) in outgoing.iter() {
                                let Some(to_guid) = session
                                    .entity_key(target)
                                    .cloned()
                                    .or_else(|| preassigned.get(&target).cloned())
                                else {
                                    continue;
                                };
                                let serialized_payload = serde_json::to_value(payload).ok();
                                edges.push(EdgeDocument {
                                    key: EdgeDocument::make_key(name, &from_guid, &to_guid),
                                    relationship_type: name.to_string(),
                                    from_guid: from_guid.clone(),
                                    to_guid,
                                    payload: serialized_payload,
                                });
                            }
                        }
                    }
                    Ok(edges)
                },
            ),
        );
        self.relationships.deserializers.insert(
            type_id,
            Box::new(|world, source_entity, targets| {
                if let Some(existing) =
                    world.get::<bevy_many_relationships::OutgoingRelationships<R>>(source_entity)
                {
                    let existing_targets: Vec<Entity> = existing.targets().collect();
                    for target in existing_targets {
                        bevy_many_relationships::remove_many_relationship::<R>(
                            world,
                            source_entity,
                            target,
                        );
                    }
                }

                for (target, payload_json) in targets {
                    let Some(payload_json) = payload_json else {
                        return Err(PersistenceError::new("Missing relationship payload"));
                    };
                    let payload: R = serde_json::from_value(payload_json)
                        .map_err(|e| PersistenceError::new(e.to_string()))?;
                    bevy_many_relationships::set_many_relationship::<R>(
                        world,
                        source_entity,
                        target,
                        payload,
                    );
                }

                Ok(())
            }),
        );
        self.relationships.payload_tracers.insert(
            name.to_string(),
            Box::new(|| trace_type::<R>()),
        );
        self.relationships.payload_validators.insert(
            name.to_string(),
            Box::new(|value| {
                serde_json::from_value::<R>(value.clone())
                    .map(|_| ())
                    .map_err(|error| error.to_string())
            }),
        );
    }
    pub fn resource_version<R: Resource + 'static>(&self) -> Option<u64> {
        self.cache
            .version_manager
            .get_version(&VersionKey::Resource(TypeId::of::<R>()))
    }

    /// Lookup the registered `TypeId` for a persisted resource by name.
    pub fn resource_type_id(&self, name: &str) -> Option<TypeId> {
        self.resources.name_to_type_id.get(name).copied()
    }

    /// Returns an iterator over all registered persisted resource TypeIds.
    /// Useful for cleanup operations like conflict reprocessing.
    pub fn persisted_resource_types(&self) -> impl Iterator<Item = TypeId> + '_ {
        self.resources.serializers.keys().copied()
    }

    pub(crate) fn resource_name_for_type(&self, type_id: TypeId) -> Option<&'static str> {
        self.resources.type_id_to_name.get(&type_id).copied()
    }

    /// Returns the number of registered persisted resources.
    pub fn persisted_resource_count(&self) -> usize {
        self.resources.removers.len()
    }

    /// Removes all persisted resources by calling each registered remover function.
    /// This method borrows self immutably and calls each remover with the world.
    pub fn remove_all_persisted_resources(&self, world: &mut World) {
        for remover in self.resources.removers.values() {
            remover(world);
        }
    }

    /// Deserialize one persisted resource during load (no version cache update).
    ///
    /// Prefer [`Self::materialize_resource`] for values read from the database;
    /// this is for manual or test application of a JSON blob.
    pub fn deserialize_resource_by_name(
        &mut self,
        world: &mut World,
        name: &str,
        value: Value,
    ) -> Result<(), PersistenceError> {
        self.hydrate_resource(world, name, value)
    }
    pub(crate) fn resource_presence_snapshot(&self, world: &World) -> Vec<(TypeId, bool)> {
        self.resources
            .presence
            .iter()
            .map(|(type_id, presence_fn)| (*type_id, presence_fn(world)))
            .collect()
    }

    pub(crate) fn update_resource_presence(&mut self, type_id: TypeId, is_present: bool) -> bool {
        self.resources
            .last_seen_present
            .insert(type_id, is_present)
            .unwrap_or(false)
    }
    pub(crate) fn relationship_type_name(&self, type_id: &TypeId) -> Option<&'static str> {
        self.relationships.type_id_to_name.get(type_id).copied()
    }

    pub(crate) fn relationship_type_entries(&self) -> Vec<(TypeId, &'static str)> {
        self.relationships
            .type_id_to_name
            .iter()
            .map(|(type_id, name)| (*type_id, *name))
            .collect()
    }

    pub(crate) fn apply_relationship_targets(
        &mut self,
        type_id: TypeId,
        world: &mut World,
        source: Entity,
        targets: Vec<(Entity, Option<Value>)>,
    ) -> Result<(), PersistenceError> {
        self.ensure_hydrating();
        if let Some(deserializer) = self.relationships.deserializers.get(&type_id) {
            deserializer(world, source, targets)?;
        }
        Ok(())
    }

    pub(crate) fn component_validator(&self, name: &str) -> Option<&SchemaValidator> {
        self.components.validators.get(name)
    }

    pub(crate) fn resource_validator(&self, name: &str) -> Option<&SchemaValidator> {
        self.resources.validators.get(name)
    }

    pub(crate) fn relationship_names(&self) -> impl Iterator<Item = &'static str> + '_ {
        self.relationships.type_id_to_name.values().copied()
    }

    pub(crate) fn relationship_payload_tracer(&self, name: &str) -> Option<&SchemaTracer> {
        self.relationships.payload_tracers.get(name)
    }

    pub(crate) fn relationship_payload_validator(&self, name: &str) -> Option<&SchemaValidator> {
        self.relationships.payload_validators.get(name)
    }

    pub(crate) fn traced_components(&self) -> Result<BTreeMap<String, SchemaFormat>, String> {
        trace_registered(&self.components.tracers)
    }

    pub(crate) fn traced_resources(&self) -> Result<BTreeMap<String, SchemaFormat>, String> {
        trace_registered(&self.resources.tracers)
    }

    pub(crate) fn traced_relationship_payloads(
        &self,
    ) -> Result<BTreeMap<String, Option<SchemaFormat>>, String> {
        let mut payloads = BTreeMap::new();
        for name in self.relationship_names() {
            let payload = match self.relationship_payload_tracer(name) {
                Some(tracer) => Some(tracer()?),
                None => None,
            };
            payloads.insert(name.to_string(), payload);
        }
        Ok(payloads)
    }
}

fn trace_registered(
    tracers: &HashMap<String, SchemaTracer>,
) -> Result<BTreeMap<String, SchemaFormat>, String> {
    let mut formats = BTreeMap::new();
    for (name, tracer) in tracers {
        formats.insert(name.clone(), tracer()?);
    }
    Ok(formats)
}
