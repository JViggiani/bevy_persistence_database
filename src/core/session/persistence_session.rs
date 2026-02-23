//! Core ECS‐to‐Arango bridge: defines `PersistenceSession`.
//! Handles local cache, change tracking, and commit logic (create/update/delete).

use crate::core::db::connection::{
    BEVY_PERSISTENCE_DATABASE_BEVY_TYPE_FIELD, BEVY_PERSISTENCE_DATABASE_METADATA_FIELD,
    BEVY_PERSISTENCE_DATABASE_VERSION_FIELD, DatabaseConnection, DocumentKind, PersistenceError,
    TransactionOperation,
};
use crate::core::persist::Persist;
use crate::core::versioning::version_manager::{VersionKey, VersionManager};
use bevy::prelude::{Component, Entity, Resource, World, info};
use rayon::ThreadPoolBuilder;
use rayon::prelude::*;
#[cfg(feature = "bevy_many_relationship_edges")]
use serde::de::DeserializeOwned;
use serde_json::Value;
use std::{
    any::TypeId,
    collections::{HashMap, HashSet},
};

type ComponentSerializer =
    Box<dyn Fn(Entity, &World) -> Result<Option<(String, Value)>, PersistenceError> + Send + Sync>;
type ComponentDeserializer =
    Box<dyn Fn(&mut World, Entity, Value) -> Result<(), PersistenceError> + Send + Sync>;
type ResourceSerializer = Box<
    dyn Fn(&World, &PersistenceSession) -> Result<Option<(String, Value)>, PersistenceError>
        + Send
        + Sync,
>;
type ResourceDeserializer =
    Box<dyn Fn(&mut World, Value) -> Result<(), PersistenceError> + Send + Sync>;
type ResourceRemover = Box<dyn Fn(&mut World) + Send + Sync>;

/// Extracts all edge documents for a given relationship type from the world.
/// The third parameter is a map of client-side preassigned keys for entities that are new in the
/// current commit (not yet in the session's entity-key cache); it enables entity + relationship
/// to be persisted in a single commit.
type RelationshipSerializer = Box<
    dyn Fn(&World, &PersistenceSession, &HashMap<Entity, String>) -> Result<Vec<crate::core::db::connection::EdgeDocument>, PersistenceError>
        + Send
        + Sync,
>;

type RelationshipDeserializer = Box<
    dyn Fn(&mut World, Entity, Vec<(Entity, Option<Value>)>) -> Result<(), PersistenceError>
        + Send
        + Sync,
>;

#[derive(Default)]
struct ChangeTracking {
    dirty_entity_components: HashMap<Entity, HashSet<TypeId>>,
    despawned_entities: HashSet<Entity>,
    dirty_resources: HashSet<TypeId>,
    despawned_resources: HashSet<TypeId>,
    /// Entities whose relationships have been marked dirty.
    dirty_relationship_entities: HashSet<Entity>,
}

#[derive(Default)]
struct ComponentRegistry {
    serializers: HashMap<TypeId, ComponentSerializer>,
    deserializers: HashMap<String, ComponentDeserializer>,
    type_id_to_name: HashMap<TypeId, &'static str>,
    name_to_type_id: HashMap<String, TypeId>,
    presence: HashMap<String, Box<dyn Fn(&World, Entity) -> bool + Send + Sync>>,
}

#[derive(Default)]
struct ResourceRegistry {
    serializers: HashMap<TypeId, ResourceSerializer>,
    deserializers: HashMap<String, ResourceDeserializer>,
    name_to_type_id: HashMap<String, TypeId>,
    type_id_to_name: HashMap<TypeId, &'static str>,
    removers: HashMap<TypeId, ResourceRemover>,
    presence: HashMap<TypeId, Box<dyn Fn(&World) -> bool + Send + Sync>>,
    last_seen_present: HashMap<TypeId, bool>,
}

#[derive(Default)]
struct RelationshipRegistry {
    /// One serializer per relationship TypeId. Extracts edge documents.
    serializers: HashMap<TypeId, RelationshipSerializer>,
    /// One deserializer per relationship TypeId. Applies edge payloads to ECS state.
    deserializers: HashMap<TypeId, RelationshipDeserializer>,
    /// Maps relationship type name → TypeId.
    name_to_type_id: HashMap<String, TypeId>,
    /// Maps TypeId → relationship type name.
    type_id_to_name: HashMap<TypeId, &'static str>,
}

struct PersistenceCache {
    entity_keys: HashMap<Entity, String>,
    guid_to_entity: HashMap<String, Entity>,
    version_manager: VersionManager,
    /// Last-committed edge snapshot, keyed by deterministic edge key.
    /// Used for diffing to determine upserts and deletes.
    edge_snapshot: HashSet<String>,
}

impl Default for PersistenceCache {
    fn default() -> Self {
        Self {
            entity_keys: HashMap::new(),
            guid_to_entity: HashMap::new(),
            version_manager: VersionManager::new(),
            edge_snapshot: HashSet::new(),
        }
    }
}

pub(crate) struct DirtyState {
    dirty_entity_components: HashMap<Entity, HashSet<TypeId>>,
    despawned_entities: HashSet<Entity>,
    dirty_resources: HashSet<TypeId>,
    despawned_resources: HashSet<TypeId>,
    dirty_relationship_entities: HashSet<Entity>,
}

impl DirtyState {
    pub(crate) fn from_parts(
        dirty_entity_components: HashMap<Entity, HashSet<TypeId>>,
        despawned_entities: HashSet<Entity>,
        dirty_resources: HashSet<TypeId>,
        despawned_resources: HashSet<TypeId>,
    ) -> Self {
        Self {
            dirty_entity_components,
            despawned_entities,
            dirty_resources,
            despawned_resources,
            dirty_relationship_entities: HashSet::new(),
        }
    }

    pub(crate) fn from_parts_with_relationships(
        dirty_entity_components: HashMap<Entity, HashSet<TypeId>>,
        despawned_entities: HashSet<Entity>,
        dirty_resources: HashSet<TypeId>,
        despawned_resources: HashSet<TypeId>,
        dirty_relationship_entities: HashSet<Entity>,
    ) -> Self {
        Self {
            dirty_entity_components,
            despawned_entities,
            dirty_resources,
            despawned_resources,
            dirty_relationship_entities,
        }
    }

    pub(crate) fn into_parts(
        self,
    ) -> (
        HashMap<Entity, HashSet<TypeId>>,
        HashSet<Entity>,
        HashSet<TypeId>,
        HashSet<TypeId>,
        HashSet<Entity>,
    ) {
        (
            self.dirty_entity_components,
            self.despawned_entities,
            self.dirty_resources,
            self.despawned_resources,
            self.dirty_relationship_entities,
        )
    }
}

/// Manages a "unit of work": local World cache + change tracking + async runtime.
#[derive(Resource)]
pub struct PersistenceSession {
    tracking: ChangeTracking,
    components: ComponentRegistry,
    resources: ResourceRegistry,
    relationships: RelationshipRegistry,
    cache: PersistenceCache,
}

pub(crate) struct CommitData {
    pub(crate) operations: Vec<TransactionOperation>,
    pub(crate) new_entities: Vec<Entity>,
    /// Updated edge snapshot to apply on successful commit.
    pub(crate) new_edge_snapshot: HashSet<String>,
    /// Client-side preassigned keys for new entities.
    pub(crate) preassigned_keys: HashMap<Entity, String>,
}

impl PersistenceSession {
    /// Registers a component type for persistence.
    ///
    /// This method sets up both serialization and deserialization for any
    /// component that implements the `Persist` marker trait.
    pub fn register_component<T: Component + Persist>(&mut self) {
        let ser_key = T::name();
        let type_id = TypeId::of::<T>();
        self.components.type_id_to_name.insert(type_id, ser_key);
        // reverse lookup
        self.components
            .name_to_type_id
            .insert(ser_key.to_string(), type_id);
        // presence checker
        self.components.presence.insert(
            ser_key.to_string(),
            Box::new(|world: &World, entity: Entity| world.entity(entity).contains::<T>()),
        );
        self.components.serializers.insert(
            type_id,
            Box::new(
                move |entity, world| -> Result<Option<(String, Value)>, PersistenceError> {
                    if let Some(c) = world.get::<T>(entity) {
                        let v = serde_json::to_value(c)
                            .map_err(|_| PersistenceError::new("Serialization failed"))?;
                        if v.is_null() {
                            return Err(PersistenceError::new("Could not serialize"));
                        }
                        Ok(Some((ser_key.to_string(), v)))
                    } else {
                        Ok(None)
                    }
                },
            ),
        );

        let de_key = T::name();
        self.components.deserializers.insert(
            de_key.to_string(),
            Box::new(|world, entity, json_val| {
                let comp: T = serde_json::from_value(json_val)
                    .map_err(|e| PersistenceError::new(e.to_string()))?;
                world.entity_mut(entity).insert(comp);
                Ok(())
            }),
        );
    }

    /// Registers a resource type for persistence.
    ///
    /// This method sets up both serialization and deserialization for any
    /// resource that implements the `Persist` marker trait.
    pub fn register_resource<R: Resource + Persist>(&mut self) {
        let ser_key = R::name();
        let type_id = std::any::TypeId::of::<R>();
        self.resources.type_id_to_name.insert(type_id, ser_key);
        self.resources.presence.insert(
            type_id,
            Box::new(|world: &World| world.get_resource::<R>().is_some()),
        );
        // Insert serializer into map keyed by TypeId
        self.resources.serializers.insert(
            type_id,
            Box::new(move |world, _session| {
                // Fetch and serialize the resource
                if let Some(r) = world.get_resource::<R>() {
                    let v = serde_json::to_value(r)
                        .map_err(|e| PersistenceError::new(e.to_string()))?;
                    if v.is_null() {
                        return Err(PersistenceError::new("Could not serialize"));
                    }
                    Ok(Some((ser_key.to_string(), v)))
                } else {
                    Ok(None)
                }
            }),
        );

        let de_key = R::name();
        self.resources.deserializers.insert(
            de_key.to_string(),
            Box::new(|world, json_val| {
                let res: R = serde_json::from_value(json_val)
                    .map_err(|e| PersistenceError::new(e.to_string()))?;
                world.insert_resource(res);
                Ok(())
            }),
        );
        self.resources
            .name_to_type_id
            .insert(de_key.to_string(), type_id);
        
        // Register remover function
        self.resources.removers.insert(
            type_id,
            Box::new(|world| {
                world.remove_resource::<R>();
            }),
        );
    }

    /// Manually mark a resource as needing persistence.
    pub fn mark_resource_dirty<R: Resource + Persist>(&mut self) {
        self.tracking.dirty_resources.insert(TypeId::of::<R>());
    }

    /// Manually mark a persisted resource as having been removed.
    pub fn mark_resource_despawned<R: Resource + Persist>(&mut self) {
        self.tracking.despawned_resources.insert(TypeId::of::<R>());
    }

    pub(crate) fn mark_resource_despawned_type_id(&mut self, type_id: TypeId) {
        self.tracking.despawned_resources.insert(type_id);
    }

    /// Mark a specific persisted component type as dirty for an entity.
    pub(crate) fn mark_entity_component_dirty(&mut self, entity: Entity, component: TypeId) {
        self.tracking
            .dirty_entity_components
            .entry(entity)
            .or_default()
            .insert(component);
    }

    /// Mark an entity's relationships as dirty.
    pub(crate) fn mark_relationship_entity_dirty(&mut self, entity: Entity) {
        self.tracking.dirty_relationship_entities.insert(entity);
    }

    /// Register a relationship type for edge persistence.
    /// The serializer closure extracts all edge documents for this relationship type.
    pub fn register_relationship(
        &mut self,
        type_id: TypeId,
        name: &'static str,
        serializer: RelationshipSerializer,
    ) {
        self.relationships.type_id_to_name.insert(type_id, name);
        self.relationships
            .name_to_type_id
            .insert(name.to_string(), type_id);
        self.relationships.serializers.insert(type_id, serializer);
    }

    /// Register a built-in Bevy `Relationship` component for edge persistence.
    ///
    /// This iterates all entities that have `R` and a cached GUID, extracting
    /// each relationship target to build `EdgeDocument`s. Disabled when the
    /// `bevy_many_relationship_edges` feature is enabled.
    #[allow(deprecated)]
    #[cfg(not(feature = "bevy_many_relationship_edges"))]
    pub fn register_bevy_relationship<R: Component + bevy::ecs::relationship::Relationship>(
        &mut self,
        name: &'static str,
    ) {
        use crate::core::db::connection::EdgeDocument;
        let type_id = TypeId::of::<R>();
        self.register_relationship(
            type_id,
            name,
            Box::new(move |world, session, preassigned: &HashMap<Entity, String>| {
                let mut edges = Vec::new();
                #[allow(deprecated)]
                for entity_ref in world.iter_entities() {
                    let from_entity = entity_ref.id();
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
            }),
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
                    world.entity_mut(source_entity).insert(<R as From<Entity>>::from(target));
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
    #[allow(deprecated)]
    #[cfg(feature = "bevy_many_relationship_edges")]
    pub fn register_many_relationship<R: serde::Serialize + DeserializeOwned + Send + Sync + 'static>(
        &mut self,
        name: &'static str,
    ) {
        use crate::core::db::connection::EdgeDocument;
        let type_id = TypeId::of::<R>();
        self.register_relationship(
            type_id,
            name,
            Box::new(move |world, session, preassigned: &HashMap<Entity, String>| {
                let mut edges = Vec::new();
                #[allow(deprecated)]
                for entity_ref in world.iter_entities() {
                    let from_entity = entity_ref.id();
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
            }),
        );
        self.relationships.deserializers.insert(
            type_id,
            Box::new(|world, source_entity, targets| {
                if let Some(existing) = world.get::<bevy_many_relationships::OutgoingRelationships<R>>(source_entity) {
                    let existing_targets: Vec<Entity> = existing.targets().copied().collect();
                    for target in existing_targets {
                        bevy_many_relationships::remove_many_relationship::<R>(world, source_entity, target);
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
    }

    /// Replace the edge snapshot cache with a new set of keys.
    pub(crate) fn set_edge_snapshot(&mut self, snapshot: HashSet<String>) {
        self.cache.edge_snapshot = snapshot;
    }

    /// Return the tracked version for a persisted resource type, if cached.
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

    /// Run the registered resource deserializer for a given persisted resource name.
    pub fn deserialize_resource_by_name(
        &self,
        world: &mut World,
        name: &str,
        value: Value,
    ) -> Result<(), PersistenceError> {
        if let Some(deser) = self.resources.deserializers.get(name) {
            deser(world, value)
        } else {
            Err(PersistenceError::new(format!(
                "no deserializer registered for resource {name}"
            )))
        }
    }

    /// Manually mark an entity as having been removed.
    pub fn mark_despawned(&mut self, entity: Entity) {
        self.tracking.despawned_entities.insert(entity);
    }

    pub(crate) fn take_dirty_state(&mut self) -> DirtyState {
        DirtyState {
            dirty_entity_components: std::mem::take(&mut self.tracking.dirty_entity_components),
            despawned_entities: std::mem::take(&mut self.tracking.despawned_entities),
            dirty_resources: std::mem::take(&mut self.tracking.dirty_resources),
            despawned_resources: std::mem::take(&mut self.tracking.despawned_resources),
            dirty_relationship_entities: std::mem::take(&mut self.tracking.dirty_relationship_entities),
        }
    }

    pub(crate) fn restore_dirty_state(&mut self, state: DirtyState) {
        for (entity, dirty) in state.dirty_entity_components {
            self.tracking
                .dirty_entity_components
                .entry(entity)
                .or_default()
                .extend(dirty);
        }
        self.tracking
            .despawned_entities
            .extend(state.despawned_entities);
        self.tracking.dirty_resources.extend(state.dirty_resources);
        self.tracking
            .despawned_resources
            .extend(state.despawned_resources);
        self.tracking
            .dirty_relationship_entities
            .extend(state.dirty_relationship_entities);
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

    pub(crate) fn version_manager(&self) -> &VersionManager {
        &self.cache.version_manager
    }

    pub(crate) fn version_manager_mut(&mut self) -> &mut VersionManager {
        &mut self.cache.version_manager
    }

    pub(crate) fn entity_keys(&self) -> &HashMap<Entity, String> {
        &self.cache.entity_keys
    }

    pub(crate) fn entity_key(&self, entity: Entity) -> Option<&String> {
        self.cache.entity_keys.get(&entity)
    }

    pub(crate) fn insert_entity_key(&mut self, entity: Entity, key: String) {
        if let Some(existing) = self.cache.entity_keys.insert(entity, key.clone()) {
            if existing != key {
                self.cache.guid_to_entity.remove(&existing);
            }
        }
        self.cache.guid_to_entity.insert(key, entity);
    }

    pub(crate) fn entity_by_key(&self, key: &str) -> Option<Entity> {
        self.cache.guid_to_entity.get(key).copied()
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
        &self,
        type_id: TypeId,
        world: &mut World,
        source: Entity,
        targets: Vec<(Entity, Option<Value>)>,
    ) -> Result<(), PersistenceError> {
        if let Some(deserializer) = self.relationships.deserializers.get(&type_id) {
            deserializer(world, source, targets)?;
        }
        Ok(())
    }

    pub(crate) fn component_deserializer(
        &self,
        name: &str,
    ) -> Option<&ComponentDeserializer> {
        self.components.deserializers.get(name)
    }

    pub(crate) fn component_deserializers(
        &self,
    ) -> impl Iterator<Item = (&String, &ComponentDeserializer)> {
        self.components.deserializers.iter()
    }

    #[cfg(test)]
    pub(crate) fn resource_deserializer(&self, name: &str) -> Option<&ResourceDeserializer> {
        self.resources.deserializers.get(name)
    }

    #[cfg(test)]
    pub(crate) fn clear_dirty_entity_components(&mut self) {
        self.tracking.dirty_entity_components.clear();
    }

    #[cfg(test)]
    pub(crate) fn is_entity_dirty(&self, entity: Entity) -> bool {
        self.tracking.dirty_entity_components.contains_key(&entity)
    }

    #[cfg(test)]
    pub(crate) fn is_dirty_entity_components_empty(&self) -> bool {
        self.tracking.dirty_entity_components.is_empty()
    }

    #[cfg(test)]
    pub(crate) fn is_despawned_entities_empty(&self) -> bool {
        self.tracking.despawned_entities.is_empty()
    }

    #[cfg(test)]
    pub(crate) fn is_resource_dirty(&self, type_id: TypeId) -> bool {
        self.tracking.dirty_resources.contains(&type_id)
    }

    /// Testing constructor w/ mock DB.
    #[cfg(test)]
    pub fn new_mocked() -> Self {
        Self::new()
    }

    /// Create a new session.
    pub fn new() -> Self {
        Self {
            tracking: ChangeTracking::default(),
            components: ComponentRegistry::default(),
            resources: ResourceRegistry::default(),
            relationships: RelationshipRegistry::default(),
            cache: PersistenceCache::default(),
        }
    }

    /// Fetch the document for the given key from `db` and deserialize its components
    /// into `world` for `entity`. Also caches the document version.
    pub async fn fetch_and_insert_document(
        &mut self,
        db: &(dyn DatabaseConnection + 'static),
        store: &str,
        world: &mut World,
        key: &str,
        entity: Entity,
        component_names: &[&'static str],
    ) -> Result<(), PersistenceError> {
        if let Some((doc, version)) = db.fetch_document(store, key).await? {
            // Cache the version
            self.cache
                .version_manager
                .set_version(VersionKey::Entity(key.to_string()), version);

            // Deserialize requested components
            for &comp_name in component_names {
                if let Some(val) = doc.get(comp_name) {
                    if let Some(deser) = self.components.deserializers.get(comp_name) {
                        deser(world, entity, val.clone())?;
                    }
                }
            }
        }
        Ok(())
    }

    /// Fetch each registered resource's JSON blob from `db`
    /// and run the registered deserializer to insert it into `world`.
    pub async fn fetch_and_insert_resources(
        &mut self,
        db: &(dyn DatabaseConnection + 'static),
        store: &str,
        world: &mut World,
    ) -> Result<(), PersistenceError> {
        for (res_name, deser) in self.resources.deserializers.iter() {
            if let Some((val, version)) = db.fetch_resource(store, res_name).await? {
                // Cache the version based on the resource's TypeId using the map.
                if let Some(type_id) = self.resources.name_to_type_id.get(res_name) {
                    self.cache
                        .version_manager
                        .set_version(VersionKey::Resource(*type_id), version);
                }
                deser(world, val)?;
            }
        }
        Ok(())
    }

    /// Fetch each named component from `db` for the given document `key` and
    /// run the registered deserializer to insert it into `world` for `entity`.
    pub async fn fetch_and_insert_components(
        &self,
        db: &(dyn DatabaseConnection + 'static),
        store: &str,
        world: &mut World,
        key: &str,
        entity: Entity,
        component_names: &[&'static str],
    ) -> Result<(), PersistenceError> {
        for &comp_name in component_names {
            if let Some(val) = db.fetch_component(store, key, comp_name).await? {
                if let Some(deser) = self.components.deserializers.get(comp_name) {
                    deser(world, entity, val)?;
                }
            }
        }
        Ok(())
    }

    /// Prepare all operations (deletions, creations, updates of entities and resources)
    /// using a Rayon pool of `thread_count` threads.
    ///
    /// **Client-side GUID generation**: new entities that don't yet have a cached
    /// key are assigned a UUID v4 *before* the CreateDocument is built, and the
    /// key is included in the document payload under `key_field`.
    pub(crate) fn _prepare_commit(
        session: &PersistenceSession,
        world: &World,
        dirty_entity_components: &HashMap<Entity, HashSet<TypeId>>,
        despawned_entities: &HashSet<Entity>,
        dirty_resources: &HashSet<TypeId>,
        despawned_resources: &HashSet<TypeId>,
        dirty_relationship_entities: &HashSet<Entity>,
        thread_count: usize,
        key_field: &str,
        store: &str,
    ) -> Result<CommitData, PersistenceError> {
        if store.is_empty() {
            return Err(PersistenceError::new("store must be provided for commit"));
        }
        let mut operations = Vec::new();

        // Pre-compute client-side GUIDs for new entities.
        // Entities in dirty_entity_components that don't have a cached key need one.
        let mut preassigned_keys: HashMap<Entity, String> = HashMap::new();
        for &entity in dirty_entity_components.keys() {
            if session.cache.entity_keys.get(&entity).is_none() {
                // Check if the entity already has a Guid component
                let guid = world.get::<crate::bevy::components::Guid>(entity);
                let key = if let Some(guid) = guid {
                    guid.id().to_string()
                } else {
                    uuid::Uuid::new_v4().to_string()
                };
                preassigned_keys.insert(entity, key);
            }
        }

        fn insert_meta(
            data: &mut serde_json::Map<String, Value>,
            kind: DocumentKind,
            version: u64,
        ) {
            let mut meta = serde_json::Map::new();
            meta.insert(
                BEVY_PERSISTENCE_DATABASE_VERSION_FIELD.to_string(),
                serde_json::json!(version),
            );
            meta.insert(BEVY_PERSISTENCE_DATABASE_BEVY_TYPE_FIELD.to_string(), serde_json::json!(kind.as_str()));
            data.insert(BEVY_PERSISTENCE_DATABASE_METADATA_FIELD.to_string(), Value::Object(meta));
        }

        // 1) Deletions (order matters less, do sequentially)
        for &entity in despawned_entities {
            if let Some(key) = session.cache.entity_keys.get(&entity) {
                let version_key = VersionKey::Entity(key.clone());
                let current_version = session
                    .cache
                    .version_manager
                    .get_version(&version_key)
                    .ok_or_else(|| PersistenceError::new("Missing version for deletion"))?;
                operations.push(TransactionOperation::DeleteDocument {
                    store: store.to_string(),
                    kind: crate::core::db::connection::DocumentKind::Entity,
                    key: key.clone(),
                    expected_current_version: current_version,
                });
            }
        }

        for &resource_type_id in despawned_resources {
            let Some(name) = session.resources.type_id_to_name.get(&resource_type_id) else {
                continue;
            };
            let version_key = VersionKey::Resource(resource_type_id);
            let Some(current_version) = session.cache.version_manager.get_version(&version_key) else {
                continue;
            };
            operations.push(TransactionOperation::DeleteDocument {
                store: store.to_string(),
                kind: crate::core::db::connection::DocumentKind::Resource,
                key: name.to_string(),
                expected_current_version: current_version,
            });
        }

        // Build a Rayon pool
        let pool = ThreadPoolBuilder::new()
            .num_threads(thread_count)
            .build()
            .map_err(|e| PersistenceError::new(format!("ThreadPool error: {}", e)))?;

        // 2) Creations & Updates (entities)
        let entity_ops_result: Result<Vec<_>, PersistenceError> = pool.install(|| {
            dirty_entity_components
                .par_iter()
                .map(|(&entity, dirty_components)| {
                    let mut data_map = serde_json::Map::new();

                    let component_type_ids: Vec<TypeId> =
                        dirty_components.iter().copied().collect();

                    for component_type_id in component_type_ids {
                        let Some(serializer) = session
                            .components
                            .serializers
                            .get(&component_type_id)
                        else {
                            continue;
                        };
                        if let Some((field_name, value)) = serializer(entity, world)? {
                            data_map.insert(field_name, value);
                        }
                    }
                    if data_map.is_empty() {
                        return Ok(None);
                    }
                    if let Some(key) = session.cache.entity_keys.get(&entity) {
                        // update existing
                        let version_key = VersionKey::Entity(key.clone());
                        let current_version = session
                            .cache
                            .version_manager
                            .get_version(&version_key)
                            .ok_or_else(|| PersistenceError::new("Missing version for update"))?;
                        let next_version = current_version + 1;
                        insert_meta(&mut data_map, DocumentKind::Entity, next_version);
                        Ok(Some((
                            TransactionOperation::UpdateDocument {
                                store: store.to_string(),
                                kind: DocumentKind::Entity,
                                key: key.clone(),
                                expected_current_version: current_version,
                                patch: Value::Object(data_map),
                            },
                            None,
                        )))
                    } else if let Some(key) = preassigned_keys.get(&entity) {
                        // create new document with client-side GUID
                        data_map.insert(key_field.to_string(), Value::String(key.clone()));
                        insert_meta(&mut data_map, DocumentKind::Entity, 1);
                        let document = Value::Object(data_map);
                        Ok(Some((
                            TransactionOperation::CreateDocument {
                                store: store.to_string(),
                                kind: DocumentKind::Entity,
                                data: document,
                            },
                            Some(entity),
                        )))
                    } else {
                        // Shouldn't happen - entity should have a cached key or preassigned key
                        Err(PersistenceError::new(format!(
                            "Entity {:?} has no cached key and no preassigned key",
                            entity
                        )))
                    }
                })
                .filter_map(|res| res.transpose())
                .collect::<Result<Vec<(TransactionOperation, Option<Entity>)>, PersistenceError>>()
        });

        let (entity_ops, created): (Vec<TransactionOperation>, Vec<Option<Entity>>) =
            match entity_ops_result {
                Ok(ops_and_entities) => ops_and_entities.into_iter().unzip(),
                Err(e) => return Err(e),
            };

        operations.extend(entity_ops);
        let newly_created_entities: Vec<Entity> = created.into_iter().flatten().collect();

        // 3) Resources (in parallel)
        let resource_ops_result: Result<Vec<_>, PersistenceError> = pool.install(|| {
            let mut resource_ops = Vec::new();

            for &resource_type_id in dirty_resources {
                if despawned_resources.contains(&resource_type_id) {
                    continue;
                }
                if let Some(serializer) = session.resources.serializers.get(&resource_type_id) {
                    match serializer(world, session) {
                        Ok(Some((name, mut value))) => {
                            let version_key = VersionKey::Resource(resource_type_id);
                            if let Some(current_version) =
                                session.cache.version_manager.get_version(&version_key)
                            {
                                // update existing resource
                                let next_version = current_version + 1;
                                if let Some(obj) = value.as_object_mut() {
                                    insert_meta(obj, DocumentKind::Resource, next_version);
                                }
                                resource_ops.push(TransactionOperation::UpdateDocument {
                                    store: store.to_string(),
                                    kind: DocumentKind::Resource,
                                    key: name,
                                    expected_current_version: current_version,
                                    patch: value,
                                });
                            } else {
                                // create new resource
                                if let Some(obj) = value.as_object_mut() {
                                    // Use backend-specific key field instead of hardcoding "_key"
                                    obj.insert(key_field.to_string(), Value::String(name.clone()));
                                    insert_meta(obj, DocumentKind::Resource, 1);
                                }
                                resource_ops.push(TransactionOperation::CreateDocument {
                                    store: store.to_string(),
                                    kind: DocumentKind::Resource,
                                    data: value,
                                });
                            }
                        }
                        Ok(None) => {} // Nothing to do if serializer returns None
                        Err(e) => return Err(e),
                    }
                }
            }

            Ok(resource_ops)
        });

        let resource_ops = resource_ops_result?;
        operations.extend(resource_ops);

        // 4) Relationship edge operations (diff-based)
        // Only run if there are registered relationships and dirty entities
        let has_relationships = !session.relationships.serializers.is_empty();
        let has_dirty_rels = !dirty_relationship_entities.is_empty() || !despawned_entities.is_empty();
        let mut new_edge_snapshot: HashSet<String> = session.cache.edge_snapshot.clone();

        if has_relationships && has_dirty_rels {
            use crate::core::db::connection::EdgeDocument;

            // Build the current desired edge set from all relationship serializers
            let mut current_edges: HashMap<String, EdgeDocument> = HashMap::new();
            for (_type_id, serializer) in session.relationships.serializers.iter() {
                let edges = serializer(world, session, &preassigned_keys)?;
                for edge in edges {
                    current_edges.insert(edge.key.clone(), edge);
                }
            }

            // Compute diff against snapshot
            let current_keys: HashSet<String> = current_edges.keys().cloned().collect();

            // Edges to upsert: in current but not in snapshot (new edges)
            let to_upsert: Vec<EdgeDocument> = current_keys
                .difference(&session.cache.edge_snapshot)
                .filter_map(|k| current_edges.get(k).cloned())
                .collect();

            // Edges to delete: in snapshot but not in current (removed edges)
            let to_delete: Vec<String> = session
                .cache
                .edge_snapshot
                .difference(&current_keys)
                .cloned()
                .collect();

            if !to_upsert.is_empty() {
                operations.push(TransactionOperation::UpsertEdges {
                    store: store.to_string(),
                    edges: to_upsert,
                });
            }

            if !to_delete.is_empty() {
                operations.push(TransactionOperation::DeleteEdges {
                    store: store.to_string(),
                    keys: to_delete,
                });
            }

            // Update the snapshot for post-commit
            new_edge_snapshot = current_keys;
        }

        info!(
            "[_prepare_commit] Prepared {} operations.",
            operations.len()
        );
        Ok(CommitData {
            operations,
            new_entities: newly_created_entities,
            new_edge_snapshot,
            preassigned_keys,
        })
    }
}


#[cfg(test)]
mod arango_session {
    use super::*;
    use crate::core::persist::Persist;
    use crate::core::db::connection::EdgeDocument;
    use crate::bevy::registration::COMPONENT_REGISTRY;
    use bevy::prelude::World;
    use bevy_persistence_database_derive::persist;
    use serde_json::json;
    use std::any::TypeId;

    fn setup() {
        // Clear the global registry to avoid test pollution from other modules
        let mut registry = COMPONENT_REGISTRY.lock().unwrap();
        registry.clear();
    }

    #[persist(resource)]
    struct MyRes {
        value: i32,
    }
    #[persist(component)]
    struct MyComp {
        value: i32,
    }

    #[test]
    fn new_session_is_empty() {
        setup();
        let session = PersistenceSession::new_mocked();
        assert!(session.is_dirty_entity_components_empty());
        assert!(session.is_despawned_entities_empty());
    }

    #[test]
    fn deserializer_inserts_component() {
        setup();
        let mut world = World::new();
        let entity = world.spawn_empty().id();

        let mut session = PersistenceSession::new();
        session.register_component::<MyComp>();

        let deserializer = session.component_deserializer(MyComp::name()).unwrap();
        deserializer(&mut world, entity, json!({"value": 42})).unwrap();

        assert_eq!(world.get::<MyComp>(entity).unwrap().value, 42);
    }

    #[test]
    fn deserializer_inserts_resource() {
        setup();
        let mut world = World::new();

        let mut session = PersistenceSession::new();
        session.register_resource::<MyRes>();

        let deserializer = session.resource_deserializer(MyRes::name()).unwrap();
        deserializer(&mut world, json!({"value": 5})).unwrap();

        assert_eq!(world.resource::<MyRes>().value, 5);
    }

    #[test]
    fn prepare_commit_emits_resource_delete_when_marked_despawned() {
        setup();
        let world = World::new();

        let mut session = PersistenceSession::new();
        session.register_resource::<MyRes>();

        let tid = TypeId::of::<MyRes>();
        session
            .version_manager_mut()
            .set_version(VersionKey::Resource(tid), 3);

        let dirty_entity_components: HashMap<Entity, HashSet<TypeId>> = HashMap::new();
        let despawned_entities = HashSet::new();
        let dirty_resources = HashSet::new();
        let mut despawned_resources = HashSet::new();
        despawned_resources.insert(tid);
        let dirty_relationship_entities = HashSet::new();

        let data = PersistenceSession::_prepare_commit(
            &session,
            &world,
            &dirty_entity_components,
            &despawned_entities,
            &dirty_resources,
            &despawned_resources,
            &dirty_relationship_entities,
            1,
            "_key",
            "store",
        )
        .unwrap();

        assert_eq!(data.operations.len(), 1);
        match &data.operations[0] {
            TransactionOperation::DeleteDocument {
                store,
                kind,
                key,
                expected_current_version,
            } => {
                assert_eq!(store, "store");
                assert!(matches!(kind, DocumentKind::Resource));
                assert_eq!(key, MyRes::name());
                assert_eq!(*expected_current_version, 3);
            }
            other => panic!("expected resource delete operation, got {other:?}"),
        }
    }

    #[test]
    fn prepare_commit_preassigns_guid_for_new_entity() {
        setup();
        let mut world = World::new();

        let mut session = PersistenceSession::new();
        session.register_component::<MyComp>();

        let entity = world.spawn(MyComp { value: 7 }).id();
        let tid = TypeId::of::<MyComp>();
        let mut dirty_entity_components: HashMap<Entity, HashSet<TypeId>> = HashMap::new();
        dirty_entity_components.insert(entity, [tid].into_iter().collect());

        let data = PersistenceSession::_prepare_commit(
            &session,
            &world,
            &dirty_entity_components,
            &HashSet::new(),
            &HashSet::new(),
            &HashSet::new(),
            &HashSet::new(),
            1,
            "_key",
            "store",
        )
        .unwrap();

        // Should have exactly one operation (CreateDocument)
        assert_eq!(data.operations.len(), 1);
        assert!(matches!(
            &data.operations[0],
            TransactionOperation::CreateDocument { .. }
        ));

        // The CreateDocument data should include a preassigned key field
        if let TransactionOperation::CreateDocument { data: doc, .. } = &data.operations[0] {
            let key = doc
                .get("_key")
                .and_then(|v| v.as_str())
                .expect("document should contain a preassigned _key");
            assert!(!key.is_empty(), "preassigned key should be non-empty UUID");

            // CommitData.preassigned_keys should also contain this entity→key mapping
            assert_eq!(data.preassigned_keys.get(&entity).map(|s| s.as_str()), Some(key));
        }
    }

    #[test]
    fn prepare_commit_uses_existing_guid_component_for_new_entity() {
        setup();
        let mut world = World::new();

        let mut session = PersistenceSession::new();
        session.register_component::<MyComp>();

        // Spawn entity with a pre-existing Guid component
        let entity = world
            .spawn((
                MyComp { value: 7 },
                crate::bevy::components::Guid::new("my-custom-guid".to_string()),
            ))
            .id();
        let tid = TypeId::of::<MyComp>();
        let mut dirty_entity_components: HashMap<Entity, HashSet<TypeId>> = HashMap::new();
        dirty_entity_components.insert(entity, [tid].into_iter().collect());

        let data = PersistenceSession::_prepare_commit(
            &session,
            &world,
            &dirty_entity_components,
            &HashSet::new(),
            &HashSet::new(),
            &HashSet::new(),
            &HashSet::new(),
            1,
            "_key",
            "store",
        )
        .unwrap();

        // The document should use the Guid we manually set
        if let TransactionOperation::CreateDocument { data: doc, .. } = &data.operations[0] {
            assert_eq!(
                doc.get("_key").and_then(|v| v.as_str()),
                Some("my-custom-guid"),
                "document should use the pre-existing Guid component value"
            );

            // CommitData.preassigned_keys should also reflect this
            assert_eq!(data.preassigned_keys.get(&entity).map(|s| s.as_str()), Some("my-custom-guid"));
        } else {
            panic!("expected CreateDocument operation");
        }
    }

    #[test]
    fn prepare_commit_edge_diff_adds_new_edges() {
        setup();
        let mut world = World::new();

        let mut session = PersistenceSession::new();
        // Register a relationship serializer that always returns two fixed edges
        let tid = TypeId::of::<MyComp>(); // reuse type id for testing
        session.register_relationship(
            tid,
            "TestRel",
            Box::new(|_world, _session, _preassigned| {
                use crate::core::db::connection::EdgeDocument;
                Ok(vec![
                    EdgeDocument {
                        key: EdgeDocument::make_key("TestRel", "guid_a", "guid_b"),
                        relationship_type: "TestRel".to_string(),
                        from_guid: "guid_a".to_string(),
                        to_guid: "guid_b".to_string(),
                        payload: None,
                    },
                    EdgeDocument {
                        key: EdgeDocument::make_key("TestRel", "guid_c", "guid_d"),
                        relationship_type: "TestRel".to_string(),
                        from_guid: "guid_c".to_string(),
                        to_guid: "guid_d".to_string(),
                        payload: None,
                    },
                ])
            }),
        );

        // Start with empty snapshot, mark some entity as having dirty relationships
        let dummy_entity = world.spawn_empty().id();
        let mut dirty_relationship_entities = HashSet::new();
        dirty_relationship_entities.insert(dummy_entity);

        let data = PersistenceSession::_prepare_commit(
            &session,
            &world,
            &HashMap::new(),
            &HashSet::new(),
            &HashSet::new(),
            &HashSet::new(),
            &dirty_relationship_entities,
            1,
            "_key",
            "store",
        )
        .unwrap();

        // Should have one UpsertEdges operation with 2 edges
        let upsert_ops: Vec<_> = data
            .operations
            .iter()
            .filter(|op| matches!(op, TransactionOperation::UpsertEdges { .. }))
            .collect();
        assert_eq!(upsert_ops.len(), 1);
        if let TransactionOperation::UpsertEdges { edges, .. } = upsert_ops[0] {
            assert_eq!(edges.len(), 2);
        }

        // new_edge_snapshot should contain both edge keys
        assert_eq!(data.new_edge_snapshot.len(), 2);
        assert!(data
            .new_edge_snapshot
            .contains(&EdgeDocument::make_key("TestRel", "guid_a", "guid_b")));
        assert!(data
            .new_edge_snapshot
            .contains(&EdgeDocument::make_key("TestRel", "guid_c", "guid_d")));
    }

    #[test]
    fn prepare_commit_edge_diff_deletes_removed_edges() {
        setup();
        let mut world = World::new();

        let mut session = PersistenceSession::new();
        // Register a serializer that returns NO edges (they've all been removed)
        let tid = TypeId::of::<MyComp>();
        session.register_relationship(
            tid,
            "TestRel",
            Box::new(|_world, _session, _preassigned| Ok(vec![])),
        );

        // Pre-populate snapshot with edges that should be deleted
        let old_key = EdgeDocument::make_key("TestRel", "guid_a", "guid_b");
        session.set_edge_snapshot([old_key.clone()].into_iter().collect());

        let dummy_entity = world.spawn_empty().id();
        let mut dirty_relationship_entities = HashSet::new();
        dirty_relationship_entities.insert(dummy_entity);

        let data = PersistenceSession::_prepare_commit(
            &session,
            &world,
            &HashMap::new(),
            &HashSet::new(),
            &HashSet::new(),
            &HashSet::new(),
            &dirty_relationship_entities,
            1,
            "_key",
            "store",
        )
        .unwrap();

        // Should have one DeleteEdges operation with 1 key
        let delete_ops: Vec<_> = data
            .operations
            .iter()
            .filter(|op| matches!(op, TransactionOperation::DeleteEdges { .. }))
            .collect();
        assert_eq!(delete_ops.len(), 1);
        if let TransactionOperation::DeleteEdges { keys, .. } = delete_ops[0] {
            assert_eq!(keys.len(), 1);
            assert_eq!(keys[0], old_key);
        }

        // new_edge_snapshot should be empty (all edges removed)
        assert!(data.new_edge_snapshot.is_empty());
    }

    #[test]
    fn edge_document_make_key_is_deterministic() {
        use crate::core::db::connection::EdgeDocument;
        let k1 = EdgeDocument::make_key("ChildOf", "aaa", "bbb");
        let k2 = EdgeDocument::make_key("ChildOf", "aaa", "bbb");
        assert_eq!(k1, k2);
        assert_eq!(k1, "ChildOf:aaa:bbb");
    }

    #[test]
    fn no_edge_ops_when_relationships_not_dirty() {
        setup();
        let world = World::new();

        let mut session = PersistenceSession::new();
        let tid = TypeId::of::<MyComp>();
        session.register_relationship(
            tid,
            "TestRel",
            Box::new(|_world, _session, _preassigned| {
                use crate::core::db::connection::EdgeDocument;
                Ok(vec![EdgeDocument {
                    key: EdgeDocument::make_key("TestRel", "a", "b"),
                    relationship_type: "TestRel".to_string(),
                    from_guid: "a".to_string(),
                    to_guid: "b".to_string(),
                    payload: None,
                }])
            }),
        );

        // Empty dirty_relationship_entities → no edge ops
        let data = PersistenceSession::_prepare_commit(
            &session,
            &world,
            &HashMap::new(),
            &HashSet::new(),
            &HashSet::new(),
            &HashSet::new(),
            &HashSet::new(), // no dirty relationships
            1,
            "_key",
            "store",
        )
        .unwrap();

        // Should have no edge operations
        assert!(data
            .operations
            .iter()
            .all(|op| !matches!(
                op,
                TransactionOperation::UpsertEdges { .. }
                    | TransactionOperation::DeleteEdges { .. }
            )));
    }
}
