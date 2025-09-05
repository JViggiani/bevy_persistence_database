//! A Bevy `Plugin` for integrating `bevy_persistence_database`.
//!
//! This plugin simplifies the setup process by managing the `PersistenceSession`
//! as a resource and automatically adding systems for change detection.

use crate::registration::COMPONENT_REGISTRY;
use crate::{PersistenceError, PersistenceSession, DatabaseConnection, Guid, Persist, TransactionOperation, Collection};
use crate::db::connection::DatabaseConnectionResource;
use crate::versioning::version_manager::VersionKey;
use bevy::app::PluginGroupBuilder;
use bevy::prelude::*;
use bevy::tasks::{IoTaskPool, TaskPool, Task};
use futures_lite::future;
use once_cell::sync::Lazy;
use std::any::TypeId;
use std::collections::{HashMap, HashSet};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Mutex,
};
use tokio::runtime::Runtime;
use tokio::sync::oneshot;

use crate::query::deferred_ops::DeferredWorldOperations;
use crate::query::immediate_world_ptr::ImmediateWorldPtr;
use crate::query::PersistenceQueryCache;

static TOKIO_RUNTIME: Lazy<Arc<Runtime>> = Lazy::new(|| {
    Arc::new(
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap(),
    )
});

/// A component holding the future result of a commit operation.
#[derive(Component)]
struct CommitTask(Task<Result<(Vec<String>, Vec<Entity>), PersistenceError>>);

/// A component that tracks the state of a multi-batch commit operation.
#[derive(Component)]
struct MultiBatchCommitTracker {
    correlation_id: u64,
    remaining_batches: Arc<AtomicUsize>,
    result_sender: Arc<Mutex<Option<oneshot::Sender<Result<(), PersistenceError>>>>>,
}

/// A `SystemSet` for grouping the core persistence systems into ordered phases.
#[derive(SystemSet, Debug, Clone, PartialEq, Eq, Hash)]
pub enum PersistenceSystemSet {
    /// Systems that run before the main commit logic, like change detection.
    PreCommit,
    /// The exclusive system that finalizes the commit.
    Commit,
}

/// An event fired when a background commit task is complete.
#[derive(Event)]
pub struct CommitCompleted(pub Result<Vec<String>, PersistenceError>, pub Vec<Entity>, pub Option<u64>);

/// A resource used to track which `Persist` types have been registered with an `App`.
/// This prevents duplicate systems from being added.
#[derive(Resource, Default)]
pub struct RegisteredPersistTypes(pub HashSet<TypeId>);

/// An event that users fire to trigger a commit.
#[derive(Event, Clone)]
pub struct TriggerCommit {
    /// An optional ID to correlate this trigger with a `CommitCompleted` event.
    pub correlation_id: Option<u64>,
}

impl Default for TriggerCommit {
    fn default() -> Self {
        Self { correlation_id: None }
    }
}

/// A state machine resource to track the commit lifecycle.
#[derive(Resource, Default, PartialEq, Debug)]
pub enum CommitStatus {
    #[default]
    Idle,
    InProgress,
    InProgressAndDirty,
}

#[derive(Resource)]
pub struct TokioRuntime(pub Arc<Runtime>);

impl TokioRuntime {
    pub fn block_on<F: std::future::Future>(&self, fut: F) -> F::Output {
        self.0.block_on(fut)
    }
}

/// A system that handles the `TriggerCommit` event to change the `CommitStatus`.
fn handle_commit_trigger(world: &mut World) {
    // We only process one commit at a time.
    // Use a temporary scope to manage borrows.
    let mut should_commit = false;
    let mut correlation_id = None;

    world.resource_scope(|world, mut events: Mut<Events<TriggerCommit>>| {
        let mut status = world.resource_mut::<CommitStatus>();
        if !events.is_empty() {
            // Drain all events. We only care that at least one was sent.
            // The correlation ID of the first one is taken, others are ignored for now.
            let first_trigger = events.drain().next().unwrap();

            match *status {
                CommitStatus::Idle => {
                    info!("[handle_commit_trigger] TriggerCommit event received. Status is Idle.");
                    should_commit = true;
                    correlation_id = first_trigger.correlation_id;
                }
                CommitStatus::InProgress => {
                    info!("[handle_commit_trigger] TriggerCommit event received while another is in progress. Queuing.");
                    *status = CommitStatus::InProgressAndDirty;
                }
                CommitStatus::InProgressAndDirty => {
                    // A commit is already in progress and another is already queued.
                    // We can ignore subsequent trigger events.
                }
            }
        }
    });

    if !should_commit {
        return;
    }

    let plugin_config = world.resource::<PersistencePluginConfig>().clone();

    // 1) isolate dirty sets from the session
    let (dirty_entities, despawned_entities, dirty_resources) = {
        let mut session = world.resource_mut::<PersistenceSession>();
        (
            std::mem::take(&mut session.dirty_entities),
            std::mem::take(&mut session.despawned_entities),
            std::mem::take(&mut session.dirty_resources),
        )
    };

    // 2) prepare commit with those sets
    let commit_data = match PersistenceSession::_prepare_commit(
        world.resource::<PersistenceSession>(),
        world,
        &dirty_entities,
        &despawned_entities,
        &dirty_resources,
        plugin_config.thread_count,
    ) {
        Ok(data) if data.operations.is_empty() => {
            // nothing to do → send completion and restore dirty sets
            world.send_event(CommitCompleted(Ok(vec![]), vec![], correlation_id));
            let mut session = world.resource_mut::<PersistenceSession>();
            session.dirty_entities.extend(dirty_entities);
            session.despawned_entities.extend(despawned_entities);
            session.dirty_resources.extend(dirty_resources);
            return;
        }
        Ok(data) => data,
        Err(e) => {
            // prepare failed → send error and restore dirty sets
            world.send_event(CommitCompleted(Err(e.clone()), vec![], correlation_id));
            let mut session = world.resource_mut::<PersistenceSession>();
            session.dirty_entities.extend(dirty_entities);
            session.despawned_entities.extend(despawned_entities);
            session.dirty_resources.extend(dirty_resources);
            return;
        }
    };

    // 3) spawn the async task(s)
    *world.resource_mut::<CommitStatus>() = CommitStatus::InProgress;
    let runtime = world.resource::<TokioRuntime>().0.clone();
    let thread_pool = IoTaskPool::get();
    let db = world.resource::<PersistenceSession>().db.clone();

    let all_operations = commit_data.operations;
    let new_entities = commit_data.new_entities;

    if plugin_config.batching_enabled && all_operations.len() > plugin_config.commit_batch_size {
        let batch_size = plugin_config.commit_batch_size;
        let session = world.resource::<PersistenceSession>();
        
        // Group operations by entity
        let mut entity_ops: HashMap<Entity, Vec<TransactionOperation>> = HashMap::new();
        let mut new_entity_ops: Vec<(TransactionOperation, Entity)> = Vec::new();
        let mut resource_ops: Vec<TransactionOperation> = Vec::new();
        let mut new_entity_idx = 0;
        
        // Categorize operations
        for op in all_operations {
            match &op {
                TransactionOperation::UpdateDocument { collection: Collection::Entities, key, .. } => {
                    // Find entity for this key
                    if let Some(entity) = session.entity_keys.iter().find(|(_, k)| *k == key).map(|(e, _)| *e) {
                        entity_ops.entry(entity).or_default().push(op);
                    }
                },
                TransactionOperation::DeleteDocument { collection: Collection::Entities, key, .. } => {
                    // Find entity for this key
                    if let Some(entity) = session.entity_keys.iter().find(|(_, k)| *k == key).map(|(e, _)| *e) {
                        entity_ops.entry(entity).or_default().push(op);
                    }
                },
                TransactionOperation::CreateDocument { collection: Collection::Entities, .. } => {
                    // New entities get their operation paired with the entity index
                    if let Some(entity) = new_entities.get(new_entity_idx) {
                        new_entity_ops.push((op, *entity));
                        new_entity_idx += 1;
                    }
                },
                // Resource operations go in their own group
                _ => resource_ops.push(op),
            }
        }
        
        // Create batches with balanced operations
        let mut batches: Vec<Vec<TransactionOperation>> = Vec::new();
        let mut batch_entities: Vec<HashSet<Entity>> = Vec::new();
        let mut batch_new_entities: Vec<Vec<Entity>> = Vec::new();
        let mut current_batch = Vec::new();
        let mut current_batch_entities = HashSet::new();
        let mut current_batch_new_entities = Vec::new();
        
        // Add entity operations in batches
        for (entity, ops) in entity_ops {
            if current_batch.len() + ops.len() > batch_size && !current_batch.is_empty() {
                // Current batch is full, start a new one
                batches.push(std::mem::take(&mut current_batch));
                batch_entities.push(std::mem::take(&mut current_batch_entities));
                batch_new_entities.push(std::mem::take(&mut current_batch_new_entities));
            }
            
            // Add all operations for this entity to the current batch
            current_batch.extend(ops);
            current_batch_entities.insert(entity);
        }
        
        // Add new entity operations in batches
        for (op, entity) in new_entity_ops {
            if current_batch.len() + 1 > batch_size && !current_batch.is_empty() {
                // Current batch is full, start a new one
                batches.push(std::mem::take(&mut current_batch));
                batch_entities.push(std::mem::take(&mut current_batch_entities));
                batch_new_entities.push(std::mem::take(&mut current_batch_new_entities));
            }
            
            current_batch.push(op);
            current_batch_new_entities.push(entity);
        }
        
        // Add resource operations to the first batch, or create a new batch if needed
        if current_batch.len() + resource_ops.len() > batch_size && !current_batch.is_empty() {
            batches.push(std::mem::take(&mut current_batch));
            batch_entities.push(std::mem::take(&mut current_batch_entities));
            batch_new_entities.push(std::mem::take(&mut current_batch_new_entities));
        }
        current_batch.extend(resource_ops);
        
        // Push the final batch if not empty
        if !current_batch.is_empty() {
            batches.push(current_batch);
            batch_entities.push(current_batch_entities);
            batch_new_entities.push(current_batch_new_entities);
        }
        
        let num_batches = batches.len();
        info!(
            "[handle_commit_trigger] Splitting commit into {} batches of size ~{}.",
            num_batches, batch_size
        );

        if let Some(cid) = correlation_id {
            if let Some(listener) = world.resource_mut::<CommitEventListeners>().0.remove(&cid) {
                world.spawn(MultiBatchCommitTracker {
                    correlation_id: cid,
                    remaining_batches: Arc::new(AtomicUsize::new(num_batches)),
                    // wrap sender in Arc<Mutex<Option<>>>
                    result_sender: Arc::new(Mutex::new(Some(listener))),
                });
            }
        }

        // Create dirty resource subsets - divide them across batches
        let mut resource_sets = Vec::with_capacity(num_batches);
        for _ in 0..num_batches {
            resource_sets.push(HashSet::new());
        }
        
        // Distribute resource types across batches
        for (i, res_type) in dirty_resources.iter().enumerate() {
            let batch_idx = i % num_batches;
            resource_sets[batch_idx].insert(*res_type);
        }

        // Spawn a task for each batch
        for (i, (batch_ops, batch_entities_set)) in batches.into_iter().zip(batch_entities.into_iter()).enumerate() {
            let batch_db = db.clone();
            let batch_runtime = runtime.clone();
            let batch_new_entities = batch_new_entities.get(i).cloned().unwrap_or_default();
            
            // Task spawning - the block_on is necessary for the Tokio runtime
            let task = thread_pool.spawn(async move {
                batch_runtime
                    .block_on(async {
                        batch_db
                            .execute_transaction(batch_ops)
                            .await
                            .map(|keys| (keys, batch_new_entities))
                    })
            });
            
            // Each batch gets its own subset of entities and resources
            let meta = CommitMeta {
                dirty_entities: batch_entities_set,
                despawned_entities: if i == 0 { despawned_entities.clone() } else { HashSet::new() },
                dirty_resources: resource_sets[i].clone(),
            };
            
            world.spawn((CommitTask(task), TriggerID(correlation_id), meta));
        }
    } else {
        let task = thread_pool.spawn(async move {
            runtime.block_on(async {
                db.execute_transaction(all_operations)
                    .await
                    .map(|keys| (keys, new_entities))
            })
        });

        world.spawn((
            CommitTask(task),
            TriggerID(correlation_id),
            CommitMeta {
                dirty_entities,
                despawned_entities,
                dirty_resources,
            },
        ));
    }
}

/// A component to correlate a commit task with its trigger event.
#[derive(Component)]
struct TriggerID(Option<u64>);

/// Carries exactly the dirty‐sets for one in‐flight commit.
#[derive(Component)]
struct CommitMeta {
    dirty_entities: HashSet<Entity>,
    despawned_entities: HashSet<Entity>,
    dirty_resources: HashSet<TypeId>,
}

/// A system that polls the running commit task and updates the state machine upon completion.
fn handle_commit_completed(
    mut commands: Commands,
    mut query: Query<(Entity, &mut CommitTask, &TriggerID, Option<&mut CommitMeta>)>,
    mut session: ResMut<PersistenceSession>,
    mut status: ResMut<CommitStatus>,
    mut completed: EventWriter<CommitCompleted>,
    mut triggers: EventWriter<TriggerCommit>,
    mut trackers: Query<(Entity, &MultiBatchCommitTracker)>,
) {
    // Keep track of entities to despawn
    let mut to_despawn = Vec::new();
    // Track if any batch had an error - errors should force status to Idle
    let mut had_error = false;
    
    for (ent, mut task, trigger_id, meta_opt) in &mut query {
        if let Some(result) = future::block_on(future::poll_once(&mut task.0)) {
            let cid = trigger_id.0;
            let mut is_final_batch = true;
            let mut should_send_result = true;

            // Set had_error if any batch has an error
            if result.is_err() {
                had_error = true;
            }

            if let Some(correlation_id) = cid {
                if let Some((tracker_entity, tracker)) = trackers
                    .iter_mut()
                    .find(|(_, t)| t.correlation_id == correlation_id)
                {
                    let remaining = tracker
                        .remaining_batches
                        .fetch_sub(1, Ordering::SeqCst)
                        - 1;
                    is_final_batch = remaining == 0;

                    // Only take and send on the channel if we have an error or it's the final batch
                    if result.is_err() || is_final_batch {
                        // Take the sender if we need to send a result
                        if let Some(sender) = tracker.result_sender.lock().unwrap().take() {
                            if result.is_err() {
                                let _ = sender.send(Err(result.as_ref().err().unwrap().clone()));
                            } else if is_final_batch {
                                let _ = sender.send(Ok(()));
                            }
                        }
                        
                        // Schedule the tracker for removal
                        commands.entity(tracker_entity).remove::<MultiBatchCommitTracker>();
                    } else {
                        // For intermediate successful batches, don't send a completion event
                        should_send_result = false;
                    }
                }
            }

            if let Some(mut meta) = meta_opt {
                // Process metadata regardless of batch position
                let event_res = match &result {
                    Ok((new_keys, new_entities)) => {
                        // assign GUIDs + initial versions
                        for (e, key) in new_entities.iter().zip(new_keys.iter()) {
                            commands.entity(*e).insert(Guid::new(key.clone()));
                            session.entity_keys.insert(*e, key.clone());
                            session.version_manager.set_version(VersionKey::Entity(key.clone()), 1);
                        }
                        // bump resource versions
                        for tid in &meta.dirty_resources {
                            let vk = VersionKey::Resource(*tid);
                            let nv = session.version_manager.get_version(&vk).unwrap_or(0) + 1;
                            session.version_manager.set_version(vk, nv);
                        }
                        
                        // bump existing-entity versions
                        for &entity in meta.dirty_entities.iter() {
                            // Skip entities that were newly created in this commit
                            if !new_entities.contains(&entity) {
                                // Only update versions for existing entities (ones with keys)
                                if let Some(key) = session.entity_keys.get(&entity) {
                                    let vk = VersionKey::Entity(key.clone());
                                    if let Some(v) = session.version_manager.get_version(&vk) {
                                        session.version_manager.set_version(vk, v + 1);
                                    }
                                }
                            }
                        }
                        
                        // remove versions for deleted entities
                        for e in &meta.despawned_entities {
                            if let Some(key) = session.entity_keys.get(e).cloned() {
                                session.version_manager.remove_version(&VersionKey::Entity(key));
                            }
                        }
                        Ok(new_keys.clone())
                    }
                    Err(err) => {
                        // restore dirty sets on failure
                        session.dirty_entities.extend(meta.dirty_entities.drain());
                        session.despawned_entities.extend(meta.despawned_entities.drain());
                        session.dirty_resources.extend(meta.dirty_resources.drain());
                        Err(err.clone())
                    }
                };
                
                // Only send completion event for the final batch or errors
                if should_send_result && (is_final_batch || result.is_err()) {
                    completed.write(CommitCompleted(event_res, vec![], cid));
                }
            } else if let Err(e) = &result {
                // A non-meta batch failed. Signal failure.
                if should_send_result {
                    completed.write(CommitCompleted(Err(e.clone()), vec![], cid));
                }
            }

            // Schedule this entity for despawn 
            to_despawn.push(ent);

            // Update status if this is the final batch or there was an error
            if is_final_batch || had_error {
                // Check if we need to trigger a chained commit
                let should_trigger_next = !had_error && *status == CommitStatus::InProgressAndDirty;
                
                // Update status to Idle
                *status = CommitStatus::Idle;
                
                // Only trigger next commit if we determined we needed to
                if should_trigger_next {
                    triggers.write(TriggerCommit::default());
                }
            }
        }
    }
    
    // If we had any error, force status to Idle regardless of other batches
    if had_error {
        *status = CommitStatus::Idle;
    }
    
    // Despawn all entities at once
    for entity in to_despawn {
        commands.entity(entity).despawn();
    }
}

/// A system that automatically marks entities with changed components as dirty.
pub fn auto_dirty_tracking_entity_system<T: Component + Persist>(
    mut session: ResMut<PersistenceSession>,
    query: Query<Entity, Or<(Added<T>, Changed<T>)>>,
) {
    for entity in query.iter() {
        debug!(
            "Marking entity {:?} as dirty due to component {}",
            entity,
            std::any::type_name::<T>()
        );
        session.dirty_entities.insert(entity);
    }
}

/// A system that automatically marks changed resources as dirty.
pub fn auto_dirty_tracking_resource_system<T: Resource + Persist>(
    mut session: ResMut<PersistenceSession>,
    resource: Option<Res<T>>,
) {
    if let Some(resource) = resource {
        if resource.is_changed() {
            session.mark_resource_dirty::<T>();
        }
    }
}

/// A system that automatically marks despawned entities as needing deletion.
fn auto_despawn_tracking_system(
    mut session: ResMut<PersistenceSession>,
    mut removed: RemovedComponents<Guid>,
) {
    for entity in removed.read() {
        session.mark_despawned(entity);
    }
}

/// Configuration for the persistence plugin.
#[derive(Resource, Clone)]
pub struct PersistencePluginConfig {
    pub batching_enabled: bool,
    pub commit_batch_size: usize,
    pub thread_count: usize,
}

impl Default for PersistencePluginConfig {
    fn default() -> Self {
        Self {
            batching_enabled: true,
            commit_batch_size: 1000,
            thread_count: 4, // default to 4 threads
        }
    }
}

/// A Bevy `Plugin` that sets up `bevy_persistence_database`.
pub struct PersistencePluginCore {
    db: Arc<dyn DatabaseConnection>,
    config: PersistencePluginConfig,
}

impl PersistencePluginCore {
    /// Creates a new `PersistencePluginCore` with the given database connection.
    pub fn new(db: Arc<dyn DatabaseConnection>) -> Self {
        Self {
            db,
            config: PersistencePluginConfig::default(),
        }
    }

    /// Configures the persistence plugin.
    pub fn with_config(mut self, config: PersistencePluginConfig) -> Self {
        self.config = config;
        self
    }
}

#[derive(Resource, Default)]
pub(crate) struct CommitEventListeners(pub(crate) HashMap<u64, oneshot::Sender<Result<(), PersistenceError>>>);

fn commit_event_listener(
    mut events: EventReader<CommitCompleted>,
    mut listeners: ResMut<CommitEventListeners>,
) {
    for event in events.read() {
        if let Some(id) = event.2 {
            if let Some(sender) = listeners.0.remove(&id) {
                info!("Found listener for commit {}. Sending result.", id);
                let result = match &event.0 {
                    Ok(_) => Ok(()),
                    Err(e) => Err(e.clone()),
                };
                let _ = sender.send(result);
            }
        }
    }
}

impl Plugin for PersistencePluginCore {
    fn build(&self, app: &mut App) {
        // Initialize the global IO task pool so IoTaskPool::get() won't panic
        IoTaskPool::get_or_init(|| TaskPool::new());

        let session = PersistenceSession::new(self.db.clone());
        app.insert_resource(session);
        app.insert_resource(self.config.clone());
        // Add the database connection as a resource
        app.insert_resource(DatabaseConnectionResource(self.db.clone()));
        app.init_resource::<RegisteredPersistTypes>();
        app.add_event::<TriggerCommit>();
        app.add_event::<CommitCompleted>();
        app.init_resource::<CommitStatus>();
        app.init_resource::<CommitEventListeners>();

        // Insert the dedicated Tokio runtime from the global static.
        app.insert_resource(TokioRuntime(TOKIO_RUNTIME.clone()));

        // Add the query cache
        app.init_resource::<PersistenceQueryCache>();
        // Initialize deferred world ops queue
        app.init_resource::<DeferredWorldOperations>();

        // Insert an initial raw world pointer so it's available before any user systems run.
        {
            let ptr: *mut World = app.world_mut() as *mut World;
            bevy::log::trace!("PersistencePluginCore: inserting initial ImmediateWorldPtr {:p}", ptr);
            if app.world().get_resource::<ImmediateWorldPtr>().is_none() {
                app.insert_resource(ImmediateWorldPtr::new(ptr));
            } else {
                app.world_mut().resource_mut::<ImmediateWorldPtr>().set(ptr);
            }
        }

        // Publisher function for the raw world pointer
        fn publish_immediate_world_ptr(world: &mut World) {
            let ptr: *mut World = world as *mut World;
            if world.get_resource::<ImmediateWorldPtr>().is_none() {
                world.insert_resource(ImmediateWorldPtr::new(ptr));
            } else {
                world.resource_mut::<ImmediateWorldPtr>().set(ptr);
            }
        }

        // Update pointer at the very start of the frame
        app.add_systems(First, publish_immediate_world_ptr);

        // Remove the process_queued_component_data system - we don't need it anymore

        // Iterate over the registration functions from the global registry.
        // Using .iter() instead of .drain() prevents test pollution.
        let registry = COMPONENT_REGISTRY.lock().unwrap();
        for reg_fn in registry.iter() {
            reg_fn(app);
        }

        // Configure the order of our system sets.
        app.configure_sets(
            PostUpdate,
            (PersistenceSystemSet::PreCommit, PersistenceSystemSet::Commit).chain(),
        );

        // Apply queued world mutations (entity spawns, component inserts) in this frame.
        // Use an exclusive system to get &mut World.
        fn apply_deferred_world_ops(world: &mut World) {
            // Drain the queue via the public method
            let mut pending = world.resource::<DeferredWorldOperations>().drain();
            // Apply all queued ops
            for op in pending.drain(..) {
                op(world);
            }
        }

        // Add both PreCommit and Commit-phase systems, ensuring deferred ops are applied
        // first in PostUpdate so pass-through systems see Update loads deterministically.
        app.add_systems(
            PostUpdate,
            (
                (
                    apply_deferred_world_ops,
                    publish_immediate_world_ptr,
                    auto_despawn_tracking_system,
                    handle_commit_trigger,
                    commit_event_listener,
                )
                    .in_set(PersistenceSystemSet::PreCommit),
                handle_commit_completed.in_set(PersistenceSystemSet::Commit),
            ),
        );
    }
}

/// A "bundle" plugin for standard Bevy apps. This is the recommended way to
/// use the plugin.
#[derive(Clone)]
pub struct PersistencePlugins(pub Arc<dyn DatabaseConnection>);

// Conditionally add Bevy's LogPlugin only if no global subscriber is set and the plugin
// hasn't already been added to this App. This avoids "already set" errors but still
// enables logging when nothing has initialized tracing yet.
#[derive(Clone)]
struct MaybeAddLogPlugin;

impl Plugin for MaybeAddLogPlugin {
    fn build(&self, app: &mut App) {
        // Use Bevy's re-export of tracing to avoid adding a direct dependency
        let already_has_subscriber = bevy::log::tracing::dispatcher::has_been_set();
        let already_added = app.is_plugin_added::<bevy::log::LogPlugin>();
        if !already_has_subscriber && !already_added {
            app.add_plugins(bevy::log::LogPlugin::default());
        }
    }
}

impl PluginGroup for PersistencePlugins {
    fn build(self) -> PluginGroupBuilder {
        MinimalPlugins
            .build()
            // Only initialize logging if nothing else has already set a global subscriber
            .add(MaybeAddLogPlugin)
            .add(PersistencePluginCore::new(self.0.clone()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Persist;
    use serde::{Serialize, Deserialize};
    
    // Define a test component that implements Persist
    #[derive(Component, Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct TestHealth {
        value: i32
    }
    
    impl Persist for TestHealth {
        fn name() -> &'static str {
            "TestHealth"
        }
    }
    
    #[test]
    fn test_read_only_access_doesnt_mark_dirty() {
        // Set up a minimal app with our system
        let mut app = App::new();
        
        // Create a mock session and insert it as a resource
        let mock_db = crate::db::MockDatabaseConnection::new();
        let session = PersistenceSession::new(Arc::new(mock_db));
        app.insert_resource(session);
        
        // Add our component tracking system
        app.add_systems(Update, auto_dirty_tracking_entity_system::<TestHealth>);
        
        // Create an entity with our test component
        let entity = app.world_mut().spawn(TestHealth { value: 100 }).id();
        
        // First update will mark it as dirty because it was just added
        app.update();
        
        // Clear the dirty entities for our test
        {
            let mut session = app.world_mut().resource_mut::<PersistenceSession>();
            session.dirty_entities.clear();
        }
        
        // Read the component without modifying it
        {
            let health = app.world().get::<TestHealth>(entity).unwrap();
            assert_eq!(health.value, 100);
        }
        
        // Update the app again - this should trigger the tracking system
        app.update();
        
        // Verify the entity wasn't marked dirty after read-only access
        {
            let session = app.world().resource::<PersistenceSession>();
            assert!(
                !session.dirty_entities.contains(&entity),
                "Entity was incorrectly marked dirty after read-only access"
            );
        }
        
        // Now modify the component
        {
            let mut health = app.world_mut().get_mut::<TestHealth>(entity).unwrap();
            health.value = 200;
        }
        
        // Update again - should mark as dirty
        app.update();
        
        // Verify the entity was marked dirty after modification
        {
            let session = app.world().resource::<PersistenceSession>();
            assert!(
                session.dirty_entities.contains(&entity),
                "Entity should be marked dirty after modification"
            );
        }
    }
}