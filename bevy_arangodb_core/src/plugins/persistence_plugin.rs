//! A Bevy `Plugin` for integrating `bevy_arangodb`.
//!
//! This plugin simplifies the setup process by managing the `PersistenceSession`
//! as a resource and automatically adding systems for change detection.

use crate::registration::COMPONENT_REGISTRY;
use crate::resources::persistence_session::{_prepare_commit, EntityMetadata};
use crate::{
    db::TransactionResult, DatabaseConnection, DocumentId, Guid, Persist, PersistenceError,
    PersistenceSession,
};
use bevy::app::PluginGroupBuilder;
use bevy::prelude::*;
use bevy::tasks::{IoTaskPool, Task};
use futures_lite::future;
use std::any::TypeId;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::oneshot;

/// The final, processed result of a commit, ready to be applied to the World.
struct CommitOutcome {
    created: Vec<(Entity, DocumentId)>,
    updated: Vec<(Entity, DocumentId)>,
    deleted: Vec<Entity>,
}

/// A component holding the future result of a commit operation.
#[derive(Component)]
pub struct CommitTask(Task<Result<CommitOutcome, PersistenceError>>);

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
pub struct CommitCompleted(
    pub Result<TransactionResult, PersistenceError>,
    pub Vec<Entity>,
    pub Option<u64>,
);

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

    // --- STAGE 1: Prepare Data on Main Thread ---
    let commit_data = {
        let session = world.resource::<PersistenceSession>();
        match _prepare_commit(session, world) {
            Ok(data) => {
                if data.operations.is_empty() {
                    // If there are no operations, we still need to send a completion event
                    // to unblock any waiting `commit_and_wait` calls.
                    world.send_event(CommitCompleted(
                        Ok(TransactionResult::default()),
                        vec![],
                        correlation_id,
                    ));
                    return;
                }
                data
            }
            Err(e) => {
                error!("Failed to prepare commit: {}", e);
                world.send_event(CommitCompleted(Err(e), vec![], correlation_id));
                return;
            }
        }
    };

    // Update status and spawn the task
    *world.resource_mut::<CommitStatus>() = CommitStatus::InProgress;
    info!("[handle_commit_trigger] CommitStatus set to InProgress. Spawning task.");

    let thread_pool = IoTaskPool::get();
    let db = world.resource::<PersistenceSession>().db.clone();
    let new_entities = commit_data.new_entities;
    let updated_entities = commit_data.updated_entities;
    let deleted_entities = commit_data.deleted_entities;
    let operations = commit_data.operations;

    let task = thread_pool.spawn(async move {
        // Await the future directly. Do NOT use block_on.
        // This lets Bevy's task pool manage the async execution.
        debug!("Executing transaction for commit...");
        match db.execute_transaction(operations).await {
            Ok(result) => {
                debug!("Transaction successful. Processing results.");
                // Zip the results back together into a clean structure.
                let created = new_entities
                    .into_iter()
                    .zip(result.created.into_iter())
                    .collect();
                let updated = updated_entities
                    .into_iter()
                    .zip(result.updated.into_iter())
                    .collect();

                Ok(CommitOutcome {
                    created,
                    updated,
                    deleted: deleted_entities,
                })
            }
            Err(e) => {
                error!("Transaction failed: {}", e);
                Err(e)
            }
        }
    });

    // Spawn a new entity to hold the task and the correlation ID.
    world.spawn((CommitTask(task), TriggerID(correlation_id)));
}

/// A component to correlate a commit task with its trigger event.
#[derive(Component)]
struct TriggerID(Option<u64>);

/// A system that polls the running commit task and updates the state machine upon completion.
fn handle_commit_completed(
    mut commands: Commands,
    mut tasks: Query<(Entity, &mut CommitTask, &TriggerID)>,
    mut session: ResMut<PersistenceSession>,
    mut status: ResMut<CommitStatus>,
    mut completed_events: EventWriter<CommitCompleted>,
    mut trigger_events: EventWriter<TriggerCommit>,
) {
    for (task_entity, mut task, trigger_id) in &mut tasks {
        debug!("[handle_commit_completed] Polling a commit task for correlation ID {:?}.", trigger_id.0);
        
        match future::block_on(future::poll_once(&mut task.0)) {
            Some(result) => {
                info!("[handle_commit_completed] Task is finished for correlation ID {:?}. Processing result.", trigger_id.0);
                let event_result;

                match result {
                    Ok(outcome) => {
                        info!(
                            "Commit successful for correlation ID {:?}. Created: {}, Updated: {}, Deleted: {}.",
                            trigger_id.0,
                            outcome.created.len(),
                            outcome.updated.len(),
                            outcome.deleted.len()
                        );

                        // Handle newly created entities
                        for (entity, doc_id) in outcome.created.iter() {
                            debug!("Assigning new Guid to entity {:?}: key={}, rev={}", entity, doc_id.key, doc_id.rev);
                            commands.entity(*entity).insert(Guid::new(doc_id.key.clone()));
                            session.entity_meta.insert(
                                *entity,
                                EntityMetadata {
                                    key: doc_id.key.clone(),
                                    rev: doc_id.rev.clone(),
                                },
                            );
                            // This entity was successfully created, so it's no longer dirty.
                            session.dirty_entities.remove(entity);
                            session.component_changes.remove(entity); // Clear its change set
                        }

                        // Handle updated entities by updating their revisions in the cache
                        for (entity, doc_id) in outcome.updated.iter() {
                            if let Some(meta) = session.entity_meta.get_mut(entity) {
                                debug!("Updating rev for entity {:?}: key={}, old_rev={}, new_rev={}", entity, doc_id.key, meta.rev, doc_id.rev);
                                // The key should match, but we update the revision.
                                assert_eq!(meta.key, doc_id.key);
                                meta.rev = doc_id.rev.clone();
                            } else {
                                warn!("Could not find metadata for updated entity {:?} to update its revision.", entity);
                            }
                            // This entity was successfully updated, so it's no longer dirty.
                            session.dirty_entities.remove(entity);
                            session.component_changes.remove(entity); // Clear its change set
                        }

                        // Handle deleted entities by removing them from the metadata cache
                        for entity in outcome.deleted.iter() {
                            debug!("Removing metadata for deleted entity {:?}", entity);
                            session.entity_meta.remove(entity);
                            // This entity was successfully deleted, so it's no longer despawned.
                            session.despawned_entities.remove(entity);
                        }

                        // Only clear resource flags if we are not immediately starting another commit.
                        // Entity flags are now handled surgically above.
                        if *status != CommitStatus::InProgressAndDirty {
                            session.dirty_resources.clear();
                            session.despawned_resources.clear();
                            debug!("Commit successful, returning to Idle. Cleared resource dirty flags.");
                        } else {
                            debug!("Commit successful, but world is dirty. Resource dirty flags will be handled by the next commit.");
                        }

                        event_result = Ok(TransactionResult {
                            created: outcome.created.into_iter().map(|(_, id)| id).collect(),
                            updated: outcome.updated.into_iter().map(|(_, id)| id).collect(),
                        });
                    }
                    Err(e) => {
                        error!(
                            "Commit failed for correlation ID {:?}: {}",
                            trigger_id.0, e
                        );
                        // Surgically handle conflicts, otherwise wipe the session state
                        // as the entire transaction is now suspect.
                        match &e {
                            PersistenceError::Conflict { key } => {
                                let conflicting_entity = session.entity_meta.iter()
                                    .find(|(_entity, meta)| &meta.key == key)
                                    .map(|(entity, _meta)| *entity);

                                if let Some(entity) = conflicting_entity {
                                    warn!("Conflict for entity {:?} (key {}). Removing from dirty set to prevent retry loop.", entity, key);
                                    session.dirty_entities.remove(&entity);
                                    session.component_changes.remove(&entity); // Clear its change set
                                } else {
                                    warn!("Conflict for key {}, but no matching entity in cache.", key);
                                }
                            }
                            _ => {
                                warn!("Non-conflict error. Clearing all dirty state as a precaution.");
                                session.dirty_entities.clear();
                                session.despawned_entities.clear();
                                session.dirty_resources.clear();
                                session.despawned_resources.clear();
                                session.component_changes.clear(); // Clear all component changes
                            }
                        }
                        event_result = Err(e);
                    }
                }
                // The task is complete, so we can despawn the task entity.
                commands.entity(task_entity).despawn();

                // Check if another commit was requested while this one was running.
                if *status == CommitStatus::InProgressAndDirty {
                    info!("Commit completed, but world is dirty. Triggering another commit.");
                    // Set status back to Idle and immediately trigger a new commit.
                    // The `handle_commit_trigger` system will run in the same tick.
                    *status = CommitStatus::Idle;
                    trigger_events.write( TriggerCommit { correlation_id: trigger_id.0 } );
                } else {
                    *status = CommitStatus::Idle;
                }

                // Send the completion event for any waiting `commit_and_wait` calls.
                completed_events.write(CommitCompleted(event_result, vec![], trigger_id.0));
            },
            None => {
                trace!("[handle_commit_completed] Task for correlation ID {:?} not yet complete", trigger_id.0);
            }
        }
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
        // Record exactly which component changed
        session
            .component_changes
            .entry(entity)
            .or_default()
            .insert(T::name().to_string());
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

/// A Bevy `Plugin` that sets up `bevy_arangodb`.
pub struct PersistencePluginCore {
    db: Arc<dyn DatabaseConnection>,
}

impl PersistencePluginCore {
    /// Creates a new `PersistencePluginCore` with the given database connection.
    pub fn new(db: Arc<dyn DatabaseConnection>) -> Self {
        Self { db }
    }
}

#[derive(Resource, Default)]
pub(crate) struct CommitEventListeners(pub(crate) HashMap<u64, oneshot::Sender<Result<(), PersistenceError>>>);

fn commit_event_listener(
    mut events: EventReader<CommitCompleted>,
    mut listeners: ResMut<CommitEventListeners>,
) {
    for event in events.read() {
        debug!("Processing CommitCompleted event with correlation ID {:?}", event.2);
        if let Some(id) = event.2 {
            if let Some(sender) = listeners.0.remove(&id) {
                info!("Found listener for commit {}. Sending result.", id);
                let result = match &event.0 {
                    Ok(_) => {
                        debug!("Sending successful result for commit {}", id);
                        Ok(())
                    },
                    Err(e) => {
                        debug!("Sending error result for commit {}: {}", id, e);
                        Err(e.clone())
                    },
                };
                match sender.send(result) {
                    Ok(_) => debug!("Successfully sent result for commit {}", id),
                    Err(_) => warn!("Failed to send result for commit {} - receiver dropped", id),
                }
            } else {
                debug!("No listener found for commit with ID {}", id);
            }
        } else {
            debug!("CommitCompleted event has no correlation ID");
        }
    }
}

impl Plugin for PersistencePluginCore {
    fn build(&self, app: &mut App) {
        let session = PersistenceSession::new(self.db.clone());
        app.insert_resource(session);
        app.init_resource::<RegisteredPersistTypes>();
        app.add_event::<TriggerCommit>();
        app.add_event::<CommitCompleted>();
        app.init_resource::<CommitStatus>();
        app.init_resource::<CommitEventListeners>();


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

        // Add both PreCommit and Commit-phase systems:
        app.add_systems(
            PostUpdate,
            (
                (
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

impl PluginGroup for PersistencePlugins {
    fn build(self) -> PluginGroupBuilder {
        MinimalPlugins
            .build()
            .add(PersistencePluginCore::new(self.0.clone()))
    }
}