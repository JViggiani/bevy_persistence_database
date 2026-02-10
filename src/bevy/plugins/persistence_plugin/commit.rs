use super::listeners::take_commit_listener;
use super::plugin::PersistencePluginConfig;
use super::runtime::TokioRuntime;
use crate::bevy::components::Guid;
use crate::core::db::{DatabaseConnection, PersistenceError, TransactionOperation};
use crate::core::session::PersistenceSession;
use crate::core::session::persistence_session::DirtyState;
use crate::core::versioning::version_manager::VersionKey;
use bevy::prelude::*;
use std::any::TypeId;
use std::collections::{HashMap, HashSet};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};
use tokio::sync::oneshot;

#[derive(Component)]
pub(super) struct CommitTask {
    receiver: Option<oneshot::Receiver<Result<(Vec<String>, Vec<Entity>), PersistenceError>>>,
}

#[derive(Component)]
pub(super) struct MultiBatchCommitTracker {
    correlation_id: u64,
    remaining_batches: Arc<AtomicUsize>,
    result_sender: Arc<Mutex<Option<oneshot::Sender<Result<(), PersistenceError>>>>>,
}

/// Message emitted when a background commit task is complete.
#[derive(Message)]
pub struct CommitCompleted {
    pub result: Result<Vec<String>, PersistenceError>,
    pub dirty_entities: Vec<Entity>,
    pub correlation_id: Option<u64>,
}

/// Message that users send to trigger a commit.
#[derive(Message, Clone)]
pub struct TriggerCommit {
    /// An optional ID to correlate this trigger with a `CommitCompleted` event.
    pub correlation_id: Option<u64>,
    /// Connection to use directly for this commit.
    pub target_connection: Arc<dyn DatabaseConnection>,
    /// Store to write into for this commit.
    pub store: String,
}

/// A state machine resource to track the commit lifecycle.
#[derive(Resource, Default, PartialEq, Debug)]
pub enum CommitStatus {
    #[default]
    Idle,
    InProgress,
    InProgressAndDirty,
}

#[derive(Component)]
pub(super) struct TriggerId {
    correlation_id: Option<u64>,
}

#[derive(Component)]
pub(super) struct CommitMeta {
    dirty_entity_components: HashMap<Entity, HashSet<TypeId>>,
    despawned_entities: HashSet<Entity>,
    dirty_resources: HashSet<TypeId>,
    despawned_resources: HashSet<TypeId>,
    connection: Arc<dyn DatabaseConnection>,
    store: String,
}

pub(super) fn handle_commit_trigger(ecs: &mut World) {
    let mut should_commit = false;
    let mut correlation_id = None;
    let mut requested_connection: Option<Arc<dyn DatabaseConnection>> = None;
    let mut requested_store: Option<String> = None;

    ecs.resource_scope(|ecs, mut events: Mut<Messages<TriggerCommit>>| {
        let mut status = ecs.resource_mut::<CommitStatus>();
        if !events.is_empty() {
            let first_trigger = events.drain().next().unwrap();
            requested_connection = Some(first_trigger.target_connection.clone());
            requested_store = Some(first_trigger.store.clone());

            match *status {
                CommitStatus::Idle => {
                    bevy::log::info!("[handle_commit_trigger] TriggerCommit received while Idle");
                    should_commit = true;
                    correlation_id = first_trigger.correlation_id;
                }
                CommitStatus::InProgress => {
                    bevy::log::info!(
                        "[handle_commit_trigger] TriggerCommit received while busy; queueing"
                    );
                    *status = CommitStatus::InProgressAndDirty;
                }
                CommitStatus::InProgressAndDirty => {
                    // A commit is already in progress and another is already queued.
                }
            }
        }
    });

    if !should_commit {
        return;
    }

    let connection = if let Some(conn) = requested_connection {
        conn
    } else {
        let err = PersistenceError::new("TriggerCommit missing target_connection");
        ecs.write_message(CommitCompleted {
            result: Err(err.clone()),
            dirty_entities: vec![],
            correlation_id,
        });
        bevy::log::error!(%err, "failed to select database connection before commit");
        return;
    };

    let store = if let Some(store) = requested_store {
        if store.is_empty() {
            let err = PersistenceError::new("TriggerCommit store must be non-empty");
            ecs.write_message(CommitCompleted {
                result: Err(err.clone()),
                dirty_entities: vec![],
                correlation_id,
            });
            bevy::log::error!(%err, "invalid store for commit");
            return;
        }
        store
    } else {
        let err = PersistenceError::new("TriggerCommit missing store");
        ecs.write_message(CommitCompleted {
            result: Err(err.clone()),
            dirty_entities: vec![],
            correlation_id,
        });
        bevy::log::error!(%err, "failed to select store before commit");
        return;
    };

    let plugin_config = ecs.resource::<PersistencePluginConfig>().clone();

    // 1) isolate dirty sets from the session
    let (dirty_entity_components, despawned_entities, dirty_resources, despawned_resources) = {
        let mut session = ecs.resource_mut::<PersistenceSession>();
        session.take_dirty_state().into_parts()
    };

    // 2) prepare commit with those sets
    let commit_data = match PersistenceSession::_prepare_commit(
        ecs.resource::<PersistenceSession>(),
        ecs,
        &dirty_entity_components,
        &despawned_entities,
        &dirty_resources,
        &despawned_resources,
        plugin_config.thread_count,
        connection.document_key_field(),
        &store,
    ) {
        Ok(data) if data.operations.is_empty() => {
            ecs.write_message(CommitCompleted {
                result: Ok(vec![]),
                dirty_entities: vec![],
                correlation_id,
            });
            let mut session = ecs.resource_mut::<PersistenceSession>();
            session.restore_dirty_state(DirtyState::from_parts(
                dirty_entity_components,
                despawned_entities,
                dirty_resources,
                despawned_resources,
            ));
            return;
        }
        Ok(data) => data,
        Err(e) => {
            ecs.write_message(CommitCompleted {
                result: Err(e.clone()),
                dirty_entities: vec![],
                correlation_id,
            });
            let mut session = ecs.resource_mut::<PersistenceSession>();
            session.restore_dirty_state(DirtyState::from_parts(
                dirty_entity_components,
                despawned_entities,
                dirty_resources,
                despawned_resources,
            ));
            return;
        }
    };

    // 3) spawn the async task(s)
    *ecs.resource_mut::<CommitStatus>() = CommitStatus::InProgress;
    let runtime = ecs.resource::<TokioRuntime>().runtime.clone();
    let db = connection.clone();

    let all_operations = commit_data.operations;
    let new_entities = commit_data.new_entities;

    if plugin_config.batching_enabled && all_operations.len() > plugin_config.commit_batch_size {
        spawn_batched_commit_tasks(
            ecs,
            correlation_id,
            plugin_config.commit_batch_size,
            &plugin_config,
            runtime,
            db,
            store,
            all_operations,
            new_entities,
            dirty_entity_components,
            despawned_entities,
            dirty_resources,
            despawned_resources,
        );
    } else {
        spawn_single_commit_task(
            ecs,
            correlation_id,
            runtime,
            db,
            store,
            all_operations,
            new_entities,
            dirty_entity_components,
            despawned_entities,
            dirty_resources,
            despawned_resources,
        );
    }
}

#[allow(clippy::too_many_arguments)]
fn spawn_single_commit_task(
    ecs: &mut World,
    correlation_id: Option<u64>,
    runtime: Arc<tokio::runtime::Runtime>,
    db: Arc<dyn DatabaseConnection>,
    store: String,
    operations: Vec<TransactionOperation>,
    new_entities: Vec<Entity>,
    dirty_entity_components: HashMap<Entity, HashSet<TypeId>>,
    despawned_entities: HashSet<Entity>,
    dirty_resources: HashSet<TypeId>,
    despawned_resources: HashSet<TypeId>,
) {
    let db_for_task = db.clone();
    let (tx, rx) = oneshot::channel();
    runtime.spawn(async move {
        bevy::log::trace!("commit task started (single batch)");
        let res = db_for_task
            .execute_transaction(operations)
            .await
            .map(|keys| (keys, new_entities));
        bevy::log::trace!("commit runtime task completed send");
        let _ = tx.send(res);
    });

    ecs.spawn((
        CommitTask { receiver: Some(rx) },
        TriggerId { correlation_id },
        CommitMeta {
            dirty_entity_components,
            despawned_entities,
            dirty_resources,
            despawned_resources,
            connection: db,
            store,
        },
    ));
}

#[derive(Default)]
struct CommitBatch {
    operations: Vec<TransactionOperation>,
    dirty_entities: HashSet<Entity>,
    new_entities: Vec<Entity>,
}

#[allow(clippy::too_many_arguments)]
fn spawn_batched_commit_tasks(
    ecs: &mut World,
    correlation_id: Option<u64>,
    batch_size: usize,
    _plugin_config: &PersistencePluginConfig,
    runtime: Arc<tokio::runtime::Runtime>,
    db: Arc<dyn DatabaseConnection>,
    store: String,
    operations: Vec<TransactionOperation>,
    new_entities: Vec<Entity>,
    dirty_entity_components: HashMap<Entity, HashSet<TypeId>>,
    despawned_entities: HashSet<Entity>,
    dirty_resources: HashSet<TypeId>,
    despawned_resources: HashSet<TypeId>,
) {
    let session = ecs.resource::<PersistenceSession>();

    let mut key_to_entity: HashMap<String, Entity> = HashMap::new();
    for (entity, key) in session.entity_keys().iter() {
        key_to_entity.insert(key.clone(), *entity);
    }

    // Split operations into logical groups
    let mut entity_ops: HashMap<Entity, Vec<TransactionOperation>> = HashMap::new();
    let mut new_entity_ops: Vec<(TransactionOperation, Entity)> = Vec::new();
    let mut resource_ops: Vec<TransactionOperation> = Vec::new();
    let mut new_entity_idx = 0;

    for op in operations {
        match &op {
            TransactionOperation::UpdateDocument {
                kind: crate::core::db::connection::DocumentKind::Entity,
                key,
                ..
            }
            | TransactionOperation::DeleteDocument {
                kind: crate::core::db::connection::DocumentKind::Entity,
                key,
                ..
            } => {
                if let Some(entity) = key_to_entity.get(key) {
                    entity_ops.entry(*entity).or_default().push(op);
                }
            }
            TransactionOperation::CreateDocument {
                kind: crate::core::db::connection::DocumentKind::Entity,
                ..
            } => {
                if let Some(entity) = new_entities.get(new_entity_idx) {
                    new_entity_ops.push((op, *entity));
                    new_entity_idx += 1;
                }
            }
            _ => resource_ops.push(op),
        }
    }

    let mut batches: Vec<CommitBatch> = Vec::new();
    let mut current = CommitBatch::default();

    let push_current = |batches: &mut Vec<CommitBatch>, current: &mut CommitBatch| {
        if !current.operations.is_empty() {
            batches.push(std::mem::take(current));
        }
    };

    for (entity, ops) in entity_ops {
        if current.operations.len() + ops.len() > batch_size && !current.operations.is_empty() {
            push_current(&mut batches, &mut current);
        }
        current.operations.extend(ops);
        current.dirty_entities.insert(entity);
    }

    for (op, entity) in new_entity_ops {
        if current.operations.len() + 1 > batch_size && !current.operations.is_empty() {
            push_current(&mut batches, &mut current);
        }
        current.operations.push(op);
        current.new_entities.push(entity);
    }

    if current.operations.len() + resource_ops.len() > batch_size && !current.operations.is_empty() {
        push_current(&mut batches, &mut current);
    }
    current.operations.extend(resource_ops);
    push_current(&mut batches, &mut current);

    let num_batches = batches.len();
    bevy::log::info!(
        "[handle_commit_trigger] Splitting commit into {} batches of size ~{}.",
        num_batches,
        batch_size
    );

    if let (Some(cid), Some(listener)) =
        (correlation_id, correlation_id.and_then(|cid| take_commit_listener(ecs, cid)))
    {
        bevy::log::debug!(
            "registered multi-batch tracker for correlation_id={cid} batches={}",
            num_batches
        );
        ecs.spawn(MultiBatchCommitTracker {
            correlation_id: cid,
            remaining_batches: Arc::new(AtomicUsize::new(num_batches)),
            result_sender: Arc::new(Mutex::new(Some(listener))),
        });
    }

    // Divide resource types across batches
    let mut resource_sets = Vec::with_capacity(num_batches);
    for _ in 0..num_batches {
        resource_sets.push(HashSet::new());
    }
    for (i, res_type) in dirty_resources.iter().enumerate() {
        let batch_idx = i % num_batches;
        resource_sets[batch_idx].insert(*res_type);
    }

    for (i, batch) in batches.into_iter().enumerate() {
        let batch_db = db.clone();
        let batch_runtime = runtime.clone();
        let batch_new_entities = batch.new_entities.clone();

        let (tx, rx) = oneshot::channel();
        batch_runtime.spawn(async move {
            bevy::log::trace!("commit batch task started (batched)");
            let res = batch_db
                .execute_transaction(batch.operations)
                .await
                .map(|keys| (keys, batch_new_entities));
            bevy::log::trace!("commit batch runtime task completed send");
            let _ = tx.send(res);
        });

        // Each batch gets its own subset of dirty entities and resources
        let mut batch_dirty_entities: HashMap<Entity, HashSet<TypeId>> = HashMap::new();
        for entity in &batch.dirty_entities {
            if let Some(dirty) = dirty_entity_components.get(entity) {
                batch_dirty_entities.insert(*entity, dirty.clone());
            }
        }

        let meta = CommitMeta {
            dirty_entity_components: batch_dirty_entities,
            despawned_entities: if i == 0 {
                despawned_entities.clone()
            } else {
                HashSet::new()
            },
            dirty_resources: resource_sets[i].clone(),
            despawned_resources: if i == 0 {
                despawned_resources.clone()
            } else {
                HashSet::new()
            },
            connection: db.clone(),
            store: store.clone(),
        };

        ecs.spawn((
            CommitTask { receiver: Some(rx) },
            TriggerId { correlation_id },
            meta,
        ));
    }
}

pub(super) fn handle_commit_completed(
    mut commands: Commands,
    mut query: Query<(Entity, &mut CommitTask, &TriggerId, Option<&mut CommitMeta>)>,
    mut session: ResMut<PersistenceSession>,
    mut status: ResMut<CommitStatus>,
    mut completed: MessageWriter<CommitCompleted>,
    mut triggers: MessageWriter<TriggerCommit>,
    mut trackers: Query<(Entity, &MultiBatchCommitTracker)>,
) {
    static PENDING_LOG_COUNT: AtomicUsize = AtomicUsize::new(0);

    let mut to_despawn = Vec::new();
    let mut had_error = false;

    for (ent, mut task, trigger_id, meta_opt) in &mut query {
        if let Some(mut receiver) = task.receiver.take() {
            let result: Result<(Vec<String>, Vec<Entity>), PersistenceError> =
                match receiver.try_recv() {
                    Ok(res) => res,
                    Err(tokio::sync::oneshot::error::TryRecvError::Empty) => {
                        task.receiver = Some(receiver);
                        continue;
                    }
                    Err(tokio::sync::oneshot::error::TryRecvError::Closed) => {
                        bevy::log::error!("commit task channel closed before result");
                        Err(PersistenceError::new(
                            "Commit task cancelled before completion",
                        ))
                    }
                };

            let cid = trigger_id.correlation_id;
            let mut is_final_batch = true;
            let mut should_send_result = true;
            let mut commit_connection: Option<Arc<dyn DatabaseConnection>> = None;
            let mut commit_store: Option<String> = None;
            let mut tracker_found = false;

            if result.is_err() {
                had_error = true;
            }

            if let Some(correlation_id) = cid {
                if let Some((tracker_entity, tracker)) =
                    trackers.iter_mut().find(|(_, t)| t.correlation_id == correlation_id)
                {
                    tracker_found = true;
                    let remaining = tracker.remaining_batches.fetch_sub(1, Ordering::SeqCst) - 1;
                    is_final_batch = remaining == 0;

                    if result.is_err() || is_final_batch {
                        if let Some(sender) = tracker.result_sender.lock().unwrap().take() {
                            if let Err(err) = &result {
                                let _ = sender.send(Err(err.clone()));
                            } else if is_final_batch {
                                let _ = sender.send(Ok(()));
                            }
                        }

                        commands
                            .entity(tracker_entity)
                            .remove::<MultiBatchCommitTracker>();
                    } else {
                        should_send_result = false;
                    }
                }
            }

            if let Err(err) = &result {
                bevy::log::error!(
                    "commit batch completed with error (cid={:?} tracker_found={} final_batch={} err={})",
                    cid,
                    tracker_found,
                    is_final_batch,
                    err
                );
            } else {
                bevy::log::trace!(
                    "commit batch completed ok (cid={:?} tracker_found={} final_batch={})",
                    cid,
                    tracker_found,
                    is_final_batch
                );
            }

            if let Some(mut meta) = meta_opt {
                commit_connection = Some(meta.connection.clone());
                commit_store = Some(meta.store.clone());

                let event_res = match &result {
                    Ok((new_keys, new_entities)) => {
                        apply_commit_success(&mut commands, &mut session, &meta, new_keys, new_entities);
                        Ok(new_keys.clone())
                    }
                    Err(err) => {
                        restore_dirty_state_on_failure(&mut session, &mut meta);
                        Err(err.clone())
                    }
                };

                if should_send_result && (is_final_batch || result.is_err()) {
                    bevy::log::debug!(
                        "emitting CommitCompleted for cid={:?} final_batch={} err={}",
                        cid,
                        is_final_batch,
                        result.is_err()
                    );
                    completed.write(CommitCompleted {
                        result: event_res,
                        dirty_entities: vec![],
                        correlation_id: cid,
                    });
                }
            } else if let Err(e) = &result {
                if should_send_result {
                    completed.write(CommitCompleted {
                        result: Err(e.clone()),
                        dirty_entities: vec![],
                        correlation_id: cid,
                    });
                }
            }

            to_despawn.push(ent);

            if is_final_batch || had_error {
                let should_trigger_next = !had_error && *status == CommitStatus::InProgressAndDirty;
                *status = CommitStatus::Idle;

                if should_trigger_next {
                    if let (Some(conn), Some(store)) = (commit_connection.clone(), commit_store.clone()) {
                        triggers.write(TriggerCommit {
                            correlation_id: None,
                            target_connection: conn,
                            store,
                        });
                    }
                }
            }
        } else if PENDING_LOG_COUNT.fetch_add(1, Ordering::Relaxed) < 5 {
            bevy::log::debug!("commit task still pending (cid={:?})", trigger_id.correlation_id);
        }
    }

    if had_error {
        *status = CommitStatus::Idle;
    }

    for entity in to_despawn {
        commands.entity(entity).despawn();
    }
}

fn apply_commit_success(
    commands: &mut Commands,
    session: &mut PersistenceSession,
    meta: &CommitMeta,
    new_keys: &[String],
    new_entities: &[Entity],
) {
    for (e, key) in new_entities.iter().zip(new_keys.iter()) {
        commands.entity(*e).insert(Guid::new(key.clone()));
        session.insert_entity_key(*e, key.clone());
        session
            .version_manager_mut()
            .set_version(VersionKey::Entity(key.clone()), 1);
    }

    for tid in &meta.dirty_resources {
        if meta.despawned_resources.contains(tid) {
            continue;
        }
        let vk = VersionKey::Resource(*tid);
        let nv = session.version_manager().get_version(&vk).unwrap_or(0) + 1;
        session.version_manager_mut().set_version(vk, nv);
    }

    for tid in &meta.despawned_resources {
        session
            .version_manager_mut()
            .remove_version(&VersionKey::Resource(*tid));
    }

    for &entity in meta.dirty_entity_components.keys() {
        if new_entities.contains(&entity) {
            continue;
        }
        if let Some(key) = session.entity_key(entity) {
            let vk = VersionKey::Entity(key.clone());
            if let Some(v) = session.version_manager().get_version(&vk) {
                session.version_manager_mut().set_version(vk, v + 1);
            }
        }
    }

    for e in &meta.despawned_entities {
        if let Some(key) = session.entity_key(*e).cloned() {
            session
                .version_manager_mut()
                .remove_version(&VersionKey::Entity(key));
        }
    }
}

fn restore_dirty_state_on_failure(session: &mut PersistenceSession, meta: &mut CommitMeta) {
    let dirty_entity_components: HashMap<Entity, HashSet<TypeId>> =
        meta.dirty_entity_components.drain().collect();
    let despawned_entities = std::mem::take(&mut meta.despawned_entities);
    let dirty_resources = std::mem::take(&mut meta.dirty_resources);
    let despawned_resources = std::mem::take(&mut meta.despawned_resources);
    session.restore_dirty_state(DirtyState::from_parts(
        dirty_entity_components,
        despawned_entities,
        dirty_resources,
        despawned_resources,
    ));
}
