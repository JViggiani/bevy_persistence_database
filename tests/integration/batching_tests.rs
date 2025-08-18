use bevy::prelude::*;
use bevy_arangodb_core::{
    commit_sync, CommitStatus, PersistenceError, Guid,
    persistence_plugin::PersistencePlugins, MockDatabaseConnection, PersistencePluginCore,
    persistence_plugin::PersistencePluginConfig, Collection, TransactionOperation,
};
use bevy_arangodb_core::PersistentQuery;
use bevy::prelude::With;
use crate::common::{make_app, run_async, Health};
use std::sync::Arc;
use std::time::{Duration, Instant};
use bevy_arangodb_derive::db_matrix_test;

#[db_matrix_test]
fn test_successful_batch_commit_of_new_entities() {
    let (db, _c) = setup();
    let mut app = make_app(db.clone(), 2);

    // spawn 10 new entities
    for i in 0..10 {
        app.world_mut().spawn(Health { value: i });
    }
    app.update();

    // commit
    let res = commit_sync(&mut app);
    assert!(res.is_ok());

    // all entities got a Guid
    // pull out a mutable World to iterate
    let world_ref = app.world_mut();
    let count = world_ref.query::<&Guid>().iter(world_ref).count();
    assert_eq!(count, 10);

    // loading back from DB
    let mut app2 = App::new();
    app2.add_plugins(PersistencePlugins(db.clone()));
    fn load(mut pq: PersistentQuery<&Health, With<Health>>) { let _ = pq.iter_with_loading().count(); }
    app2.add_systems(bevy::prelude::Update, load);
    app2.update();
    let loaded = app2.world_mut().query::<&Health>().iter(&app2.world()).count();
    assert_eq!(loaded, 10);
}

#[db_matrix_test]
fn test_batch_commit_with_updates_and_deletes() {
    let (db, _c) = setup();
    let mut app = make_app(db.clone(), 2);

    // initial 5 entities
    let ids: Vec<_> = (0..5)
        .map(|i| app.world_mut().spawn(Health { value: i }).id())
        .collect();
    app.update();
    commit_sync(&mut app).unwrap();

    // update first two, delete last two
    app.world_mut().get_mut::<Health>(ids[0]).unwrap().value = 100;
    app.world_mut().get_mut::<Health>(ids[1]).unwrap().value = 101;
    app.world_mut().entity_mut(ids[3]).despawn();
    app.world_mut().entity_mut(ids[4]).despawn();
    app.update();

    let res = commit_sync(&mut app);
    assert!(res.is_ok());
    assert_eq!(*app.world().resource::<CommitStatus>(), CommitStatus::Idle);

    let mut app2 = App::new();
    app2.add_plugins(PersistencePlugins(db.clone()));
    fn load(mut pq: PersistentQuery<&Health, With<Health>>) { let _ = pq.iter_with_loading().count(); }
    app2.add_systems(bevy::prelude::Update, load);
    app2.update();
    // expect 3 left
    let vals: Vec<_> = app2.world_mut().query::<&Health>().iter(&app2.world()).map(|h| h.value).collect();
    assert_eq!(vals.len(), 3);
    assert!(vals.contains(&100) && vals.contains(&101) && vals.contains(&2));
}

#[test]
fn test_batch_commit_failure_propagates() {
    let (db, _c) = crate::common::setup_sync();
    let mut app = make_app(db.clone(), 2);

    // initial 5
    let ids: Vec<_> = (0..5)
        .map(|i| app.world_mut().spawn(Health { value: i }).id())
        .collect();
    app.update();
    commit_sync(&mut app).unwrap();

    // induce conflict on the third entity
    let guid = app.world().get::<Guid>(ids[2]).unwrap().id().to_string();
    let (_doc, ver) = run_async(db.fetch_document(&guid)).unwrap().unwrap();
    // bump version directly
    let bad = serde_json::json!({"_key": guid,"bevy_persistence_version":ver+1});
    run_async(db.execute_transaction(vec![
        TransactionOperation::UpdateDocument {
            collection: Collection::Entities,
            key: guid.clone(),
            expected_current_version: ver,
            patch: bad.clone(),
        }
    ])).unwrap();

    // modify all locally
    for id in &ids {
        app.world_mut().get_mut::<Health>(*id).unwrap().value += 10;
    }
    app.update();

    let res = commit_sync(&mut app);
    assert!(matches!(res, Err(PersistenceError::Conflict{ key }) if key == guid));
    assert_eq!(*app.world().resource::<CommitStatus>(), CommitStatus::Idle);
}

#[test]
fn test_concurrent_batch_execution() {
    // Create a mock database that introduces a delay for each batch
    let mut db = MockDatabaseConnection::new();
    let batch_count = 5;
    let batch_delay = Duration::from_millis(50);

    // Configure the mock to delay each transaction by batch_delay
    db.expect_execute_transaction()
        .times(batch_count)
        .returning(move |_ops| {
            Box::pin(async move {
                tokio::time::sleep(batch_delay).await;
                Ok(vec![])
            })
        });

    let db_arc = Arc::new(db);
    let batch_size = 2;
    let entity_count = batch_count * batch_size;

    // Create an app with batching enabled and configured batch size
    let config = PersistencePluginConfig {
        batching_enabled: true,
        commit_batch_size: batch_size,
        thread_count: 4,
    };
    let plugin = PersistencePluginCore::new(db_arc.clone()).with_config(config.clone());
    let mut app = App::new();
    app.add_plugins(plugin);

    // Spawn enough entities to create `batch_count` batches
    for i in 0..entity_count {
        app.world_mut().spawn(Health { value: i as i32 });
    }
    app.update();

    // Measure time for the commit operation
    let start_time = Instant::now();
    let res = commit_sync(&mut app);
    let elapsed = start_time.elapsed();

    assert!(res.is_ok());

    // Total time should be slightly more than one batch delay, but much less than all delays combined
    let total_sequential_delay = batch_delay * batch_count as u32;
    println!("Elapsed time for concurrent commit: {:?}", elapsed);
    println!("Total sequential delay would be: {:?}", total_sequential_delay);

    assert!(
        elapsed < total_sequential_delay,
        "Concurrent execution was not faster than sequential."
    );
    assert!(
        elapsed > batch_delay,
        "Elapsed time should be at least one batch delay."
    );
    // A reasonable upper bound for concurrency with some overhead
    assert!(
        elapsed < batch_delay * 2,
        "Concurrent execution took too long."
    );
}

#[test]
fn test_atomic_multi_batch_commit() {
    // Create a mock database that will succeed for the first N-1 batches but fail on the last one
    let mut db = MockDatabaseConnection::new();
    let batch_count = 3;
    let batch_to_fail = 2; // Zero-indexed, so this is the third batch
    
    let mut call_count = 0;
    db.expect_execute_transaction()
        .times(batch_count)
        .returning(move |_| {
            let current_batch = call_count;
            call_count += 1;
            
            Box::pin(async move {
                // Sleep to simulate network latency
                tokio::time::sleep(Duration::from_millis(20)).await;
                
                // Make the specified batch fail
                if current_batch == batch_to_fail {
                    return Err(PersistenceError::new("Simulated failure in batch"));
                }
                
                // Return empty keys for successful batches
                Ok(vec![])
            })
        });
    
    let db_arc = Arc::new(db);
    let config = PersistencePluginConfig {
        batching_enabled: true,
        commit_batch_size: 3,
        thread_count: 2,
    };
    let plugin = PersistencePluginCore::new(db_arc.clone()).with_config(config);
    let mut app = App::new();
    app.add_plugins(plugin);
    
    // Spawn enough entities to create multiple batches
    let entity_count = batch_count * 3; // use commit_batch_size = 3
    for i in 0..entity_count {
        app.world_mut().spawn(Health { value: i as i32 });
    }
    app.update();
    
    // Attempt the commit operation
    let result = commit_sync(&mut app);
    
    // The entire operation should fail due to the failure in one batch
    assert!(result.is_err());
    assert!(matches!(result, Err(PersistenceError::General(msg)) if msg.contains("Simulated failure")));
    
    // The status should be reset to Idle after a failure
    assert_eq!(*app.world().resource::<CommitStatus>(), CommitStatus::Idle);
    
    // No entity should have a Guid since the commit failed atomically
    let world_ref = app.world_mut();
    let guid_count = world_ref.query::<&Guid>().iter(world_ref).count();
    assert_eq!(guid_count, 0, "No entity should have a Guid after atomic failure");
}

#[test]
fn test_batches_respect_config_max_ops() {
    use std::sync::Arc;
    use bevy_arangodb_core::{
        persistence_plugin::PersistencePluginConfig,
        PersistencePluginCore,
        MockDatabaseConnection,
        commit_sync,
    };

    // Configure a non-trivial batch size and entity count
    let batch_size = 7usize;
    let entity_count = 25usize;
    let expected_batches = (entity_count + batch_size - 1) / batch_size;

    // Mock DB: ensure we get exactly expected_batches transactions and that
    // each batch contains <= batch_size operations.
    let mut db = MockDatabaseConnection::new();
    db.expect_execute_transaction()
        .times(expected_batches)
        .returning(move |ops| {
            assert!(
                ops.len() <= batch_size,
                "Batch too large: got {}, limit {}",
                ops.len(),
                batch_size
            );
            // Simulate success
            Box::pin(async { Ok(vec![]) })
        });

    // Build app with batching enabled
    let config = PersistencePluginConfig {
        batching_enabled: true,
        commit_batch_size: batch_size,
        thread_count: 4,
    };
    let plugin = PersistencePluginCore::new(Arc::new(db)).with_config(config);
    let mut app = App::new();
    app.add_plugins(plugin);

    // Spawn enough entities to require multiple batches
    for i in 0..entity_count {
        app.world_mut().spawn(Health { value: i as i32 });
    }
    app.update();

    // Commit: mock assertions will validate batch sizing and call count
    let res = commit_sync(&mut app);
    assert!(res.is_ok(), "Commit should succeed with mocked DB");
}