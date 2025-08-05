use bevy::prelude::*;
use bevy_arangodb_core::{
    commit, PersistenceQuery, CommitStatus, PersistenceError, Guid,
    persistence_plugin::PersistencePlugins, MockDatabaseConnection, PersistencePluginCore,
    persistence_plugin::PersistencePluginConfig, Collection, TransactionOperation, Persist,
};
use crate::common::{setup, make_app};
use crate::common::Health;
use std::sync::Arc;
use std::time::{Duration, Instant};

#[tokio::test]
async fn test_successful_batch_commit_of_new_entities() {
    let (db, _c) = setup().await;
    let mut app = make_app(db.clone(), 2);

    // spawn 10 new entities
    for i in 0..10 {
        app.world_mut().spawn(Health { value: i });
    }
    app.update();

    // commit
    let res = commit(&mut app).await;
    assert!(res.is_ok());

    // all entities got a Guid
    // pull out a mutable World to iterate
    let world_ref = app.world_mut();
    let count = world_ref.query::<&Guid>().iter(world_ref).count();
    assert_eq!(count, 10);

    // loading back from DB
    let mut app2 = App::new();
    app2.add_plugins(PersistencePlugins(db.clone()));
    let loaded = PersistenceQuery::new(db.clone())
        .with::<Health>()
        .fetch_into(app2.world_mut())
        .await;
    assert_eq!(loaded.len(), 10);
}

#[tokio::test]
async fn test_batch_commit_with_updates_and_deletes() {
    let (db, _c) = setup().await;
    let mut app = make_app(db.clone(), 2);

    // initial 5 entities
    let ids: Vec<_> = (0..5)
        .map(|i| app.world_mut().spawn(Health { value: i }).id())
        .collect();
    app.update();
    commit(&mut app).await.unwrap();

    // update first two, delete last two
    app.world_mut().get_mut::<Health>(ids[0]).unwrap().value = 100;
    app.world_mut().get_mut::<Health>(ids[1]).unwrap().value = 101;
    app.world_mut().entity_mut(ids[3]).despawn();
    app.world_mut().entity_mut(ids[4]).despawn();
    app.update();

    let res = commit(&mut app).await;
    assert!(res.is_ok());
    assert_eq!(*app.world().resource::<CommitStatus>(), CommitStatus::Idle);

    let mut app2 = App::new();
    app2.add_plugins(PersistencePlugins(db.clone()));
    let loaded = PersistenceQuery::new(db.clone())
        .with::<Health>()
        .fetch_into(app2.world_mut())
        .await;
    // expect 3 left: values 100,101,2
    assert_eq!(loaded.len(), 3);
    let vals: Vec<_> = loaded.iter()
        .map(|e| app2.world().get::<Health>(*e).unwrap().value)
        .collect();
    assert!(vals.contains(&100));
    assert!(vals.contains(&101));
    assert!(vals.contains(&2));
}

#[tokio::test]
async fn test_batch_commit_failure_propagates() {
    let (db, _c) = setup().await;
    let mut app = make_app(db.clone(), 2);

    // initial 5
    let ids: Vec<_> = (0..5)
        .map(|i| app.world_mut().spawn(Health { value: i }).id())
        .collect();
    app.update();
    commit(&mut app).await.unwrap();

    // induce conflict on the third entity
    let guid = app.world().get::<Guid>(ids[2]).unwrap().id().to_string();
    let (_doc, ver) = db.fetch_document(&guid).await.unwrap().unwrap();
    // bump version directly
    let bad = serde_json::json!({"_key": guid,"bevy_persistence_version":ver+1});
    db.execute_transaction(vec![
        TransactionOperation::UpdateDocument {
            collection: Collection::Entities,
            key: guid.clone(),
            expected_current_version: ver,
            patch: bad.clone(),
        }
    ]).await.unwrap();

    // modify all locally
    for id in &ids {
        app.world_mut().get_mut::<Health>(*id).unwrap().value += 10;
    }
    app.update();

    let res = commit(&mut app).await;
    assert!(matches!(res, Err(PersistenceError::Conflict{ key }) if key == guid));
    assert_eq!(*app.world().resource::<CommitStatus>(), CommitStatus::Idle);
}

#[tokio::test]
async fn test_concurrent_batch_execution() {
    // Create a mock database that introduces a delay for each batch
    let mut db = MockDatabaseConnection::new();
    let batch_count = 5;
    let batch_delay = Duration::from_millis(50);

    // Configure the mock to delay each transaction by batch_delay
    db.expect_execute_transaction()
        .times(batch_count)
        .returning(move |_| {
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
    let plugin = PersistencePluginCore::new(db_arc.clone()).with_config(config);
    let mut app = App::new();
    app.add_plugins(plugin);

    // Spawn enough entities to create `batch_count` batches
    for i in 0..entity_count {
        app.world_mut().spawn(Health { value: i as i32 });
    }
    app.update();

    // Measure time for the commit operation
    let start_time = Instant::now();
    let res = commit(&mut app).await;
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
