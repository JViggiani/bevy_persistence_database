use bevy::prelude::App;
use bevy_arangodb_core::{
    commit_sync, persistence_plugin::PersistencePlugins, PersistenceQuery, PersistencePluginCore, 
    persistence_plugin::PersistencePluginConfig
};
use std::time::Instant;
use crate::common::*;

// Mark this test as ignored by default - run manually to benchmark the system
#[ignore]
#[test]
fn test_persist_many_entities() {
    let (db, _container) = setup_sync();
    
    // Use a higher entity count to better demonstrate parallel performance
    let count = 50_000;
    let thread_count = 8; // Explicitly set a higher thread count
    
    // Create app with explicit configuration
    let mut app = App::new();
    let config = PersistencePluginConfig {
        batching_enabled: true,
        commit_batch_size: 1000,
        thread_count,
    };
    app.add_plugins(PersistencePluginCore::new(db.clone()).with_config(config));
    
    // --- spawn phase ---
    let start_spawn = Instant::now();
    for _ in 0..count {
        app.world_mut().spawn(Health { value: 42 });
    }
    app.update();
    // --- prepare & commit phase ---
    let start_commit = Instant::now();
    let res = commit_sync(&mut app);
    let duration_commit = start_commit.elapsed();
    res.expect("Bulk commit failed");
    
    println!(
        "Committed {} entities using {} threads in {:.2?} ({:.0} entities/sec)",
        count, 
        thread_count,
        duration_commit,
        count as f32 / duration_commit.as_secs_f32()
    );
    
    // --- fetch phase in a new session ---
    let mut app2 = App::new();
    app2.add_plugins(PersistencePlugins(db.clone()));

    let start_fetch = Instant::now();
    let loaded = run_async(
        PersistenceQuery::new(db.clone())
            .with::<Health>()
            .fetch_into(app2.world_mut()),
    );
    let duration_fetch = start_fetch.elapsed();
    
    println!(
        "Fetched {} entities in {:.2?} ({:.0} entities/sec)",
        loaded.len(),
        duration_fetch,
        loaded.len() as f32 / duration_fetch.as_secs_f32()
    );
    
    assert_eq!(loaded.len(), count, "Loaded entity count mismatch");
    assert!(duration_commit.as_secs_f32() < 60.0, "Commit too slow: {:?}", duration_commit);
    assert!(duration_fetch.as_secs_f32() < 60.0, "Fetch too slow: {:?}", duration_fetch);
}
