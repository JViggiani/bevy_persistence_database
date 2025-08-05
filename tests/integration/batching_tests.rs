use bevy::prelude::*;
use bevy_arangodb::{
    commit, PersistenceQuery, CommitStatus, PersistenceError, Guid,
    PersistencePlugins,
};
use crate::common::{setup, make_app};
use crate::common::Health;

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
        bevy_arangodb::TransactionOperation::UpdateDocument {
            collection: bevy_arangodb::Collection::Entities,
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
