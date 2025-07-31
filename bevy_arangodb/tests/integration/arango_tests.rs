use arangors::{AqlQuery, ClientError};
use bevy_arangodb::{ArangoDbConnection,};
use serde_json::json;
use std::collections::HashMap;
use serde_json::Value;

use crate::common::*;

#[tokio::test]
async fn test_raw_arango_conflict_and_merge() {
    let _guard = DB_LOCK.lock().await;
    let db = setup().await;

    // 1. Create an initial document.
    let initial_doc = json!({
        "Health": { "value": 100 },
        "Position": { "x": 10.0, "y": 10.0 }
    });

    let arango_db = db
        .as_any()
        .downcast_ref::<ArangoDbConnection>()
        .expect("Tests require a real ArangoDbConnection");

    let coll = arango_db
        .db
        .collection("entities")
        .await
        .expect("Failed to get collection");

    let meta = coll
        .create_document(initial_doc, Default::default())
        .await
        .expect("Failed to create initial document");

    let header = meta.header().expect("Response should have a header");
    let key = header._key.clone();
    let mut rev = header._rev.clone();
    tracing::info!(key, rev, "Initial document created");

    // 2. Simulate an external write that changes Health and gets a new revision.
    let aql = format!(
        "UPDATE \"{}\" WITH {{ `Health`: {{ \"value\": 200 }} }} IN entities RETURN NEW._rev",
        key
    );
    let new_revs: Vec<String> = db.query(aql, Default::default()).await.unwrap();
    let external_rev = new_revs[0].clone();
    tracing::info!(external_rev, "External write complete");
    assert_ne!(rev, external_rev);

    // 3. Simulate the app's first commit attempt with the old revision. This should fail.
    let conflicting_doc = json!({
        "Health": { "value": 100 }, // App's stale data
        "Position": { "x": 50.0, "y": 10.0 }, // App's new data
        "_key": key,
        "_rev": rev, // Using the old revision
    });

    let aql = "REPLACE @doc IN entities OPTIONS { ignoreRevs: false }";
    let mut bind_vars = HashMap::new();
    bind_vars.insert("doc", conflicting_doc.clone());

    let query = AqlQuery::builder().query(aql).bind_vars(bind_vars).build();
    let conflict_result: Result<Vec<serde_json::Value>, _> = arango_db.db.aql_query(query).await;

    tracing::info!(?conflict_result, "Result of conflicting commit");
    assert!(conflict_result.is_err());
    if let Err(ClientError::Arango(e)) = conflict_result {
        assert_eq!(
            e.error_num(),
            1200,
            "Expected precondition failed error (1200)"
        );
    } else {
        panic!("Expected a ClientError::Arango, but got something else");
    }
    tracing::info!("Successfully detected conflict, as expected.");

    // 4. Simulate reloading the document to get the latest state and revision.
    let doc_after_conflict = coll
        .document::<serde_json::Value>(&key)
        .await
        .expect("Failed to fetch document after conflict");
    let reloaded_doc = doc_after_conflict.document;
    rev = doc_after_conflict.header._rev;
    tracing::info!(?reloaded_doc, rev, "Reloaded document from DB");
    assert_eq!(rev, external_rev);
    assert_eq!(reloaded_doc["Health"]["value"], 200);

    // 5. Simulate the successful merge commit.
    // We take the reloaded doc and apply our local change (the new Position).
    let mut merged_doc = reloaded_doc;
    merged_doc["Position"]["x"] = json!(50.0);
    merged_doc["_key"] = json!(key.clone());
    merged_doc["_rev"] = json!(rev.clone());

    let aql = "REPLACE @doc IN entities OPTIONS { ignoreRevs: false } RETURN NEW";
    let mut bind_vars = HashMap::new();
    bind_vars.insert("doc", merged_doc.clone());
    let query = AqlQuery::builder().query(aql).bind_vars(bind_vars).build();

    let merge_result: Result<Vec<serde_json::Value>, _> = arango_db.db.aql_query(query).await;
    tracing::info!(?merge_result, "Result of merge commit");
    assert!(merge_result.is_ok(), "Merge commit failed");

    // 6. Verify the final state in the database.
    let final_doc_from_db = coll
        .document::<serde_json::Value>(&key)
        .await
        .expect("Failed to fetch final document")
        .document;

    assert_eq!(final_doc_from_db["Health"]["value"], 200); // External change was preserved.
    assert_eq!(final_doc_from_db["Position"]["x"], 50.0); // Our change was applied.
    tracing::info!("Merge successful and final state is correct!");
}

#[tokio::test]
async fn test_raw_arango_update_logic() {
    let _guard = DB_LOCK.lock().await;
    let db = setup().await;
    let arango_db = db
        .as_any()
        .downcast_ref::<ArangoDbConnection>()
        .expect("Tests require a real ArangoDbConnection");
    let coll = arango_db.db.collection("entities").await.unwrap();

    // 1. Create initial document
    let initial_doc = json!({ "Health": { "value": 100 }, "Position": { "x": 10.0, "y": 10.0 } });
    let meta = coll.create_document(initial_doc, Default::default()).await.unwrap();
    let key = meta.header().unwrap()._key.clone();
    let initial_rev = meta.header().unwrap()._rev.clone();

    // 2. External write changes Health
    let aql_external = format!("UPDATE \"{}\" WITH {{ `Health`: {{ \"value\": 200 }} }} IN entities RETURN NEW._rev", key);
    let external_rev: String = db.query(aql_external, Default::default()).await.unwrap().remove(0);

    // 3. FLAWED MERGE ATTEMPT: Mimic the Bevy app's current flawed logic inside a transaction.
    // The patch incorrectly includes the reloaded Health data.
    let flawed_patch = json!({
        "Health": { "value": 200 }, // This is the reloaded value, not a local change.
        "Position": { "x": 50.0, "y": 10.0 }
    });
    let flawed_result = db.execute_transaction(vec![
        bevy_arangodb::TransactionOperation::UpdateDocument {
            key: key.clone(),
            rev: external_rev.clone(),
            patch: flawed_patch,
        }
    ]).await;

    tracing::info!(?flawed_result, "Result of FLAWED merge with transactional UPDATE");
    assert!(flawed_result.is_err(), "Flawed transactional UPDATE should have failed with a conflict");
    if let Err(bevy_arangodb::PersistenceError::Conflict { .. }) = flawed_result {
        // This is the expected outcome
    } else {
        panic!("Expected a Conflict error");
    }
    tracing::info!("SUCCESS: Reproduced the Bevy app's failure in the raw test.");

    // 4. CORRECT MERGE ATTEMPT: Use a patch with ONLY the locally changed data.
    let correct_patch = json!({
        "Position": { "x": 50.0, "y": 10.0 } // Only the component that actually changed locally.
    });
    let correct_result = db.execute_transaction(vec![
        bevy_arangodb::TransactionOperation::UpdateDocument {
            key: key.clone(),
            rev: external_rev.clone(),
            patch: correct_patch,
        }
    ]).await;

    tracing::info!(?correct_result, "Result of CORRECT merge with transactional UPDATE");
    assert!(correct_result.is_ok(), "Correct transactional UPDATE should have succeeded");

    // 5. VERIFY: The final document has the merged state.
    let final_doc = coll.document::<Value>(&key).await.unwrap().document;
    assert_eq!(final_doc["Health"]["value"], 200); // External change was preserved.
    assert_eq!(final_doc["Position"]["x"], 50.0); // Our change was applied.
    tracing::info!("SUCCESS: The correct merge logic works as expected.");
}