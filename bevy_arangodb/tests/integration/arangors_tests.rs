use bevy_arangodb_core::arangors::{
    document::options::UpdateOptions,
    transaction::{TransactionCollections, TransactionSettings},
    ClientError, Connection,
};
use serde_json::json;
use crate::common::DB_LOCK;
use std::env;

#[tokio::test]
#[ignore] // Ignore by default as it's for isolated debugging. Run with --ignored flag.
async fn test_arangors_optimistic_locking_in_transaction() {
    let _guard = DB_LOCK.lock().await;

    // Connect directly to the database using environment variables
    let db_url = env::var("ARANGODB_URL").unwrap_or_else(|_| "http://localhost:8529".to_string());
    let db_user = env::var("ARANGODB_USER").unwrap_or_else(|_| "root".to_string());
    let db_pass = env::var("ARANGODB_PASS").unwrap_or_else(|_| "password".to_string());
    let db_name = env::var("ARANGODB_DB").unwrap_or_else(|_| "_system".to_string());

    let conn = Connection::establish_jwt(&db_url, &db_user, &db_pass).await.unwrap();
    let db = conn.db(&db_name).await.unwrap();
    let collection_name = "arangors_trx_test";

    // 1. Ensure collection exists and is clean
    let _ = db.drop_collection(collection_name).await;
    let collection = db.create_collection(collection_name).await.unwrap();
    println!("Created collection '{}'", collection_name);

    // 2. Create an initial document
    let initial_doc = json!({ "value": 100 });
    let create_response = collection.create_document(initial_doc, Default::default()).await.unwrap();
    let doc_header = create_response.header().unwrap();
    let doc_key = doc_header._key.clone();
    let initial_rev = doc_header._rev.clone();
    println!("Created doc '{}' with rev '{}'", doc_key, initial_rev);

    // 3. Start a transaction and try to update with a STALE revision
    println!("\n--- Attempting update with STALE revision inside a transaction ---");
    let trx_settings = TransactionSettings::builder()
        .collections(TransactionCollections::builder().write(vec![collection_name.to_string()]).build())
        .build();
    let trx = db.begin_transaction(trx_settings).await.unwrap();
    let trx_collection = trx.collection(collection_name).await.unwrap();

    let stale_patch = json!({ "value": 90, "_rev": "wrong_rev" });
    let update_options = UpdateOptions::builder().ignore_revs(false).build();
    let stale_update_result = trx_collection.update_document(&doc_key, stale_patch, update_options).await;
    
    assert!(stale_update_result.is_err(), "Update with stale revision should have failed");
    if let Err(ClientError::Arango(e)) = stale_update_result {
        println!("Successfully failed with expected error: {}", e);
        assert_eq!(e.error_num(), 1200, "Expected error 1200 (precondition failed)");
    } else {
        panic!("Expected a conflict error, but got something else or success: {:?}", stale_update_result.err());
    }
    trx.abort().await.unwrap();
    println!("Transaction aborted as expected.");

    // 4. Start a new transaction and update with the CORRECT revision
    println!("\n--- Attempting update with CORRECT revision inside a transaction ---");
    let trx_settings_2 = TransactionSettings::builder()
        .collections(TransactionCollections::builder().write(vec![collection_name.to_string()]).build())
        .build();
    let trx2 = db.begin_transaction(trx_settings_2).await.unwrap();
    let trx_collection2 = trx2.collection(collection_name).await.unwrap();
    
    let correct_patch = json!({ "value": 80, "_rev": initial_rev.clone() });
    let update_options = UpdateOptions::builder().ignore_revs(false).build();
    let successful_update_result = trx_collection2.update_document(&doc_key, correct_patch, update_options).await;

    assert!(successful_update_result.is_ok(), "Update with correct revision failed: {:?}", successful_update_result.err());
    let binding = successful_update_result.unwrap();
    let new_header = binding.header().unwrap();
    let new_rev = new_header._rev.clone();
    println!("Successfully updated doc '{}' to new rev '{}'", doc_key, &new_rev);
    assert_ne!(initial_rev, new_rev);

    trx2.commit().await.unwrap();
    println!("Transaction committed successfully.");

    // 5. Verify the final state of the document
    let final_doc = collection.document::<serde_json::Value>(&doc_key).await.unwrap();
    assert_eq!(final_doc.document.get("value").unwrap().as_i64().unwrap(), 80);
    assert_eq!(final_doc.header._rev, new_rev);
    println!("\nFinal document state is correct.");
}
