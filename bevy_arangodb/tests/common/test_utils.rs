use bevy::prelude::*;
use bevy_arangodb::{commit, dump_persistence_state, PersistenceError};
use std::time::{Duration, Instant};
use tracing::{error, info, warn};

/// Runs a commit with timeout and detailed diagnostics
pub async fn diagnostic_commit(app: &mut App, timeout: Duration) -> Result<(), PersistenceError> {
    info!("Starting diagnostic_commit with timeout {:?}", timeout);
    let start = Instant::now();

    // First try a normal commit
    let commit_future = commit(app);
    let result = tokio::time::timeout(timeout, commit_future).await;

    match result {
        Ok(commit_result) => {
            if let Err(e) = &commit_result {
                error!("COMMIT FAILED with error: {}", e);
                warn!("Dumping persistence state at failure:");
                dump_persistence_state(app);
            } else {
                info!("Commit completed successfully in {:?}", start.elapsed());
            }
            commit_result
        }
        Err(_) => {
            error!("COMMIT TIMEOUT after {:?}", timeout);
            warn!("Dumping persistence state at timeout:");
            dump_persistence_state(app);

            // The logic in commit_and_wait is now more robust, so this is less likely to be needed.
            // Keeping it for now as a last-ditch diagnostic effort in case of hangs.
            app.update();
            warn!("Forced an additional app.update() on timeout");

            // Return a timeout error
            Err(PersistenceError::Generic(format!(
                "Commit timed out after {:?}",
                timeout
            )))
        }
    }
}
