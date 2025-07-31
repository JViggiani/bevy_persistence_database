# bevy_arangodb

A Bevy plugin for persisting ECS state to ArangoDB, supporting components, resources, and querying. Built with async Rust and designed for seamless integration into Bevy's ECS architecture.

## Features

- **Component & Resource Persistence**: Automatically persist Bevy components and resources to ArangoDB documents.
- **Ergonomic API**: Simple derive macros and minimal boilerplate to mark types for persistence.
- **Type-Safe Querying**: Build complex queries with a type-safe DSL that leverages Rust's type system.
- **Change Detection**: Leverages Bevy's built-in change detection to efficiently sync only modified data.
- **Async/Await Support**: Full async support with a convenient `commit` function for easy integration.
- **Optimistic Locking**: Automatic version management prevents data loss from concurrent modifications.

## Quick Start

1. Derive persisted types:

    ```rust
    use bevy_arangodb::Persist;

    #[persist(component)]
    pub struct Health {
        pub value: i32,
    }

    #[persist(resource)]
    pub struct GameSettings {
        pub difficulty: f32,
        pub map_name: String,
    }
    ```

2. Configure your Bevy `App`:

    ```rust
    use bevy::prelude::*;
    use bevy_arangodb::{PersistencePlugins, ArangoDbConnection, commit};
    use std::sync::Arc;

    #[tokio::main]
    async fn main() {
        // 1) Connect to ArangoDB
        let db = Arc::new(
            ArangoDbConnection::connect(
                "http://127.0.0.1:8529", "root", "password", "mydb"
            )
            .await
            .unwrap()
        );

        // 2) Build Bevy app with plugin
        let mut app = App::new();
        app.add_plugins(DefaultPlugins);
        app.add_plugins(PersistencePlugins(db.clone()));

        // 3) Spawn entities or insert resources
        app.world_mut().spawn(Health { value: 42 });
        app.insert_resource(GameSettings { difficulty: 1.0, map_name: "level1".into() });

        // 4) Run one tick to detect changes
        app.update();

        // 5) Persist all changes.
        // `commit` is a helper that fires a `TriggerCommit` event and waits for
        // a `CommitCompleted` event. You can also fire the event manually.
        commit(&mut app).await.unwrap();
    }
    ```

3. Query persisted data:

    ```rust
    use bevy_arangodb::{PersistenceQuery, PersistencePlugins, DatabaseConnection};
    use std::sync::Arc;
    use bevy::prelude::*;

    async fn load_data(db: Arc<dyn DatabaseConnection>) {
        let mut app = App::new();
        app.add_plugins(PersistencePlugins(db.clone()));

        // Only fetch Health > 10, also load Position if present
        let entities = PersistenceQuery::new(db.clone())
            .with::<Position>()
            .filter(Health::value().gt(10))
            .fetch_into(app.world_mut())
            .await;

        println!("Loaded {} entities", entities.len());
    }
    ```

## Optimistic Locking

The library automatically handles versioning to prevent race conditions when multiple processes access the same data. Each entity and resource document contains a `bevy_persistence_version` field that is checked during updates.

When a version conflict is detected, the library returns a `PersistenceError::Conflict`. Your application can implement its own conflict resolution strategy:

```rust
use bevy_arangodb::{commit, PersistenceError, PersistenceQuery};

// Example: Last Write Wins strategy with retry
async fn commit_with_retry(app: &mut App, max_retries: u32) -> Result<(), PersistenceError> {
    for _ in 0..max_retries {
        match commit(app).await {
            Ok(()) => return Ok(()),
            Err(PersistenceError::Conflict { key }) => {
                // Reload the conflicted entity
                let query = PersistenceQuery::new(db.clone())
                    .with::<Health>()
                    .with::<Position>();
                query.fetch_into(app.world_mut()).await;
                
                // Re-apply your changes here
                // ...
                
                // Try again
                continue;
            }
            Err(e) => return Err(e),
        }
    }
    Err(PersistenceError::General("Max retries exceeded".into()))
}
```

The versioning is completely managed by the library - you don't need to worry about it unless you're handling conflicts.

See the `concurrency_tests.rs` file for more examples of merge strategies. 