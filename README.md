# bevy_persistence_database

Persistence for Bevy ECS to ArangoDB or Postgres with an idiomatic Bevy Query API.

## Highlights
- **Bevy Query API consistency**
  - `PersistenceQuery` SystemParam derefs to Bevy's Query, so iter/get/single/get_many/contains/iter_combinations work as usual.
  - Explicit load trigger: `ensure_loaded` performs DB I/O at the point the user deems most useful, then you use pass-throughs on world data.
- **Smart caching and explicit refresh**
  - Identical loads in the same frame coalesce into a single DB call.
  - Repeated identical loads hit the cache. Use `force_refresh` to bypass it.
- **Presence and value filters**
  - Type-driven presence via `With<T>`, `Without<T>`, and `Or<(…)>`.
  - Optional components in Q (`Option<&T>`) are fetched when present.
  - Value filters: eq, ne, gt, gte, lt, lte, combined with and/or/in.
  - Key filtering via `Guid::key_field().eq("...")` or `.in_([...])`.
- **Resources**
  - `#[persist(resource)]` resources are persisted and fetched alongside any query.
- **Optimistic concurrency and conflict handling**
  - Per-document versioning with conflict detection.
- **Batching and parallel commit**
  - Configurable batching with a thread pool and concurrent transaction execution.
- **Backends**
  - ArangoDB and Postgres, selected at build time via features.

## Install
Add the crate with the backends you need:

```toml
[dependencies]
bevy = { version = "0.17", default-features = false, features = ["bevy_log"] }
bevy_persistence_database = { version = "0.2.2", features = ["arango", "postgres"] }
```

## Backends and features
- Enable features on `bevy_persistence_database`:
  - `arango`: builds the ArangoDB backend (arangors).
  - `postgres`: builds the Postgres backend (tokio-postgres).
- Provide an `Arc<dyn DatabaseConnection>` at startup:
  - **Arango:**
    ```rust
    await ArangoDbConnection::ensure_database(url, user, pass, db_name)
    let db = ArangoDbConnection::connect(url, user, pass, db_name).await?
    ```
  - **Postgres:**
    ```rust
    PostgresDbConnection::ensure_database(host, user, pass, db_name, Some(port)).await?
    let db = PostgresDbConnection::connect(host, user, pass, db_name, Some(port)).await?
    ```

## Quickstart
### 1) Persist-able types

```rust
use bevy_persistence_database::persist;

#[persist(component)]
#[derive(Clone)]
pub struct Health { pub value: i32 }

#[persist(component)]
pub struct Position { pub x: f32, pub y: f32 }

#[persist(resource)]
#[derive(Clone)]
pub struct GameSettings { pub difficulty: f32, pub map_name: String }
```

### 2) Add the plugin with a real database connection

```rust
use bevy::prelude::*;
use bevy_persistence_database::{PersistencePlugins, ArangoDbConnection};
use bevy_persistence_database::persistence_plugin::PersistencePluginConfig;
use std::sync::Arc;

fn main() {
    // Build a small runtime to create the DB connection up-front
    let rt = tokio::runtime::Runtime::new().unwrap();

    // Arango example (see tests/common/setup.rs for the same pattern)
    let url = "http://127.0.0.1:8529";
    let user = "root";
    let pass = "password";
    let db_name = "bevy_example";
    rt.block_on(ArangoDbConnection::ensure_database(url, user, pass, db_name)).unwrap();
    let db = rt.block_on(ArangoDbConnection::connect(url, user, pass, db_name)).unwrap();
    let db = Arc::new(db) as Arc<dyn bevy_persistence_database::DatabaseConnection>;

    App::new()
      .add_plugins(PersistencePlugins::new(db).with_config(PersistencePluginConfig {
        // Pick a default store name for commits/queries that don't override it.
        default_store: "bevy_example".into(),
        ..Default::default()
      }))
      .run();
}
```

## Loading pattern
- Use `PersistenceQuery` as a SystemParam.
- Add value filters via `.where(...)` (alias of `.filter(...)`).
- Explicitly trigger the load with `.ensure_loaded()`; then use pass-throughs on world-only data.

```rust
use bevy::prelude::*;
use bevy_persistence_database::{PersistenceQuery, Guid};

fn sys(
    mut pq: PersistenceQuery<(&Health, Option<&Position>), (With<Health>, Without<Creature>, Or<(With<PlayerName>,)>)>
) {
  let count = pq
    // Optional: point this query at a specific store instead of the default
    .store("bevy_example")
        .where(Health::value().gt(100))
        .ensure_loaded()
        .iter()
        .count();
    info!("Loaded {} entities", count);
}
```

## Pass-throughs are world-only
After a load, `PersistenceQuery` derefs to `bevy::prelude::Query`. Use standard accessors without new DB I/O:
```rust
fn pass_through(mut pq: PersistenceQuery<&Health>) {
    let _ = pq.ensure_loaded();

    // single
    if let Ok((_e, h)) = pq.get_single() {
        info!("single: {}", h.value);
    }

    // get_many (array-of-entities)
    let [a, b] = [e1, e2];
    let Ok([(_, ha), (_, hb)]) = pq.get_many([a, b]) else { return; };

    // iter_combinations
    for [(_e1, h1), (_e2, h2)] in pq.iter_combinations() {
        info!("pair: {} {}", h1.value, h2.value);
    }
}
```

## Key filtering
- Single key:
```rust
use bevy_persistence_database::{PersistenceQuery, Guid};

fn by_key(mut pq: PersistenceQuery<&Health>) {
    let _ = pq.where(Guid::key_field().eq("my-key")).ensure_loaded();
}
```
- Multiple keys via IN:
```rust
let _ = pq.where(Guid::key_field().in_(&["k1","k2","k3"])).ensure_loaded();
```

## Optional Q semantics and presence via F
- `Option<&T>` in Q fetches T only when present, without gating presence.
- Presence is driven by type-level filters in F (`With<T>`, `Without<T>`, `Or<(... )>`), which gate backend queries.

## Caching and force-refresh
- Default `CachePolicy` is `UseCache`. Identical loads coalesce within a frame and reuse cached results across frames.
- Use `force_refresh` to bypass cache and overwrite world state:
```rust
let _ = pq.force_refresh().ensure_loaded();
```

## Resources
- `#[persist(resource)]` resources are fetched alongside any query call; versions are tracked and updated on commits automatically.

## Committing Changes

### Manual Commit Required
Changes are NOT automatically committed. You must explicitly trigger commits:

```rust
use bevy_persistence_database::TriggerCommit;

fn manual_commit(mut events: EventWriter<TriggerCommit>) {
    // Send an event to trigger a commit
  events.send(TriggerCommit {
    correlation_id: None,
    target_connection: /* your Arc<dyn DatabaseConnection> */ db.clone(),
    store: "bevy_example".into(),
  });
}
```

Or use the convenience functions:
```rust
use bevy_persistence_database::{commit, commit_sync};

// Async commit (non-blocking)
commit(&mut app, db.clone(), "bevy_example");

// Synchronous commit (blocks until complete)  
commit_sync(&mut app, db.clone(), "bevy_example");
```

The commit system processes any TriggerCommit events each frame and clears the event queue after committing.

## Joining

Efficiently join query results without loading unnecessary entities:

```rust
use bevy_persistence_database::{PersistenceQuery, query::join::Join};

fn join_example(
    mut common: PersistenceQuery<(&Health, &Position)>,
    mut names: PersistenceQuery<&PlayerName>,
) {
    // Smart-join loads only the intersection from the database
    let joined = names.join_filtered(&mut common);
    
    // Now we can iterate over entities that have all components
    for (entity, (health, position, name)) in joined.iter() {
        println!("Entity with health {}, position ({}, {}) and name {}",
            health.value, position.x, position.y, name.name);
    }
}
```

The join operation is smart and efficient:
- Only loads entities at the intersection of both queries
- Materializes components only for matched entities
- Avoids loading unneeded entities into the world
- Can be used in a single system without separate load steps

### Transmutation
Convert between component types dynamically:

```rust
use bevy_persistence_database::query::QueryDataToComponents;

fn transmute_example(mut pq: PersistenceQuery<&Health>) {
    pq.ensure_loaded();
    
    // Convert to different component types at runtime
    let components = pq.transmute::<(&Health, &Position)>();
}
```

## Advanced Configuration

### Plugin Configuration
```rust
use bevy_persistence_database::{PersistencePluginCore, persistence_plugin::PersistencePluginConfig};

let config = PersistencePluginConfig {
    batching_enabled: true,
    commit_batch_size: 100,
    thread_count: 4,
  default_store: "bevy_example".into(),
};

let plugin = PersistencePluginCore::new(db).with_config(config);
app.add_plugins(plugin);
```

### Version Management
The library includes automatic version management for optimistic concurrency control:

```rust
use bevy_persistence_database::VersionManager;

// Versions are automatically tracked and incremented on commits
// Conflicts are detected and can be handled via error handling
```

## Scheduling notes
- Loads can run in Update or PostUpdate.
- Deferred world mutations from loads are applied before PreCommit.
- Schedule readers after `PersistenceSystemSet::PreCommit` to observe fresh data in the same frame.
- The commit system runs in `PersistenceSystemSet::Commit`.

## Testing
The library includes comprehensive test utilities for both backends:

```rust
#[cfg(test)]
mod tests {
    use bevy_persistence_database::db_matrix_test;
    
    #[db_matrix_test]
    fn test_both_backends() {
        let (db, _guard) = setup();
        // Test runs against both Arango and Postgres
    }
}
```

## Error Handling
All database operations return `Result<T, PersistenceError>`:

```rust
use bevy_persistence_database::PersistenceError;

match pq.ensure_loaded() {
    Ok(_) => { /* handle success */ }
    Err(PersistenceError::ConnectionError(e)) => { /* handle connection issues */ }
    Err(PersistenceError::VersionConflict) => { /* handle optimistic locking conflicts */ }
    // ... other error types
}
```