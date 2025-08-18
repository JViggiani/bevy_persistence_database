# bevy_arangodb

Persistence for Bevy ECS to ArangoDB or Postgres with an idiomatic Bevy Query API.

Highlights
- Bevy Query API consistency
  - PersistentQuery SystemParam derefs to Bevy’s Query, so iter/get/single/contains/iter_combinations work as usual.
  - Explicit load trigger: ensure_loaded performs DB I/O, then you use pass-throughs on world data.
- Smart caching and explicit refresh
  - Identical loads in the same frame coalesce into a single DB call.
  - Repeated identical loads hit the cache. Use force_refresh to bypass it.
- Presence and value filters
  - Type-driven presence via With<T>, Without<T>, and Or<(…)>.
  - Optional components in Q (Option<&T>) are fetched when present.
  - Value filters: eq, ne, gt, gte, lt, lte, combined with and/or.
  - Key filtering via Guid::key_field().eq("…").
- Resources
  - #[persist(resource)] resources are persisted and fetched alongside any query.
- Optimistic concurrency and conflict handling
  - Per-document versioning, conflict detection, strategies validated by tests (last-write-wins via reload, three-way merge).
- Change detection 
  - Leverages Bevy's change detection to only sync only modified data.
- Batching and parallel commit
  - Configurable batching with a thread pool for preparing operations and concurrent transaction execution.
- Backends
  - ArangoDB and Postgres, selected at build time via features.

Install
Add the core and derive crates, enabling one or both backends.

```toml
[dependencies]
bevy = { version = "0.13", default-features = false } # or your Bevy profile
bevy_arangodb_core = { path = "bevy_arangodb/bevy_arangodb_core", features = ["arango", "postgres"] }
bevy_arangodb_derive = { path = "bevy_arangodb/bevy_arangodb_derive" }
```

Backends and features
- Enable features on bevy_arangodb_core:
  - arango: builds the ArangoDB backend (arangors).
  - postgres: builds the Postgres backend (tokio-postgres).
- Provide an Arc<dyn DatabaseConnection> at startup:
  - Arango:
    - await ArangoDbConnection::ensure_database(url, user, pass, db_name)
    - let db = ArangoDbConnection::connect(url, user, pass, db_name).await?
  - Postgres:
    - PostgresDbConnection::ensure_database(host, user, pass, db_name, Some(port)).await?
    - let db = PostgresDbConnection::connect(host, user, pass, db_name, Some(port)).await?

Quickstart
1) Declare Persist-able types with a single attribute. The macro derives serde and registers systems.

```rust
use bevy_arangodb_derive::persist;

#[persist(component)]
#[derive(Clone)]
pub struct Health { pub value: i32 }

#[persist(component)]
pub struct Position { pub x: f32, pub y: f32 }

#[persist(resource)]
#[derive(Clone)]
pub struct GameSettings { pub difficulty: f32, pub map_name: String }
```

2) Add the plugin with a database connection.

```rust
use bevy::prelude::*;
use bevy_arangodb_core::{PersistencePluginCore, commit_sync};
use std::sync::Arc;

fn main() {
    // Construct your DB connection up-front (Arango or Postgres)
    // let db: Arc<dyn DatabaseConnection> = Arc::new(ArangoDbConnection::connect(...).await?);
    // or
    // let db: Arc<dyn DatabaseConnection> = Arc::new(PostgresDbConnection::connect(...).await?);

    let db: Arc<dyn bevy_arangodb_core::DatabaseConnection> = /* ... */ unimplemented!();

    let mut app = App::new();
    app.add_plugins(PersistencePluginCore::new(db.clone()));

    // Spawn and commit synchronously
    let e = app.world_mut().spawn(Health { value: 100 }).id();
    app.update();
    commit_sync(&mut app).expect("commit failed");

    // Later: fetch with PersistentQuery
    app.add_systems(Update, |mut pq: bevy_arangodb_core::PersistentQuery<&Health>| {
        let _ = pq.ensure_loaded(); // triggers DB load once
        for (_e, h) in pq.iter() {
            bevy::log::info!("Loaded Health: {}", h.value);
        }
    });
    app.run();
}
```

PersistentQuery: world-only pass-through with explicit loads
- Use ensure_loaded to trigger DB I/O. After that, call any Bevy Query methods on the same SystemParam (Deref).
- Caching prevents duplicate loads in the same frame and across frames until invalidation.

Examples
Presence and pass-through
```rust
use bevy::prelude::*;
use bevy_arangodb_core::PersistentQuery;

fn load_and_iter(mut pq: PersistentQuery<(&Health, &Position), (With<Health>, With<Position>)>) {
    let _ = pq.ensure_loaded(); // I/O occurs here
    // Pass-through on world state:
    for (_e, (h, p)) in pq.iter() {
        bevy::log::info!("H={} at ({},{})", h.value, p.x, p.y);
    }
}
```

Value filters
- Each component gets field accessors via the macro (Health::value(), Position::x(), etc).
- Combine with and()/or().

```rust
fn gt_100(mut pq: PersistentQuery<&Health>) {
    let _ = pq.filter(Health::value().gt(100)).ensure_loaded();
}

fn mixed(mut pq: PersistentQuery<(&Health, &Position)>) {
    let expr = Health::value().gte(100).and(Position::x().lt(50.0));
    let _ = pq.filter(expr).ensure_loaded();
}
```

Key filtering
```rust
use bevy_arangodb_core::Guid;

fn by_key(mut pq: PersistentQuery<&Health>) {
    let expr = Guid::key_field().eq("my-guid");
    let _ = pq.filter(expr).ensure_loaded();
}
```

Optional components
- Ask for Option<&T> in Q; the component is inserted when present.

```rust
fn optional(mut pq: PersistentQuery<(&Health, Option<&Position>)>) {
    let _ = pq.ensure_loaded();
}
```

OR presence via Or<(…)> in F
- Use Or<(With<A>, With<B>, …)> to fetch entities that have any of the listed components.

```rust
fn or_presence(mut pq: PersistentQuery<Or<(With<Health>, With<Creature>)>>) {
    let _ = pq.ensure_loaded();
}
```

Caching and refresh
- Default policy: UseCache. Identical loads coalesce (validated in tests).
- force_refresh bypasses cache to overwrite local state.

```rust
fn refresh(mut pq: PersistentQuery<&Health>) {
    let _ = pq.force_refresh().ensure_loaded();
}
```

Resources
- #[persist(resource)] resources are fetched alongside any query call; versions are tracked and updated on commits automatically.

Commits, batching, and conflicts
- Commits are event-driven under the hood; commit_sync provides a simple synchronous wrapper for tests and tools.
- Configure batching for large commits:

```rust
use bevy_arangodb_core::{PersistencePluginCore, persistence_plugin::PersistencePluginConfig};
let config = PersistencePluginConfig { batching_enabled: true, commit_batch_size: 1000, thread_count: 8 };
app.add_plugins(PersistencePluginCore::new(db.clone()).with_config(config));
```

- Optimistic locking with per-document versioning; conflicts return PersistenceError::Conflict { key }.
- Strategies:
  - Last-write-wins: reload by key, reapply your local changes, commit again.
  - Three-way merge: load “theirs”, merge with “mine”, commit.

Manual builder (advanced)
- A lower-level PersistenceQuery builder exists for out-of-system usage and keys-only retrieval.

```rust
use bevy_arangodb_core::query::persistence_query::PersistenceQuery;
use std::sync::Arc;

let q = PersistenceQuery::new(db.clone())
    .with::<Health>()
    .filter(Health::value().eq(100));
let keys = q.fetch_ids().await;
```

Postgres performance notes
- Schema is created automatically with a GIN index on entities.doc using jsonb_path_ops.
- Presence uses the jsonb existence operator doc ? 'Component'; equality and relational comparisons use typed casts.
- You can add targeted functional indexes for hot fields if needed.

ArangoDB notes
- AQL is generated from the same backend-agnostic filter tree. Collections are auto-created.

Testing matrix
- The test suite runs across both backends (when both features are enabled). Use:
  - BEVY_ARANGODB_TEST_BACKENDS=arango,postgres cargo test
- Containers auto-start for integration tests. Set:
  - BEVY_ARANGODB_KEEP_CONTAINER=1 to keep Arango running
  - BEVY_POSTGRESDB_KEEP_CONTAINER=1 to keep Postgres running