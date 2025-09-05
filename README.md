# bevy_persistence_database

Persistence for Bevy ECS to ArangoDB or Postgres with an idiomatic Bevy Query API.

Highlights
- Bevy Query API consistency
  - PersistentQuery SystemParam derefs to Bevy’s Query, so iter/get/single/get_many/contains/iter_combinations work as usual.
  - Explicit load trigger: ensure_loaded performs DB I/O, then you use pass-throughs on world data.
- Smart caching and explicit refresh
  - Identical loads in the same frame coalesce into a single DB call.
  - Repeated identical loads hit the cache. Use force_refresh to bypass it.
- Presence and value filters
  - Type-driven presence via With<T>, Without<T>, and Or<(…)>.
  - Optional components in Q (Option<&T>) are fetched when present.
  - Value filters: eq, ne, gt, gte, lt, lte, combined with and/or/in.
  - Key filtering via Guid::key_field().eq("…") or .in_([...]).
- Resources
  - #[persist(resource)] resources are persisted and fetched alongside any query.
- Optimistic concurrency and conflict handling
  - Per-document versioning with conflict detection.
- Batching and parallel commit
  - Configurable batching with a thread pool and concurrent transaction execution.
- Backends
  - ArangoDB and Postgres, selected at build time via features.

Install
Add the core and derive crates, enabling one or both backends.

```toml
[dependencies]
bevy = { version = "0.16" }
bevy_persistence_database_core = { path = "bevy_persistence_database/bevy_persistence_database_core", features = ["arango", "postgres"] }
bevy_persistence_database_derive = { path = "bevy_persistence_database/bevy_persistence_database_derive" }
```

Backends and features
- Enable features on bevy_persistence_database_core:
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
1) Persist-able types.

```rust
use bevy_persistence_database_derive::persist;

#[persist(component)]
#[derive(Clone)]
pub struct Health { pub value: i32 }

#[persist(component)]
pub struct Position { pub x: f32, pub y: f32 }

#[persist(resource)]
#[derive(Clone)]
pub struct GameSettings { pub difficulty: f32, pub map_name: String }
```

2) Add the plugin with a real database connection.

```rust
use bevy::prelude::*;
use bevy_persistence_database_core::{persistence_plugin::PersistencePlugins, ArangoDbConnection};
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
    let db = Arc::new(db) as Arc<dyn bevy_persistence_database_core::DatabaseConnection>;

    App::new()
        .add_plugins(PersistencePlugins(db))
        .run();
}
```

Loading pattern
- Use PersistentQuery as a SystemParam.
- Add value filters via .where(...) (alias of .filter(...)).
- Explicitly trigger the load with .ensure_loaded(); then use pass-throughs on world-only data.

```rust
use bevy::prelude::*;
use bevy_persistence_database_core::PersistentQuery;

fn sys(
    mut pq: PersistentQuery<(&Health, Option<&Position>), (With<Health>, Without<Creature>, Or<(With<PlayerName>,)>)>
) {
    let count = pq
        .where(Health::value().gt(100))
        .ensure_loaded()
        .iter()
        .count();
    info!("Loaded {} entities", count);
}
```

Pass-throughs are world-only
After a load, PersistentQuery derefs to bevy::prelude::Query. Use standard accessors without new DB I/O:
```rust
fn pass_through(mut pq: PersistentQuery<&Health>) {
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

Key filtering
- Single key:
```rust
use bevy_persistence_database_core::{PersistentQuery, Guid};

fn by_key(mut pq: PersistentQuery<&Health>) {
    let _ = pq.where(Guid::key_field().eq("my-key")).ensure_loaded();
}
```
- Multiple keys via IN:
```rust
let _ = pq.where(Guid::key_field().in_(&["k1","k2","k3"])).ensure_loaded();
```

Optional Q semantics and presence via F
- Option<&T> in Q fetches T only when present, without gating presence.
- Presence is driven by type-level filters in F (With<T>, Without<T>, Or<(... )>), which gate backend queries.

Caching and force-refresh
- Default CachePolicy is UseCache. Identical loads coalesce within a frame and reuse cached results across frames.
- Use force_refresh to bypass cache and overwrite world state:
```rust
let _ = pq.force_refresh().ensure_loaded();
```

Resources
- #[persist(resource)] resources are fetched alongside any query call; versions are tracked and updated on commits automatically.

Scheduling notes
- Loads can run in Update or PostUpdate.
- Deferred world mutations from loads are applied before PreCommit; schedule readers after PersistenceSystemSet::PreCommit to observe fresh data in the same frame.