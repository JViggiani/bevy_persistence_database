# bevy_arangodb

A plugin that transparently bridges Bevy's in-memory ECS with ArangoDB persistence.
Use a **Unit-of-Work** (`ArangoSession`) for fast in-memory updates and commit changes
(create/update/delete) back to ArangoDB, and an **AQL builder** (`ArangoQuery`) for loading data.

## Features

• Local cache with `bevy_ecs::World`  
• Dirty tracking + `commit()` for create/update/delete  
• AQL query builder: `.with::<T>()` + `.filter(...)`  
• Serde-powered component/resource serialization  
• Pluggable backend: real (`arangors`) or mockable (`MockDatabaseConnection`)

## Installation

Add to your `Cargo.toml`:

```toml
[dependencies]
bevy_arangodb = "0.1"
arangors       = "0.6"
tokio          = { version = "1", features = ["rt-multi-thread", "macros"] }
serde          = { version = "1.0", features = ["derive"] }
serde_json     = "1.0"
```

## Getting Started

Derive your components for persistence:

```rust
use serde::{Serialize, Deserialize};
use bevy::prelude::Component;

#[derive(Component, Serialize, Deserialize)]
struct Health { value: i32 }
```

Establish a session and load data:

```rust
use bevy_arangodb::{ArangoSession, ArangoQuery, ArangoDbConnection};
use std::sync::Arc;

async fn run() {
    let db = ArangoDbConnection::connect("http://127.0.0.1:8529", "root", "password", "mydb")
        .await
        .unwrap();
    let mut session = ArangoSession::new(db); // uses real backend

    // Load all entities with Health and Position
    let entities = ArangoQuery::new(session.db.clone())
        .with::<Health>()
        .with::<Position>()
        .filter("doc.value > 10")
        .fetch_into(&mut session);

    // Operate in-memory via `session.local_world`...
    // Then persist changes:
    session.commit();
}
```

## Testing

A mock backend lets you mock persistence logic:

```rust
let mut mock_db = MockDatabaseConnection::new();
mock_db.expect_create_document()
    .withf(|key, data| key=="0" && data.get("Health").is_some())
    .returning(|_, _| Box::pin(async { Ok(()) }));

let mut session = ArangoSession::new_mocked(Arc::new(mock_db));
// spawn & mark_dirty...
session.commit();
```

## Troubleshooting

### Missing `Serialize`/`Deserialize` impl

If you see a compiler error like:

```
error[E0277]: the trait bound `Position: serde::ser::Serialize` is not satisfied
 --> src/main.rs:10:6
  |
10 |     Position,
  |      ^^^^^^^ the trait `serde::ser::Serialize` is not implemented for `Position`
  |
  = note: add `#[derive(Serialize)]` or manually implement `serde::ser::Serialize` for `Position`
```

Ensure all nested components of your Bevy components implement `Serialize`/`Deserialize`.

## Best Practices

• All persisted components and resources **must** derive  
  `serde::Serialize` + `serde::Deserialize`.  
  Otherwise `commit()` will return a serialization error.

• Avoid storing non-Serde types (e.g. raw pointers) in your components.  
  Serialization failures at runtime will cause `commit()` to return an `ArangoError`.

• Prefer simple, flat data structures (`i32`, `String`, simple `struct`s) for best performance.  
  Nested `Vec<T>` and `HashMap<K,V>` are supported but can produce large JSON payloads.

• Handle `commit()` errors gracefully:
  ```rust
  if let Err(e) = session.commit() {
    eprintln!("Failed to persist data: {}", e);
  }
  ```
