//! # Bevy ArangoDB
//!
//! A plugin for the Bevy game engine to persist components and resources to ArangoDB.
//!
//! ## Usage
//!
//! Add `bevy_arangodb` to your `Cargo.toml`:
//!
//! ```toml
//! [dependencies]
//! bevy_arangodb = { git = "..." } # Or from crates.io when published
//! ```
//!
//! Then, add the `ArangoPlugin` to your Bevy `App`.

/// Publicly re-export all items from the core library.
pub use bevy_arangodb_core::*;

/// Publicly re-export the `Persist` derive macro.
pub use bevy_arangodb_derive::Persist;
