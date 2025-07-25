//! A thread-safe, runtime mechanism for component registration.
//!
//! This avoids the pitfalls of link-time static collection by using a `once_cell`
//! global to store registration functions. The `Persist` derive macro pushes
//! functions into this registry, and the `PersistencePlugin` consumes them.

use bevy::app::App;
use once_cell::sync::Lazy;
use std::sync::Mutex;

/// A type alias for a function that can register a component with a Bevy App.
pub type RegistrationFn = fn(&mut App);

/// The global, thread-safe registry for component registration functions.
pub static COMPONENT_REGISTRY: Lazy<Mutex<Vec<RegistrationFn>>> =
    Lazy::new(|| Mutex::new(Vec::new()));
