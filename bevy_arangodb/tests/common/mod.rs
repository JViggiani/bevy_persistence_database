// Declare the sub-modules
pub mod components;
pub mod resources;
pub mod setup;

// Re-export all items from the sub-modules to make them easily accessible
pub use components::*;
pub use resources::*;
pub use setup::*;
