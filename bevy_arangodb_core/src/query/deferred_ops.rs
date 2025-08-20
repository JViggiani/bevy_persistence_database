use bevy::prelude::{Resource, World};
use std::sync::Mutex;

/// Thread-safe queue of world mutations to be applied later in the frame.
#[derive(Resource, Default)]
pub(crate) struct DeferredWorldOperations(Mutex<Vec<Box<dyn FnOnce(&mut World) + Send>>>);

impl DeferredWorldOperations {
    pub fn push(&self, op: Box<dyn FnOnce(&mut World) + Send>) {
        self.0.lock().unwrap().push(op);
    }

    /// Drain and return all pending world operations.
    pub fn drain(&self) -> Vec<Box<dyn FnOnce(&mut World) + Send>> {
        let mut guard = self.0.lock().unwrap();
        let mut out = Vec::new();
        std::mem::swap(&mut *guard, &mut out);
        out
    }
}
