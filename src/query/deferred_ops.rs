use bevy::prelude::{Resource, World};
use std::sync::Mutex;

/// Thread-safe queue of world mutations to be applied later in the frame.
#[derive(Resource, Default)]
pub(crate) struct DeferredWorldOperations {
    queue: Mutex<Vec<Box<dyn FnOnce(&mut World) + Send>>>,
}

impl DeferredWorldOperations {
    pub fn push(&self, op: Box<dyn FnOnce(&mut World) + Send>) {
        self.queue.lock().unwrap().push(op);
    }

    /// Drain and return all pending world operations.
    pub fn drain(&self) -> Vec<Box<dyn FnOnce(&mut World) + Send>> {
        let mut guard = self.queue.lock().unwrap();
        let mut out = Vec::new();
        std::mem::swap(&mut *guard, &mut out);
        out
    }
}
