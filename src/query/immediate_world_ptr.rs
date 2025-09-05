use bevy::prelude::{Resource, World};

/// Unsafe world pointer published each frame to enable immediate application of DB results.
#[derive(Resource, Clone, Copy)]
pub struct ImmediateWorldPtr(pub(crate) *mut World);

impl ImmediateWorldPtr {
    #[inline]
    pub fn new(ptr: *mut World) -> Self {
        Self(ptr)
    }
    #[inline]
    pub fn set(&mut self, ptr: *mut World) {
        self.0 = ptr;
    }
    #[inline]
    pub fn as_world(&self) -> &World {
        // Main thread only; pointer provided by exclusive systems
        unsafe { &*self.0 }
    }
    #[inline]
    pub fn as_world_mut(&self) -> &mut World {
        // Main thread only; pointer provided by exclusive systems
        unsafe { &mut *self.0 }
    }
}

// Safety: used only on the main thread in exclusive systems; we uphold Bevy's aliasing rules.
unsafe impl Send for ImmediateWorldPtr {}
unsafe impl Sync for ImmediateWorldPtr {}
