use bevy::prelude::{App, Resource, TaskPoolPlugin};
use once_cell::sync::Lazy;
use std::sync::Arc;
use tokio::runtime::Runtime;

pub(crate) fn ensure_task_pools(app: &mut App) {
    if !app.is_plugin_added::<TaskPoolPlugin>() {
        app.add_plugins(TaskPoolPlugin::default());
    }
}

static TOKIO_RUNTIME: Lazy<Arc<Runtime>> = Lazy::new(|| {
    Arc::new(
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap(),
    )
});

#[derive(Resource)]
pub struct TokioRuntime {
    pub runtime: Arc<Runtime>,
}

impl TokioRuntime {
    pub fn block_on<F: std::future::Future>(&self, fut: F) -> F::Output {
        self.runtime.block_on(fut)
    }

    pub(crate) fn shared() -> Self {
        Self {
            runtime: TOKIO_RUNTIME.clone(),
        }
    }
}
