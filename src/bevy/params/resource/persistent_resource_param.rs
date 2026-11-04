//! Persisted resource accessors. Each reads the resource from the world and, while it is
//! absent, asks once for a load from the database. The load runs when the system's commands
//! apply; the resource is then visible to later systems and to this one on its next run.
//! Mutations are marked dirty by Bevy change detection, like any persisted resource.

use bevy::ecs::system::SystemParam;
use bevy::prelude::{Commands, Local, Mut, Res, ResMut, Resource, World};

use crate::bevy::plugins::persistence_plugin::{PersistencePluginConfig, TokioRuntime};
use crate::core::db::connection::DatabaseConnectionResource;
use crate::core::persist::Persist;
use crate::core::session::PersistenceSession;

/// Builder settings and request state of one system's resource accessor.
#[derive(Default)]
pub struct ResourceRequest {
    store: Option<String>,
    force_refresh: bool,
    requested: bool,
}

impl ResourceRequest {
    /// Queue a load of `T` if it is absent and not yet requested, or if a refresh was asked.
    fn request<T: Resource + Persist>(
        &mut self,
        present: bool,
        commands: &mut Commands,
        config: &PersistencePluginConfig,
    ) {
        let force_refresh = std::mem::take(&mut self.force_refresh);
        let store = self.store.take();
        if !force_refresh && (present || self.requested) {
            return;
        }
        self.requested = true;
        let store = store.unwrap_or_else(|| config.default_store.clone());
        let name = T::name();
        commands.queue(move |world: &mut World| load_resource(world, &store, name));
    }
}

fn load_resource(world: &mut World, store: &str, name: &'static str) {
    let db = world
        .resource::<DatabaseConnectionResource>()
        .connection
        .clone();
    let runtime = world.resource::<TokioRuntime>().runtime.clone();
    world.resource_scope(|world, mut session: Mut<PersistenceSession>| {
        if let Err(err) = runtime.block_on(session.materialize_resource(&*db, store, world, name))
        {
            bevy::log::error!(%err, store, "failed to load persisted resource {}", name);
        }
    });
}

/// Read-only persisted resource accessor. Requests a load while the resource is absent.
#[derive(SystemParam)]
pub struct PersistentRes<'w, 's, T: Resource + Persist> {
    resource: Option<Res<'w, T>>,
    commands: Commands<'w, 's>,
    config: Res<'w, PersistencePluginConfig>,
    request: Local<'s, ResourceRequest>,
}

/// Mutable persisted resource accessor. Requests a load while the resource is absent.
#[derive(SystemParam)]
pub struct PersistentResMut<'w, 's, T: Resource + Persist> {
    resource: Option<ResMut<'w, T>>,
    commands: Commands<'w, 's>,
    config: Res<'w, PersistencePluginConfig>,
    request: Local<'s, ResourceRequest>,
}

impl<'w, 's, T: Resource + Persist> PersistentRes<'w, 's, T> {
    /// Override the store for the next load of this resource.
    pub fn store(mut self, store: impl Into<String>) -> Self {
        self.request.store = Some(store.into());
        self
    }

    /// Reload from persistence on the next `get`, even if the resource exists in the world.
    pub fn force_refresh(mut self) -> Self {
        self.request.force_refresh = true;
        self
    }

    /// The resource as the world holds it now. While it is absent, the first call asks
    /// for a load; `None` until that load has applied.
    pub fn get(&mut self) -> Option<&T> {
        self.request
            .request::<T>(self.resource.is_some(), &mut self.commands, &self.config);
        self.resource.as_deref()
    }
}

impl<'w, 's, T: Resource + Persist> PersistentResMut<'w, 's, T> {
    /// Override the store for the next load of this resource.
    pub fn store(mut self, store: impl Into<String>) -> Self {
        self.request.store = Some(store.into());
        self
    }

    /// Reload from persistence on the next `get_mut`, even if the resource exists in the world.
    pub fn force_refresh(mut self) -> Self {
        self.request.force_refresh = true;
        self
    }

    /// The resource as the world holds it now, mutably. While it is absent, the first call
    /// asks for a load; `None` until that load has applied.
    pub fn get_mut(&mut self) -> Option<Mut<'_, T>> {
        self.request
            .request::<T>(self.resource.is_some(), &mut self.commands, &self.config);
        self.resource.as_mut().map(|resource| resource.reborrow())
    }
}
