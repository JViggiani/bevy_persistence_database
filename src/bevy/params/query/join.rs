use std::any::type_name;

use bevy::ecs::query::{QueryData, QueryFilter};
use bevy::ecs::system::QueryLens;
use bevy::prelude::World;

use super::load_options::and_filters;
use super::loader::{ComponentSelection, EntityLoad};
use super::persistence_query_system_param::PersistentQuery;
use super::presence_spec::{FilterSupported, ToPresenceSpec, collect_presence_components};
use super::query_data_to_components::QueryDataToComponents;

impl<'w, 's, Q, F> PersistentQuery<'w, 's, Q, F>
where
    Q: QueryData<ReadOnly = Q> + QueryDataToComponents,
    F: QueryFilter + ToPresenceSpec + FilterSupported,
{
    /// Join two PersistentQuery params:
    /// - Requests one database load of the intersection of both specs (every joined
    ///   component present, both presence filters, both sides' builder settings).
    /// - Returns a Bevy `QueryLens` over the world-only intersection of both queries.
    ///
    /// Like [`load`](Self::load), the rows arrive when this system's commands apply.
    pub fn join_filtered<'a, 'w2, 's2, Q2, F2, NewD, NewF>(
        &'a mut self,
        other: &'a mut PersistentQuery<'w2, 's2, Q2, F2>,
    ) -> QueryLens<'a, NewD, NewF>
    where
        Q2: QueryData<ReadOnly = Q2> + QueryDataToComponents,
        F2: QueryFilter + ToPresenceSpec + FilterSupported,
        NewD: QueryData,
        NewF: QueryFilter,
    {
        let options = std::mem::take(&mut *self.options)
            .merge(std::mem::take(&mut *other.options));

        let mut fetch = Vec::new();
        Q::push_names(&mut fetch);
        Q2::push_names(&mut fetch);

        let lhs = <F as ToPresenceSpec>::to_presence_spec();
        let rhs = <F2 as ToPresenceSpec>::to_presence_spec();
        for expr in [lhs.expr(), rhs.expr()].into_iter().flatten() {
            collect_presence_components(expr, &mut fetch);
        }

        // A true intersection: every joined component must be present.
        let mut with: Vec<&'static str> = fetch.clone();
        with.extend(lhs.withs().iter().copied());
        with.extend(rhs.withs().iter().copied());
        let mut without: Vec<&'static str> = lhs.withouts().to_vec();
        without.extend(rhs.withouts().iter().copied());

        let load = EntityLoad::new(
            type_name::<Q>(),
            &[type_name::<Q2>()],
            ComponentSelection {
                with,
                without,
                fetch,
            },
            and_filters(lhs.expr().cloned(), rhs.expr().cloned()),
            true,
            options,
            &self.config.default_store,
        );
        self.commands.queue(move |world: &mut World| load.run(world));

        self.query.join_filtered(&mut other.query)
    }
}
