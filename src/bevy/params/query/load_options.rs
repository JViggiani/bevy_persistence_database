//! Per-call knobs for one `PersistentQuery` load: value filter, cache policy, pagination,
//! store override and relationship depths. They live in the param's `Local` state and are
//! taken by the next `load`, so each load starts clean and nothing leaks into another system.

use std::any::TypeId;
use std::collections::HashMap;

use crate::core::query::FilterExpression;

use super::cache::CachePolicy;

/// Specifies which relationship types to load and to what depth.
///
/// `per_type` maps individual relationship type IDs to their requested depth.
/// `all` sets a fallback depth applied to every registered relationship type not
/// already covered by `per_type`.
#[derive(Default)]
pub struct RelationshipLoadSpec {
    pub per_type: HashMap<TypeId, usize>,
    pub all: Option<usize>,
}

impl RelationshipLoadSpec {
    pub fn is_empty(&self) -> bool {
        self.per_type.is_empty() && self.all.is_none()
    }

    /// Expand `all` depth into per-type entries using the provided relationship registry
    /// entries (type_id → name pairs), then return the resolved map.
    pub fn resolve(
        self,
        registered: impl IntoIterator<Item = (TypeId, &'static str)>,
    ) -> HashMap<TypeId, usize> {
        let mut result: HashMap<TypeId, usize> =
            self.per_type.into_iter().filter(|(_, d)| *d > 0).collect();
        if let Some(all_depth) = self.all {
            if all_depth > 0 {
                for (type_id, _) in registered {
                    let entry = result.entry(type_id).or_insert(0);
                    *entry = (*entry).max(all_depth);
                }
            }
        }
        result
    }

    fn merge(mut self, other: Self) -> Self {
        for (type_id, depth) in other.per_type {
            let entry = self.per_type.entry(type_id).or_insert(0);
            *entry = (*entry).max(depth);
        }
        self.all = self.all.max(other.all);
        self
    }
}

/// Settings gathered by the `PersistentQuery` builder methods for its next load.
#[derive(Default)]
pub struct LoadOptions {
    pub(crate) filter: Option<FilterExpression>,
    pub(crate) cache_policy: CachePolicy,
    pub(crate) store: Option<String>,
    pub(crate) page_size: Option<usize>,
    pub(crate) relationships: RelationshipLoadSpec,
}

impl LoadOptions {
    /// Combine the options of both sides of a join: filters AND together, a refresh on
    /// either side refreshes the join, and the left side wins store and page size.
    pub(crate) fn merge(self, other: Self) -> Self {
        Self {
            filter: and_filters(self.filter, other.filter),
            cache_policy: match (self.cache_policy, other.cache_policy) {
                (CachePolicy::UseCache, CachePolicy::UseCache) => CachePolicy::UseCache,
                _ => CachePolicy::ForceRefresh,
            },
            store: self.store.or(other.store),
            page_size: self.page_size.or(other.page_size),
            relationships: self.relationships.merge(other.relationships),
        }
    }
}

/// AND two optional filters; either side may be absent.
pub(crate) fn and_filters(
    lhs: Option<FilterExpression>,
    rhs: Option<FilterExpression>,
) -> Option<FilterExpression> {
    match (lhs, rhs) {
        (Some(a), Some(b)) => Some(a.and(b)),
        (a, b) => a.or(b),
    }
}
