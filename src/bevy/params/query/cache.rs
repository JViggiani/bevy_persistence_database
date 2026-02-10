use bevy::prelude::Resource;
use std::collections::HashSet;
use std::sync::Mutex;

/// Caching policy for persistent queries
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CachePolicy {
    /// Use cache if possible, only query DB when needed
    UseCache,
    /// Always refresh from the database
    ForceRefresh,
}

impl Default for CachePolicy {
    fn default() -> Self {
        CachePolicy::UseCache
    }
}

/// Cache for persistent queries to avoid duplicating database calls
/// Uses interior mutability to allow multiple systems to access it safely
#[derive(Resource, Default)]
pub struct PersistenceQueryCache {
    cache: Mutex<HashSet<u64>>,
}

impl PersistenceQueryCache {
    /// Check if a query hash is in the cache
    pub fn contains(&self, hash: u64) -> bool {
        bevy::log::trace!("PersistenceQueryCache::contains - attempting to lock cache");
        let result = self.cache.lock().unwrap().contains(&hash);
        bevy::log::trace!(
            "PersistenceQueryCache::contains - lock released, result: {}",
            result
        );
        result
    }

    /// Insert a query hash into the cache
    pub fn insert(&self, hash: u64) {
        bevy::log::trace!("PersistenceQueryCache::insert - attempting to lock cache");
        self.cache.lock().unwrap().insert(hash);
        bevy::log::trace!(
            "PersistenceQueryCache::insert - lock released, hash {} inserted",
            hash
        );
    }
}
