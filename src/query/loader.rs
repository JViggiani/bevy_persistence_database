use std::hash::{Hash, Hasher};
use bevy::prelude::{Entity, Mut, World};
use bevy::ecs::query::{QueryData, QueryFilter};
use crate::{Guid, PersistenceSession, BEVY_PERSISTENCE_VERSION_FIELD};
use crate::versioning::version_manager::VersionKey;
use crate::query::filter_expression::FilterExpression;
use crate::query::persistence_query_specification::{PersistenceQuerySpecification, PaginationConfig};
use crate::query::cache::CachePolicy;
use crate::query::persistence_query_system_param::PersistentQuery;
use crate::query::tls_config::{take_pagination_config};
use rayon::prelude::*;

impl<'w, 's, Q: QueryData<ReadOnly = Q> + 'static, F: QueryFilter + 'static>
    PersistentQuery<'w, 's, Q, F>
{
    #[inline]
    pub(crate) fn apply_one_document(
        world: &mut World,
        session: &mut PersistenceSession,
        doc: &serde_json::Value,
        comps: &[&'static str],
        allow_overwrite: bool,
        key_field: &str,
    ) {
        let Some(key) = doc.get(key_field).and_then(|v| v.as_str()).map(|s| s.to_string()) else {
            bevy::log::trace!("apply_one_document: skipping doc missing key '{}'", key_field);
            return;
        };
        let version = doc
            .get(BEVY_PERSISTENCE_VERSION_FIELD)
            .and_then(|v| v.as_u64())
            .unwrap_or(1);

        // Try to find an existing entity by Guid first.
        let existing_entity = world
            .query::<(Entity, &Guid)>()
            .iter(world)
            .find(|(_, g)| g.id() == key)
            .map(|(e, _)| e);

        // Resolve or spawn the entity for this key
        let (entity, existed) = if let Some(existing) = existing_entity {
            if !session.entity_keys.contains_key(&existing) {
                session.entity_keys.insert(existing, key.clone());
            }
            (existing, true)
        } else if let Some((candidate, _)) = session
            .entity_keys
            .iter()
            .find(|(_, k)| **k == key)
            .map(|(e, k)| (*e, k.clone()))
        {
            if world.get_entity(candidate).is_ok() {
                (candidate, true)
            } else {
                let e = world.spawn(Guid::new(key.clone())).id();
                session.entity_keys.insert(e, key.clone());
                (e, false)
            }
        } else {
            let e = world.spawn(Guid::new(key.clone())).id();
            session.entity_keys.insert(e, key.clone());
            (e, false)
        };

        if existed && !allow_overwrite {
            bevy::log::trace!("apply_one_document: skip overwrite entity={:?} key={}", entity, key);
            return;
        }

        // Cache/refresh version
        session
            .version_manager
            .set_version(VersionKey::Entity(key.clone()), version);

        // Insert components - optimized for common case
        if !comps.is_empty() {
            // Process most common components first with direct lookup
            let mut component_values: Vec<(&str, serde_json::Value)> = Vec::with_capacity(comps.len());
            
            for &comp_name in comps {
                if let Some(val) = doc.get(comp_name) {
                    component_values.push((comp_name, val.clone()));
                }
            }
            
            // Sort alphabetically instead of by frequency since component_presence 
            // isn't a frequency counter but a different type
            component_values.sort_by_key(|(name, _)| *name);
            
            for (comp_name, val) in component_values {
                if let Some(deser) = session.component_deserializers.get(comp_name) {
                    if let Err(e) = deser(world, entity, val) {
                        bevy::log::error!("Failed to deserialize component {}: {}", comp_name, e);
                    }
                    
                    // No need to update component_presence counter
                }
            }
        } else {
            for (registered_name, deser) in session.component_deserializers.iter() {
                if let Some(val) = doc.get(registered_name) {
                    if let Err(e) = deser(world, entity, val.clone()) {
                        bevy::log::error!("Failed to deserialize component {}: {}", registered_name, e);
                    }
                }
            }
        }
    }

    pub(crate) fn process_documents(
        &mut self,
        documents: Vec<serde_json::Value>,
        comp_names: &[&'static str],
        allow_overwrite: bool,
    ) {
        let key_field = self.db.0.document_key_field();
        let explicit_components = comp_names.to_vec();
        bevy::log::trace!(
            "PQ::process_documents: deferring {} docs; comps={:?}; allow_overwrite={}",
            documents.len(),
            explicit_components,
            allow_overwrite
        );

        // For large document sets, use parallel processing with chunks
        if documents.len() > 1000 {
            // Process documents in parallel chunks
            const CHUNK_SIZE: usize = 250;
            let chunks: Vec<Vec<serde_json::Value>> = documents
                .chunks(CHUNK_SIZE)
                .map(|chunk| chunk.to_vec())
                .collect();
                
            chunks.into_par_iter().for_each(|chunk| {
                // Create a single operation that processes a chunk of documents
                let docs_clone = chunk.clone();
                let comps = explicit_components.clone();
                let allow = allow_overwrite;
                let key_field = key_field.to_string();
                
                self.ops.push(Box::new(move |world: &mut World| {
                    bevy::log::trace!("PQ::process_documents/op: applying deferred chunk of {} documents", docs_clone.len());
                    world.resource_scope(|world, mut session: Mut<PersistenceSession>| {
                        for doc in &docs_clone {
                            Self::apply_one_document(world, &mut session, doc, &comps, allow, &key_field);
                        }
                    });
                }));
            });
        } else {
            // Original processing for smaller sets
            for doc in documents {
                let doc_clone = doc.clone();
                let comps = explicit_components.clone();
                let allow = allow_overwrite;
                let key_field = key_field.to_string();

                self.ops.push(Box::new(move |world: &mut World| {
                    bevy::log::trace!("PQ::process_documents/op: applying deferred document");
                    world.resource_scope(|world, mut session: Mut<PersistenceSession>| {
                        Self::apply_one_document(world, &mut session, &doc_clone, &comps, allow, &key_field);
                    });
                }));
            }
        }
    }

    pub(crate) fn execute_combined_load(
        &mut self,
        cache_policy: CachePolicy,
        presence_with: Vec<&'static str>,
        presence_without: Vec<&'static str>,
        fetch_only: Vec<&'static str>,
        value_filters: Option<FilterExpression>,
        hash_salts: &[&'static str],
        force_full_docs: bool,
    ) {
        // Compute cache hash
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        std::any::type_name::<Q>().hash(&mut hasher);
        for &salt in hash_salts {
            salt.hash(&mut hasher);
        }
        for &name in &presence_with {
            name.hash(&mut hasher);
        }
        for &name in &presence_without {
            name.hash(&mut hasher);
        }
        for &name in &fetch_only {
            name.hash(&mut hasher);
        }
        if let Some(expr) = &value_filters {
            format!("{:?}", expr).hash(&mut hasher);
        }
        let query_hash = hasher.finish();

        bevy::log::debug!(
            "PQ::execute_combined_load enter: type={} hash={:#x} cache_policy={:?} salts={:?}",
            std::any::type_name::<Q>(),
            query_hash,
            cache_policy,
            hash_salts
        );

        let should_query_db = match cache_policy {
            CachePolicy::ForceRefresh => true,
            CachePolicy::UseCache => !self.cache.contains(query_hash),
        };
        
        bevy::log::debug!(
            "PQ::execute_combined_load: should_query_db={} presence_with={:?} presence_without={:?} fetch_only={:?} expr={:?}",
            should_query_db, presence_with, presence_without, fetch_only, value_filters
        );

        if should_query_db {
            let spec = PersistenceQuerySpecification {
                presence_with: presence_with.clone(),
                presence_without: presence_without.clone(),
                fetch_only: fetch_only.clone(),
                value_filters: value_filters.clone(),
                return_full_docs: force_full_docs || (presence_with.is_empty() && presence_without.is_empty()),
                pagination: take_pagination_config(),
            };
            
            // Check if pagination is active
            if let Some(page_size) = spec.pagination.as_ref().map(|p| p.page_size) {
                // Determine allow_overwrite based on cache policy or presence filters
                let allow_overwrite = matches!(cache_policy, CachePolicy::ForceRefresh) || !presence_with.is_empty();
                self.process_paginated_load(&spec, page_size, allow_overwrite);
            } else {
                // Original single-load code
                match self.runtime.block_on(self.db.0.execute_documents(&spec)) {
                    Ok(documents) => {
                        bevy::log::debug!(
                            "PQ::execute_combined_load: backend returned {} documents; immediate_world_ptr={}",
                            documents.len(),
                            self.world_ptr.is_some()
                        );
                        let allow_overwrite =
                            matches!(cache_policy, CachePolicy::ForceRefresh) || !presence_with.is_empty();

                        let comps_to_deser: Vec<&'static str> = if presence_with.is_empty() && presence_without.is_empty() {
                            Vec::new()
                        } else {
                            fetch_only.clone()
                        };

                        if let Some(ptr_res) = &self.world_ptr {
                            let world: &mut World = ptr_res.as_world_mut();

                            let key_field = self.db.0.document_key_field();
                            for doc in documents {
                                world.resource_scope(|world, mut session: Mut<PersistenceSession>| {
                                    Self::apply_one_document(world, &mut session, &doc, &comps_to_deser, allow_overwrite, key_field);
                                });
                            }

                            bevy::log::trace!("PQ::immediate_apply: world.flush()");
                            world.flush();

                            // Note: inner query (self.query) has a stale view of the world
                            // Pass-through methods like get_many() use fresh world queries instead

                            // Compare what the inner Query vs a fresh QueryState sees after immediate apply.
                            let inner_cnt = self.query.iter().count();
                            bevy::log::trace!("PQ::immediate_apply: inner_query_iter_count={}", inner_cnt);

                            // Warm-up current archetypes count for diagnostics via fresh QueryState
                            let lhs_cnt = {
                                let mut qs: bevy::ecs::query::QueryState<(Entity, Q), F> =
                                    bevy::ecs::query::QueryState::new(world);
                                qs.iter(&*world).count()
                            };
                            bevy::log::trace!("PQ::immediate_apply: fresh_qstate_iter_count={}", lhs_cnt);

                            // Fetch resources immediately
                            world.resource_scope(|world, mut session: Mut<PersistenceSession>| {
                                let rt = world
                                    .resource::<crate::plugins::persistence_plugin::TokioRuntime>()
                                    .0
                                    .clone();
                                let db = self.db.0.clone();
                                bevy::log::trace!("PQ::immediate_apply: fetching resources");
                                rt.block_on(session.fetch_and_insert_resources(&*db, world)).ok();
                            });
                        } else {
                            bevy::log::trace!("PQ::execute_combined_load: deferring {} docs", documents.len());
                            self.process_documents(documents, &comps_to_deser, allow_overwrite);

                            // Also fetch resources alongside any query (deferred)
                            let db = self.db.0.clone();
                            self.ops.push(Box::new(move |world: &mut World| {
                                world.resource_scope(|world, mut session: Mut<PersistenceSession>| {
                                    let rt = world
                                        .resource::<crate::plugins::persistence_plugin::TokioRuntime>()
                                        .0
                                        .clone();
                                    bevy::log::trace!("PQ::execute_combined_load: fetching resources");
                                    // Use db directly rather than dereferencing it
                                    rt.block_on(session.fetch_and_insert_resources(&*db, world)).ok();
                                });
                            }));
                        }
                    }
                    Err(e) => {
                        bevy::log::error!("Error fetching documents: {}", e);
                        return;
                    }
                }
            }
            
            // Cache the query hash if successful
            if !matches!(cache_policy, CachePolicy::ForceRefresh) {
                bevy::log::trace!("PQ::execute_combined_load: caching hash {:#x}", query_hash);
                self.cache.insert(query_hash);
            }
        } else {
            bevy::log::trace!("Skipping DB query - using cached results for hash={:#x}", query_hash);
        }
    }
    
    fn process_paginated_load(
        &mut self,
        spec: &PersistenceQuerySpecification,
        page_size: usize,
        allow_overwrite: bool,
    ) {
        bevy::log::debug!("Processing paginated load with page_size={}", page_size);
        
        // First get total count
        let total = match self.runtime.block_on(self.db.0.count_documents(spec)) {
            Ok(count) => count,
            Err(e) => {
                bevy::log::error!("Error counting documents: {}", e);
                return;
            }
        };
        
        let pages = (total + page_size - 1) / page_size; // Ceiling division
        bevy::log::debug!("Paginated load: total={} pages={}", total, pages);
        
        // Process each page
        let mut processed = 0;
        for page in 0..pages {
            let mut page_spec = spec.clone();
            page_spec.pagination = Some(PaginationConfig {
                page_size,
                page_number: page,
            });
            
            match self.runtime.block_on(self.db.0.execute_documents(&page_spec)) {
                Ok(documents) => {
                    processed += documents.len();
                    bevy::log::debug!(
                        "Loaded page {}/{} with {} documents (total processed: {}/{})",
                        page + 1, pages, documents.len(), processed, total
                    );
                    
                    let comps_to_deser = if spec.presence_with.is_empty() && spec.presence_without.is_empty() {
                        Vec::new()
                    } else {
                        spec.fetch_only.clone()
                    };
                    
                    self.process_documents(documents, &comps_to_deser, allow_overwrite);
                }
                Err(e) => {
                    bevy::log::error!("Error fetching documents page {}: {}", page, e);
                    break;
                }
            }
        }
    }
}