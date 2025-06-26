use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::marker::PhantomData;
use std::fmt;

#[derive(Debug)]
pub struct ArangoError(String);

impl fmt::Display for ArangoError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ArangoDB error: {}", self.0)
    }
}

impl std::error::Error for ArangoError {}

// Mock ArangoDB client for this proof of concept
#[derive(Clone)]
pub struct ArangoClient {
    // In reality, this would be an actual ArangoDB client
    mock_data: HashMap<String, String>,
}

impl ArangoClient {
    pub fn new(fail_connection: bool) -> Result<Self, ArangoError> {
        if fail_connection {
            return Err(ArangoError("Failed to connect to mock DB".to_string()));
        }

        let mut mock_data = HashMap::new();
        
        // Simulate some entities with components
        mock_data.insert("entity_1_Health".to_string(), r#"{"value": 100}"#.to_string());
        mock_data.insert("entity_1_Position".to_string(), r#"{"x": 1.0, "y": 2.0, "z": 3.0}"#.to_string());
        mock_data.insert("entity_2_Health".to_string(), r#"{"value": 75}"#.to_string());
        mock_data.insert("entity_2_Position".to_string(), r#"{"x": 4.0, "y": 5.0, "z": 6.0}"#.to_string());
        mock_data.insert("entity_3_Health".to_string(), r#"{"value": 50}"#.to_string());
        mock_data.insert("entity_3_SpecialPower".to_string(), r#"{"name": "Invisibility"}"#.to_string());
        
        Ok(Self { mock_data })
    }
    
    pub fn query_component<T: Component>(&self, entity_id: &str) -> Option<T> 
    where 
        T: for<'de> Deserialize<'de>
    {
        let key = format!("{}_{}", entity_id, T::component_name());
        self.mock_data.get(&key)
            .and_then(|json| serde_json::from_str(json).ok())
    }
    
    pub fn query_entities_with_components(&self, component_names: &[&str]) -> Vec<String> {
        let mut entity_ids = std::collections::HashSet::new();
        
        for (key, _) in &self.mock_data {
            if let Some(underscore_pos) = key.rfind('_') {
                let entity_id = &key[..underscore_pos];
                let component_name = &key[underscore_pos + 1..];
                
                if component_names.contains(&component_name) {
                    entity_ids.insert(entity_id.to_string());
                }
            }
        }
        
        // Only return entities that have ALL requested components
        entity_ids.into_iter()
            .filter(|entity_id| {
                component_names.iter().all(|component_name| {
                    let key = format!("{}_{}", entity_id, component_name);
                    self.mock_data.contains_key(&key)
                })
            })
            .collect()
    }
}

// Component trait that our components must implement
pub trait Component: Serialize + for<'de> Deserialize<'de> + Sized {
    fn component_name() -> &'static str;
}

// Entity type
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Entity(String);

impl Entity {
    pub fn new(id: &str) -> Self {
        Self(id.to_string())
    }
    
    pub fn id(&self) -> &str {
        &self.0
    }
}

// Custom World that uses ArangoDB
pub struct ArangoWorld {
    client: ArangoClient,
}

impl ArangoWorld {
    pub fn new(fail_connection: bool) -> Result<Self, ArangoError> {
        Ok(Self {
            client: ArangoClient::new(fail_connection)?,
        })
    }
    
    pub fn query<T: ArangoQueryData>(&self) -> ArangoQuery<T> {
        ArangoQuery::new(&self.client)
    }
}

// Trait for query data types
pub trait ArangoQueryData {
    type Item;
    
    fn component_names() -> Vec<&'static str>;
    fn fetch_from_client(client: &ArangoClient, entity: &Entity) -> Option<Self::Item>;
}

// Implementation for single components
impl<T: Component> ArangoQueryData for &T {
    type Item = T;
    
    fn component_names() -> Vec<&'static str> {
        vec![T::component_name()]
    }
    
    fn fetch_from_client(client: &ArangoClient, entity: &Entity) -> Option<Self::Item> {
        client.query_component::<T>(entity.id())
    }
}

// Implementation for tuples of components (up to 2 for this example)
impl<A: Component, B: Component> ArangoQueryData for (&A, &B) {
    type Item = (A, B);
    
    fn component_names() -> Vec<&'static str> {
        vec![A::component_name(), B::component_name()]
    }
    
    fn fetch_from_client(client: &ArangoClient, entity: &Entity) -> Option<Self::Item> {
        let a = client.query_component::<A>(entity.id())?;
        let b = client.query_component::<B>(entity.id())?;
        Some((a, b))
    }
}

// Implementation for Entity queries
impl ArangoQueryData for Entity {
    type Item = Entity;
    
    fn component_names() -> Vec<&'static str> {
        vec![] // Entity doesn't require specific components
    }
    
    fn fetch_from_client(_client: &ArangoClient, entity: &Entity) -> Option<Self::Item> {
        Some(entity.clone())
    }
}

// Implementation for (Entity, Component) tuples
impl<T: Component> ArangoQueryData for (Entity, &T) {
    type Item = (Entity, T);
    
    fn component_names() -> Vec<&'static str> {
        vec![T::component_name()]
    }
    
    fn fetch_from_client(client: &ArangoClient, entity: &Entity) -> Option<Self::Item> {
        let component = client.query_component::<T>(entity.id())?;
        Some((entity.clone(), component))
    }
}

// Custom Query type that mirrors Bevy's Query API
pub struct ArangoQuery<'w, T: ArangoQueryData> {
    client: &'w ArangoClient,
    _phantom: PhantomData<T>,
}

impl<'w, T: ArangoQueryData> ArangoQuery<'w, T> {
    fn new(client: &'w ArangoClient) -> Self {
        Self {
            client,
            _phantom: PhantomData,
        }
    }
    
    // Iterator that yields query results
    pub fn iter(&self) -> ArangoQueryIter<'w, T> {
        let component_names = T::component_names();
        let entities = if component_names.is_empty() {
            // For Entity-only queries, return all known entities
            vec!["entity_1".to_string(), "entity_2".to_string(), "entity_3".to_string()]
        } else {
            self.client.query_entities_with_components(&component_names)
        };
        
        ArangoQueryIter {
            client: self.client,
            entities,
            current_index: 0,
            _phantom: PhantomData,
        }
    }
    
    // Get a specific entity's components
    pub fn get(&self, entity: Entity) -> Option<T::Item> {
        T::fetch_from_client(self.client, &entity)
    }
    
    // Check if query is empty
    pub fn is_empty(&self) -> bool {
        self.iter().next().is_none()
    }
    
    // Get single result (panics if not exactly one)
    pub fn single(&self) -> T::Item {
        let mut iter = self.iter();
        let first = iter.next().expect("Query returned no results");
        if iter.next().is_some() {
            panic!("Query returned multiple results");
        }
        first
    }
}

// Iterator for query results
pub struct ArangoQueryIter<'w, T: ArangoQueryData> {
    client: &'w ArangoClient,
    entities: Vec<String>,
    current_index: usize,
    _phantom: PhantomData<T>,
}

impl<'w, T: ArangoQueryData> Iterator for ArangoQueryIter<'w, T> {
    type Item = T::Item;
    
    fn next(&mut self) -> Option<Self::Item> {
        while self.current_index < self.entities.len() {
            let entity_id = &self.entities[self.current_index];
            self.current_index += 1;
            
            let entity = Entity::new(entity_id);
            if let Some(result) = T::fetch_from_client(self.client, &entity) {
                return Some(result);
            }
        }
        None
    }
}

// Make the query iterable with for loops
impl<'w, 'a, T: ArangoQueryData> IntoIterator for &'a ArangoQuery<'w, T> {
    type Item = T::Item;
    type IntoIter = ArangoQueryIter<'w, T>;
    
    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

// System function type that takes ArangoWorld
pub type ArangoSystem = fn(&ArangoWorld);

// Simple scheduler for running systems
pub struct ArangoApp {
    world: ArangoWorld,
    systems: Vec<ArangoSystem>,
}

impl ArangoApp {
    pub fn new(fail_connection: bool) -> Result<Self, ArangoError> {
        Ok(Self {
            world: ArangoWorld::new(fail_connection)?,
            systems: Vec::new(),
        })
    }
    
    pub fn add_system(mut self, system: ArangoSystem) -> Self {
        self.systems.push(system);
        self
    }
    
    pub fn run_once(&self) {
        for system in &self.systems {
            system(&self.world);
        }
    }
}
