use crate::query::cache::CachePolicy;
use crate::query::filter_expression::FilterExpression;
use crate::query::persistence_query_specification::PaginationConfig;

thread_local! {
    static PQ_ADDITIONAL_COMPONENTS: std::cell::RefCell<Vec<&'static str>> = const { std::cell::RefCell::new(Vec::new()) };
    static PQ_FILTER_EXPRESSION: std::cell::RefCell<Option<FilterExpression>> = const { std::cell::RefCell::new(None) };
    static PQ_CACHE_POLICY: std::cell::RefCell<CachePolicy> = const { std::cell::RefCell::new(CachePolicy::UseCache) };
    static PQ_WITHOUT_COMPONENTS: std::cell::RefCell<Vec<&'static str>> = const { std::cell::RefCell::new(Vec::new()) };
    static PAGINATION_SIZE: std::cell::Cell<Option<usize>> = const { std::cell::Cell::new(None) };
    static PQ_STORE: std::cell::RefCell<Option<String>> = const { std::cell::RefCell::new(None) };
}

// Filter helpers
pub fn set_filter(expr: FilterExpression) {
    PQ_FILTER_EXPRESSION.with(|f| *f.borrow_mut() = Some(expr));
}
pub fn take_filter() -> Option<FilterExpression> {
    PQ_FILTER_EXPRESSION.with(|f| f.borrow_mut().take())
}

// Cache policy helpers
pub fn set_cache_policy(policy: CachePolicy) {
    PQ_CACHE_POLICY.with(|p| *p.borrow_mut() = policy);
}
pub fn take_cache_policy() -> CachePolicy {
    PQ_CACHE_POLICY.with(|p| {
        let cur = *p.borrow();
        *p.borrow_mut() = CachePolicy::UseCache;
        cur
    })
}

// Presence component helpers
pub fn push_additional_component(name: &'static str) {
    PQ_ADDITIONAL_COMPONENTS.with(|c| c.borrow_mut().push(name));
}
pub fn drain_additional_components() -> Vec<&'static str> {
    PQ_ADDITIONAL_COMPONENTS.with(|c| c.borrow_mut().drain(..).collect())
}

// Without component helpers
pub fn push_without_component(name: &'static str) {
    PQ_WITHOUT_COMPONENTS.with(|w| w.borrow_mut().push(name));
}
pub fn drain_without_components() -> Vec<&'static str> {
    PQ_WITHOUT_COMPONENTS.with(|w| w.borrow_mut().drain(..).collect())
}

// Store helpers
pub fn set_store(store: impl Into<String>) {
    PQ_STORE.with(|s| *s.borrow_mut() = Some(store.into()));
}

pub fn take_store() -> Option<String> {
    PQ_STORE.with(|s| s.borrow_mut().take())
}

/// Set the pagination size for the next query
pub fn set_pagination_size(size: usize) {
    PAGINATION_SIZE.with(|cell| cell.set(Some(size)));
}

/// Take the pagination configuration for the current query
pub fn take_pagination_config() -> Option<PaginationConfig> {
    PAGINATION_SIZE.with(|cell| {
        let size = cell.replace(None);
        size.map(|page_size| PaginationConfig {
            page_size,
            page_number: 0,
        })
    })
}
