use crate::query::filter_expression::FilterExpression;

/// Pagination configuration for database queries.
#[derive(Debug, Clone)]
pub struct PaginationConfig {
    pub page_size: usize,
    pub page_number: usize,
}

/// Specification for a database fetch, including presence and value filters.
#[derive(Debug, Clone, Default)]
pub struct PersistenceQuerySpecification {
    pub presence_with: Vec<&'static str>,
    pub presence_without: Vec<&'static str>,
    pub fetch_only: Vec<&'static str>,
    pub value_filters: Option<FilterExpression>,
    pub return_full_docs: bool,
    pub pagination: Option<PaginationConfig>,
}