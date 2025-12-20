use crate::db::connection::DocumentKind;
use crate::query::filter_expression::FilterExpression;

/// Pagination configuration for database queries.
#[derive(Debug, Clone)]
pub struct PaginationConfig {
    pub page_size: usize,
    pub page_number: usize,
}

/// Specification for a database fetch, including presence and value filters.
#[derive(Debug, Clone)]
pub struct PersistenceQuerySpecification {
    pub store: String,
    pub kind: DocumentKind,
    pub presence_with: Vec<&'static str>,
    pub presence_without: Vec<&'static str>,
    pub fetch_only: Vec<&'static str>,
    pub value_filters: Option<FilterExpression>,
    pub return_full_docs: bool,
    pub pagination: Option<PaginationConfig>,
}

impl Default for PersistenceQuerySpecification {
    fn default() -> Self {
        Self {
            store: String::new(),
            kind: DocumentKind::Entity,
            presence_with: Vec::new(),
            presence_without: Vec::new(),
            fetch_only: Vec::new(),
            value_filters: None,
            return_full_docs: false,
            pagination: None,
        }
    }
}