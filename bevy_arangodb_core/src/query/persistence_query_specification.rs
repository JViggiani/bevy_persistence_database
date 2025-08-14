use crate::query::filter_expression::FilterExpression;

/// Backend-agnostic query specification constructed by the SystemParam.
/// Public so DatabaseConnection::build_query can accept it.
#[derive(Clone, Debug, Default)]
pub struct PersistenceQuerySpecification {
    pub presence_with: Vec<&'static str>,
    pub presence_without: Vec<&'static str>,
    pub fetch_only: Vec<&'static str>,
    pub value_filters: Option<FilterExpression>,
    pub return_full_docs: bool,
}