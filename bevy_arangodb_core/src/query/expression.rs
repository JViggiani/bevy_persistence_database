//! Defines a Domain-Specific Language (DSL) for building type-safe database queries.
//!
//! TODO(deprecation): Migrate callers to type-driven filters and backend-specific
//! query builders (see roadmap Step 2b/4). Keep this module until migration is complete.

use serde::Serialize;
use serde_json::Value;
use std::collections::HashMap;
use crate::DatabaseConnection;

/// Represents a part of a database query, forming an expression tree.
#[deprecated(
    since = "0.1.0",
    note = "Query DSL is deprecated; prefer type-driven filters on PersistentQuery and backend query builders."
)]
#[derive(Clone, Debug)]
pub enum Expression {
    /// A literal value, like a number or string.
    Literal(Value),
    /// A reference to a component's field, e.g., `doc.Health.value`.
    Field {
        component_name: &'static str,
        field_name: &'static str,
    },
    /// A binary operation, like `==`, `>`, `AND`.
    BinaryOp {
        op: BinaryOperator,
        lhs: Box<Expression>,
        rhs: Box<Expression>,
    },
    /// A reference to the document's unique key field.
    DocumentKey,
    /// A presence check on a component: present => doc.`T` != null, absent => doc.`T` == null
    Presence {
        component_name: &'static str,
        present: bool,
    },
}

/// Represents the different binary operators available in database queries.
#[derive(Clone, Debug)]
pub enum BinaryOperator {
    Eq, Gt, Gte, Lt, Lte, And, Or, In,
}

impl Expression {
    #[deprecated(since = "0.1.0", note = "Deprecated DSL; prefer type-driven filters")]
    /// Combines this expression with another using the `AND` operator.
    pub fn and(self, other: Expression) -> Expression {
        Expression::BinaryOp {
            op: BinaryOperator::And,
            lhs: Box::new(self),
            rhs: Box::new(other),
        }
    }

    #[deprecated(since = "0.1.0", note = "Deprecated DSL; prefer type-driven filters")]
    /// Combines this expression with another using the `OR` operator.
    pub fn or(self, other: Expression) -> Expression {
        Expression::BinaryOp {
            op: BinaryOperator::Or,
            lhs: Box::new(self),
            rhs: Box::new(other),
        }
    }

    #[deprecated(since = "0.1.0", note = "Deprecated DSL; prefer type-driven filters")]
    /// Creates an equality comparison (`==`).
    pub fn eq<T: Serialize>(self, value: T) -> Expression {
        Expression::BinaryOp {
            op: BinaryOperator::Eq,
            lhs: Box::new(self),
            rhs: Box::new(Expression::Literal(serde_json::to_value(value).unwrap())),
        }
    }

    #[deprecated(since = "0.1.0", note = "Deprecated DSL; prefer type-driven filters")]
    /// Creates a greater-than comparison (`>`).
    pub fn gt<T: Serialize>(self, value: T) -> Expression {
        Expression::BinaryOp {
            op: BinaryOperator::Gt,
            lhs: Box::new(self),
            rhs: Box::new(Expression::Literal(serde_json::to_value(value).unwrap())),
        }
    }

    #[deprecated(since = "0.1.0", note = "Deprecated DSL; prefer type-driven filters")]
    /// Creates a greater-than-or-equal comparison (`>=`).
    pub fn gte<T: Serialize>(self, value: T) -> Expression {
        Expression::BinaryOp {
            op: BinaryOperator::Gte,
            lhs: Box::new(self),
            rhs: Box::new(Expression::Literal(serde_json::to_value(value).unwrap())),
        }
    }

    #[deprecated(since = "0.1.0", note = "Deprecated DSL; prefer type-driven filters")]
    /// Creates a less-than comparison (`<`).
    pub fn lt<T: Serialize>(self, value: T) -> Expression {
        Expression::BinaryOp {
            op: BinaryOperator::Lt,
            lhs: Box::new(self),
            rhs: Box::new(Expression::Literal(serde_json::to_value(value).unwrap())),
        }
    }

    #[deprecated(since = "0.1.0", note = "Deprecated DSL; prefer type-driven filters")]
    /// Creates a less-than-or-equal comparison (`<=`).
    pub fn lte<T: Serialize>(self, value: T) -> Expression {
        Expression::BinaryOp {
            op: BinaryOperator::Lte,
            lhs: Box::new(self),
            rhs: Box::new(Expression::Literal(serde_json::to_value(value).unwrap())),
        }
    }

    #[deprecated(since = "0.1.0", note = "Deprecated DSL; prefer type-driven filters")]
    /// Presence check: doc.`component_name` != null
    pub fn with_component(component_name: &'static str) -> Expression {
        Expression::Presence { component_name, present: true }
    }

    #[deprecated(since = "0.1.0", note = "Deprecated DSL; prefer type-driven filters")]
    /// Absence check: doc.`component_name` == null
    pub fn without_component(component_name: &'static str) -> Expression {
        Expression::Presence { component_name, present: false }
    }
}

/// Translates an `Expression` tree into an AQL string and bind variables.
pub(crate) fn translate_expression(
    expr: &Expression,
    bind_vars: &mut HashMap<String, Value>,
    db: &dyn DatabaseConnection,
) -> String {
    match expr {
        Expression::Literal(val) => {
            let bind_name = format!("bevy_arangodb_bind_{}", bind_vars.len());
            bind_vars.insert(bind_name.clone(), val.clone());
            format!("@{}", bind_name)
        }
        Expression::Field { component_name, field_name } => {
            if field_name.is_empty() {
                format!("doc.`{}`", component_name)
            } else {
                format!("doc.`{}`.`{}`", component_name, field_name)
            }
        }
        Expression::DocumentKey => {
            format!("doc.{}", db.document_key_field())
        }
        Expression::BinaryOp { op, lhs, rhs } => {
            match op {
                BinaryOperator::In => {
                    let lhs_str = translate_expression(lhs, bind_vars, db);
                    let rhs_str = translate_expression(rhs, bind_vars, db);
                    format!("({} IN {})", lhs_str, rhs_str)
                },
                _ => {
                    let op_str = match op {
                        BinaryOperator::Eq => "==",
                        BinaryOperator::Gt => ">",
                        BinaryOperator::Gte => ">=",
                        BinaryOperator::Lt => "<",
                        BinaryOperator::Lte => "<=",
                        BinaryOperator::And => "AND",
                        BinaryOperator::Or => "OR",
                        BinaryOperator::In => unreachable!(), // Handled above
                    };
                    let lhs_str = translate_expression(lhs, bind_vars, db);
                    let rhs_str = translate_expression(rhs, bind_vars, db);
                    format!("({} {} {})", lhs_str, op_str, rhs_str)
                }
            }
        }
        Expression::Presence { component_name, present } => {
            if *present {
                format!("doc.`{}` != null", component_name)
            } else {
                format!("doc.`{}` == null", component_name)
            }
        }
    }
}