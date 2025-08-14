use serde::Serialize;
use serde_json::Value;

/// Internal, typed value-expression tree for backend-agnostic filtering.
/// Public for macro-generated accessors and tests.
#[derive(Clone, Debug)]
pub enum ValueExpr {
    Literal(Value),
    Field { component_name: &'static str, field_name: &'static str },
    BinaryOp { op: BinaryOp, lhs: Box<ValueExpr>, rhs: Box<ValueExpr> },
    DocumentKey,
}

#[derive(Clone, Debug)]
pub enum BinaryOp {
    Eq,
    Ne,
    Gt,
    Gte,
    Lt,
    Lte,
    And,
    Or,
    In,
}

impl ValueExpr {
    pub fn field(component_name: &'static str, field_name: &'static str) -> Self {
        ValueExpr::Field { component_name, field_name }
    }

    pub fn and(self, other: ValueExpr) -> ValueExpr {
        ValueExpr::BinaryOp { op: BinaryOp::And, lhs: Box::new(self), rhs: Box::new(other) }
    }
    pub fn or(self, other: ValueExpr) -> ValueExpr {
        ValueExpr::BinaryOp { op: BinaryOp::Or, lhs: Box::new(self), rhs: Box::new(other) }
    }

    pub fn eq<T: Serialize>(self, value: T) -> ValueExpr {
        ValueExpr::BinaryOp {
            op: BinaryOp::Eq,
            lhs: Box::new(self),
            rhs: Box::new(ValueExpr::Literal(serde_json::to_value(value).unwrap())),
        }
    }
    pub fn gt<T: Serialize>(self, value: T) -> ValueExpr {
        ValueExpr::BinaryOp {
            op: BinaryOp::Gt,
            lhs: Box::new(self),
            rhs: Box::new(ValueExpr::Literal(serde_json::to_value(value).unwrap())),
        }
    }
    pub fn gte<T: Serialize>(self, value: T) -> ValueExpr {
        ValueExpr::BinaryOp {
            op: BinaryOp::Gte,
            lhs: Box::new(self),
            rhs: Box::new(ValueExpr::Literal(serde_json::to_value(value).unwrap())),
        }
    }
    pub fn lt<T: Serialize>(self, value: T) -> ValueExpr {
        ValueExpr::BinaryOp {
            op: BinaryOp::Lt,
            lhs: Box::new(self),
            rhs: Box::new(ValueExpr::Literal(serde_json::to_value(value).unwrap())),
        }
    }
    pub fn lte<T: Serialize>(self, value: T) -> ValueExpr {
        ValueExpr::BinaryOp {
            op: BinaryOp::Lte,
            lhs: Box::new(self),
            rhs: Box::new(ValueExpr::Literal(serde_json::to_value(value).unwrap())),
        }
    }
    pub fn in_<T: Serialize>(self, value: T) -> ValueExpr {
        ValueExpr::BinaryOp {
            op: BinaryOp::In,
            lhs: Box::new(self),
            rhs: Box::new(ValueExpr::Literal(serde_json::to_value(value).unwrap())),
        }
    }
}

/// Backend-agnostic query specification constructed by the SystemParam.
/// Public so DatabaseConnection::build_query can accept it.
#[derive(Clone, Debug, Default)]
pub struct QuerySpec {
    pub presence_with: Vec<&'static str>,
    pub presence_without: Vec<&'static str>,
    pub fetch_only: Vec<&'static str>,
    pub value_filters: Option<ValueExpr>,
    pub return_full_docs: bool,
}
