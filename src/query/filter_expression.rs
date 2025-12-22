use serde::Serialize;
use serde_json::Value;

/// Backend-agnostic filter expression tree used to build database queries.
#[derive(Clone, Debug)]
pub enum FilterExpression {
    Literal(Value),
    Field {
        component_name: &'static str,
        field_name: &'static str,
    },
    BinaryOperator {
        op: BinaryOperator,
        lhs: Box<FilterExpression>,
        rhs: Box<FilterExpression>,
    },
    DocumentKey,
}

#[derive(Clone, Debug)]
pub enum BinaryOperator {
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

impl FilterExpression {
    pub fn field(component_name: &'static str, field_name: &'static str) -> Self {
        FilterExpression::Field {
            component_name,
            field_name,
        }
    }

    pub fn and(self, other: FilterExpression) -> FilterExpression {
        FilterExpression::BinaryOperator {
            op: BinaryOperator::And,
            lhs: Box::new(self),
            rhs: Box::new(other),
        }
    }

    pub fn or(self, other: FilterExpression) -> FilterExpression {
        FilterExpression::BinaryOperator {
            op: BinaryOperator::Or,
            lhs: Box::new(self),
            rhs: Box::new(other),
        }
    }

    pub fn eq<T: Serialize>(self, value: T) -> FilterExpression {
        FilterExpression::BinaryOperator {
            op: BinaryOperator::Eq,
            lhs: Box::new(self),
            rhs: Box::new(FilterExpression::Literal(
                serde_json::to_value(value).unwrap(),
            )),
        }
    }

    pub fn gt<T: Serialize>(self, value: T) -> FilterExpression {
        FilterExpression::BinaryOperator {
            op: BinaryOperator::Gt,
            lhs: Box::new(self),
            rhs: Box::new(FilterExpression::Literal(
                serde_json::to_value(value).unwrap(),
            )),
        }
    }

    pub fn gte<T: Serialize>(self, value: T) -> FilterExpression {
        FilterExpression::BinaryOperator {
            op: BinaryOperator::Gte,
            lhs: Box::new(self),
            rhs: Box::new(FilterExpression::Literal(
                serde_json::to_value(value).unwrap(),
            )),
        }
    }

    pub fn lt<T: Serialize>(self, value: T) -> FilterExpression {
        FilterExpression::BinaryOperator {
            op: BinaryOperator::Lt,
            lhs: Box::new(self),
            rhs: Box::new(FilterExpression::Literal(
                serde_json::to_value(value).unwrap(),
            )),
        }
    }

    pub fn lte<T: Serialize>(self, value: T) -> FilterExpression {
        FilterExpression::BinaryOperator {
            op: BinaryOperator::Lte,
            lhs: Box::new(self),
            rhs: Box::new(FilterExpression::Literal(
                serde_json::to_value(value).unwrap(),
            )),
        }
    }

    pub fn in_<T: Serialize>(self, value: T) -> FilterExpression {
        FilterExpression::BinaryOperator {
            op: BinaryOperator::In,
            lhs: Box::new(self),
            rhs: Box::new(FilterExpression::Literal(
                serde_json::to_value(value).unwrap(),
            )),
        }
    }
}
