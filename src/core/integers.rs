//! Integers every store keeps exactly.
//!
//! A value is written as JSON. Postgres `jsonb` keeps any JSON integer, but
//! ArangoDB stores one outside the signed 64-bit range as a double and hands
//! it back rounded: `u64::MAX` reads back as `1.8446744073709552e19`, which no
//! longer decodes as a `u64` (measured on ArangoDB 3.12.5, 2026-11-04; signed
//! 64-bit integers, `i64::MIN` and `i64::MAX` included, round-trip exactly).
//!
//! So as a value is written, every JSON integer outside the signed 64-bit
//! range is replaced by a tagged decimal string,
//! `{"bevy_persistence_integer": "18446744073709551615"}`
//! ([`BEVY_PERSISTENCE_DATABASE_INTEGER_FIELD`]), and as it is read every tag
//! becomes the exact number again. Integers inside the range stay plain
//! numbers, so queries on them compare numerically as before; a tagged
//! integer matches a query only by equality (`eq`, `in_`), never by order.

use std::fmt;

use serde_json::{Map, Number, Value};

use crate::core::db::connection::BEVY_PERSISTENCE_DATABASE_INTEGER_FIELD;

/// A tagged integer whose digits are no `u64`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IntegerTagError {
    /// The tag's value as stored.
    pub tag: Value,
}

impl fmt::Display for IntegerTagError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "malformed `{BEVY_PERSISTENCE_DATABASE_INTEGER_FIELD}` tag {}", self.tag)
    }
}

impl std::error::Error for IntegerTagError {}

/// Whether every store keeps `number` exactly: a float, or an integer in the
/// signed 64-bit range.
fn every_store_keeps(number: &Number) -> bool {
    !number.is_u64() || number.as_i64().is_some()
}

/// `value` as it is stored: each integer outside the signed 64-bit range
/// tagged, at any depth.
pub fn tag_integers(value: Value) -> Value {
    match value {
        Value::Number(number) if !every_store_keeps(&number) => {
            let mut tag = Map::new();
            tag.insert(BEVY_PERSISTENCE_DATABASE_INTEGER_FIELD.to_owned(), Value::String(number.to_string()));
            Value::Object(tag)
        }
        Value::Array(items) => Value::Array(items.into_iter().map(tag_integers).collect()),
        Value::Object(fields) => Value::Object(fields.into_iter().map(|(name, field)| (name, tag_integers(field))).collect()),
        other => other,
    }
}

/// `value` as it was written: each tagged integer its exact number again,
/// at any depth.
pub fn untag_integers(value: Value) -> Result<Value, IntegerTagError> {
    match value {
        Value::Array(items) => items.into_iter().map(untag_integers).collect::<Result<_, _>>().map(Value::Array),
        Value::Object(fields) if fields.len() == 1 && fields.contains_key(BEVY_PERSISTENCE_DATABASE_INTEGER_FIELD) => {
            let tag = fields.into_iter().next().map(|(_, tag)| tag).unwrap_or(Value::Null);
            match tag.as_str().and_then(|digits| digits.parse::<u64>().ok()) {
                Some(integer) => Ok(Value::Number(integer.into())),
                None => Err(IntegerTagError { tag }),
            }
        }
        Value::Object(fields) => fields
            .into_iter()
            .map(|(name, field)| untag_integers(field).map(|field| (name, field)))
            .collect::<Result<Map<_, _>, _>>()
            .map(Value::Object),
        other => Ok(other),
    }
}
