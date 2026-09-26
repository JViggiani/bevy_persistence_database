//! Build one JSON value that matches a traced [`SchemaFormat`].

use serde_json::{Map, Value};

use super::trace::{SchemaFormat, VariantFormat};

/// How many documents are needed to land on every enum variant at least once.
pub(crate) fn variant_span(format: &SchemaFormat) -> usize {
    fn walk(format: &SchemaFormat, best: &mut usize) {
        match format {
            SchemaFormat::Option(inner) | SchemaFormat::Seq(inner) | SchemaFormat::Newtype { inner, .. } => {
                walk(inner, best);
            }
            SchemaFormat::Map { key, value } => {
                walk(key, best);
                walk(value, best);
            }
            SchemaFormat::Tuple(fields) | SchemaFormat::TupleStruct { fields, .. } => {
                for field in fields {
                    walk(field, best);
                }
            }
            SchemaFormat::Struct { fields, .. } => {
                for (_, field) in fields {
                    walk(field, best);
                }
            }
            SchemaFormat::Enum { variants, .. } => {
                *best = (*best).max(variants.len());
                for (_, variant) in variants {
                    match variant {
                        VariantFormat::Unit => {}
                        VariantFormat::Newtype(inner) => walk(inner, best),
                        VariantFormat::Tuple(fields) => {
                            for field in fields {
                                walk(field, best);
                            }
                        }
                        VariantFormat::Struct(fields) => {
                            for (_, field) in fields {
                                walk(field, best);
                            }
                        }
                    }
                }
            }
            _ => {}
        }
    }
    let mut best = 1;
    walk(format, &mut best);
    best
}

/// A JSON value of `format`. Enum variants cycle by `index`.
pub(crate) fn synthesize(format: &SchemaFormat, index: usize) -> Value {
    match format {
        SchemaFormat::Unit | SchemaFormat::UnitStruct { .. } => Value::Null,
        SchemaFormat::Bool => Value::Bool(false),
        SchemaFormat::I8
        | SchemaFormat::I16
        | SchemaFormat::I32
        | SchemaFormat::I64
        | SchemaFormat::I128
        | SchemaFormat::U8
        | SchemaFormat::U16
        | SchemaFormat::U32
        | SchemaFormat::U64
        | SchemaFormat::U128 => Value::from(0),
        SchemaFormat::F32 | SchemaFormat::F64 => Value::from(0.0),
        SchemaFormat::Char => Value::String("a".to_string()),
        SchemaFormat::Str => Value::String(String::new()),
        SchemaFormat::Bytes => Value::Array(Vec::new()),
        SchemaFormat::Option(_) => Value::Null,
        SchemaFormat::Seq(inner) => Value::Array(vec![synthesize(inner, index)]),
        SchemaFormat::Map { key, value } => {
            if matches!(key.as_ref(), SchemaFormat::Str) {
                let mut obj = Map::new();
                obj.insert(String::new(), synthesize(value, index));
                Value::Object(obj)
            } else {
                Value::Object(Map::new())
            }
        }
        SchemaFormat::Tuple(fields) => {
            Value::Array(fields.iter().map(|field| synthesize(field, index)).collect())
        }
        SchemaFormat::Newtype { inner, .. } => synthesize(inner, index),
        SchemaFormat::TupleStruct { fields, .. } => {
            Value::Array(fields.iter().map(|field| synthesize(field, index)).collect())
        }
        SchemaFormat::Struct { fields, .. } => {
            let mut obj = Map::new();
            for (name, field) in fields {
                obj.insert(name.clone(), synthesize(field, index));
            }
            Value::Object(obj)
        }
        SchemaFormat::Enum { variants, .. } => {
            if variants.is_empty() {
                return Value::Null;
            }
            let (name, variant) = &variants[index % variants.len()];
            match variant {
                VariantFormat::Unit => Value::String(name.clone()),
                VariantFormat::Newtype(inner) => tagged(name, synthesize(inner, index)),
                VariantFormat::Tuple(fields) => tagged(
                    name,
                    Value::Array(fields.iter().map(|field| synthesize(field, index)).collect()),
                ),
                VariantFormat::Struct(fields) => {
                    let mut obj = Map::new();
                    for (field_name, field) in fields {
                        obj.insert(field_name.clone(), synthesize(field, index));
                    }
                    tagged(name, Value::Object(obj))
                }
            }
        }
    }
}

fn tagged(name: &str, value: Value) -> Value {
    let mut obj = Map::new();
    obj.insert(name.to_string(), value);
    Value::Object(obj)
}
