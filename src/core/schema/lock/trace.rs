//! Trace a `Deserialize` type into the JSON shape migrations and the schema lock use.
//!
//! The tracer drives `Deserialize` the way a human-readable format does, and walks every
//! enum variant. Types that call `deserialize_any` (untagged enums, `flatten`, raw
//! `serde_json::Value`) fail with the type's path.

use std::collections::{HashMap, HashSet};

use serde::de::{
    self, DeserializeOwned, DeserializeSeed, EnumAccess, MapAccess, SeqAccess, VariantAccess,
    Visitor,
};

const MAX_DEPTH: u32 = 32;
const MAX_PASSES: usize = 256;

/// JSON shape of one persisted type.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum SchemaFormat {
    Unit,
    Bool,
    I8,
    I16,
    I32,
    I64,
    I128,
    U8,
    U16,
    U32,
    U64,
    U128,
    F32,
    F64,
    Char,
    Str,
    Bytes,
    Option(Box<SchemaFormat>),
    Seq(Box<SchemaFormat>),
    Map {
        key: Box<SchemaFormat>,
        value: Box<SchemaFormat>,
    },
    Tuple(Vec<SchemaFormat>),
    UnitStruct {
        name: String,
    },
    Newtype {
        name: String,
        inner: Box<SchemaFormat>,
    },
    TupleStruct {
        name: String,
        fields: Vec<SchemaFormat>,
    },
    Struct {
        name: String,
        fields: Vec<(String, SchemaFormat)>,
    },
    Enum {
        name: String,
        variants: Vec<(String, VariantFormat)>,
    },
}

/// One enum variant's payload.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum VariantFormat {
    Unit,
    Newtype(Box<SchemaFormat>),
    Tuple(Vec<SchemaFormat>),
    Struct(Vec<(String, SchemaFormat)>),
}

#[derive(Debug)]
struct TraceError(String);

impl std::fmt::Display for TraceError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for TraceError {}

impl de::Error for TraceError {
    fn custom<T: std::fmt::Display>(msg: T) -> Self {
        TraceError(msg.to_string())
    }
}

struct SeenEnum {
    path: String,
    count: usize,
    chosen: usize,
}

enum Frame {
    Seq(Vec<SchemaFormat>),
    Tuple(Vec<SchemaFormat>),
    Struct {
        name: String,
        fields: Vec<(String, SchemaFormat)>,
    },
    Map {
        key: Option<SchemaFormat>,
        value: Option<SchemaFormat>,
    },
    Slot(Option<SchemaFormat>),
}

struct Recorder<'a> {
    picks: &'a HashMap<String, usize>,
    path: Vec<String>,
    pending_fields: Vec<String>,
    frames: Vec<Frame>,
    root: Option<SchemaFormat>,
    seen: Vec<SeenEnum>,
    expect_name: Option<&'static str>,
    depth: u32,
}

impl<'a> Recorder<'a> {
    fn new(picks: &'a HashMap<String, usize>) -> Self {
        Self {
            picks,
            path: Vec::new(),
            pending_fields: Vec::new(),
            frames: Vec::new(),
            root: None,
            seen: Vec::new(),
            expect_name: None,
            depth: 0,
        }
    }

    fn enter(&mut self) -> Result<(), TraceError> {
        self.depth += 1;
        if self.depth > MAX_DEPTH {
            self.depth -= 1;
            Err(TraceError(format!(
                "schema trace exceeded depth {MAX_DEPTH} at `{}`; the type may be recursive",
                self.path.join(".")
            )))
        } else {
            Ok(())
        }
    }

    fn exit(&mut self) {
        self.depth = self.depth.saturating_sub(1);
    }

    fn emit(&mut self, format: SchemaFormat) {
        match self.frames.last_mut() {
            Some(Frame::Seq(children) | Frame::Tuple(children)) => children.push(format),
            Some(Frame::Struct { fields, .. }) => {
                let name = self.pending_fields.pop().unwrap_or_default();
                fields.push((name, format));
            }
            Some(Frame::Map { key, value }) => {
                if key.is_none() {
                    *key = Some(format);
                } else {
                    *value = Some(format);
                }
            }
            Some(Frame::Slot(slot)) => *slot = Some(format),
            None => self.root = Some(format),
        }
    }

    fn leaf<T>(&mut self, format: SchemaFormat, result: Result<T, TraceError>) -> Result<T, TraceError> {
        self.exit();
        let value = result?;
        self.emit(format);
        Ok(value)
    }
}

impl<'de> de::Deserializer<'de> for &mut Recorder<'_> {
    type Error = TraceError;

    fn is_human_readable(&self) -> bool {
        true
    }

    fn deserialize_any<V: Visitor<'de>>(self, _visitor: V) -> Result<V::Value, Self::Error> {
        Err(TraceError(format!(
            "type at `{}` requires deserialize_any (untagged enum, flatten, or serde_json::Value) and cannot be traced",
            self.path.join(".")
        )))
    }

    fn deserialize_bool<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        self.enter()?;
        self.leaf(SchemaFormat::Bool, visitor.visit_bool(false))
    }

    fn deserialize_i8<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        self.enter()?;
        self.leaf(SchemaFormat::I8, visitor.visit_i8(0))
    }

    fn deserialize_i16<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        self.enter()?;
        self.leaf(SchemaFormat::I16, visitor.visit_i16(0))
    }

    fn deserialize_i32<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        self.enter()?;
        self.leaf(SchemaFormat::I32, visitor.visit_i32(0))
    }

    fn deserialize_i64<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        self.enter()?;
        self.leaf(SchemaFormat::I64, visitor.visit_i64(0))
    }

    fn deserialize_i128<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        self.enter()?;
        self.leaf(SchemaFormat::I128, visitor.visit_i128(0))
    }

    fn deserialize_u8<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        self.enter()?;
        self.leaf(SchemaFormat::U8, visitor.visit_u8(0))
    }

    fn deserialize_u16<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        self.enter()?;
        self.leaf(SchemaFormat::U16, visitor.visit_u16(0))
    }

    fn deserialize_u32<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        self.enter()?;
        self.leaf(SchemaFormat::U32, visitor.visit_u32(0))
    }

    fn deserialize_u64<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        self.enter()?;
        self.leaf(SchemaFormat::U64, visitor.visit_u64(0))
    }

    fn deserialize_u128<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        self.enter()?;
        self.leaf(SchemaFormat::U128, visitor.visit_u128(0))
    }

    fn deserialize_f32<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        self.enter()?;
        self.leaf(SchemaFormat::F32, visitor.visit_f32(0.0))
    }

    fn deserialize_f64<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        self.enter()?;
        self.leaf(SchemaFormat::F64, visitor.visit_f64(0.0))
    }

    fn deserialize_char<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        self.enter()?;
        self.leaf(SchemaFormat::Char, visitor.visit_char('a'))
    }

    fn deserialize_str<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        if let Some(name) = self.expect_name.take() {
            return visitor.visit_str(name);
        }
        self.enter()?;
        self.leaf(SchemaFormat::Str, visitor.visit_str(""))
    }

    fn deserialize_string<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        self.deserialize_str(visitor)
    }

    fn deserialize_bytes<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        self.enter()?;
        self.leaf(SchemaFormat::Bytes, visitor.visit_bytes(&[]))
    }

    fn deserialize_byte_buf<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        self.deserialize_bytes(visitor)
    }

    fn deserialize_option<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        self.enter()?;
        self.frames.push(Frame::Slot(None));
        let result = visitor.visit_some(&mut *self);
        let inner = match self.frames.pop() {
            Some(Frame::Slot(Some(format))) => format,
            _ => SchemaFormat::Unit,
        };
        self.emit(SchemaFormat::Option(Box::new(inner)));
        self.exit();
        result
    }

    fn deserialize_unit<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        self.enter()?;
        self.leaf(SchemaFormat::Unit, visitor.visit_unit())
    }

    fn deserialize_unit_struct<V: Visitor<'de>>(
        self,
        name: &'static str,
        visitor: V,
    ) -> Result<V::Value, Self::Error> {
        self.enter()?;
        self.leaf(
            SchemaFormat::UnitStruct {
                name: name.to_string(),
            },
            visitor.visit_unit(),
        )
    }

    fn deserialize_newtype_struct<V: Visitor<'de>>(
        self,
        name: &'static str,
        visitor: V,
    ) -> Result<V::Value, Self::Error> {
        self.enter()?;
        self.frames.push(Frame::Slot(None));
        let result = visitor.visit_newtype_struct(&mut *self);
        let inner = match self.frames.pop() {
            Some(Frame::Slot(Some(format))) => format,
            _ => SchemaFormat::Unit,
        };
        self.emit(SchemaFormat::Newtype {
            name: name.to_string(),
            inner: Box::new(inner),
        });
        self.exit();
        result
    }

    fn deserialize_seq<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        self.enter()?;
        self.frames.push(Frame::Seq(Vec::new()));
        let result = visitor.visit_seq(CountingSeq {
            recorder: self,
            left: 1,
        });
        let child = match self.frames.pop() {
            Some(Frame::Seq(mut children)) => children.pop().unwrap_or(SchemaFormat::Unit),
            _ => SchemaFormat::Unit,
        };
        self.emit(SchemaFormat::Seq(Box::new(child)));
        self.exit();
        result
    }

    fn deserialize_tuple<V: Visitor<'de>>(
        self,
        len: usize,
        visitor: V,
    ) -> Result<V::Value, Self::Error> {
        self.enter()?;
        self.frames.push(Frame::Tuple(Vec::new()));
        let result = visitor.visit_seq(CountingSeq {
            recorder: self,
            left: len,
        });
        let fields = match self.frames.pop() {
            Some(Frame::Tuple(fields)) => fields,
            _ => Vec::new(),
        };
        self.emit(SchemaFormat::Tuple(fields));
        self.exit();
        result
    }

    fn deserialize_tuple_struct<V: Visitor<'de>>(
        self,
        name: &'static str,
        len: usize,
        visitor: V,
    ) -> Result<V::Value, Self::Error> {
        self.enter()?;
        self.frames.push(Frame::Tuple(Vec::new()));
        let result = visitor.visit_seq(CountingSeq {
            recorder: self,
            left: len,
        });
        let fields = match self.frames.pop() {
            Some(Frame::Tuple(fields)) => fields,
            _ => Vec::new(),
        };
        self.emit(SchemaFormat::TupleStruct {
            name: name.to_string(),
            fields,
        });
        self.exit();
        result
    }

    fn deserialize_map<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        self.enter()?;
        self.frames.push(Frame::Map {
            key: None,
            value: None,
        });
        let result = visitor.visit_map(OneEntry {
            recorder: self,
            produced: false,
        });
        let (key, value) = match self.frames.pop() {
            Some(Frame::Map {
                key: Some(key),
                value: Some(value),
            }) => (key, value),
            _ => (SchemaFormat::Unit, SchemaFormat::Unit),
        };
        self.emit(SchemaFormat::Map {
            key: Box::new(key),
            value: Box::new(value),
        });
        self.exit();
        result
    }

    fn deserialize_struct<V: Visitor<'de>>(
        self,
        name: &'static str,
        fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Self::Error> {
        self.enter()?;
        self.frames.push(Frame::Struct {
            name: name.to_string(),
            fields: Vec::new(),
        });
        let result = visitor.visit_map(FieldAccess {
            recorder: self,
            fields,
            index: 0,
        });
        if let Some(Frame::Struct { name, fields }) = self.frames.pop() {
            self.emit(SchemaFormat::Struct { name, fields });
        }
        self.exit();
        result
    }

    fn deserialize_enum<V: Visitor<'de>>(
        self,
        name: &'static str,
        variants: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Self::Error> {
        self.enter()?;
        if variants.is_empty() {
            self.exit();
            return Err(TraceError(format!("enum `{name}` has no variants")));
        }
        let path_key = self.path.join("/");
        let chosen = self
            .picks
            .get(&path_key)
            .copied()
            .unwrap_or(0)
            .min(variants.len() - 1);
        self.seen.push(SeenEnum {
            path: path_key,
            count: variants.len(),
            chosen,
        });
        let variant = variants[chosen];
        self.path.push(variant.to_string());
        self.frames.push(Frame::Slot(None));
        let result = visitor.visit_enum(EnumProbe {
            recorder: self,
            variant,
        });
        self.path.pop();
        let slot = match self.frames.pop() {
            Some(Frame::Slot(body)) => body,
            _ => None,
        };
        let variant_format = match slot {
            None => VariantFormat::Unit,
            Some(SchemaFormat::Tuple(fields)) => VariantFormat::Tuple(fields),
            Some(SchemaFormat::Struct { fields, .. }) => VariantFormat::Struct(fields),
            Some(other) => VariantFormat::Newtype(Box::new(other)),
        };
        self.emit(SchemaFormat::Enum {
            name: name.to_string(),
            variants: vec![(variant.to_string(), variant_format)],
        });
        self.exit();
        result
    }

    fn deserialize_identifier<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        self.deserialize_str(visitor)
    }

    fn deserialize_ignored_any<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        visitor.visit_unit()
    }
}

struct FieldAccess<'a, 'p> {
    recorder: &'a mut Recorder<'p>,
    fields: &'static [&'static str],
    index: usize,
}

impl<'de> MapAccess<'de> for FieldAccess<'_, '_> {
    type Error = TraceError;

    fn next_key_seed<K: DeserializeSeed<'de>>(
        &mut self,
        seed: K,
    ) -> Result<Option<K::Value>, Self::Error> {
        if self.index >= self.fields.len() {
            return Ok(None);
        }
        let name = self.fields[self.index];
        self.recorder.expect_name = Some(name);
        let key = seed.deserialize(&mut *self.recorder)?;
        self.index += 1;
        Ok(Some(key))
    }

    fn next_value_seed<V: DeserializeSeed<'de>>(
        &mut self,
        seed: V,
    ) -> Result<V::Value, Self::Error> {
        let name = self.fields[self.index - 1];
        self.recorder.pending_fields.push(name.to_string());
        self.recorder.path.push(name.to_string());
        let value = seed.deserialize(&mut *self.recorder)?;
        self.recorder.path.pop();
        Ok(value)
    }
}

struct CountingSeq<'a, 'p> {
    recorder: &'a mut Recorder<'p>,
    left: usize,
}

impl<'de> SeqAccess<'de> for CountingSeq<'_, '_> {
    type Error = TraceError;

    fn next_element_seed<T: DeserializeSeed<'de>>(
        &mut self,
        seed: T,
    ) -> Result<Option<T::Value>, Self::Error> {
        if self.left == 0 {
            return Ok(None);
        }
        self.left -= 1;
        Ok(Some(seed.deserialize(&mut *self.recorder)?))
    }
}

struct OneEntry<'a, 'p> {
    recorder: &'a mut Recorder<'p>,
    produced: bool,
}

impl<'de> MapAccess<'de> for OneEntry<'_, '_> {
    type Error = TraceError;

    fn next_key_seed<K: DeserializeSeed<'de>>(
        &mut self,
        seed: K,
    ) -> Result<Option<K::Value>, Self::Error> {
        if self.produced {
            return Ok(None);
        }
        self.produced = true;
        Ok(Some(seed.deserialize(&mut *self.recorder)?))
    }

    fn next_value_seed<V: DeserializeSeed<'de>>(
        &mut self,
        seed: V,
    ) -> Result<V::Value, Self::Error> {
        seed.deserialize(&mut *self.recorder)
    }
}

struct EnumProbe<'a, 'p> {
    recorder: &'a mut Recorder<'p>,
    variant: &'static str,
}

impl<'a, 'p, 'de> EnumAccess<'de> for EnumProbe<'a, 'p> {
    type Error = TraceError;
    type Variant = VariantProbe<'a, 'p>;

    fn variant_seed<V: DeserializeSeed<'de>>(
        self,
        seed: V,
    ) -> Result<(V::Value, Self::Variant), Self::Error> {
        self.recorder.expect_name = Some(self.variant);
        let id = seed.deserialize(&mut *self.recorder)?;
        Ok((
            id,
            VariantProbe {
                recorder: self.recorder,
            },
        ))
    }
}

struct VariantProbe<'a, 'p> {
    recorder: &'a mut Recorder<'p>,
}

impl<'de> VariantAccess<'de> for VariantProbe<'_, '_> {
    type Error = TraceError;

    fn unit_variant(self) -> Result<(), Self::Error> {
        Ok(())
    }

    fn newtype_variant_seed<T: DeserializeSeed<'de>>(
        self,
        seed: T,
    ) -> Result<T::Value, Self::Error> {
        seed.deserialize(self.recorder)
    }

    fn tuple_variant<V: Visitor<'de>>(
        self,
        len: usize,
        visitor: V,
    ) -> Result<V::Value, Self::Error> {
        self.recorder.frames.push(Frame::Tuple(Vec::new()));
        let result = visitor.visit_seq(CountingSeq {
            recorder: self.recorder,
            left: len,
        });
        let fields = match self.recorder.frames.pop() {
            Some(Frame::Tuple(fields)) => fields,
            _ => Vec::new(),
        };
        self.recorder.emit(SchemaFormat::Tuple(fields));
        result
    }

    fn struct_variant<V: Visitor<'de>>(
        self,
        fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Self::Error> {
        self.recorder.frames.push(Frame::Struct {
            name: String::new(),
            fields: Vec::new(),
        });
        let result = visitor.visit_map(FieldAccess {
            recorder: self.recorder,
            fields,
            index: 0,
        });
        if let Some(Frame::Struct { fields, .. }) = self.recorder.frames.pop() {
            self.recorder
                .emit(SchemaFormat::Struct { name: String::new(), fields });
        }
        result
    }
}

fn trace_once<T: DeserializeOwned>(
    picks: &HashMap<String, usize>,
) -> Result<(SchemaFormat, Vec<SeenEnum>), String> {
    let mut recorder = Recorder::new(picks);
    T::deserialize(&mut recorder).map_err(|error| error.0)?;
    let format = recorder
        .root
        .ok_or_else(|| "traced type produced no schema".to_string())?;
    Ok((format, recorder.seen))
}

fn merge_format(left: SchemaFormat, right: SchemaFormat) -> Result<SchemaFormat, String> {
    use SchemaFormat::*;
    match (left, right) {
        (Enum { name, variants: mut left_variants }, Enum { variants: right_variants, .. }) => {
            for (variant_name, format) in right_variants {
                if let Some((_, existing)) = left_variants
                    .iter_mut()
                    .find(|(name, _)| name == &variant_name)
                {
                    *existing = merge_variant(existing.clone(), format)?;
                } else {
                    left_variants.push((variant_name, format));
                }
            }
            Ok(Enum {
                name,
                variants: left_variants,
            })
        }
        (
            Struct {
                name,
                fields: left_fields,
            },
            Struct {
                fields: right_fields,
                ..
            },
        ) => Ok(Struct {
            name,
            fields: merge_fields(left_fields, right_fields)?,
        }),
        (Option(left), Option(right)) => Ok(Option(Box::new(merge_format(*left, *right)?))),
        (Seq(left), Seq(right)) => Ok(Seq(Box::new(merge_format(*left, *right)?))),
        (
            Map {
                key: left_key,
                value: left_value,
            },
            Map {
                key: right_key,
                value: right_value,
            },
        ) => Ok(Map {
            key: Box::new(merge_format(*left_key, *right_key)?),
            value: Box::new(merge_format(*left_value, *right_value)?),
        }),
        (Tuple(left), Tuple(right)) => Ok(Tuple(merge_list(left, right)?)),
        (
            TupleStruct {
                name,
                fields: left_fields,
            },
            TupleStruct {
                fields: right_fields,
                ..
            },
        ) => Ok(TupleStruct {
            name,
            fields: merge_list(left_fields, right_fields)?,
        }),
        (
            Newtype { name, inner: left },
            Newtype { inner: right, .. },
        ) => Ok(Newtype {
            name,
            inner: Box::new(merge_format(*left, *right)?),
        }),
        (left, right) if left == right => Ok(left),
        (left, right) => Err(format!(
            "schema passes disagree: {left:?} vs {right:?}"
        )),
    }
}

fn merge_variant(left: VariantFormat, right: VariantFormat) -> Result<VariantFormat, String> {
    use VariantFormat::*;
    match (left, right) {
        (Unit, Unit) => Ok(Unit),
        (Newtype(left), Newtype(right)) => Ok(Newtype(Box::new(merge_format(*left, *right)?))),
        (Tuple(left), Tuple(right)) => Ok(Tuple(merge_list(left, right)?)),
        (Struct(left), Struct(right)) => Ok(Struct(merge_fields(left, right)?)),
        (left, right) => Err(format!("enum variant passes disagree: {left:?} vs {right:?}")),
    }
}

fn merge_fields(
    mut left: Vec<(String, SchemaFormat)>,
    right: Vec<(String, SchemaFormat)>,
) -> Result<Vec<(String, SchemaFormat)>, String> {
    for (name, format) in right {
        if let Some((_, existing)) = left.iter_mut().find(|(existing_name, _)| existing_name == &name)
        {
            *existing = merge_format(existing.clone(), format)?;
        } else {
            left.push((name, format));
        }
    }
    Ok(left)
}

fn merge_list(
    left: Vec<SchemaFormat>,
    right: Vec<SchemaFormat>,
) -> Result<Vec<SchemaFormat>, String> {
    if left.len() != right.len() {
        return Err(format!(
            "tuple length changed between schema passes ({} vs {})",
            left.len(),
            right.len()
        ));
    }
    left.into_iter()
        .zip(right)
        .map(|(left, right)| merge_format(left, right))
        .collect()
}

/// Trace `T`, including every enum variant.
pub fn trace_type<T: DeserializeOwned>() -> Result<SchemaFormat, String> {
    let mut picks = HashMap::new();
    let mut explored: HashMap<String, HashSet<usize>> = HashMap::new();
    let mut merged: Option<SchemaFormat> = None;
    for _ in 0..MAX_PASSES {
        let (format, seen) = trace_once::<T>(&picks)?;
        merged = Some(match merged {
            None => format,
            Some(previous) => merge_format(previous, format)?,
        });
        let mut advance: Option<(String, usize)> = None;
        for seen_enum in seen.iter().rev() {
            let done = explored.entry(seen_enum.path.clone()).or_default();
            done.insert(seen_enum.chosen);
            if advance.is_some() {
                continue;
            }
            if let Some(index) = (0..seen_enum.count).find(|index| !done.contains(index)) {
                advance = Some((seen_enum.path.clone(), index));
            }
        }
        let Some((path, index)) = advance else {
            return merged.ok_or_else(|| "traced type produced no schema".to_string());
        };
        picks.insert(path, index);
    }
    Err(format!(
        "schema trace did not cover every enum variant within {MAX_PASSES} passes"
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Serialize, Deserialize, PartialEq)]
    struct Health {
        value: i32,
    }

    #[derive(Debug, Serialize, Deserialize, PartialEq)]
    enum Stance {
        Idle,
        Moving { speed: f32 },
    }

    // GIVEN a struct with one integer field
    // WHEN it is traced
    // THEN the schema names the struct and the field
    #[test]
    fn traces_struct_fields() {
        let format = trace_type::<Health>().unwrap();
        assert_eq!(
            format,
            SchemaFormat::Struct {
                name: "Health".into(),
                fields: vec![("value".into(), SchemaFormat::I32)],
            }
        );
    }

    // GIVEN an enum with a unit variant and a struct variant
    // WHEN it is traced
    // THEN both variants are present
    #[test]
    fn traces_every_enum_variant() {
        let format = trace_type::<Stance>().unwrap();
        let SchemaFormat::Enum { name, variants } = format else {
            panic!("expected enum");
        };
        assert_eq!(name, "Stance");
        let names: Vec<_> = variants.iter().map(|(name, _)| name.as_str()).collect();
        assert!(names.contains(&"Idle"), "{names:?}");
        assert!(names.contains(&"Moving"), "{names:?}");
    }

    // GIVEN serde_json::Value, which deserializes with deserialize_any
    // WHEN it is traced
    // THEN the error names deserialize_any
    #[test]
    fn rejects_deserialize_any() {
        let error = trace_type::<serde_json::Value>().unwrap_err();
        assert!(
            error.contains("deserialize_any"),
            "unexpected error: {error}"
        );
    }
}
