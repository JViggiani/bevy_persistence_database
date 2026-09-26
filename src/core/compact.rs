//! Compact binary encoding for large values stored inside JSON documents.
//!
//! Backend documents stay JSON. Values whose MessagePack encoding exceeds
//! [`DEFAULT_COMPACT_THRESHOLD_BYTES`] are stored as zstd + base64 inside a
//! small JSON envelope. MessagePack uses named fields and human-readable mode,
//! so the payload has the same shape as `serde_json::to_value` and a migration
//! step can edit it as JSON.

use std::io::Cursor;

use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use serde::{Deserialize, Deserializer, Serialize, Serializer, de::DeserializeOwned};
use serde_json::Value;

/// Envelope version written into JSON documents.
pub const ENCODING: &str = "msgpack+zstd-v1";

/// Default zstd compression level (speed / ratio trade-off for persistence).
pub const DEFAULT_ZSTD_LEVEL: i32 = 3;

/// Default MessagePack size above which session serializers use the compact
/// envelope instead of naive JSON (`256 KiB`).
pub const DEFAULT_COMPACT_THRESHOLD_BYTES: usize = 256 * 1024;

#[derive(Serialize, Deserialize)]
struct CompactEnvelope {
    encoding: String,
    /// Base64(zstd(msgpack(T))).
    payload: String,
}

/// Errors from compact encode / decode helpers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompactError(pub String);

impl std::fmt::Display for CompactError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for CompactError {}

/// True when `value` is a compact envelope for [`ENCODING`].
pub fn is_compact_envelope(value: &Value) -> bool {
    value.get("encoding").and_then(Value::as_str) == Some(ENCODING)
        && value.get("payload").and_then(Value::as_str).is_some()
}

fn msgpack_encode<T: Serialize>(value: &T) -> Result<Vec<u8>, CompactError> {
    let mut buf = Vec::new();
    value
        .serialize(
            &mut rmp_serde::Serializer::new(&mut buf)
                .with_struct_map()
                .with_human_readable(),
        )
        .map_err(|error| CompactError(format!("msgpack encode: {error}")))?;
    Ok(buf)
}

fn msgpack_decode<T: DeserializeOwned>(raw: &[u8]) -> Result<T, CompactError> {
    let mut deserializer = rmp_serde::Deserializer::new(Cursor::new(raw)).with_human_readable();
    serde_path_to_error::deserialize(&mut deserializer)
        .map_err(|error| CompactError(format!("msgpack decode at `{}`: {error}", error.path())))
}

fn zstd_base64(raw: &[u8], zstd_level: i32) -> Result<String, CompactError> {
    let compressed = zstd::encode_all(raw, zstd_level)
        .map_err(|error| CompactError(format!("zstd encode: {error}")))?;
    Ok(BASE64.encode(compressed))
}

fn reject_unknown_encoding(value: &Value) -> Result<(), CompactError> {
    let Some(encoding) = value.get("encoding").and_then(Value::as_str) else {
        return Ok(());
    };
    if value.get("payload").and_then(Value::as_str).is_some() && encoding != ENCODING {
        return Err(CompactError(format!(
            "unsupported compact persist encoding {encoding:?}"
        )));
    }
    Ok(())
}

/// MessagePack + zstd + base64 encode `value` to a payload string (no envelope).
pub fn encode<T: Serialize>(value: &T) -> Result<String, CompactError> {
    encode_with_level(value, DEFAULT_ZSTD_LEVEL)
}

/// Like [`encode`] with an explicit zstd level.
pub fn encode_with_level<T: Serialize>(value: &T, zstd_level: i32) -> Result<String, CompactError> {
    let raw = msgpack_encode(value)?;
    zstd_base64(&raw, zstd_level)
}

/// Inverse of [`encode`].
pub fn decode<T: DeserializeOwned>(payload: &str) -> Result<T, CompactError> {
    let compressed = BASE64
        .decode(payload.as_bytes())
        .map_err(|error| CompactError(format!("base64 decode: {error}")))?;
    let raw = zstd::decode_all(compressed.as_slice())
        .map_err(|error| CompactError(format!("zstd decode: {error}")))?;
    msgpack_decode(&raw)
}

/// Build a JSON [`Value`] for persistence: compact envelope when MessagePack size
/// exceeds `threshold_bytes`, otherwise naive `serde_json::to_value`.
pub fn to_persist_value<T: Serialize>(
    value: &T,
    threshold_bytes: usize,
) -> Result<Value, CompactError> {
    let raw = msgpack_encode(value)?;
    if raw.len() > threshold_bytes {
        let payload = zstd_base64(&raw, DEFAULT_ZSTD_LEVEL)?;
        Ok(serde_json::json!({
            "encoding": ENCODING,
            "payload": payload,
        }))
    } else {
        serde_json::to_value(value).map_err(|error| CompactError(format!("json encode: {error}")))
    }
}

/// Inverse of [`to_persist_value`]: compact envelope or plain JSON document body.
pub fn from_persist_value<T: DeserializeOwned>(value: Value) -> Result<T, CompactError> {
    reject_unknown_encoding(&value)?;
    if is_compact_envelope(&value) {
        let payload = value
            .get("payload")
            .and_then(Value::as_str)
            .ok_or_else(|| CompactError("compact envelope missing payload".into()))?;
        decode(payload)
    } else {
        serde_json::from_value(value).map_err(|error| CompactError(format!("json decode: {error}")))
    }
}

/// Decode a compact envelope to the JSON value `serde_json::to_value` would produce.
///
/// A value that is not an envelope is returned unchanged.
pub fn expand_envelope(value: Value) -> Result<Value, CompactError> {
    reject_unknown_encoding(&value)?;
    if !is_compact_envelope(&value) {
        return Ok(value);
    }
    let payload = value
        .get("payload")
        .and_then(Value::as_str)
        .ok_or_else(|| CompactError("compact envelope missing payload".into()))?;
    decode(payload)
}

/// Serialize `value` as a versioned compact JSON envelope.
///
/// Intended for `#[serde(with = "bevy_persistence_database::compact")]`.
pub fn serialize<T: Serialize, S: Serializer>(value: &T, serializer: S) -> Result<S::Ok, S::Error> {
    let payload = encode(value).map_err(serde::ser::Error::custom)?;
    CompactEnvelope {
        encoding: ENCODING.to_string(),
        payload,
    }
    .serialize(serializer)
}

/// Deserialize a versioned compact JSON envelope into `T`.
///
/// Intended for `#[serde(with = "bevy_persistence_database::compact")]`.
pub fn deserialize<'de, T: DeserializeOwned, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<T, D::Error> {
    let doc = CompactEnvelope::deserialize(deserializer)?;
    if doc.encoding != ENCODING {
        return Err(serde::de::Error::custom(format!(
            "unsupported compact persist encoding {:?}",
            doc.encoding
        )));
    }
    decode(&doc.payload).map_err(serde::de::Error::custom)
}

/// Newtype that always serde-encodes `T` via the compact envelope.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CompactJson<T>(pub T);

impl<T> CompactJson<T> {
    pub const fn new(value: T) -> Self {
        Self(value)
    }

    pub fn into_inner(self) -> T {
        self.0
    }
}

impl<T> std::ops::Deref for CompactJson<T> {
    type Target = T;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T> std::ops::DerefMut for CompactJson<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl<T> From<T> for CompactJson<T> {
    fn from(value: T) -> Self {
        Self(value)
    }
}

impl<T: Serialize> Serialize for CompactJson<T> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serialize(&self.0, serializer)
    }
}

impl<'de, T: DeserializeOwned> Deserialize<'de> for CompactJson<T> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserialize(deserializer).map(CompactJson)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct Sample {
        name: String,
        values: Vec<f32>,
    }

    // GIVEN an arbitrary serde value with a large array
    // WHEN compact-encoded and decoded
    // THEN the value round-trips exactly
    #[test]
    fn encode_decode_round_trips_arbitrary_struct() {
        let original = Sample {
            name: "grid".into(),
            values: (0..1000).map(|i| i as f32 * 0.01).collect(),
        };
        let payload = encode(&original).expect("encode");
        let restored: Sample = decode(&payload).expect("decode");
        assert_eq!(restored, original);
    }

    // GIVEN an arbitrary serde value
    // WHEN serialized through the JSON envelope
    // THEN the document carries the versioned encoding tag and is smaller than naive JSON
    #[test]
    fn serde_envelope_is_versioned_and_smaller_than_naive_json() {
        let original = Sample {
            name: "big".into(),
            values: vec![0.25; 2000],
        };
        let mut compact = Vec::new();
        let mut ser = serde_json::Serializer::new(&mut compact);
        serialize(&original, &mut ser).expect("compact serialize");
        let naive = serde_json::to_vec(&original).expect("naive json");
        let value: serde_json::Value = serde_json::from_slice(&compact).expect("parse envelope");
        assert_eq!(value["encoding"], ENCODING);
        assert!(
            compact.len() * 4 < naive.len(),
            "compact {} should be much smaller than naive {}",
            compact.len(),
            naive.len()
        );

        let wrapped = CompactJson(original.clone());
        let back: CompactJson<Sample> =
            serde_json::from_value(serde_json::to_value(&wrapped).unwrap()).unwrap();
        assert_eq!(*back, original);
    }

    // GIVEN an envelope with an unknown encoding tag
    // WHEN deserialized
    // THEN an error is returned
    #[test]
    fn rejects_unknown_encoding_tag() {
        let bad = serde_json::json!({
            "encoding": "postcard+zstd-v1",
            "payload": "AAAA",
        });
        let err = serde_json::from_value::<CompactJson<Sample>>(bad).unwrap_err();
        assert!(
            err.to_string()
                .contains("unsupported compact persist encoding"),
            "unexpected error: {err}"
        );
    }

    // GIVEN a small value and a high threshold
    // WHEN to_persist_value runs
    // THEN the result is plain JSON (not a compact envelope)
    #[test]
    fn to_persist_value_keeps_small_values_as_json() {
        let original = Sample {
            name: "tiny".into(),
            values: vec![1.0],
        };
        let value = to_persist_value(&original, DEFAULT_COMPACT_THRESHOLD_BYTES).unwrap();
        assert!(!is_compact_envelope(&value));
        assert_eq!(from_persist_value::<Sample>(value).unwrap(), original);
    }

    // GIVEN a large value and a low threshold
    // WHEN to_persist_value runs and the envelope is expanded
    // THEN the JSON matches serde_json::to_value
    #[test]
    fn expanded_envelope_matches_naive_json() {
        let original = Sample {
            name: "big".into(),
            values: vec![0.5; 5000],
        };
        let value = to_persist_value(&original, 64).unwrap();
        assert!(is_compact_envelope(&value));
        let expanded = expand_envelope(value).unwrap();
        let naive = serde_json::to_value(&original).unwrap();
        assert_eq!(expanded, naive);
    }

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct Row {
        label: String,
        #[serde(default)]
        extra: i32,
    }

    // GIVEN a sequence element whose compact payload omits a defaulted field
    // WHEN it is decoded
    // THEN serde fills the default
    #[test]
    fn default_field_inside_sequence_fills() {
        let stored = vec![Row {
            label: "a".into(),
            extra: 0,
        }];
        let payload = encode(&stored).unwrap();
        let restored: Vec<Row> = decode(&payload).unwrap();
        assert_eq!(restored[0].extra, 0);
        assert_eq!(restored[0].label, "a");
    }

    // GIVEN a compact payload whose field type no longer matches
    // WHEN decode fails
    // THEN the error names the field
    #[test]
    fn decode_error_names_the_field() {
        #[derive(Serialize)]
        struct Outer {
            name: String,
        }
        #[derive(Debug, Deserialize)]
        #[allow(dead_code)]
        struct ExpectNumber {
            name: i32,
        }
        let payload = encode(&Outer {
            name: "world".into(),
        })
        .unwrap();
        let err = decode::<ExpectNumber>(&payload).unwrap_err();
        assert!(
            err.0.contains("name"),
            "expected the error to name the field, got: {}",
            err.0
        );
    }
}
