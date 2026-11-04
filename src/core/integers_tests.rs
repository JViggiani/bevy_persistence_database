use std::collections::BTreeMap;

use proptest::prelude::*;
use rstest::rstest;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::core::db::connection::BEVY_PERSISTENCE_DATABASE_INTEGER_FIELD;

use super::integers::*;

/// Every place a persisted integer can sit: plain, in a sequence, as a map
/// value and inside an `Option`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Placed {
    unsigned: u64,
    signed: i64,
    list: Vec<u64>,
    keyed: BTreeMap<String, u64>,
    maybe: Option<u64>,
}

/// Whether `value` holds an integer outside the signed 64-bit range, at any depth.
fn holds_wide_integer(value: &Value) -> bool {
    match value {
        Value::Number(number) => number.is_u64() && number.as_i64().is_none(),
        Value::Array(items) => items.iter().any(holds_wide_integer),
        Value::Object(fields) => fields.values().any(holds_wide_integer),
        _ => false,
    }
}

/// `value` encoded as a store holds it, then read back.
fn round_trip<T: Serialize + for<'de> Deserialize<'de>>(value: &T) -> (Value, T) {
    let stored = tag_integers(serde_json::to_value(value).expect("encodes"));
    let read = serde_json::from_value(untag_integers(stored.clone()).expect("untags")).expect("decodes");
    (stored, read)
}

// GIVEN any u64 and i64, plain, in a sequence, as a map value and in an Option
// WHEN the value is encoded for a store and read back
// THEN it comes back exact
//   AND the stored JSON holds no integer outside the signed 64-bit range
//   AND an integer inside that range stays a plain number
#[test]
fn every_64_bit_integer_round_trips_through_the_stored_json() {
    proptest!(ProptestConfig::with_cases(256), |(unsigned in any::<u64>(), signed in any::<i64>(), list in prop::collection::vec(any::<u64>(), 0..4), maybe in any::<Option<u64>>())| {
        let placed = Placed { unsigned, signed, keyed: list.iter().map(|value| (value.to_string(), *value)).collect(), list, maybe };
        let (stored, read) = round_trip(&placed);
        prop_assert_eq!(read, placed.clone());
        prop_assert!(!holds_wide_integer(&stored), "{}", stored);
        prop_assert_eq!(&stored["signed"], &json!(signed));
        if i64::try_from(unsigned).is_ok() {
            prop_assert_eq!(&stored["unsigned"], &json!(unsigned));
        }
    });
}

// GIVEN the integer edges: each end of u64 and i64, and either side of where
//   the signed range ends
// WHEN each is encoded for a store and read back
// THEN it comes back exact, tagged only past the signed range
#[rstest]
#[case::zero(0, false)]
#[case::signed_top(i64::MAX as u64, false)]
#[case::past_signed_top(i64::MAX as u64 + 1, true)]
#[case::unsigned_top(u64::MAX, true)]
fn an_unsigned_edge_round_trips(#[case] value: u64, #[case] tagged: bool) {
    let (stored, read) = round_trip(&value);
    assert_eq!(read, value);
    assert_eq!(stored.is_object(), tagged, "{stored}");
}

// GIVEN a tag whose value is no u64: a number, letters, digits past u64::MAX
// WHEN it is read back
// THEN it is refused, naming the tag as stored
#[rstest]
#[case::number(json!(12))]
#[case::letters(json!("twelve"))]
#[case::past_u64(json!("18446744073709551616"))]
fn a_malformed_tag_is_refused(#[case] tag: Value) {
    let stored = json!({ BEVY_PERSISTENCE_DATABASE_INTEGER_FIELD: tag.clone() });
    assert_eq!(untag_integers(stored), Err(IntegerTagError { tag }));
}

// GIVEN an object holding the reserved key beside another field
// WHEN it is read back
// THEN it is left as it is: only an object with the key alone is a tag
#[test]
fn the_reserved_key_tags_only_alone() {
    let stored = json!({ BEVY_PERSISTENCE_DATABASE_INTEGER_FIELD: "12", "other": 1 });
    assert_eq!(untag_integers(stored.clone()), Ok(stored));
}
