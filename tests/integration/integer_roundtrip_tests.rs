use bevy_persistence_database::bevy::components::Guid;
use bevy_persistence_database::compact::from_persist_value;
use bevy_persistence_database::core::persist::Persist;
use bevy_persistence_database::core::session::commit_sync;

use crate::common::*;
use bevy_persistence_database_derive::db_matrix_test;

/// The largest integer a double holds exactly, 2^53.
const DOUBLE_EXACT: u64 = 1 << 53;

/// Every unsigned edge: zero, a double's exact limit either side, the signed
/// limit either side, and the top.
fn unsigned_edges() -> Vec<u64> {
    vec![
        0,
        1,
        DOUBLE_EXACT - 1,
        DOUBLE_EXACT,
        DOUBLE_EXACT + 1,
        i64::MAX as u64,
        i64::MAX as u64 + 1,
        u64::MAX - 1,
        u64::MAX,
    ]
}

/// Every signed edge: both ends, a double's exact limit either side, zero.
fn signed_edges() -> Vec<i64> {
    let exact = DOUBLE_EXACT as i64;
    vec![i64::MIN, i64::MIN + 1, -exact - 1, -exact, -1, 0, 1, exact, exact + 1, i64::MAX - 1, i64::MAX]
}

// GIVEN a resource holding every 64-bit integer edge (each end of u64 and
//   i64, either side of 2^53 and of the signed range's end) and a component
//   at the extremes
// WHEN each is committed to a store and read back as the library decodes it
// THEN every value comes back exact, on every backend
#[db_matrix_test]
fn every_64_bit_integer_round_trips() {
    let (db, _container) = setup();
    let mut app = setup_test_app(db.clone(), None);
    let edges = IntegerEdges {
        unsigned: unsigned_edges(),
        signed: signed_edges(),
    };
    let extremes = IntegerExtremes { unsigned: u64::MAX, signed: i64::MIN };
    app.insert_resource(edges.clone());
    let entity = app.world_mut().spawn(extremes.clone()).id();
    app.update();
    commit_sync(&mut app, db.clone(), TEST_STORE).expect("commit");

    let (stored, _) = run_async(db.fetch_resource(TEST_STORE, IntegerEdges::name()))
        .expect("fetch the resource")
        .expect("the resource is stored");
    assert_eq!(from_persist_value::<IntegerEdges>(stored).expect("the resource decodes"), edges);

    let guid = app.world().get::<Guid>(entity).expect("a guid after commit").id().to_string();
    let stored = run_async(db.fetch_component(TEST_STORE, &guid, IntegerExtremes::name()))
        .expect("fetch the component")
        .expect("the component is stored");
    assert_eq!(from_persist_value::<IntegerExtremes>(stored).expect("the component decodes"), extremes);
}
