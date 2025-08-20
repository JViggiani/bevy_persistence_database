use bevy::prelude::*;
use bevy_arangodb_core::{
    commit_sync, Guid, persistence_plugin::PersistencePlugins, PersistentQuery,
};
use crate::common::*;
use bevy_arangodb_derive::db_matrix_test;

fn system_without_creature(mut pq: PersistentQuery<&Guid, Without<Creature>>) {
    let _ = pq.ensure_loaded();
}

#[db_matrix_test]
fn test_presence_without_filter() {
    let (db, _container) = setup();

    // GIVEN: one entity with Creature, one with only Health
    let mut app = App::new();
    app.add_plugins(PersistencePlugins(db.clone()));
    app.world_mut().spawn(Creature { is_screaming: false });
    app.world_mut().spawn(Health { value: 42 });
    app.update();
    commit_sync(&mut app).expect("Initial commit failed");

    // WHEN: run a system that loads entities WITHOUT Creature
    let mut app2 = App::new();
    app2.add_plugins(PersistencePlugins(db.clone()));
    app2.add_systems(bevy::prelude::Update, system_without_creature);
    app2.update();

    // THEN: only the Health-only entity is present, and none have Creature
    let mut q_guid = app2.world_mut().query::<&Guid>();
    let count = q_guid.iter(&app2.world()).count();
    assert_eq!(count, 1, "Only one entity should be loaded (without Creature)");

    let mut q_creature = app2.world_mut().query::<&Creature>();
    assert_eq!(
        q_creature.iter(&app2.world()).count(),
        0,
        "No entities should have Creature component"
    );
}

fn system_type_driven_presence(mut pq: PersistentQuery<&Guid, (bevy::prelude::With<Health>, bevy::prelude::Without<Creature>)>) {
    // Load entities that have Health but do NOT have Creature
    let _ = pq.ensure_loaded();
}

#[db_matrix_test]
fn test_type_driven_presence_filters() {
    let (db, _container) = setup();

    // GIVEN: one entity with Creature+Health, one with only Health
    let mut app = App::new();
    app.add_plugins(PersistencePlugins(db.clone()));
    app.world_mut().spawn((Health { value: 1 }, Creature { is_screaming: false }));
    app.world_mut().spawn(Health { value: 2 });
    app.update();
    commit_sync(&mut app).expect("Initial commit failed");

    // WHEN: run a system that uses type-level With/Without in F
    let mut app2 = App::new();
    app2.add_plugins(PersistencePlugins(db.clone()));
    app2.add_systems(bevy::prelude::Update, system_type_driven_presence);
    app2.update();

    // THEN: only the Health-only entity is loaded (no Creature in world)
    let mut q_guid = app2.world_mut().query::<&Guid>();
    let count = q_guid.iter(&app2.world()).count();
    assert_eq!(count, 1, "Type-driven presence should exclude Creature entities");

    let mut q_creature = app2.world_mut().query::<&Creature>();
    assert_eq!(q_creature.iter(&app2.world()).count(), 0, "Loaded entities must not have Creature");
}

fn system_type_driven_or_presence(
    mut pq: PersistentQuery<
        &Guid,
        bevy::prelude::Or<(bevy::prelude::With<Health>, bevy::prelude::With<Creature>)>
    >,
) {
    // Load entities that have Health OR Creature
    let _ = pq.ensure_loaded();
}

#[db_matrix_test]
fn test_type_driven_or_presence_filters() {
    let (db, _container) = setup();

    // GIVEN: one with Health, one with Creature, one with neither
    let mut app = App::new();
    app.add_plugins(PersistencePlugins(db.clone()));
    app.world_mut().spawn(Health { value: 10 });
    app.world_mut().spawn(Creature { is_screaming: false });
    app.world_mut().spawn(Position { x: 0.0, y: 0.0 });
    app.update();
    commit_sync(&mut app).expect("Initial commit failed");

    // WHEN: run a system that uses OR(With<Health>, With<Creature>) in F
    let mut app2 = App::new();
    app2.add_plugins(PersistencePlugins(db.clone()));
    app2.add_systems(bevy::prelude::Update, system_type_driven_or_presence);
    app2.update();

    // THEN: exactly two entities are loaded (those with Health OR Creature)
    let mut q_guid = app2.world_mut().query::<&Guid>();
    let count = q_guid.iter(&app2.world()).count();
    assert_eq!(count, 2, "OR presence should include entities with Health or Creature");
}

// System: With<Health> AND (Without<Creature> AND With<Position>)
fn system_nested_tuple_presence(
    mut pq: PersistentQuery<
        &Guid,
        (
            bevy::prelude::With<Health>,
            (bevy::prelude::Without<Creature>, bevy::prelude::With<Position>),
        ),
    >,
) {
    let _ = pq.ensure_loaded();
}

#[db_matrix_test]
fn test_nested_tuple_presence_and() {
    let (db, _container) = setup();
    // GIVEN: H+P, H+C, P-only
    let mut app = App::new();
    app.add_plugins(PersistencePlugins(db.clone()));
    app.world_mut().spawn((Health { value: 1 }, Position { x: 0.0, y: 0.0 }));
    app.world_mut().spawn((Health { value: 2 }, Creature { is_screaming: false }));
    app.world_mut().spawn(Position { x: 1.0, y: 1.0 });
    app.update();
    commit_sync(&mut app).expect("Initial commit failed");

    // WHEN
    let mut app2 = App::new();
    app2.add_plugins(PersistencePlugins(db.clone()));
    app2.add_systems(bevy::prelude::Update, system_nested_tuple_presence);
    app2.update();

    // THEN: only H+P passes (Health AND Position AND NOT Creature)
    let mut q_guid = app2.world_mut().query::<&Guid>();
    assert_eq!(q_guid.iter(&app2.world()).count(), 1);
    let mut q_health = app2.world_mut().query::<&Health>();
    assert_eq!(q_health.iter(&app2.world()).count(), 1);
    let mut q_position = app2.world_mut().query::<&Position>();
    assert_eq!(q_position.iter(&app2.world()).count(), 1);
    let mut q_creature = app2.world_mut().query::<&Creature>();
    assert_eq!(q_creature.iter(&app2.world()).count(), 0);
}

// System: With<Health> AND (With<Creature> OR With<Position> OR With<PlayerName>) AND Without<PlayerName>
fn system_and_or_mix_with_without(
    mut pq: PersistentQuery<
        &Guid,
        (
            bevy::prelude::With<Health>,
            bevy::prelude::Or<(
                bevy::prelude::With<Creature>,
                bevy::prelude::With<Position>,
                bevy::prelude::With<PlayerName>,
            )>,
            bevy::prelude::Without<PlayerName>,
        ),
    >,
) {
    let _ = pq.ensure_loaded();
}

#[db_matrix_test]
fn test_and_or_mix_with_without_filters() {
    let (db, _container) = setup();

    // GIVEN: H+C, H+P, H+C+PlayerName, H-only
    let mut app = App::new();
    app.add_plugins(PersistencePlugins(db.clone()));
    app.world_mut().spawn((Health { value: 1 }, Creature { is_screaming: false }));
    app.world_mut().spawn((Health { value: 2 }, Position { x: 0.0, y: 0.0 }));
    app.world_mut().spawn((Health { value: 3 }, Creature { is_screaming: true }, PlayerName { name: "X".into() }));
    app.world_mut().spawn(Health { value: 4 });
    app.update();
    commit_sync(&mut app).expect("Initial commit failed");

    // WHEN
    let mut app2 = App::new();
    app2.add_plugins(PersistencePlugins(db.clone()));
    app2.add_systems(bevy::prelude::Update, system_and_or_mix_with_without);
    app2.update();

    // THEN: H+C and H+P match (2 entities). H+C+PlayerName excluded by Without<PlayerName>, H-only excluded by OR
    let mut q_guid = app2.world_mut().query::<&Guid>();
    assert_eq!(q_guid.iter(&app2.world()).count(), 2);
    let mut q_player = app2.world_mut().query::<&PlayerName>();
    assert_eq!(q_player.iter(&app2.world()).count(), 0);
}

// System: Optional in Q with type-driven Without in F
fn system_optional_q_with_without(
    mut pq: PersistentQuery<(Option<&Creature>, &Health), bevy::prelude::Without<Creature>>,
) {
    let _ = pq.ensure_loaded();
}

#[db_matrix_test]
fn test_optional_q_with_without_exclusion() {
    let (db, _container) = setup();

    // GIVEN: H-only, H+Creature
    let mut app = App::new();
    app.add_plugins(PersistencePlugins(db.clone()));
    app.world_mut().spawn(Health { value: 1 });
    app.world_mut().spawn((Health { value: 2 }, Creature { is_screaming: false }));
    app.update();
    commit_sync(&mut app).expect("Initial commit failed");

    // WHEN: Q asks for Option<&Creature> but F excludes Creature
    let mut app2 = App::new();
    app2.add_plugins(PersistencePlugins(db.clone()));
    app2.add_systems(bevy::prelude::Update, system_optional_q_with_without);
    app2.update();

    // THEN: only H-only entity is loaded; no Creature present after load
    let mut q_health = app2.world_mut().query::<&Health>();
    assert_eq!(q_health.iter(&app2.world()).count(), 1);
    let mut q_creature = app2.world_mut().query::<&Creature>();
    assert_eq!(q_creature.iter(&app2.world()).count(), 0);
}

// System: OR with 3 arms
fn system_or_three_arms(
    mut pq: PersistentQuery<
        &Guid,
        bevy::prelude::Or<(bevy::prelude::With<Health>, bevy::prelude::With<Creature>, bevy::prelude::With<PlayerName>)>,
    >,
) {
    let _ = pq.ensure_loaded();
}

#[db_matrix_test]
fn test_or_presence_three_arms() {
    let (db, _container) = setup();

    // GIVEN: Health-only, Creature-only, PlayerName-only, Position-only
    let mut app = App::new();
    app.add_plugins(PersistencePlugins(db.clone()));
    app.world_mut().spawn(Health { value: 1 });
    app.world_mut().spawn(Creature { is_screaming: true });
    app.world_mut().spawn(PlayerName { name: "P".into() });
    app.world_mut().spawn(Position { x: 0.0, y: 0.0 });
    app.update();
    commit_sync(&mut app).expect("Initial commit failed");

    // WHEN
    let mut app2 = App::new();
    app2.add_plugins(PersistencePlugins(db.clone()));
    app2.add_systems(bevy::prelude::Update, system_or_three_arms);
    app2.update();

    // THEN: 3 entities loaded (H or C or Name)
    let mut q_guid = app2.world_mut().query::<&Guid>();
    assert_eq!(q_guid.iter(&app2.world()).count(), 3);
}

// Presence-only complex tree: ((Health AND Position) OR PlayerName) AND NOT Creature
#[db_matrix_test]
fn test_presence_expression_complex_and_or_not() {
    let (db, _container) = setup();

    // Seed various combinations:
    // E1: H+P -> match
    // E2: PlayerName -> match
    // E3: H only -> no (needs H+P or PlayerName)
    // E4: H+P+Creature -> excluded by NOT Creature
    // E5: PlayerName+Creature -> excluded by NOT Creature
    // E6: P only -> no
    // E7: H+P+PlayerName -> match
    let mut app_seed = App::new();
    app_seed.add_plugins(PersistencePlugins(db.clone()));
    app_seed.world_mut().spawn((Health { value: 1 }, Position { x: 0.0, y: 0.0 })); // E1
    app_seed.world_mut().spawn(PlayerName { name: "pn".into() }); // E2
    app_seed.world_mut().spawn(Health { value: 2 }); // E3
    app_seed.world_mut().spawn((Health { value: 3 }, Position { x: 1.0, y: 1.0 }, Creature { is_screaming: false })); // E4
    app_seed.world_mut().spawn((PlayerName { name: "pc".into() }, Creature { is_screaming: true })); // E5
    app_seed.world_mut().spawn(Position { x: 2.0, y: 2.0 }); // E6
    app_seed.world_mut().spawn((Health { value: 4 }, Position { x: 3.0, y: 3.0 }, PlayerName { name: "both".into() })); // E7
    app_seed.update();
    commit_sync(&mut app_seed).expect("seed commit failed");

    // App under test: presence-only complex expression
    let mut app = App::new();
    app.add_plugins(PersistencePlugins(db.clone()));

    fn sys(
        mut pq: PersistentQuery<
            &Guid,
            (
                bevy::prelude::Or<(
                    (bevy::prelude::With<Health>, bevy::prelude::With<Position>),
                    bevy::prelude::With<PlayerName>
                )>,
                bevy::prelude::Without<Creature>
            ),
        >
    ) {
        let _ = pq.ensure_loaded();
    }
    app.add_systems(bevy::prelude::Update, sys);
    app.update();

    // Expect E1, E2, E7 loaded => 3
    let mut q = app.world_mut().query::<&Guid>();
    let count = q.iter(&app.world()).count();
    assert_eq!(count, 3, "expected 3 entities for ((H AND P) OR PlayerName) AND NOT Creature");
}
