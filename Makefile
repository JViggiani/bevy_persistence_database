.PHONY: test test-relationships

# Run the full test suite across both relationship backends.
# The default build covers the Bevy-native relationship path; the feature build
# covers the bevy_many_relationships path — unit tests filtered out of the
# second pass since they don't vary by feature flag.
test:
	cargo test
	cargo test --features bevy_many_relationship_edges --test integration_tests

# Run only relationship tests across both backends — fast feedback when working
# on relationship code.
test-relationships:
	cargo test --test integration_tests relationships::
	cargo test --features bevy_many_relationship_edges --test integration_tests relationships::
