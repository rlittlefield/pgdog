use crate::frontend::router::parser::Shard;
use crate::net::messages::bind::Parameter;

use super::setup::*;

#[test]
fn test_insert_numeric() {
    let mut test = QueryParserTest::new();

    let command =
        test.execute(vec![Query::new(
        "INSERT INTO sharded (id, sample_numeric) VALUES (2, -987654321.123456789::NUMERIC)",
    )
    .into()]);

    assert!(command.route().is_write());
    assert!(matches!(command.route().shard(), Shard::Direct(_)));
}

#[test]
fn test_insert_negative_sharding_key() {
    let mut test = QueryParserTest::new();

    let command = test.execute(vec![
        Query::new("INSERT INTO sharded (id) VALUES (-5)").into(),
    ]);

    assert!(command.route().is_write());
    assert!(matches!(command.route().shard(), Shard::Direct(_)));
}

#[test]
fn test_insert_with_cast_on_sharding_key() {
    let mut test = QueryParserTest::new();

    let command = test.execute(vec![
        Query::new("INSERT INTO sharded (id, value) VALUES (42::BIGINT, 'test')").into(),
    ]);

    assert!(command.route().is_write());
    assert!(matches!(command.route().shard(), Shard::Direct(_)));
}

#[test]
fn test_insert_multi_row() {
    let mut test = QueryParserTest::new();

    // Multi-row inserts go to all shards (split by query engine later)
    let command = test.execute(vec![
        Parse::named(
            "__test_multi",
            "INSERT INTO sharded (id, value) VALUES ($1, 'a'), ($2, 'b')",
        )
        .into(),
        Bind::new_params(
            "__test_multi",
            &[Parameter::new(b"0"), Parameter::new(b"2")],
        )
        .into(),
        Execute::new().into(),
        Sync.into(),
    ]);

    assert!(command.route().is_write());
    assert!(matches!(command.route().shard(), Shard::All));
}

#[test]
fn test_insert_select() {
    let mut test = QueryParserTest::new();

    let command = test.execute(vec![
        Query::new(
            "INSERT INTO sharded (id, value) SELECT id, value FROM other_table WHERE id = 1",
        )
        .into(),
    ]);

    assert!(command.route().is_write());
    assert!(command.route().shard().is_direct());
}

#[test]
fn test_insert_default_values() {
    let mut test = QueryParserTest::new();

    let command = test.execute(vec![
        Query::new("INSERT INTO sharded DEFAULT VALUES").into(),
    ]);

    assert!(command.route().is_write());
    assert!(command.route().shard().is_direct());
}

/// The round-robin fallback for a keyless insert on a plain sharded
/// table says what happened instead of claiming a key matched.
#[test]
fn test_insert_round_robin_explain_label() {
    let mut test = QueryParserTest::new().with_expanded_explain();

    let command = test.execute(vec![
        Query::new("EXPLAIN INSERT INTO sharded (value) VALUES ('no key')").into(),
    ]);

    assert!(matches!(command.route().shard(), Shard::Direct(_)));
    let lines = command.route().explain().unwrap().render_lines();
    assert!(
        lines
            .iter()
            .any(|l| l.contains("INSERT without a sharding key, shard picked round-robin")),
        "explain must report the fallback: {:?}",
        lines
    );
}
