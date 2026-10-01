use bytes::Bytes;

use super::setup::*;
use crate::frontend::router::parser::Shard;

#[test]
fn test_write_function_advisory_lock() {
    let mut test = QueryParserTest::new();

    let command = test.execute(vec![Query::new("SELECT pg_advisory_lock(123)").into()]);

    assert!(command.route().is_write());
}

/// Test every variant of pg_advisory "class" functions to ensure that they
/// all correctly deterministically hash to a Shard # based on the
/// lock ID specified.
#[test]
fn test_advisory_lock_routes_by_lock_id() {
    use crate::frontend::router::parser::route::{OverrideReason, ShardSource};

    let lock_on_shard_0 = 606;
    let lock_on_shard_1 = 505;

    let functions = [
        "pg_advisory_lock",
        "pg_advisory_lock_shared",
        "pg_try_advisory_lock",
        "pg_try_advisory_lock_shared",
        "pg_advisory_xact_lock",
        "pg_advisory_xact_lock_shared",
        "pg_try_advisory_xact_lock",
        "pg_try_advisory_xact_lock_shared",
        "pg_advisory_unlock",
    ];

    for function in functions {
        for (lock, shard) in [(lock_on_shard_0, 0), (lock_on_shard_1, 1)] {
            let mut test = QueryParserTest::new();
            let command = test.execute(vec![
                Query::new(format!("SELECT {function}({lock})")).into(),
            ]);
            let route = command.route();

            assert_eq!(route.shard(), &Shard::Direct(shard));
            assert_eq!(
                route.shard_with_priority().source(),
                &ShardSource::Override(OverrideReason::AdvisoryLock)
            );
        }
    }
}

/// A lock id the parser can't compute (a column reference, an expression
/// over unnest) used to hash as 0 and route to shard 0, fighting the
/// client's pin. With a direct pin from `SET pgdog.shard` or a comment,
/// the lock follows the pin; a resolvable id keeps the hash routing.
#[test]
fn test_advisory_lock_unresolvable_id_follows_the_pin() {
    use crate::frontend::router::parser::route::{OverrideReason, ShardSource};

    let unresolvable = "SELECT pg_advisory_xact_lock(1, ordered.key) \
         FROM (SELECT key FROM unnest(ARRAY[10, 20]) AS key ORDER BY key) AS ordered";

    let mut test = QueryParserTest::new().with_param("pgdog.shard", "1");
    let command = test.execute(vec![Query::new(unresolvable).into()]);
    let route = command.route();
    assert_eq!(route.shard(), &Shard::Direct(1));
    assert_eq!(route.shard_with_priority().source(), &ShardSource::Set);
    assert!(route.is_write());

    let mut test = QueryParserTest::new();
    let command = test.execute(vec![
        Query::new(format!("/* pgdog_shard: 1 */ {unresolvable}")).into(),
    ]);
    assert_eq!(command.route().shard(), &Shard::Direct(1));
    assert_eq!(
        command.route().shard_with_priority().source(),
        &ShardSource::Comment
    );

    // A resolvable id still hashes, pin or not.
    let mut test = QueryParserTest::new().with_param("pgdog.shard", "1");
    let command = test.execute(vec![Query::new("SELECT pg_advisory_xact_lock(606)").into()]);
    assert_eq!(command.route().shard(), &Shard::Direct(0));
    assert_eq!(
        command.route().shard_with_priority().source(),
        &ShardSource::Override(OverrideReason::AdvisoryLock)
    );

    // Without a pin the unresolvable id is not the client's problem to route.
    let mut test = QueryParserTest::new();
    let command = test.execute(vec![Query::new(unresolvable).into()]);
    assert!(command.route().is_write());
}
#[test]
fn test_write_functions_prepared() {
    let mut test = QueryParserTest::new();
    let command = test.execute(vec![
        Parse::named("test", "SELECT pg_advisory_lock($1) IS NOT NULL").into(),
        Bind::new_params(
            "test",
            &[crate::net::bind::Parameter {
                len: 4,
                data: Bytes::from(b"1234".to_vec()),
            }],
        )
        .into(),
    ]);
    assert!(command.route().is_write());
    assert_eq!(command.route().shard(), &Shard::Direct(0));
}

#[test]
fn test_write_function_nextval() {
    let mut test = QueryParserTest::new();

    let command = test.execute(vec![Query::new("SELECT nextval('234')").into()]);

    assert!(command.route().is_write());
}

#[test]
fn test_cross_shard_install_sharded_sequence() {
    let mut test = QueryParserTest::new();

    let command = test.execute(vec![
        Query::new("SELECT pgdog.install_sharded_sequence('foo', 'id')").into(),
    ]);

    assert!(command.route().is_cross_shard());
}

#[test]
fn test_install_sharded_sequence_without_schema_not_cross_shard() {
    // Without the `pgdog.` schema qualifier we should not flag the call
    // as a cross-shard function — it could be any user-defined function.
    let mut test = QueryParserTest::new();

    let command = test.execute(vec![
        Query::new("SELECT install_sharded_sequence('foo', 'id')").into(),
    ]);

    assert!(!command.route().is_cross_shard());
}
