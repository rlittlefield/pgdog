//! A cross-shard EXPLAIN streams every shard's plan text. The inner
//! statement's result processing (aggregates, ORDER BY, DISTINCT,
//! LIMIT/OFFSET) must not be applied to the plan lines: merging them as
//! the inner statement's count used to fail with "cannot cast from text to
//! bigint" and close the client connection.

use crate::net::{
    CommandComplete, Parameters, Query,
    messages::{DataRow, FromBytes, ToBytes},
};

use super::{change_config, prelude::*};

fn data_rows(messages: &[crate::net::messages::Message]) -> usize {
    messages.iter().filter(|m| m.code() == 'D').count()
}

fn plan_lines(messages: &[crate::net::messages::Message]) -> Vec<String> {
    messages
        .iter()
        .filter(|m| m.code() == 'D')
        .map(|m| {
            let row = DataRow::from_bytes(m.to_bytes()).unwrap();
            row.get::<String>(0, crate::net::Format::Text)
                .unwrap_or_default()
        })
        .collect()
}

async fn explain(client: &mut TestClient, sql: &str) -> Vec<crate::net::messages::Message> {
    client.send_simple(Query::new(sql)).await;
    let messages = client
        .read_until('Z')
        .await
        .unwrap_or_else(|err| panic!("{sql}: {err:?}"));
    assert_eq!(
        messages.iter().filter(|m| m.code() == 'T').count(),
        1,
        "{sql}: one row description: {messages:?}"
    );
    assert!(
        data_rows(&messages) >= 2,
        "{sql}: a plan from every shard: {messages:?}"
    );
    let cc = messages
        .iter()
        .find(|m| m.code() == 'C')
        .expect("command complete");
    assert_eq!(
        CommandComplete::try_from(cc.clone()).unwrap().command(),
        "EXPLAIN",
        "{sql}"
    );
    messages
}

#[tokio::test]
async fn test_cross_shard_explain_streams_every_plan() {
    let mut client = TestClient::new_sharded(Parameters::default()).await;

    for sql in [
        "EXPLAIN SELECT count(*) FROM sharded",
        "EXPLAIN (ANALYZE) SELECT count(*), max(id) FROM sharded",
        "EXPLAIN SELECT count(*) FROM sharded GROUP BY value",
        "EXPLAIN SELECT * FROM sharded ORDER BY id LIMIT 1 OFFSET 1",
        "EXPLAIN SELECT DISTINCT value FROM sharded",
        "EXPLAIN (FORMAT JSON) SELECT count(*) FROM sharded",
    ] {
        let messages = explain(&mut client, sql).await;
        // Every plan line is forwarded as the shard produced it.
        assert!(
            plan_lines(&messages)
                .iter()
                .all(|line| !line.trim().is_empty()),
            "{sql}: {messages:?}"
        );

        // The connection is still usable.
        client.send_simple(Query::new("SELECT 1")).await;
        let messages = client.read_until('Z').await.unwrap();
        assert_eq!(data_rows(&messages), 1, "{sql}: {messages:?}");
    }
}

#[tokio::test]
async fn test_cross_shard_expanded_explain_annotates_once() {
    let mut client = TestClient::new_sharded(Parameters::default()).await;
    change_config(|g| g.expanded_explain = true);

    let messages = explain(&mut client, "EXPLAIN SELECT count(*) FROM sharded").await;
    let lines = plan_lines(&messages);
    assert_eq!(
        lines
            .iter()
            .filter(|l| l.contains("PgDog Routing:"))
            .count(),
        1,
        "{lines:?}"
    );
    assert!(lines.iter().any(|l| l.contains("shard=all")), "{lines:?}");

    change_config(|g| g.expanded_explain = false);
}
