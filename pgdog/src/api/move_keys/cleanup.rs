//! Deleting the moved rows: from the source shard once the flip
//! stands, or from the target shard when the task ends before
//! flipping. Children delete before parents, so foreign keys between
//! moving tables don't block the sweep.

use tracing::{info, warn};

use super::MoveKeysTask;
use super::guards::Preflight;
use crate::backend::Cluster;
use crate::backend::databases::invalidate_lookup_keys;
use crate::backend::pool::Request;
use crate::backend::replication::logical::Error;
use crate::backend::replication::logical::move_keys::{
    KeyMoveScope, MoveTable, dependency_order, table_references,
};
use crate::util::safe_sleep;
use std::time::Duration;

const DELETE_ATTEMPTS: usize = 3;
const DELETE_RETRY_DELAY: Duration = Duration::from_secs(2);

/// Entry: the flip stands and the fleet acked (or was warned). Exit:
/// the moved rows are gone from the source shard. Failures warn with
/// the exact recovery SQL and never fail the task: the rows are inert
/// — nothing routes to them — but they occupy space and would block a
/// future move back.
pub(super) async fn source(task: &MoveKeysTask, preflight: &Preflight) {
    // Second invalidation pass: a lookup that started before the flip
    // could have re-cached the old placement after the first pass.
    // Reads were correct either way (the source rows still exist);
    // this closes the race before they stop existing.
    let keys = preflight.scope.keys().iter().cloned().collect::<Vec<_>>();
    invalidate_lookup_keys(&task.database, &keys);

    match delete_rows(
        &preflight.source,
        preflight.scope.source(),
        &preflight.scope,
    )
    .await
    {
        Ok(deleted) => info!(
            "[move keys] deleted {} row(s) from source shard {}",
            deleted,
            preflight.scope.source()
        ),
        Err(err) => warn!(
            "[move keys] could not delete the moved rows from source shard {}: {}; \
             delete them by hand (children first): {}",
            preflight.scope.source(),
            err,
            recovery_sql(&preflight.scope),
        ),
    }
}

/// Entry: the task ended before the flip. Exit: any rows the copy
/// placed on the target are gone, so a retry starts clean. Failures
/// warn with the exact recovery SQL: the preflight's residue check
/// refuses the next attempt until they're gone.
pub(super) async fn scrub_target(preflight: &Preflight) {
    match delete_rows(
        &preflight.source,
        preflight.scope.target(),
        &preflight.scope,
    )
    .await
    {
        Ok(0) => {}
        Ok(deleted) => info!(
            "[move keys] scrubbed {} copied row(s) from target shard {}",
            deleted,
            preflight.scope.target()
        ),
        Err(err) => warn!(
            "[move keys] could not scrub the copied rows from target shard {}: {}; \
             delete them by hand (children first): {}",
            preflight.scope.target(),
            err,
            recovery_sql(&preflight.scope),
        ),
    }
}

/// The manual recovery statements, children first.
fn recovery_sql(scope: &KeyMoveScope) -> String {
    let order = delete_order(scope.tables().to_vec(), &[]);
    order
        .iter()
        .map(|table| {
            format!(
                "DELETE FROM \"{}\".\"{}\" WHERE {}",
                table.schema,
                table.name,
                scope.predicate_sql(&table.sharding_column)
            )
        })
        .collect::<Vec<_>>()
        .join("; ")
}

/// Delete the scope's rows from one shard, in one transaction, with
/// retries. Returns the number of rows deleted.
async fn delete_rows(cluster: &Cluster, shard: usize, scope: &KeyMoveScope) -> Result<u64, Error> {
    let mut attempt = 0;
    loop {
        match delete_rows_once(cluster, shard, scope).await {
            Ok(deleted) => return Ok(deleted),
            Err(err) => {
                attempt += 1;
                if attempt >= DELETE_ATTEMPTS {
                    return Err(err);
                }
                warn!(
                    "[move keys] delete attempt {}/{} failed: {}; retrying",
                    attempt, DELETE_ATTEMPTS, err
                );
                safe_sleep(DELETE_RETRY_DELAY).await;
            }
        }
    }
}

async fn delete_rows_once(
    cluster: &Cluster,
    shard: usize,
    scope: &KeyMoveScope,
) -> Result<u64, Error> {
    let mut server = cluster
        .shards()
        .get(shard)
        .ok_or(crate::backend::pool::Error::NoShard(shard))?
        .primary(&Request::default())
        .await?;

    // Foreign keys between moving tables force an order: children
    // before parents. Keys referenced from outside the moving set
    // aren't our rows to delete and fail loudly instead.
    let references = table_references(&mut server).await?;
    let order = delete_order(scope.tables().to_vec(), &references);

    server.execute("BEGIN").await?;
    let mut deleted = 0u64;
    for table in &order {
        let sql = format!(
            "WITH gone AS (DELETE FROM \"{}\".\"{}\" WHERE {} RETURNING 1) \
             SELECT COUNT(*) FROM gone",
            table.schema,
            table.name,
            scope.predicate_sql(&table.sharding_column)
        );
        let result = server.fetch_all::<i64>(sql.as_str()).await;
        match result {
            Ok(count) => deleted += count.first().copied().unwrap_or(0) as u64,
            Err(err) => {
                let _ = server.execute("ROLLBACK").await;
                return Err(err.into());
            }
        }
    }
    server.execute("COMMIT").await?;

    Ok(deleted)
}

/// Children delete before their parents.
fn delete_order(
    tables: Vec<MoveTable>,
    references: &[(String, String, String, String)],
) -> Vec<MoveTable> {
    dependency_order(
        tables,
        |table| (table.schema.clone(), table.name.clone()),
        references,
        false,
    )
}

#[cfg(test)]
mod test {
    use super::*;
    use pgdog_config::DataType;

    fn table(name: &str) -> MoveTable {
        MoveTable {
            schema: "public".into(),
            name: name.into(),
            sharding_column: "tenant_id".into(),
            data_type: DataType::Bigint,
        }
    }

    fn edge(child: &str, parent: &str) -> (String, String, String, String) {
        (
            "public".into(),
            child.into(),
            "public".into(),
            parent.into(),
        )
    }

    #[test]
    fn test_delete_order_children_first() {
        // orders -> tenants, line_items -> orders.
        let tables = vec![table("tenants"), table("orders"), table("line_items")];
        let references = vec![edge("orders", "tenants"), edge("line_items", "orders")];

        let order = delete_order(tables.clone(), &references)
            .into_iter()
            .map(|table| table.name)
            .collect::<Vec<_>>();
        assert_eq!(order, vec!["line_items", "orders", "tenants"]);

        // Foreign keys to tables outside the moving set don't affect
        // the order.
        let references = vec![edge("orders", "users")];
        let order = delete_order(tables, &references)
            .into_iter()
            .map(|table| table.name)
            .collect::<Vec<_>>();
        assert_eq!(order, vec!["tenants", "orders", "line_items"]);
    }

    #[test]
    fn test_delete_order_cycle_falls_back() {
        // a -> b -> a: no valid order; declaration order is the
        // fallback and every table still appears exactly once.
        let tables = vec![table("a"), table("b")];
        let references = vec![edge("a", "b"), edge("b", "a")];

        let order = delete_order(tables, &references);
        assert_eq!(order.len(), 2);
    }
}
