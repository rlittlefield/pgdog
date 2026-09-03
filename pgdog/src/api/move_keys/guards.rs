//! The Validating phase: everything the task refuses on, and
//! everything it holds until it ends.

use std::sync::Arc;

use super::MoveKeysTask;
use crate::api::MigrationError;
use crate::api::topology_guard::TopologyGuard;
use crate::backend::Cluster;
use crate::backend::databases::{databases, medium_cluster};
use crate::backend::pool;
use crate::backend::replication::logical::Error;
use crate::backend::replication::logical::move_keys::{
    KeyMoveScope, enumerate_tables, move_lock, placement_by_lookup, replica_identity_covers_key,
    target_is_clean,
};
use crate::config::config;
use crate::frontend::router::parser::Shard;
use crate::frontend::router::sharding::lookup;

/// Shuts the caller-owned medium cluster down on drop.
struct MediumGuard(Cluster);

impl Drop for MediumGuard {
    fn drop(&mut self) {
        self.0.shutdown();
    }
}

/// Everything `run()` holds for the task's lifetime, acquired in the
/// Validating phase. Dropping it returns the cross-instance advisory
/// lock's connection, shuts the medium cluster down (closing that
/// lock's session even if this future was hard-aborted by a
/// cancellation timeout), and frees the in-flight-topology slot —
/// field order is drop order.
pub(super) struct Preflight {
    /// The serving cluster whose keys move.
    pub(super) source: Cluster,
    /// What moves: the keys (canonical), the source and target shards,
    /// and the tables.
    pub(super) scope: Arc<KeyMoveScope>,
    /// Session-scoped `pg_try_advisory_lock` on shard 0: which pgdog
    /// instance runs this MOVE KEYS.
    _lock: pool::Guard,
    /// The caller-owned, launched, non-serving one-shard cluster on
    /// shard 0: the advisory lock's session and the fleet coordination
    /// medium, without pinning a serving pool's connection for the
    /// task's lifetime.
    medium: MediumGuard,
    _topology: TopologyGuard,
}

impl Preflight {
    pub(super) fn medium(&self) -> &Cluster {
        &self.medium.0
    }
}

/// Entry: nothing held. Exit: every sharded table places by lookup
/// with a `move_query`, every key resolves to the same source shard
/// (not the target), every moving table's replica identity covers the
/// sharding column, the target holds no rows for the keys, and this
/// instance holds both the local topology slot and the cross-instance
/// advisory lock. Failure: everything acquired so far is released by
/// drop, and the task fails with the guard's error.
pub(super) async fn preflight(task: &MoveKeysTask) -> Result<Preflight, MigrationError> {
    let topology = TopologyGuard::acquire(&task.database)?;

    let source = databases()
        .schema_owner(&task.database)
        .map_err(Error::from)?;

    placement_by_lookup(&source)?;

    let shards = source.shards().len();
    if task.target >= shards {
        return Err(Error::KeyMoveTargetOutOfRange {
            target: task.target,
            shards,
        }
        .into());
    }

    let source_shard = resolve_source_shard(task, &source).await?;

    // The tables whose rows move, resolved on the source shard.
    let omnisharded = config()
        .config
        .omnisharded_tables
        .iter()
        .find(|tables| tables.database == task.database)
        .map(|tables| tables.tables.clone())
        .unwrap_or_default();
    let tables = enumerate_tables(&source, source_shard, &omnisharded).await?;
    let scope = Arc::new(KeyMoveScope::new(
        &task.keys,
        source_shard,
        task.target,
        tables,
    )?);

    // DELETE and identity-only UPDATE events carry only identity
    // columns: without coverage the WAL filter can't see the key.
    replica_identity_covers_key(&source, source_shard, scope.tables()).await?;

    // Leftovers from a crashed prior attempt would collide with the
    // copy.
    target_is_clean(&source, &scope).await?;

    // Cross-instance mutex, arbitrated by shard 0 of the database.
    // Session-scoped: held (checked out) in the Preflight until the
    // task ends; the medium shutdown that follows it in drop order
    // closes the session even if the connection was returned to the
    // pool.
    let medium = MediumGuard(medium_cluster(&task.database).map_err(Error::from)?);
    let lock = move_lock(&medium.0).await?;

    Ok(Preflight {
        source,
        scope,
        _lock: lock,
        medium,
        _topology: topology,
    })
}

/// Resolve every key through the configured lookup: all must live on
/// the same shard, and that shard must not be the target.
async fn resolve_source_shard(
    task: &MoveKeysTask,
    source: &Cluster,
) -> Result<usize, MigrationError> {
    let schema = source.sharding_schema();
    let rule = source
        .sharded_tables()
        .first()
        .cloned()
        .ok_or(Error::KeyMoveNoTables)?;
    let cache = schema.tables.lookup_cache();

    let mut source_shard: Option<(String, usize)> = None;
    for key in &task.keys {
        let translated = lookup::resolve_for_table(source, cache, &rule, key)
            .await
            .map_err(|response| Error::Lookup(response.message))?;
        let shard = match lookup::parse_shard_index(translated.as_ref(), schema.shards)
            .map_err(|err| Error::Lookup(err.to_string()))?
        {
            Shard::Direct(shard) => shard,
            other => {
                return Err(Error::Lookup(format!(
                    "key \"{}\" resolved to {:?} instead of one shard",
                    key, other
                ))
                .into());
            }
        };

        if shard == task.target {
            return Err(Error::KeyAlreadyOnTarget {
                key: key.clone(),
                shard,
            }
            .into());
        }
        match &source_shard {
            None => source_shard = Some((key.clone(), shard)),
            Some((_, expected)) if *expected == shard => {}
            Some((_, expected)) => {
                return Err(Error::KeysSpanShards {
                    key: key.clone(),
                    expected: *expected,
                    got: shard,
                }
                .into());
            }
        }
    }

    source_shard
        .map(|(_, shard)| shard)
        .ok_or_else(|| Error::Lookup("no keys to move".into()).into())
}
