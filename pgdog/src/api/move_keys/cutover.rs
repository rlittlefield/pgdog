//! The cutover: park for the operator, pause the moving keys
//! fleet-wide, drain replication to zero, flip the keys' placement,
//! and invalidate every instance's cached translations.

use std::collections::HashSet;
use std::time::{Duration, Instant};

use pgdog_config::CutoverTimeoutAction;
use tokio::select;
use tokio::time::interval;
use tracing::{info, warn};

use super::guards::Preflight;
use super::{CutoverOutcome, MoveKeysStatus, MoveKeysTask};
use crate::api::MigrationError;
use crate::api::async_task::AsyncTaskContext;
use crate::api::cutover_registry;
use crate::backend::databases::{databases, invalidate_lookup_keys};
use crate::backend::fleet::barrier;
use crate::backend::fleet::{self, Coordinator, Discovery};
use crate::backend::key_move::{self, KeyMovePayload};
use crate::backend::pool::Request;
use crate::backend::replication::logical::Error;
use crate::backend::replication::logical::orchestrator::ReplicationWaiter;
use crate::config::config;
use crate::net::bind::Parameter;
use tokio_util::sync::CancellationToken;

/// Releases the keyed barrier — and the omni barrier riding along —
/// on drop, so every error path resumes writes. The omni barrier
/// covers the mapping table: it's omnisharded, and an application
/// write to it would race the flip.
struct BarrierGuard {
    database: String,
}

impl BarrierGuard {
    fn arm(database: &str, keys: &[String]) -> Self {
        barrier::start_keys(database, keys);
        barrier::start(database);
        Self {
            database: database.to_string(),
        }
    }
}

impl Drop for BarrierGuard {
    fn drop(&mut self) {
        barrier::stop_keys(&self.database);
        barrier::stop(&self.database);
    }
}

/// Why the park ended.
pub(super) enum Parked {
    /// An operator `CUTOVER` targeted this task.
    CutoverRequested,
    /// The task was cancelled, or the stream ended on its own: the
    /// task returns.
    Ended,
}

/// Entry: caught up, replication streaming. Exit: an operator
/// `CUTOVER` arrived, or the task ended (cancelled, or the stream
/// finished by itself). Failure: a stream error while parked
/// propagates.
pub(super) async fn park(
    task: &MoveKeysTask,
    ctx: &AsyncTaskContext<MoveKeysTask>,
    token: &CancellationToken,
    waiter: &mut ReplicationWaiter,
) -> Result<Parked, MigrationError> {
    ctx.set_status(MoveKeysStatus::AwaitingCutover);
    let cutover =
        cutover_registry::register_cutover(ctx.root_id(), &task.database, Some(task.target));

    select! {
        _ = token.cancelled() => {
            waiter.stop();
            waiter.wait().await?;
            Ok(Parked::Ended)
        }
        _ = cutover.requested() => Ok(Parked::CutoverRequested),
        res = waiter.wait() => {
            // The stream ended on its own: an error, or the slot
            // drained after a stop.
            res?;
            Ok(Parked::Ended)
        }
    }
}

/// Coordination failures fail the task in auto mode (no operator to
/// retry) and re-park it otherwise.
fn repark_or_fail(task: &MoveKeysTask, why: Error) -> Result<CutoverOutcome, MigrationError> {
    if task.auto_cutover {
        Err(why.into())
    } else {
        warn!("[move keys] {}; re-parking", why);
        Ok(CutoverOutcome::Aborted)
    }
}

/// Entry: caught up (or an operator forced it). Exit `Done`: the keys'
/// placement flipped and every instance heard about it. Exit
/// `Aborted`: every barrier — local and peers' — was released and
/// replication continues. The flip is the point of no return:
/// everything after it is best-effort and the cutover reports success.
pub(super) async fn run(
    task: &MoveKeysTask,
    ctx: &AsyncTaskContext<MoveKeysTask>,
    waiter: &mut ReplicationWaiter,
    preflight: &Preflight,
) -> Result<CutoverOutcome, MigrationError> {
    let scope = &preflight.scope;
    let mut keys = scope.keys().iter().cloned().collect::<Vec<_>>();
    keys.sort();

    // Other pgdog instances must pause these keys too. An instance
    // running an older config without this database isn't registered
    // on the medium: refuse rather than diverge.
    let fleet = databases()
        .schema_owner(&task.database)
        .map_err(Error::from)?;
    let coordination = match Coordinator::discover(key_move::TOPIC, &fleet, preflight.medium())
        .await
        .map_err(Error::from)?
    {
        Discovery::Solo => None,
        Discovery::Missing(missing) => {
            return repark_or_fail(task, Error::InstancesNotRegistered(missing));
        }
        Discovery::Ready(mut coordination) => {
            let payload = KeyMovePayload {
                keys: keys.clone(),
                source: scope.source(),
                target: scope.target(),
            };
            coordination.set_payload(payload.to_json().map_err(Error::from)?);
            Some(*coordination)
        }
    };

    ctx.set_status(MoveKeysStatus::Draining);
    let barrier = BarrierGuard::arm(&task.database, &keys);

    // Arm every peer and wait for their acks before draining: until
    // they all park the keys, the drain can't converge.
    if let Some(coordination) = &coordination {
        fleet::protocol::ensure_tables(coordination.medium())
            .await
            .map_err(Error::from)?;
        match coordination
            .broadcast_and_await(key_move::STATE_ARMED, key_move::ARM_ACK_TIMEOUT)
            .await
            .map_err(Error::from)?
        {
            None => {}
            Some(stragglers) => {
                coordination.publish(key_move::STATE_RELEASED).await;
                drop(barrier);
                return repark_or_fail(task, Error::InstancesNotArmed(stragglers));
            }
        }
    }

    // The peers' abandoned-coordinator failsafe measures the armed
    // row's age: keep it fresh for as long as we hold their barriers,
    // however long the drain and stream shutdown take.
    let refresh = coordination
        .as_ref()
        .map(|coordination| coordination.keep_fresh(key_move::STATE_ARMED));

    if !drain(waiter).await {
        drop(refresh);
        if let Some(coordination) = &coordination {
            coordination.publish(key_move::STATE_RELEASED).await;
        }
        drop(barrier);
        return repark_or_fail(task, Error::AbortTimeout);
    }

    // Stop streaming and join the stream tasks: the real
    // drain-to-zero. Slots are dropped as the streams wind down.
    waiter.stop();
    let stopped = waiter.wait().await;
    drop(refresh);
    if let Err(err) = stopped {
        if let Some(coordination) = &coordination {
            coordination.publish(key_move::STATE_RELEASED).await;
        }
        return Err(err.into());
    }

    // Flip the placement on every shard: the mapping table is
    // omnisharded, so each shard's copy must agree. The first
    // successful flip is the point of no return; a partial failure is
    // reverted best-effort and the cutover aborts.
    ctx.set_status(MoveKeysStatus::Flipping);
    if let Err(err) = flip(task, &keys, scope.target()).await {
        warn!(
            "[move keys] placement flip failed: {}; reverting to shard {}",
            err,
            scope.source()
        );
        if let Err(revert) = flip(task, &keys, scope.source()).await {
            warn!(
                "[move keys] the revert failed too: {}; run the move_query by hand \
                 with $2 = {} for every key to restore a consistent mapping",
                revert,
                scope.source()
            );
        }
        if let Some(coordination) = &coordination {
            coordination.publish(key_move::STATE_RELEASED).await;
        }
        drop(barrier);
        return Err(err);
    }

    // The flip stands. Local statements re-route the moment the
    // barrier drops: the invalidated cache re-runs the lookups.
    invalidate_lookup_keys(&task.database, &keys);
    drop(barrier);

    info!(
        "[move keys] {} key(s) of \"{}\" now live on shard {}",
        keys.len(),
        task.database,
        scope.target()
    );

    // Peers invalidate and resume on the activation. Stragglers only
    // warn: their stale cache entries still point at the source, whose
    // rows exist until cleanup, and the silence failsafe releases
    // their barriers.
    if let Some(coordination) = &coordination {
        match coordination
            .broadcast_and_await(key_move::STATE_ACTIVATED, key_move::ACTIVATE_ACK_TIMEOUT)
            .await
        {
            Ok(None) => {}
            Ok(Some(stragglers)) => warn!(
                "[move keys] instance(s) [{}] haven't invalidated their lookup caches yet; \
                 their next statements re-route once they do",
                stragglers
            ),
            Err(err) => warn!(
                "[move keys] failed to signal the flip to the peers: {}; \
                 their silence failsafe resumes writes and their caches refresh on the next miss",
                err
            ),
        }
    }

    Ok(CutoverOutcome::Done)
}

/// Run every distinct `move_query` for every key on every shard
/// primary, setting the keys' placement to `shard`.
async fn flip(task: &MoveKeysTask, keys: &[String], shard: usize) -> Result<(), MigrationError> {
    // Re-resolve: a reload may have replaced the cluster since the
    // preflight.
    let cluster = databases()
        .schema_owner(&task.database)
        .map_err(Error::from)?;

    let move_queries = cluster
        .sharded_tables()
        .iter()
        .filter_map(|table| table.move_query.clone())
        .collect::<HashSet<_>>();

    let mut shard_number = shard.to_string();
    shard_number.shrink_to_fit();

    for (number, cluster_shard) in cluster.shards().iter().enumerate() {
        let mut server = cluster_shard
            .primary(&Request::default())
            .await
            .map_err(Error::from)?;
        for query in &move_queries {
            for key in keys {
                let params = [
                    Parameter::new(key.as_bytes()),
                    Parameter::new(shard_number.as_bytes()),
                ];
                server
                    .fetch_all_params::<crate::net::messages::DataRow>(query, &params)
                    .await
                    .map_err(|err| {
                        Error::Lookup(format!(
                            "move_query failed on shard {} for key \"{}\": {}",
                            number, key, err
                        ))
                    })?;
            }
        }
    }

    Ok(())
}

/// Drain replication to zero while the keys' writes are parked.
/// `false` means the drain timed out with the `abort` action.
async fn drain(waiter: &ReplicationWaiter) -> bool {
    let general = &config().config.general;
    let threshold = general.cutover_replication_lag_threshold;
    let last_transaction_delay = Duration::from_millis(general.cutover_last_transaction_delay);
    let timeout = Duration::from_millis(general.cutover_timeout);
    let timeout_action = general.cutover_timeout_action;

    // In-flight writes for the moving keys finish and replicate; new
    // ones park at the barrier. Other keys' WAL doesn't hold the lag
    // up — the filter skips it and keepalives advance.
    let started = Instant::now();
    let mut check = interval(Duration::from_millis(50));
    loop {
        check.tick().await;

        let lag = waiter.lag().await;
        let last_transaction = waiter.last_transaction().await;
        let drained = lag.is_some_and(|lag| lag <= threshold)
            && last_transaction.is_none_or(|last| last > last_transaction_delay);

        if drained {
            return true;
        }

        if started.elapsed() > timeout {
            match timeout_action {
                CutoverTimeoutAction::Abort => {
                    warn!(
                        "[move keys] drain timed out after {:?}; resuming writes",
                        timeout
                    );
                    return false;
                }
                CutoverTimeoutAction::Cutover => return true,
            }
        }
    }
}
