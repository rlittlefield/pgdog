//! The Validating phase: everything the task refuses on, and
//! everything it holds until it ends.

use std::collections::HashSet;
use std::sync::LazyLock;
use std::time::Duration;

use parking_lot::Mutex;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio::time::{MissedTickBehavior, interval, timeout};
use tokio_util::sync::CancellationToken;
use tracing::error;

use super::AddShardTask;
use crate::api::MigrationError;
use crate::backend::Cluster;
use crate::backend::databases::{databases, provisioning_cluster};
use crate::backend::pool;
use crate::backend::replication::logical::Error;
use crate::backend::replication::logical::add_shard::{
    destination_is_empty, placement_stable, provisioning_lock,
};
use crate::config::config;

/// Databases with a topology change (ADD SHARD) in flight *in this
/// process*: a cheap refusal for a second concurrent command here.
/// Cross-instance exclusion is the advisory lock on the new shard
/// (see [`provisioning_lock`]), watched by the [`LockWatchdog`].
static TOPOLOGY_TASKS: LazyLock<Mutex<HashSet<String>>> = LazyLock::new(Default::default);

/// Removes the database from the in-flight topology registry on drop.
struct TopologyGuard {
    database: String,
}

impl TopologyGuard {
    fn acquire(database: &str) -> Result<Self, Error> {
        let mut tasks = TOPOLOGY_TASKS.lock();
        if !tasks.insert(database.to_string()) {
            return Err(Error::TopologyChangeInProgress(database.to_string()));
        }
        Ok(Self {
            database: database.to_string(),
        })
    }
}

impl Drop for TopologyGuard {
    fn drop(&mut self) {
        TOPOLOGY_TASKS.lock().remove(&self.database);
    }
}

/// Shuts the caller-owned provisioning cluster down on drop.
struct DestinationGuard(Cluster);

impl Drop for DestinationGuard {
    fn drop(&mut self) {
        self.0.shutdown();
    }
}

/// How often the provisioning lock's session is probed while held,
/// and how long one probe may take before the session is presumed
/// dead.
const LOCK_PROBE_INTERVAL: Duration = Duration::from_secs(10);

/// Owns and watches the session holding the cross-instance advisory
/// lock. The lock is session-scoped and Postgres never notifies a
/// holder that lost it: the session dying (killed backend, network
/// partition, server restart) releases the lock silently while this
/// task keeps running, letting another instance acquire it. The
/// watchdog probes the session continuously — "session alive" is
/// exactly "lock still held" — and cancels `lost` the moment a probe
/// fails or times out. The long phases select on `lost`, and the
/// cutover requests a fresh on-demand probe right before the point of
/// no return.
struct LockWatchdog {
    lost: CancellationToken,
    probe: mpsc::Sender<oneshot::Sender<bool>>,
    watcher: JoinHandle<()>,
}

impl LockWatchdog {
    fn spawn(mut lock: pool::Guard, database: &str) -> Self {
        let lost = CancellationToken::new();
        let (probe, mut requests) = mpsc::channel::<oneshot::Sender<bool>>(1);
        let trip = lost.clone();
        let database = database.to_string();
        let watcher = tokio::spawn(async move {
            let mut tick = interval(LOCK_PROBE_INTERVAL);
            tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
            loop {
                let reply = tokio::select! {
                    _ = tick.tick() => None,
                    request = requests.recv() => match request {
                        Some(reply) => Some(reply),
                        // The Preflight was dropped: the task ended.
                        None => return,
                    },
                };
                let alive = matches!(
                    timeout(LOCK_PROBE_INTERVAL, lock.execute("SELECT 1")).await,
                    Ok(Ok(_))
                );
                if let Some(reply) = reply {
                    let _ = reply.send(alive);
                }
                if !alive {
                    error!(
                        "[add shard] the provisioning lock's session for \"{}\" died; \
                         the lock is released and another instance may take it",
                        database
                    );
                    trip.cancel();
                    return;
                }
            }
        });
        Self {
            lost,
            probe,
            watcher,
        }
    }

    /// Probe the session right now and wait for the verdict.
    async fn ensure_held(&self) -> Result<(), Error> {
        if self.lost.is_cancelled() {
            return Err(Error::ProvisioningLockLost);
        }
        let (reply, verdict) = oneshot::channel();
        if self.probe.send(reply).await.is_err() {
            return Err(Error::ProvisioningLockLost);
        }
        match verdict.await {
            Ok(true) => Ok(()),
            _ => Err(Error::ProvisioningLockLost),
        }
    }
}

impl Drop for LockWatchdog {
    fn drop(&mut self) {
        // Ends the probe loop, dropping the lock's connection; the
        // destination shutdown that follows in Preflight's drop order
        // closes its session.
        self.watcher.abort();
    }
}

/// Everything `run()` holds for the task's lifetime, acquired in the
/// Validating phase. Dropping it stops the lock watchdog (returning
/// the advisory lock's connection), shuts the provisioning cluster
/// down (closing that lock's session even if this future was
/// hard-aborted by a cancellation timeout), and frees the
/// in-flight-topology slot — field order is drop order.
pub(super) struct Preflight {
    /// The serving cluster the new shard joins.
    pub(super) source: Cluster,
    /// The database's omnisharded tables; empty means the schema-only
    /// path.
    pub(super) omni_tables: Vec<String>,
    /// Watches the session holding the session-scoped
    /// `pg_try_advisory_lock` on the new shard: which pgdog instance
    /// runs this ADD SHARD.
    lock: LockWatchdog,
    /// The caller-owned, launched, non-serving one-shard cluster for
    /// the new shard. The activated shard gets fresh pools from the
    /// registry reload.
    destination: DestinationGuard,
    _topology: TopologyGuard,
}

impl Preflight {
    pub(super) fn destination(&self) -> &Cluster {
        &self.destination.0
    }

    /// Resolves when the provisioning lock's session dies. Exclusivity
    /// is gone with the session, so long waits select on this and
    /// abort.
    pub(super) async fn lock_lost(&self) {
        self.lock.lost.cancelled().await
    }

    /// Has a probe already found the lock's session dead?
    pub(super) fn lock_held(&self) -> Result<(), Error> {
        if self.lock.lost.is_cancelled() {
            Err(Error::ProvisioningLockLost)
        } else {
            Ok(())
        }
    }

    /// Probe the lock's session right now: the last check before the
    /// cutover's point of no return.
    pub(super) async fn ensure_lock_held(&self) -> Result<(), Error> {
        self.lock.ensure_held().await
    }
}

/// Entry: nothing held. Exit: the cluster is placement-stable, the
/// named shard is the next one, this instance holds both the local
/// topology slot and the cross-instance advisory lock, and the new
/// shard is empty. Failure: everything acquired so far is released by
/// drop, and the task fails with the guard's error.
pub(super) async fn preflight(task: &AddShardTask) -> Result<Preflight, MigrationError> {
    let topology = TopologyGuard::acquire(&task.database)?;

    let source = databases()
        .schema_owner(&task.database)
        .map_err(Error::from)?;

    placement_stable(&source)?;

    // Declared shards are added in order: pure config math, so it runs
    // before anything touches the new shard.
    if task.shard != source.shards().len() {
        return Err(Error::ProvisioningShardNotNext {
            declared: task.shard,
            expected: source.shards().len(),
        }
        .into());
    }

    let destination =
        DestinationGuard(provisioning_cluster(&task.database, task.shard).map_err(Error::from)?);

    // Cross-instance mutex, arbitrated by the new shard itself.
    // Session-scoped: the watchdog holds the connection checked out
    // and probes it until the task ends; the destination shutdown that
    // follows it in drop order closes the session even if the
    // connection was returned to the pool.
    let lock = LockWatchdog::spawn(provisioning_lock(&destination.0).await?, &task.database);

    destination_is_empty(&destination.0).await?;

    let omni_tables = config()
        .config
        .omnisharded_tables
        .iter()
        .find(|tables| tables.database == task.database)
        .map(|tables| tables.tables.clone())
        .unwrap_or_default();

    Ok(Preflight {
        source,
        omni_tables,
        lock,
        destination,
        _topology: topology,
    })
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_topology_guard_excludes_concurrent_changes() {
        let guard = TopologyGuard::acquire("guard_test_db").unwrap();
        assert!(TopologyGuard::acquire("guard_test_db").is_err());
        // A different database is unaffected.
        let other = TopologyGuard::acquire("guard_test_other").unwrap();
        drop(guard);
        // Released: can acquire again.
        let _again = TopologyGuard::acquire("guard_test_db").unwrap();
        drop(other);
    }
}
