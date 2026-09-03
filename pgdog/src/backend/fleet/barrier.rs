//! Pause writes for a database while its topology changes underneath
//! them.
//!
//! Two scopes:
//!
//! - Omnisharded writes, used by `ADD SHARD`: omni writes must reach
//!   every shard, so they pause for the brief window between draining
//!   replication to the new shard and swapping it into the topology.
//! - Writes for specific sharding key values, used by `MOVE KEYS`:
//!   they pause between draining replication and flipping the keys'
//!   placement. Writes for other keys flow normally; writes on sharded
//!   tables that carry no key at all (broadcasts) can touch moving
//!   rows, so they park too while any key is paused for the database.
//!
//! Everything else — sharded traffic for unaffected keys, all reads —
//! flows normally. Like maintenance mode, this is independent from the
//! config and holds across config reloads.
//!
use std::{
    collections::{HashMap, HashSet},
    future::{Future, IntoFuture},
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use arc_swap::ArcSwap;
use once_cell::sync::Lazy;
use parking_lot::Mutex;
use tokio::sync::broadcast;
use tracing::warn;

static WRITE_BARRIER: Lazy<WriteBarrier> = Lazy::new(|| WriteBarrier {
    state: ArcSwap::from_pointee(BarrierState::default()),
    write_lock: Mutex::new(()),
});

/// Databases with a keyed barrier armed. The router's cheap gate for
/// recording sharding key values on routes: zero in steady state.
static KEYS_ARMED: AtomicUsize = AtomicUsize::new(0);

pub(crate) fn waiter(database: &str) -> Option<Waiter> {
    WRITE_BARRIER.get_waiter(database)
}

/// Get a [`Waiter`] for a statement whose write touches one of the
/// paused sharding keys, or a sharded table without naming any key
/// (a broadcast can touch moving rows). `None` when nothing applies.
// Consumed by the MOVE KEYS cutover hook.
#[allow(dead_code)]
pub(crate) fn key_waiter(
    database: &str,
    route_keys: &[String],
    unkeyed_sharded_write: bool,
) -> Option<Waiter> {
    WRITE_BARRIER.get_key_waiter(database, route_keys, unkeyed_sharded_write)
}

/// Whether any database has a keyed barrier armed. Cheap: the router
/// checks this per statement to skip key recording in steady state.
// Consumed by the MOVE KEYS route plumbing.
#[allow(dead_code)]
pub(crate) fn any_keys_armed() -> bool {
    KEYS_ARMED.load(Ordering::Acquire) > 0
}

/// What a woken [`Waiter`] re-checks before proceeding: the barrier
/// may have been re-armed between the release and the task waking up.
enum Recheck {
    /// The omnisharded write barrier for the database.
    Omni,
    /// The keyed barrier, with the statement's keys.
    Keys {
        keys: Vec<String>,
        unkeyed_sharded_write: bool,
    },
}

/// Future that resolves once the paused writes resume for a database.
///
/// Wraps the broadcast receiver so callers can simply `.await` it; it
/// resolves when the channel is closed (the sender is dropped by `stop`).
pub(crate) struct Waiter {
    receiver: broadcast::Receiver<()>,
    database: String,
    recheck: Recheck,
}

impl IntoFuture for Waiter {
    type Output = ();
    type IntoFuture = Pin<Box<dyn Future<Output = ()> + Send>>;

    fn into_future(mut self) -> Self::IntoFuture {
        Box::pin(async move {
            // Resolves when the channel is closed (sender dropped).
            let _ = self.receiver.recv().await;

            // Re-check in case the barrier was re-armed between the
            // release and this task waking up.
            let waiter = match &self.recheck {
                Recheck::Omni => WRITE_BARRIER.get_waiter(&self.database),
                Recheck::Keys {
                    keys,
                    unkeyed_sharded_write,
                } => WRITE_BARRIER.get_key_waiter(&self.database, keys, *unkeyed_sharded_write),
            };
            if let Some(waiter) = waiter {
                let _ = waiter.await;
            }
        })
    }
}

pub fn start(database: &str) {
    WRITE_BARRIER.add(database);
    warn!(
        "writes to omnisharded tables are paused for database \"{}\"",
        database
    );
}

pub fn stop(database: &str) {
    WRITE_BARRIER.remove(database);
    warn!(
        "writes to omnisharded tables resumed for database \"{}\"",
        database
    );
}

/// Whether omnisharded writes are currently paused for a database.
#[allow(dead_code)] // TODO: remove once provisioning consumes this
pub fn is_on(database: &str) -> bool {
    WRITE_BARRIER.get_waiter(database).is_some()
}

/// Pause writes for these sharding key values. Arming again while
/// armed unions the key sets.
// Consumed by the MOVE KEYS cutover and follower.
#[allow(dead_code)]
pub(crate) fn start_keys(database: &str, keys: &[String]) {
    WRITE_BARRIER.add_keys(database, keys);
    warn!(
        "writes for {} sharding key(s) are paused for database \"{}\"",
        keys.len(),
        database
    );
}

/// Resume writes for all paused sharding keys of a database.
// Consumed by the MOVE KEYS cutover and follower.
#[allow(dead_code)]
pub(crate) fn stop_keys(database: &str) {
    WRITE_BARRIER.remove_keys(database);
    warn!(
        "writes for paused sharding keys resumed for database \"{}\"",
        database
    );
}

/// Whether any sharding keys are currently paused for a database.
// Consumed by the MOVE KEYS follower re-arm check.
#[allow(dead_code)]
pub(crate) fn keys_on(database: &str) -> bool {
    WRITE_BARRIER.state.load().keys.contains_key(database)
}

#[derive(Debug)]
struct WriteBarrier {
    state: ArcSwap<BarrierState>,
    write_lock: Mutex<()>,
}

#[derive(Clone, Debug, Default)]
struct BarrierState {
    // Databases whose omnisharded writes are paused.
    databases: HashMap<String, broadcast::Sender<()>>,
    // Databases with specific sharding key values paused.
    keys: HashMap<String, KeyBarrier>,
}

#[derive(Clone, Debug)]
struct KeyBarrier {
    keys: HashSet<String>,
    sender: broadcast::Sender<()>,
}

impl WriteBarrier {
    /// Get a [`Waiter`] that resolves once omnisharded writes resume
    /// for the database, or `None` if they aren't paused right now.
    fn get_waiter(&self, database: &str) -> Option<Waiter> {
        let state = self.state.load();

        if state.databases.is_empty() {
            return None;
        }

        state.databases.get(database).map(|sender| Waiter {
            receiver: sender.subscribe(),
            database: database.to_string(),
            recheck: Recheck::Omni,
        })
    }

    /// Get a [`Waiter`] for a write naming one of the paused keys, or
    /// naming no key at all on a sharded table while any key is paused.
    fn get_key_waiter(
        &self,
        database: &str,
        route_keys: &[String],
        unkeyed_sharded_write: bool,
    ) -> Option<Waiter> {
        let state = self.state.load();

        if state.keys.is_empty() {
            return None;
        }

        let barrier = state.keys.get(database)?;
        let parked =
            unkeyed_sharded_write || route_keys.iter().any(|key| barrier.keys.contains(key));
        parked.then(|| Waiter {
            receiver: barrier.sender.subscribe(),
            database: database.to_string(),
            recheck: Recheck::Keys {
                keys: route_keys.to_vec(),
                unkeyed_sharded_write,
            },
        })
    }

    /// Pause omnisharded writes for a database.
    fn add(&self, database: &str) {
        let _guard = self.write_lock.lock();
        let state = self.state.load();
        let mut next = BarrierState::clone(&state);

        // Keep the existing channel if already paused, so current
        // waiters stay valid.
        next.databases
            .entry(database.to_string())
            .or_insert_with(|| broadcast::channel(1).0);

        self.state.store(Arc::new(next));
    }

    /// Resume omnisharded writes for a database and wake its waiters
    /// by dropping (closing) the channel.
    fn remove(&self, database: &str) {
        let _guard = self.write_lock.lock();
        let state = self.state.load();
        let mut next = BarrierState::clone(&state);

        next.databases.remove(database);

        self.state.store(Arc::new(next));
    }

    /// Pause writes for these sharding key values.
    fn add_keys(&self, database: &str, keys: &[String]) {
        let _guard = self.write_lock.lock();
        let state = self.state.load();
        let mut next = BarrierState::clone(&state);

        // Keep the existing channel if already paused, so current
        // waiters stay valid; the key sets union.
        let barrier = next
            .keys
            .entry(database.to_string())
            .or_insert_with(|| KeyBarrier {
                keys: HashSet::new(),
                sender: broadcast::channel(1).0,
            });
        barrier.keys.extend(keys.iter().cloned());

        KEYS_ARMED.store(next.keys.len(), Ordering::Release);
        self.state.store(Arc::new(next));
    }

    /// Resume writes for all paused keys of a database and wake its
    /// waiters by dropping (closing) the channel.
    fn remove_keys(&self, database: &str) {
        let _guard = self.write_lock.lock();
        let state = self.state.load();
        let mut next = BarrierState::clone(&state);

        next.keys.remove(database);

        KEYS_ARMED.store(next.keys.len(), Ordering::Release);
        self.state.store(Arc::new(next));
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::time::Duration;
    use tokio::time::timeout;

    #[tokio::test]
    async fn test_no_barrier_no_waiter() {
        assert!(waiter("omni_test_db_a").is_none());
    }

    #[tokio::test]
    async fn test_waiter_resolves_on_stop() {
        start("omni_test_db_b");
        assert!(is_on("omni_test_db_b"));

        let waiter = waiter("omni_test_db_b").unwrap();
        let handle = tokio::spawn(waiter.into_future());

        // Not resolved while armed.
        assert!(!handle.is_finished());

        stop("omni_test_db_b");
        timeout(Duration::from_secs(1), handle)
            .await
            .expect("waiter should resolve after stop")
            .unwrap();
        assert!(!is_on("omni_test_db_b"));
    }

    #[tokio::test]
    async fn test_barrier_is_per_database() {
        start("omni_test_db_c");
        assert!(waiter("omni_test_db_other").is_none());
        stop("omni_test_db_c");
    }

    #[tokio::test]
    async fn test_rearm_keeps_waiters_parked() {
        start("omni_test_db_d");
        let waiter = waiter("omni_test_db_d").unwrap();
        let handle = tokio::spawn(waiter.into_future());

        // Release and immediately re-arm: the waiter re-checks and
        // stays parked.
        stop("omni_test_db_d");
        start("omni_test_db_d");
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(!handle.is_finished());

        stop("omni_test_db_d");
        timeout(Duration::from_secs(1), handle)
            .await
            .expect("waiter should resolve after final stop")
            .unwrap();
    }

    #[tokio::test]
    async fn test_double_start_keeps_existing_waiters() {
        start("omni_test_db_e");
        let waiter = waiter("omni_test_db_e").unwrap();
        let handle = tokio::spawn(waiter.into_future());

        // Re-arming while already armed must not orphan waiters.
        start("omni_test_db_e");
        stop("omni_test_db_e");

        timeout(Duration::from_secs(1), handle)
            .await
            .expect("waiter should resolve")
            .unwrap();
    }

    fn keys(values: &[&str]) -> Vec<String> {
        values.iter().map(|v| v.to_string()).collect()
    }

    #[tokio::test]
    async fn test_key_barrier_parks_matching_keys_only() {
        start_keys("keys_test_db_a", &keys(&["11", "12"]));
        assert!(keys_on("keys_test_db_a"));
        // The router's cheap gate opens while any keys are armed.
        // (Not asserted false after: other tests share the global.)
        assert!(any_keys_armed());

        // A write naming a paused key parks.
        assert!(key_waiter("keys_test_db_a", &keys(&["11"]), false).is_some());
        // Any match in the statement's key set parks it.
        assert!(key_waiter("keys_test_db_a", &keys(&["7", "12"]), false).is_some());
        // Other keys flow.
        assert!(key_waiter("keys_test_db_a", &keys(&["7"]), false).is_none());
        // A sharded write naming no key at all parks: a broadcast can
        // touch moving rows.
        assert!(key_waiter("keys_test_db_a", &[], true).is_some());
        // Other databases are unaffected.
        assert!(key_waiter("keys_test_other", &keys(&["11"]), false).is_none());
        // The keyed barrier is separate from the omni barrier.
        assert!(waiter("keys_test_db_a").is_none());

        let parked = key_waiter("keys_test_db_a", &keys(&["11"]), false).unwrap();
        let handle = tokio::spawn(parked.into_future());
        assert!(!handle.is_finished());

        stop_keys("keys_test_db_a");
        timeout(Duration::from_secs(1), handle)
            .await
            .expect("waiter should resolve after stop_keys")
            .unwrap();
        assert!(!keys_on("keys_test_db_a"));
        assert!(key_waiter("keys_test_db_a", &keys(&["11"]), false).is_none());
    }

    #[tokio::test]
    async fn test_key_barrier_rearm_keeps_waiters_parked() {
        start_keys("keys_test_db_b", &keys(&["11"]));
        let parked = key_waiter("keys_test_db_b", &keys(&["11"]), false).unwrap();
        let handle = tokio::spawn(parked.into_future());

        // Release and immediately re-arm: the waiter re-checks its
        // keys and stays parked.
        stop_keys("keys_test_db_b");
        start_keys("keys_test_db_b", &keys(&["11"]));
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(!handle.is_finished());

        stop_keys("keys_test_db_b");
        timeout(Duration::from_secs(1), handle)
            .await
            .expect("waiter should resolve after final stop")
            .unwrap();
    }

    #[tokio::test]
    async fn test_key_barrier_rearm_unions_keys() {
        start_keys("keys_test_db_c", &keys(&["11"]));
        let parked = key_waiter("keys_test_db_c", &keys(&["11"]), false).unwrap();
        let handle = tokio::spawn(parked.into_future());

        // Arming more keys keeps existing waiters valid.
        start_keys("keys_test_db_c", &keys(&["12"]));
        assert!(key_waiter("keys_test_db_c", &keys(&["11"]), false).is_some());
        assert!(key_waiter("keys_test_db_c", &keys(&["12"]), false).is_some());

        stop_keys("keys_test_db_c");
        timeout(Duration::from_secs(1), handle)
            .await
            .expect("waiter should resolve")
            .unwrap();
    }
}
