//! MOVE KEYS: multi-instance coordination for single-key reshards.
//!
//! The cutover pauses writes for the moving keys on every pgdog
//! instance, flips their placement, and invalidates every instance's
//! lookup cache. Shard 0 of the database being resharded is the shared
//! medium: every instance serving the database can reach it.
//!
//! Unlike ADD SHARD, whose agents exist because `provisioning` config
//! entries declare them, a key move has no config marker: every
//! eligible database (a `schema_admin` user and at least one
//! `lookup_result = "shard"` table, the same set MOVE KEYS can run on)
//! gets a standing follower listening on the medium, so arming reaches
//! every instance in milliseconds rather than on the next poll.
//!
//! There is no persistent state to converge: a restarted instance has
//! an empty lookup cache and reads the flipped placement on its first
//! statement. Startup only re-arms the barriers when a cutover is in
//! flight, before the listener opens.

use std::collections::HashSet;
use std::time::Duration;

use once_cell::sync::Lazy;
use parking_lot::Mutex;
use pgdog_config::{ConfigAndUsers, LookupResult};
use serde::{Deserialize, Serialize};
use tokio::time::sleep;
use tracing::{debug, info, warn};

use crate::backend::Error;
use crate::backend::databases::{invalidate_lookup_keys, medium_cluster};
use crate::backend::fleet::protocol::{self, StateRow};
use crate::backend::fleet::{Follower, Topic, barrier};
use crate::config::config;

/// MOVE KEYS coordination, on shard 0 of the database as the medium.
pub(crate) const TOPIC: Topic = Topic::new("key_move");

/// How long the coordinator waits for every peer to pause the keys.
// Consumed by the MOVE KEYS cutover.
#[allow(dead_code)]
pub(crate) const ARM_ACK_TIMEOUT: Duration = Duration::from_secs(10);

/// How long the coordinator waits for every peer to invalidate its
/// caches. The flip stands either way; a straggler's stale cache
/// entries still point at the source, whose rows exist until cleanup.
// Consumed by the MOVE KEYS cutover.
#[allow(dead_code)]
pub(crate) const ACTIVATE_ACK_TIMEOUT: Duration = Duration::from_secs(15);

pub(crate) const STATE_ARMED: &str = "armed";
pub(crate) const STATE_ACTIVATED: &str = "activated";
pub(crate) const STATE_RELEASED: &str = "released";

/// Bounds the startup convergence check per database: a hanging medium
/// must not stall boot.
const STARTUP_CHECK_TIMEOUT: Duration = Duration::from_secs(5);

/// What a coordinated move covers, riding the fleet state as JSON.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct KeyMovePayload {
    pub(crate) keys: Vec<String>,
    pub(crate) source: usize,
    pub(crate) target: usize,
}

impl KeyMovePayload {
    // Consumed by the MOVE KEYS cutover.
    #[allow(dead_code)]
    pub(crate) fn to_json(&self) -> Result<String, Error> {
        serde_json::to_string(self).map_err(|err| {
            std::io::Error::new(std::io::ErrorKind::InvalidData, err.to_string()).into()
        })
    }

    fn from_state(row: &StateRow) -> Option<Self> {
        let payload = row.payload.as_deref()?;
        match serde_json::from_str(payload) {
            Ok(payload) => Some(payload),
            Err(err) => {
                warn!("key move state carries an unreadable payload: {}", err);
                None
            }
        }
    }
}

/// Called at startup and after every reload: make sure every eligible
/// database has a follower.
pub fn on_config_change() {
    ensure_followers();
}

/// Databases a key move can run on: a `schema_admin` user and at least
/// one sharded table with `lookup_result = "shard"`.
fn eligible(config: &ConfigAndUsers) -> Vec<String> {
    let mut names = config
        .config
        .sharded_tables
        .iter()
        .filter(|table| table.lookup_result == LookupResult::Shard)
        .map(|table| table.database.as_str())
        .collect::<Vec<_>>();
    names.sort_unstable();
    names.dedup();

    names
        .into_iter()
        .filter(|name| {
            config
                .users
                .users
                .iter()
                .any(|user| &user.database == name && user.schema_admin)
        })
        .map(|name| name.to_string())
        .collect()
}

/// Re-arm the keyed barriers for cutovers that were in flight when
/// this instance started, before it serves a single query. Bounded per
/// database. An `activated` state needs nothing: a fresh instance has
/// an empty lookup cache and reads the flipped placement.
pub async fn converge_at_startup() {
    for database in eligible(&config()) {
        let armed = tokio::time::timeout(STARTUP_CHECK_TIMEOUT, armed_state(&database)).await;
        match armed {
            Ok(Ok(Some(payload))) => {
                info!(
                    r#"a key move cutover for "{}" is in flight; pausing its keys before serving"#,
                    database
                );
                barrier::start_keys(&database, &payload.keys);
                barrier::start(&database);
            }
            Ok(Ok(None)) => {}
            Ok(Err(err)) => debug!(
                r#"key move startup check for "{}" failed: {}; the follower retries"#,
                database, err
            ),
            Err(_) => debug!(
                r#"key move startup check for "{}" timed out; the follower retries"#,
                database
            ),
        }
    }
}

/// The armed payload for a database's in-flight cutover, `None` when
/// no fresh armed state exists.
async fn armed_state(database: &str) -> Result<Option<KeyMovePayload>, Error> {
    let cluster = medium_cluster(database)?;
    let state = protocol::read_state(&cluster, TOPIC).await;
    cluster.shutdown();

    Ok(match state? {
        Some(row)
            if row.state == STATE_ARMED
                && !row.coordinator_silent()
                && row.coordinator != crate::backend::fleet::registry::node_id() =>
        {
            KeyMovePayload::from_state(&row)
        }
        _ => None,
    })
}

//
// The per-instance follower.
//

/// Databases with a running follower on this instance.
static FOLLOWERS: Lazy<Mutex<HashSet<String>>> = Lazy::new(Default::default);

struct FollowerSlot(String);

impl Drop for FollowerSlot {
    fn drop(&mut self) {
        FOLLOWERS.lock().remove(&self.0);
    }
}

/// Spawn a follower for every eligible database that doesn't have one.
fn ensure_followers() {
    for database in eligible(&config()) {
        if FOLLOWERS.lock().insert(database.clone()) {
            info!(r#"key move follower started for "{}""#, database);
            crate::tasks::spawn("key move follower", follower_loop(database));
        }
    }
}

/// Register on the medium and follow the coordinator's state until the
/// database stops being eligible (removed from the config).
async fn follower_loop(database: String) {
    let _slot = FollowerSlot(database.clone());
    let mut follower: Option<Follower> = None;
    let mut armed = false;

    loop {
        if !eligible(&config()).iter().any(|name| name == &database) {
            break;
        }

        match follower_tick(&database, &mut follower, &mut armed).await {
            Ok(()) => {
                if let Some(follower) = follower.as_mut() {
                    follower.wake().await;
                }
            }
            Err(err) => {
                debug!(r#"key move follower for "{}": {}"#, database, err);
                if let Some(follower) = follower.take() {
                    follower.shutdown();
                }
                sleep(Duration::from_secs(1)).await;
            }
        }
    }

    disarm(&database, &mut armed);
    if let Some(follower) = follower.take() {
        follower.shutdown();
    }
    info!(r#"key move follower stopped for "{}""#, database);
}

async fn follower_tick(
    database: &str,
    follower_slot: &mut Option<Follower>,
    armed: &mut bool,
) -> Result<(), Error> {
    if follower_slot.is_none() {
        let cluster = medium_cluster(database)?;
        *follower_slot = Some(Follower::connect(cluster, TOPIC).await?);
    }
    let follower = follower_slot.as_mut().expect("follower just ensured");

    // Registration doubles as the heartbeat the coordinator's
    // completeness check reads.
    follower.heartbeat().await?;
    let follower = follower_slot.as_ref().expect("follower just ensured");

    let me = crate::backend::fleet::registry::node_id();
    match follower.state().await? {
        // Our own task drives the local barriers directly.
        Some(row) if row.coordinator == me => {}
        Some(row) if row.state == STATE_ARMED => {
            follow_arm(database, &row, follower, armed).await?
        }
        Some(row) if row.state == STATE_ACTIVATED => {
            follow_activation(database, &row, follower, armed).await?;
        }
        Some(row) if row.state == STATE_RELEASED => {
            if disarm(database, armed) {
                info!(
                    r#"key move released: resuming writes for the paused keys of "{}""#,
                    database
                );
            }
            follower.ack(row.epoch, STATE_RELEASED).await?;
        }
        _ => {
            disarm(database, armed);
        }
    }

    Ok(())
}

/// Release both barriers if this follower armed them. Returns whether
/// it did, so callers log with their own context.
fn disarm(database: &str, armed: &mut bool) -> bool {
    if *armed {
        barrier::stop_keys(database);
        barrier::stop(database);
        *armed = false;
        true
    } else {
        false
    }
}

/// Pause the moving keys (and omni writes: the mapping table is
/// omnisharded, and an application write to it would race the flip)
/// and ack. Re-checked every tick, like the ADD SHARD agent.
async fn follow_arm(
    database: &str,
    row: &StateRow,
    follower: &Follower,
    armed: &mut bool,
) -> Result<(), Error> {
    // Failsafe: a live coordinator keeps the armed row fresh; one that
    // died mid-drain must not park writes forever.
    if row.coordinator_silent() {
        if disarm(database, armed) {
            warn!(
                r#"key move coordinator for "{}" went silent; resuming writes"#,
                database
            );
        }
        return Ok(());
    }

    let Some(payload) = KeyMovePayload::from_state(row) else {
        // Unreadable payload: don't ack an arm we can't honor.
        return Ok(());
    };

    if !barrier::keys_on(database) {
        info!(
            r#"key move armed by another instance: pausing {} key(s) for "{}""#,
            payload.keys.len(),
            database
        );
    }
    barrier::start_keys(database, &payload.keys);
    if !barrier::is_on(database) {
        barrier::start(database);
    }
    *armed = true;
    follower.ack(row.epoch, STATE_ARMED).await
}

/// The placement flipped: invalidate the cached translations so the
/// next statement per key reads it fresh, release the barriers, ack.
async fn follow_activation(
    database: &str,
    row: &StateRow,
    follower: &Follower,
    armed: &mut bool,
) -> Result<(), Error> {
    if let Some(payload) = KeyMovePayload::from_state(row) {
        invalidate_lookup_keys(database, &payload.keys);
        info!(
            r#"key move activated by another instance: {} key(s) of "{}" moved to shard {}"#,
            payload.keys.len(),
            database,
            payload.target
        );
    }
    disarm(database, armed);
    follower.ack(row.epoch, STATE_ACTIVATED).await
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_eligible() {
        let config_source = r#"
[[sharded_tables]]
database = "prod"
column = "tenant_id"
lookup_query = "SELECT shard_id FROM tenants WHERE id = $1"
lookup_result = "shard"

[[sharded_tables]]
database = "hashed"
column = "user_id"
"#;
        let users_source = r#"
[[users]]
name = "admin"
database = "prod"
password = "x"
schema_admin = true

[[users]]
name = "admin"
database = "hashed"
password = "x"
schema_admin = true

[[users]]
name = "app"
database = "no_admin"
password = "x"
"#;
        let config = ConfigAndUsers {
            config: toml::from_str(config_source).unwrap(),
            users: toml::from_str(users_source).unwrap(),
            ..Default::default()
        };

        // Only databases with a shard-mode lookup AND a schema_admin
        // user are eligible.
        assert_eq!(eligible(&config), vec!["prod".to_string()]);
    }

    #[test]
    fn test_payload_round_trip() {
        let payload = KeyMovePayload {
            keys: vec!["11".into(), "O'Brien".into()],
            source: 0,
            target: 2,
        };
        let json = payload.to_json().unwrap();
        let row = StateRow {
            state: STATE_ARMED.into(),
            epoch: 1,
            coordinator: 2,
            age_secs: 0,
            payload: Some(json),
        };
        let parsed = KeyMovePayload::from_state(&row).unwrap();
        assert_eq!(parsed.keys, payload.keys);
        assert_eq!(parsed.source, 0);
        assert_eq!(parsed.target, 2);

        // No payload, no parse.
        let row = StateRow {
            payload: None,
            ..row
        };
        assert!(KeyMovePayload::from_state(&row).is_none());
    }
}
