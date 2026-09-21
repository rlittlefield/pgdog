//! Pending shards: a declared shard serves only once the cluster
//! confirms it.
//!
//! The config is declarative: every `[[databases]]` entry describes a
//! shard in its final shape, including shards that hold no data yet.
//! Which of them serve is not the config's call — the cluster keeps
//! that state, in the `pgdog.config` marker that `SETUP SCHEMA` and
//! every `ADD SHARD` cutover stamp on each shard. Shard 0's primary is
//! the arbiter: when its marker reports M shards, declared shards
//! `M..` are pending — excluded from the serving topology until
//! `ADD SHARD` provisions them and stamps the new count. The same
//! manifest is correct before, during and after a shard is added;
//! there is nothing to flag first or clean up after.
//!
//! Degradations, in order of trust:
//! - No marker (`SETUP SCHEMA` never ran): the declared topology is
//!   served as-is.
//! - No `schema_admin` user to ask with, or shard 0 unreachable at
//!   startup: served as declared, the latter loudly.
//! - The marker reports more shards than the config declares: the
//!   config is missing entries pgdog can't invent (a manifest lagging
//!   the fleet); served as declared, loudly.
//!
//! While running, classification only promotes. A reload gates new
//! trailing entries through [`carry_over`] and the background check
//! activates them once the marker confirms; a serving shard is never
//! demoted by a background read, which could race the moment in a
//! cutover where the topology swapped but the markers aren't stamped
//! yet.

use std::collections::{BTreeSet, HashMap};
use std::time::Duration;

use pgdog_config::ConfigAndUsers;
use tokio::time::sleep;
use tracing::{debug, info, warn};

use crate::backend::Cluster;
use crate::backend::Error;
use crate::backend::databases::{reclassify_pending, shard_zero_cluster};
use crate::backend::pool::Request;
use crate::config::config;

/// Background marker checks before giving up; a reload retries.
const ATTEMPTS: usize = 5;
const RETRY_DELAY: Duration = Duration::from_secs(3);

/// Bounds each marker read: classification runs before the listener
/// opens at startup, and a hanging shard 0 must not stall boot.
const MARKER_TIMEOUT: Duration = Duration::from_secs(5);

/// Called after every reload: check pending entries against the
/// cluster's markers in the background and activate the confirmed
/// ones.
pub fn on_config_change() {
    for database in pending_databases(&config()) {
        crate::tasks::spawn("pending shard check", async move {
            check_pending(&database).await;
        });
    }
}

/// Called at startup, before the databases registry is built: ask
/// shard 0 of every multi-shard database how many shards the cluster
/// has, and hold the declared shards it doesn't confirm as pending.
pub async fn classify_at_startup() {
    let config = config();
    for database in candidates(&config) {
        let declared = declared_shards(&config, &database);
        match marker_shards(&database).await {
            Ok(Some(reported)) => {
                if reported > declared {
                    warn!(
                        r#""{}" declares {} shard(s), but the cluster reports {}: the config is missing entries pgdog can't invent; serving the declared topology"#,
                        database, declared, reported
                    );
                } else if reported < declared {
                    info!(
                        r#"shard(s) {}..{} of "{}" are declared but the cluster reports {} shard(s); pending until ADD SHARD provisions them"#,
                        reported,
                        declared - 1,
                        database,
                        reported
                    );
                }
                if let Err(err) = reclassify_pending(&database, reported, false) {
                    warn!(
                        r#"could not exclude pending shards of "{}": {}"#,
                        database, err
                    );
                }
            }
            Ok(None) => debug!(
                r#"no pgdog.config marker on shard 0 of "{}"; serving the declared topology (SETUP SCHEMA stamps the marker)"#,
                database
            ),
            Err(Error::NoSchemaAdmin(_)) => debug!(
                r#""{}" has no schema_admin user to read the pgdog.config marker with; serving the declared topology"#,
                database
            ),
            Err(err) => warn!(
                r#"could not read the pgdog.config marker on shard 0 of "{}": {}; serving the declared topology as-is (RELOAD re-checks)"#,
                database, err
            ),
        }
    }
}

/// Reloads gate new trailing shard entries: carry the derived pending
/// state of the running config into the just-loaded one, holding
/// entries beyond the running serving boundary as pending until a
/// marker read confirms them. A database new to the config is trusted
/// as declared, like the startup bootstrap.
pub(crate) fn carry_over(new: &mut ConfigAndUsers, old: &ConfigAndUsers) {
    let mut serving: HashMap<&str, BTreeSet<usize>> = HashMap::new();
    for entry in &old.config.databases {
        let shards = serving.entry(entry.name.as_str()).or_default();
        if !entry.provisioning {
            shards.insert(entry.shard);
        }
    }
    for entry in new.config.databases.iter_mut() {
        if let Some(shards) = serving.get(entry.name.as_str())
            && !shards.is_empty()
        {
            entry.provisioning = entry.shard >= shards.len();
        }
    }
}

/// One background check: promote pending shards the marker confirms.
/// Never demotes — see the module doc.
async fn check_pending(database: &str) {
    for attempt in 1..=ATTEMPTS {
        match marker_shards(database).await {
            Ok(Some(reported)) => {
                let serving = serving_shards(&config(), database);
                if reported > serving {
                    info!(
                        r#"the cluster reports {} shard(s) for "{}"; activating the confirmed pending shard(s)"#,
                        reported, database
                    );
                    if let Err(err) = reclassify_pending(database, reported, true) {
                        warn!(
                            r#"could not activate confirmed shards of "{}": {}; reload to retry"#,
                            database, err
                        );
                    }
                }
                // reported <= serving: still pending, or a marker read
                // that raced a cutover's stamp; either way not ours to
                // touch while serving.
                return;
            }
            Ok(None) => {
                warn!(
                    r#""{}" has pending shard(s) but no pgdog.config marker on shard 0; provision them with ADD SHARD, or restart pgdog to serve the declared topology unverified"#,
                    database
                );
                return;
            }
            Err(err) => {
                if attempt == ATTEMPTS {
                    warn!(
                        r#"could not check the pending shard(s) of "{}": {}; reload to retry"#,
                        database, err
                    );
                } else {
                    sleep(RETRY_DELAY).await;
                }
            }
        }
    }
}

/// Databases worth asking about: more than one distinct declared
/// shard.
fn candidates(config: &ConfigAndUsers) -> Vec<String> {
    let mut names = config
        .config
        .databases
        .iter()
        .map(|database| database.name.as_str())
        .collect::<Vec<_>>();
    names.sort_unstable();
    names.dedup();

    names
        .into_iter()
        .filter(|name| declared_shards(config, name) > 1)
        .map(str::to_string)
        .collect()
}

/// Databases with pending entries to re-check.
fn pending_databases(config: &ConfigAndUsers) -> Vec<String> {
    let mut names = config
        .config
        .databases
        .iter()
        .filter(|database| database.provisioning)
        .map(|database| database.name.to_string())
        .collect::<Vec<_>>();
    names.sort_unstable();
    names.dedup();
    names
}

/// Distinct declared shard numbers, pending or not.
fn declared_shards(config: &ConfigAndUsers, database: &str) -> usize {
    config
        .config
        .databases
        .iter()
        .filter(|entry| entry.name == database)
        .map(|entry| entry.shard)
        .collect::<BTreeSet<_>>()
        .len()
}

/// Distinct serving shard numbers.
fn serving_shards(config: &ConfigAndUsers, database: &str) -> usize {
    config
        .config
        .databases
        .iter()
        .filter(|entry| entry.name == database && !entry.provisioning)
        .map(|entry| entry.shard)
        .collect::<BTreeSet<_>>()
        .len()
}

/// Read the shard count shard 0's `pgdog.config` marker reports.
/// `None` when the marker table or row doesn't exist: the cluster
/// never had `SETUP SCHEMA` or a cutover stamp it.
async fn marker_shards(database: &str) -> Result<Option<usize>, Error> {
    // The cluster is always shut down before the timeout propagates:
    // a dropped-at-await future would leak its launched pools.
    let cluster = shard_zero_cluster(database)?;
    let marker = tokio::time::timeout(MARKER_TIMEOUT, read_marker(&cluster)).await;
    cluster.shutdown();
    match marker {
        Ok(marker) => marker,
        Err(_) => Err(std::io::Error::new(
            std::io::ErrorKind::TimedOut,
            "pgdog.config marker read timed out",
        )
        .into()),
    }
}

async fn read_marker(cluster: &Cluster) -> Result<Option<usize>, Error> {
    let mut server = cluster
        .shards()
        .first()
        .ok_or(crate::backend::pool::Error::NoShard(0))?
        .primary(&Request::default())
        .await?;

    let installed: Vec<String> = server
        .fetch_all("SELECT COALESCE(to_regclass('pgdog.config')::text, '')")
        .await?;
    if installed.first().map(|s| s.is_empty()).unwrap_or(true) {
        return Ok(None);
    }

    let shards: Vec<i32> = server.fetch_all("SELECT shards FROM pgdog.config").await?;
    Ok(shards
        .first()
        .and_then(|&shards| (shards > 0).then_some(shards as usize)))
}

#[cfg(test)]
mod test {
    use super::*;

    fn config_from(source: &str) -> ConfigAndUsers {
        ConfigAndUsers {
            config: toml::from_str(source).unwrap(),
            ..Default::default()
        }
    }

    #[test]
    fn test_candidates() {
        let config = config_from(
            r#"
[[databases]]
name = "prod"
host = "10.0.0.1"
shard = 0

[[databases]]
name = "prod"
host = "10.0.0.2"
shard = 1

[[databases]]
name = "solo"
host = "10.0.0.3"
shard = 0

[[databases]]
name = "solo"
host = "10.0.0.4"
shard = 0
role = "replica"
"#,
        );
        // Multi-shard databases only; replicas don't add shards.
        assert_eq!(candidates(&config), vec!["prod".to_string()]);
        assert_eq!(declared_shards(&config, "prod"), 2);
        assert_eq!(declared_shards(&config, "solo"), 1);
    }

    #[test]
    fn test_carry_over_gates_new_trailing_entries() {
        let old = config_from(
            r#"
[[databases]]
name = "prod"
host = "10.0.0.1"
shard = 0

[[databases]]
name = "prod"
host = "10.0.0.2"
shard = 1
"#,
        );
        let mut new = config_from(
            r#"
[[databases]]
name = "prod"
host = "10.0.0.1"
shard = 0

[[databases]]
name = "prod"
host = "10.0.0.2"
shard = 1

[[databases]]
name = "prod"
host = "10.0.0.3"
shard = 2

[[databases]]
name = "fresh"
host = "10.0.0.9"
shard = 0
"#,
        );
        carry_over(&mut new, &old);

        let flags: Vec<bool> = new
            .config
            .databases
            .iter()
            .map(|entry| entry.provisioning)
            .collect();
        // Serving entries carry, the new trailing shard is gated, a
        // database new to the config is trusted as declared.
        assert_eq!(flags, vec![false, false, true, false]);
    }

    #[test]
    fn test_carry_over_preserves_pending() {
        let mut old = config_from(
            r#"
[[databases]]
name = "prod"
host = "10.0.0.1"
shard = 0

[[databases]]
name = "prod"
host = "10.0.0.2"
shard = 1
"#,
        );
        old.config.databases[1].provisioning = true;

        let mut new = config_from(
            r#"
[[databases]]
name = "prod"
host = "10.0.0.1"
shard = 0

[[databases]]
name = "prod"
host = "10.0.0.2"
shard = 1
"#,
        );
        carry_over(&mut new, &old);
        assert!(!new.config.databases[0].provisioning);
        assert!(new.config.databases[1].provisioning);
        assert_eq!(serving_shards(&new, "prod"), 1);
        assert_eq!(pending_databases(&new), vec!["prod".to_string()]);
    }
}
