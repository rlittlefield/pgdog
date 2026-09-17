//! One topology change per database at a time: `ADD SHARD` and
//! `MOVE KEYS` both hold this for their lifetime, so they exclude each
//! other and themselves on this instance.

use std::collections::HashSet;
use std::sync::LazyLock;

use parking_lot::Mutex;

use crate::backend::replication::logical::Error;

/// Databases with a topology change in flight. A second concurrent
/// change to the same database is refused.
static TOPOLOGY_TASKS: LazyLock<Mutex<HashSet<String>>> = LazyLock::new(Default::default);

/// Removes the database from the in-flight topology registry on drop.
pub(crate) struct TopologyGuard {
    database: String,
}

impl TopologyGuard {
    pub(crate) fn acquire(database: &str) -> Result<Self, Error> {
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
