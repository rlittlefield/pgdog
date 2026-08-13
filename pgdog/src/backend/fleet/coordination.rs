//! The coordination seam for topology cutovers.
//!
//! A cutover that changes where a database's rows live (`ADD SHARD`,
//! `MOVE KEYS`) must pause the affected writes on every pgdog instance
//! serving the database before it drains replication and flips the
//! topology. This build coordinates nothing: [`Coordinator::discover`]
//! always returns [`Discovery::Solo`] after warning the operator, so
//! the cutovers' coordination branches are never taken and only this
//! instance's write barrier pauses.
//!
//! A fleet-coordination implementation replaces this module's body,
//! keeping the type and method signatures: `discover` returns
//! [`Discovery::Ready`] with a live [`Coordinator`] (or
//! [`Discovery::Missing`] for unregistered peers), and the handle's
//! methods drive the peers through the published states.

use std::time::Duration;

use tracing::warn;

use crate::backend::{Cluster, Error};

/// Namespaces one consumer's coordination states, e.g. `ADD SHARD`
/// and `MOVE KEYS` cutovers never see each other's.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Topic(
    // Consumed by fleet-coordination implementations of this module.
    #[allow(dead_code)] &'static str,
);

impl Topic {
    pub(crate) const fn new(name: &'static str) -> Self {
        Self(name)
    }
}

/// The outcome of discovering the fleet before a coordination attempt.
pub(crate) enum Discovery {
    /// No live peers: coordinate with nobody.
    Solo,
    /// Live peers that haven't registered on the medium — they run a
    /// config that doesn't know it. Names inside, formatted for an
    /// error message.
    // Constructed by fleet-coordination implementations of this module.
    #[allow(dead_code)]
    Missing(String),
    // Constructed by fleet-coordination implementations of this module.
    #[allow(dead_code)]
    Ready(Box<Coordinator>),
}

/// One coordination attempt: this instance leading its live peers
/// through published topic states. Never constructed in this build:
/// [`Coordinator::discover`] is always [`Discovery::Solo`].
pub(crate) struct Coordinator {}

impl Coordinator {
    /// Discover the fleet before a coordination attempt. This build
    /// has no fleet coordination: it warns and coordinates this
    /// instance only.
    pub(crate) async fn discover(
        _topic: Topic,
        _fleet: &Cluster,
        _medium: &Cluster,
    ) -> Result<Discovery, Error> {
        warn!(
            "no fleet coordination available; coordinating this instance only: \
             other pgdog instances serving this database will not pause writes \
             for this cutover"
        );
        Ok(Discovery::Solo)
    }

    /// Attach consumer-defined data to every state this attempt
    /// publishes, e.g. which keys a coordinated operation covers.
    // Consumed by the MOVE KEYS cutover.
    #[allow(dead_code)]
    pub(crate) fn set_payload(&mut self, _payload: String) {}

    /// Publish a state and wait for every peer to ack it. `Ok(None)`
    /// when everyone acked; `Ok(Some(stragglers))` on deadline.
    pub(crate) async fn broadcast_and_await(
        &self,
        _state: &str,
        _timeout: Duration,
    ) -> Result<Option<String>, Error> {
        Ok(None)
    }

    /// Publish a state without waiting for acks, best effort.
    pub(crate) async fn publish(&self, _state: &str) {}

    /// Refresh `state` for as long as the guard lives, so followers'
    /// silence failsafe only fires on a coordinator that actually
    /// died. Drop the guard before publishing any other state.
    pub(crate) fn keep_fresh(&self, _state: &'static str) -> KeepFresh {
        KeepFresh
    }
}

/// Aborts the state keepalive on drop. No keepalive runs in this
/// build; the `Drop` impl keeps the cutovers' explicit `drop(refresh)`
/// meaningful for implementations that hold one.
pub(crate) struct KeepFresh;

impl Drop for KeepFresh {
    fn drop(&mut self) {}
}
