//! MOVE KEYS coordination vocabulary: the topic and the states its
//! cutover publishes through the coordination seam
//! (`fleet::coordination`), and how long it waits for the peers'
//! acks. Shard 0 of the database being resharded is the medium: every
//! instance serving the database can reach it.

use std::time::Duration;

use crate::backend::fleet::coordination::Topic;

/// MOVE KEYS coordination, on shard 0 of the database as the medium.
pub(crate) const TOPIC: Topic = Topic::new("key_move");

/// How long the coordinator waits for every peer to pause the keys.
pub(crate) const ARM_ACK_TIMEOUT: Duration = Duration::from_secs(10);

/// How long the coordinator waits for every peer to invalidate its
/// caches. The flip stands either way; a straggler's stale cache
/// entries still point at the source, whose rows exist until cleanup.
pub(crate) const ACTIVATE_ACK_TIMEOUT: Duration = Duration::from_secs(15);

pub(crate) const STATE_ARMED: &str = "armed";
pub(crate) const STATE_ACTIVATED: &str = "activated";
pub(crate) const STATE_RELEASED: &str = "released";
