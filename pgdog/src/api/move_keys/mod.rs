//! Move-keys background task: a single-key reshard.
//!
//! Moves every row belonging to specific sharding key values from the
//! shard they live on to another, semi-live: snapshot-copies the keys'
//! rows while traffic flows, streams WAL until the target has caught
//! up, then — on operator `CUTOVER` or automatically — pauses writes
//! for just those keys fleet-wide, drains replication to zero, flips
//! the keys' placement with the configured `move_query`, invalidates
//! every instance's lookup cache, and resumes. Traffic for every other
//! key flows throughout. The rows left on the source shard are deleted
//! once the fleet has acknowledged the flip.
//!
//! Requires every sharded table to place rows via `lookup_result =
//! "shard"` with a `move_query`: stored placement is what makes a
//! single key's shard flippable.
//!
//! Each phase lives in its own module with its contract documented:
//! [`guards`] (everything the task refuses on and holds), [`copy`]
//! (publication, filtered snapshot copy, catch-up), [`cutover`] (park,
//! arm, drain, flip, invalidate), and [`cleanup`] (source deletion,
//! and the target scrub on pre-flip aborts).

mod copy;
mod guards;

use std::time::Duration;

use crate::api::task::TaskContext;
use crate::api::{MigrationError, Task};
use pgdog_stats::{MoveKeysStatus, TaskDefinition};

/// Move sharding keys to another shard: copy their rows, catch up over
/// logical replication, flip their placement.
#[derive(Display, Debug, bon::Builder)]
#[display("move_keys {database} to shard {target}")]
pub(crate) struct MoveKeysTask {
    /// The database whose keys move.
    pub database: String,
    /// The shard the keys move to.
    pub target: usize,
    /// The sharding key values that move. All must currently live on
    /// the same shard.
    pub keys: Vec<String>,
    /// Cut over automatically once the target has caught up, instead
    /// of waiting for an operator `CUTOVER`.
    #[builder(default)]
    pub auto_cutover: bool,
}

impl Task for MoveKeysTask {
    type Status = MoveKeysStatus;
    type Output = ();
    type Error = MigrationError;

    fn cancel_timeout() -> Duration {
        Duration::from_secs(60)
    }

    fn definition(&self) -> impl Into<TaskDefinition> {
        "move_keys"
    }

    // Nothing constructs this task yet: the full run loop lands with
    // the cutover phase and the MOVE KEYS admin command.
    async fn run(self, _ctx: TaskContext<Self>) -> Result<(), MigrationError> {
        unimplemented!("MOVE KEYS run loop lands with the cutover phase")
    }
}
