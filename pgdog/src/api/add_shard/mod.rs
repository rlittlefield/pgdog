//! Add-shard background task.
//!
//! Provisions a new shard for a cluster whose sharded tables are
//! placement-stable (`lookup_result = "shard"` or explicit mappings):
//! syncs DDL, snapshot-copies omnisharded tables from shard 0, streams
//! WAL until the new shard has caught up, then — on operator `CUTOVER`
//! or automatically — pauses omnisharded writes fleet-wide, drains
//! replication to zero, activates the shard in the topology, and
//! resumes. Sharded traffic and all reads flow throughout; only omni
//! writes pause, for the sub-second drain.
//!
//! The destination is the shard declared with `provisioning = true` in
//! the config: declared in its final shape, excluded from the serving
//! topology until the cutover flips the flag in the running config.
//!
//! Each phase lives in its own module with its contract documented:
//! [`guards`] (everything the task holds), [`provision`] (schema,
//! data, catch-up), [`cutover`] (park, drain, swap, finalize), and
//! [`schema_only`] (the degenerate path without omnisharded tables).

mod cutover;
mod guards;
mod provision;

use std::time::Duration;

use crate::api::task::TaskContext;
use crate::api::{MigrationError, Task};
use pgdog_stats::{AddShardStatus, TaskDefinition};

/// Outcome of a cutover attempt.
enum CutoverOutcome {
    /// The shard is in the topology.
    Done,
    /// The cutover was called off (drain timeout, or the fleet wasn't
    /// ready): every barrier was released and replication continues;
    /// the task goes back to waiting for a cutover.
    Aborted,
}

/// Add a new shard to a database: provision the shard declared with
/// `provisioning = true` in the config, catch it up over logical
/// replication, and activate it in the topology.
#[derive(Display, Debug, bon::Builder)]
#[display("add_shard {database} shard {shard}")]
pub(crate) struct AddShardTask {
    /// The database gaining a shard.
    pub database: String,
    /// The shard being added: names one of the database's
    /// `provisioning = true` entries.
    pub shard: usize,
    /// Operator-supplied publication; when absent, one is created for
    /// the omnisharded tables and dropped when the task ends.
    pub publication: Option<String>,
    /// Cut over automatically once the new shard has caught up,
    /// instead of waiting for an operator `CUTOVER`.
    #[builder(default)]
    pub auto_cutover: bool,
}

impl Task for AddShardTask {
    type Status = AddShardStatus;
    type Output = ();
    type Error = MigrationError;

    fn cancel_timeout() -> Duration {
        Duration::from_secs(60)
    }

    fn definition(&self) -> impl Into<TaskDefinition> {
        "add_shard"
    }

    // Nothing constructs this task yet: the full run loop lands with
    // the schema-only path and the ADD SHARD admin command.
    async fn run(self, _ctx: TaskContext<Self>) -> Result<(), MigrationError> {
        unimplemented!("ADD SHARD run loop lands with the schema-only path")
    }
}
