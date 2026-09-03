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

mod guards;

/// Stages of adding a shard, reported as the task's status.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Display)]
pub(crate) enum AddShardStatus {
    /// Checking that the cluster can grow in place.
    #[display("validating")]
    Validating,
    /// Running the pre-data schema-sync child task.
    #[display("syncing schema")]
    SchemaSync,
    /// Running the data-copy child task.
    #[display("syncing data")]
    SyncingData,
    /// Running the post-data schema-sync child task.
    #[display("finalizing schema")]
    FinalizingSchema,
    /// Streaming changes to catch the new shard up.
    #[display("replicating")]
    Replicating,
    /// Caught up; waiting for an operator `CUTOVER`.
    #[display("awaiting cutover")]
    AwaitingCutover,
    /// Omni writes paused; draining replication to zero.
    #[display("draining")]
    Draining,
    /// Swapping the new shard into the topology.
    #[display("swapping topology")]
    SwappingTopology,
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
