//! The provisioning phases: publication, schema sync (pre and post),
//! data copy, and replication catch-up. Each phase wraps a child task
//! and reports its status; the orchestrator threads through them.

use std::time::Duration;

use tokio::select;
use tokio::time::interval;
use tracing::info;

use super::guards::Preflight;
use super::{AddShardStatus, AddShardTask};
use crate::api::MigrationError;
use crate::api::copy_data::CopyDataTask;
use crate::api::schema_sync::{SchemaSyncPhase, SchemaSyncTask};
use crate::api::task::TaskContext;
use crate::backend::databases::databases;
use crate::backend::pool::Request;
use crate::backend::replication::logical::Error;
use crate::backend::replication::logical::orchestrator::{Orchestrator, ReplicationWaiter};
use crate::backend::replication::logical::publisher::publication::{
    create_publication, drop_publication, drop_publication_on,
};
use crate::config::config;
use crate::util::random_string;
use tokio_util::sync::CancellationToken;

/// The publication scoping replication to the omnisharded tables:
/// operator-supplied ones are trusted and never dropped; auto-created
/// ones live exactly as long as the task.
pub(super) struct Publication {
    name: String,
    created: bool,
}

impl Publication {
    /// Entry: guards passed, omni tables known. Exit: the publication
    /// exists on shard 0 and covers exactly the omni tables. Failure:
    /// nothing to clean up (an existing mismatched publication is
    /// refused, not altered).
    pub(super) async fn ensure(
        task: &AddShardTask,
        preflight: &Preflight,
    ) -> Result<Self, MigrationError> {
        let created = task.publication.is_none();
        let name = task
            .publication
            .clone()
            .unwrap_or_else(|| format!("__pgdog_add_shard_{}", random_string(12).to_lowercase()));
        if created {
            create_publication(&preflight.source, 0, &name, &preflight.omni_tables).await?;
        }
        Ok(Self { name, created })
    }

    pub(super) fn name(&self) -> &str {
        &self.name
    }

    /// Drop an auto-created publication when the task ends. On success
    /// the topology has changed, so re-resolve the cluster; the schema
    /// dump also recreated the publication on the new shard, so drop
    /// it there too. Best effort on every step.
    pub(super) async fn cleanup(&self, task: &AddShardTask, succeeded: bool) {
        if !self.created {
            return;
        }
        if let Ok(source) = databases().schema_owner(&task.database) {
            drop_publication(&source, 0, &self.name).await;
            if succeeded
                && let Some(shard) = source.shards().get(task.shard)
                && let Ok(mut server) = shard.primary(&Request::default()).await
            {
                let _ = drop_publication_on(&mut server, &self.name).await;
            }
        }
    }
}

/// Entry: guards passed. Exit: all DDL restored on the new shard
/// (pre-data phase) and the registry reloaded. Failure: propagates; no
/// replication slots exist yet.
pub(super) async fn sync_schema_pre(
    task: &AddShardTask,
    ctx: &TaskContext<AddShardTask>,
    orchestrator: Orchestrator,
) -> Result<Orchestrator, MigrationError> {
    let _ = task;
    ctx.set_status(AddShardStatus::SchemaSync);
    ctx.run(schema_sync(&orchestrator, SchemaSyncPhase::Pre))
        .await?;
    Ok(orchestrator)
}

/// A schema-sync task scoped to the provisioning destination, which
/// the registry cannot resolve.
fn schema_sync(orchestrator: &Orchestrator, phase: SchemaSyncPhase) -> SchemaSyncTask {
    SchemaSyncTask::builder()
        .databases(orchestrator.databases())
        .publication(orchestrator.publication().to_owned())
        .phase(phase)
        .ignore_errors(true)
        .fixed_destination(orchestrator.destination().clone())
        .schema_only(orchestrator.is_schema_only())
        .build()
}

/// Entry: schema restored. Exit: every omnisharded table
/// snapshot-copied onto the new shard, consistent with the durable
/// replication slot's start point. Failure: propagates; the slot
/// cleanup guard in the caller drops what the copy created.
pub(super) async fn copy_data(
    task: &AddShardTask,
    ctx: &TaskContext<AddShardTask>,
    orchestrator: Orchestrator,
) -> Result<Orchestrator, MigrationError> {
    let _ = task;
    ctx.set_status(AddShardStatus::SyncingData);
    ctx.run(
        CopyDataTask::builder()
            .orchestrator(orchestrator.clone())
            .require_replica_identity(true)
            .build(),
    )
    .await?;
    Ok(orchestrator)
}

/// Entry: data copied. Exit: post-data schema (indexes, constraints)
/// restored. Failure: propagates.
pub(super) async fn sync_schema_post(
    task: &AddShardTask,
    ctx: &TaskContext<AddShardTask>,
    mut orchestrator: Orchestrator,
) -> Result<Orchestrator, MigrationError> {
    let _ = task;
    ctx.set_status(AddShardStatus::FinalizingSchema);
    orchestrator.refresh()?;
    ctx.run(schema_sync(&orchestrator, SchemaSyncPhase::Post))
        .await?;
    Ok(orchestrator)
}

/// Entry: schema and data in place. Exit: WAL streaming from shard 0
/// to the new shard. Failure: propagates.
pub(super) async fn replicate(
    task: &AddShardTask,
    ctx: &TaskContext<AddShardTask>,
    mut orchestrator: Orchestrator,
) -> Result<ReplicationWaiter, MigrationError> {
    let _ = task;
    ctx.set_status(AddShardStatus::Replicating);
    orchestrator.refresh()?;
    Ok(orchestrator.replicate().await?)
}

/// Wait until replication lag first drops under the cutover threshold,
/// or the task is cancelled. Aborts if the provisioning lock's session
/// dies while waiting.
pub(super) async fn wait_for_catch_up(
    task: &AddShardTask,
    token: &CancellationToken,
    waiter: &ReplicationWaiter,
    preflight: &Preflight,
) -> Result<(), MigrationError> {
    let threshold = config().config.general.cutover_replication_lag_threshold;
    let mut check = interval(Duration::from_secs(1));

    loop {
        select! {
            _ = token.cancelled() => return Ok(()),
            _ = preflight.lock_lost() => return Err(Error::ProvisioningLockLost.into()),
            _ = check.tick() => {
                if let Some(lag) = waiter.lag().await
                    && lag <= threshold
                {
                    info!(
                        "[add shard] \"{}\" new shard caught up (lag: {} bytes)",
                        task.database, lag
                    );
                    return Ok(());
                }
            }
        }
    }
}

/// Stop streaming and join the stream tasks; slots are dropped as the
/// streams wind down.
pub(super) async fn stop_stream(waiter: &mut ReplicationWaiter) -> Result<(), MigrationError> {
    waiter.stop();
    waiter.wait().await?;
    Ok(())
}
