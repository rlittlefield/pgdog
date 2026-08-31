//! The copy phases: publication, filtered snapshot copy, and
//! replication catch-up. The copy and the stream carry only the moving
//! keys' rows, straight to the target shard.

use std::time::Duration;

use tokio::select;
use tokio::time::interval;
use tracing::info;

use super::guards::Preflight;
use super::{MoveKeysStatus, MoveKeysTask};
use crate::api::MigrationError;
use crate::api::async_task::AsyncTaskContext;
use crate::api::copy_data::CopyDataTask;
use crate::backend::databases::databases;
use crate::backend::replication::logical::orchestrator::{Orchestrator, ReplicationWaiter};
use crate::backend::replication::logical::publisher::publication::{
    create_publication, drop_publication,
};
use crate::config::config;
use crate::util::random_string;
use tokio_util::sync::CancellationToken;

/// The publication scoping replication to the moving tables on the
/// source shard. Always auto-created; lives exactly as long as the
/// task.
pub(super) struct Publication {
    name: String,
    source_shard: usize,
}

impl Publication {
    /// Entry: guards passed, moving tables known. Exit: the
    /// publication exists on the source shard and covers exactly the
    /// moving tables. Failure: nothing to clean up.
    pub(super) async fn ensure(preflight: &Preflight) -> Result<Self, MigrationError> {
        let name = format!("__pgdog_move_{}", random_string(12).to_lowercase());
        let tables = preflight
            .scope
            .tables()
            .iter()
            .map(|table| format!("{}.{}", table.schema, table.name))
            .collect::<Vec<_>>();
        create_publication(&preflight.source, preflight.scope.source(), &name, &tables).await?;
        Ok(Self {
            name,
            source_shard: preflight.scope.source(),
        })
    }

    pub(super) fn name(&self) -> &str {
        &self.name
    }

    /// Drop the publication when the task ends. Best effort.
    pub(super) async fn cleanup(&self, preflight: &Preflight) {
        if let Ok(source) = databases().schema_owner(preflight.source.name()) {
            drop_publication(&source, self.source_shard, &self.name).await;
        }
    }
}

/// Entry: guards passed. Exit: every moving key's rows snapshot-copied
/// onto the target shard, consistent with the durable replication
/// slot's start point. Failure: propagates; the slot cleanup guard in
/// the caller drops what the copy created, and the caller scrubs the
/// target.
pub(super) async fn copy_data(
    task: &MoveKeysTask,
    ctx: &AsyncTaskContext<MoveKeysTask>,
    orchestrator: Orchestrator,
) -> Result<Orchestrator, MigrationError> {
    let _ = task;
    ctx.set_status(MoveKeysStatus::SyncingData);
    Ok(ctx
        .run(CopyDataTask::builder().orchestrator(orchestrator).build())
        .await?)
}

/// Entry: data copied. Exit: WAL streaming from the source shard,
/// filtered to the moving keys, applying to the target. Failure:
/// propagates.
pub(super) async fn replicate(
    task: &MoveKeysTask,
    ctx: &AsyncTaskContext<MoveKeysTask>,
    mut orchestrator: Orchestrator,
) -> Result<ReplicationWaiter, MigrationError> {
    let _ = task;
    ctx.set_status(MoveKeysStatus::Replicating);
    orchestrator.refresh()?;
    Ok(orchestrator.replicate().await?)
}

/// Wait until replication lag first drops under the cutover threshold,
/// or the task is cancelled.
pub(super) async fn wait_for_catch_up(
    task: &MoveKeysTask,
    token: &CancellationToken,
    waiter: &ReplicationWaiter,
) -> Result<(), MigrationError> {
    let threshold = config().config.general.cutover_replication_lag_threshold;
    let mut check = interval(Duration::from_secs(1));

    loop {
        select! {
            _ = token.cancelled() => return Ok(()),
            _ = check.tick() => {
                if let Some(lag) = waiter.lag().await
                    && lag <= threshold
                {
                    info!(
                        "[move keys] \"{}\" target shard caught up (lag: {} bytes)",
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
