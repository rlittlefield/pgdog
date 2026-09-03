//! Logical-replication background task.
//!
//! Drives a `ReplicationWaiter` to completion. Without `auto_cutover`
//! (standalone `REPLICATE`, `copy_data`) it stops on cancellation
//! (`STOP_TASK`), cuts over on an operator `CUTOVER` addressed to this task
//! (delivered through [`ReplicationTask::cutover`]), and otherwise finishes
//! when the source slot drains (no cutover on natural drain). With
//! `auto_cutover` set (reshard) it cuts over automatically once the
//! destination has caught up.

use std::time::Duration;

use tokio::select;
use tokio_util::sync::CancellationToken;

use crate::api::Task;
use crate::api::schema_sync::SchemaSyncTask;
use crate::api::task::TaskContext;
use crate::backend::replication::logical::Error;
use crate::backend::replication::logical::orchestrator::ReplicationWaiter;
use pgdog_stats::{ReplicationDefinition, ReplicationStatus, TaskDefinition};

/// Direction of a replication task: the initial migration (`Forward`) or the
/// post-cutover reverse stream that backs a rollback (`Reverse`). A `CUTOVER`
/// on a `Reverse` task is therefore a rollback. Affects reported status only,
/// not control flow.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) enum Direction {
    #[default]
    Forward,
    Reverse,
}

/// Run the replication by driving a [`ReplicationWaiter`] to completion.
#[derive(Debug, bon::Builder)]
pub(crate) struct ReplicationTask {
    /// The running replication waiter this task drives to completion.
    pub(crate) waiter: ReplicationWaiter,
    /// Cut over automatically once the destination has caught up, instead
    /// of waiting for an operator `CUTOVER`.
    #[builder(default)]
    pub(crate) auto_cutover: bool,
    /// Replication direction. `Reverse` marks the post-cutover stream that
    /// backs a rollback; it only affects reported status, not control flow.
    #[builder(default)]
    pub(crate) direction: Direction,
    pub(crate) schema_sync: SchemaSyncTask,
}

impl Task for ReplicationTask {
    type Status = ReplicationStatus;
    type Output = ();
    type Error = Error;

    fn cancel_timeout() -> Duration {
        Duration::from_secs(60)
    }

    fn definition(&self) -> impl Into<TaskDefinition> {
        ReplicationDefinition {
            databases: self.waiter.databases(),
            reverse: self.direction == Direction::Reverse,
            auto_cutover: self.auto_cutover,
        }
    }

    async fn run(mut self, ctx: TaskContext<Self>) -> Result<(), Error> {
        let token = ctx.cancellation_token();

        ctx.set_status(ReplicationStatus::Replicating);

        if self.auto_cutover {
            return self.perform_cutover(&ctx, &token).await;
        }

        let cutover = crate::api::cutover_registry::register_cutover(
            ctx.root_id(),
            &self.waiter.source_database(),
            None,
        );

        select! {
            _ = token.cancelled() => {
                ctx.set_status(ReplicationStatus::Stopping);
                self.waiter.stop();
            }
            _ = cutover.requested() => {
                self.perform_cutover(&ctx, &token).await?;
            }
            res = self.waiter.wait() => {
                res?;
            }
        }

        Ok(())
    }
}

impl ReplicationTask {
    /// Perform the actual cutover for running replication.
    async fn perform_cutover(
        mut self,
        ctx: &TaskContext<Self>,
        token: &CancellationToken,
    ) -> Result<(), Error> {
        ctx.set_status(match self.direction {
            Direction::Forward => ReplicationStatus::CuttingOver,
            Direction::Reverse => ReplicationStatus::RollingBack,
        });
        self.waiter.cutover(token, ctx, self.schema_sync).await
    }
}
