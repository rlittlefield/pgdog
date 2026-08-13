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

mod cleanup;
mod copy;
mod cutover;
mod guards;

use std::time::Duration;

use crate::api::async_task::AsyncTaskContext;
use crate::api::{MigrationError, Task};
use crate::backend::replication::logical::orchestrator::Orchestrator;

use guards::Preflight;

/// Stages of moving keys, reported as the task's status.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Display)]
pub(crate) enum MoveKeysStatus {
    /// Checking that the keys can move and taking the locks.
    #[display("validating")]
    Validating,
    /// Copying the moving keys' rows to the target shard.
    #[display("syncing data")]
    SyncingData,
    /// Streaming changes to keep the copied rows fresh.
    #[display("replicating")]
    Replicating,
    /// Caught up; waiting for an operator `CUTOVER`.
    #[display("awaiting cutover")]
    AwaitingCutover,
    /// Writes for the moving keys paused; draining replication to zero.
    #[display("draining")]
    Draining,
    /// Flipping the keys' placement on every shard.
    #[display("flipping placement")]
    Flipping,
    /// Deleting the moved rows from the source shard.
    #[display("cleaning up")]
    CleaningUp,
}

/// Outcome of a cutover attempt.
enum CutoverOutcome {
    /// The placement flipped.
    Done,
    /// The cutover was called off (drain timeout, or the fleet wasn't
    /// ready): every barrier was released and replication continues;
    /// the task goes back to waiting for a cutover.
    Aborted,
}

/// How the copy-and-cutover phase ended.
enum Completion {
    /// The placement flipped: the moved rows own the target shard.
    Flipped,
    /// The task ended without flipping (cancelled, or the stream ended
    /// on its own): the copied rows must be scrubbed from the target.
    Stopped,
}

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

    async fn run(self, ctx: AsyncTaskContext<Self>) -> Result<(), MigrationError> {
        // Take the cancellation token so a `STOP_TASK` winds the
        // children down cooperatively.
        let token = ctx.cancellation_token();

        // Refuse anything that would corrupt or collide, and take
        // every lock the task holds until it ends.
        ctx.set_status(MoveKeysStatus::Validating);
        let preflight = guards::preflight(&self).await?;

        // Publication for the moving tables on the source shard;
        // dropped when the task ends.
        let publication = copy::Publication::ensure(&preflight).await?;
        let orchestrator = Orchestrator::for_key_move(
            &self.database,
            publication.name(),
            preflight.scope.clone(),
        )?;

        let result = self
            .copy_and_cutover(&ctx, &token, orchestrator, &preflight)
            .await;

        // A run that ended before the flip leaves this move's rows on
        // the target: scrub them so a retry starts clean.
        if !matches!(result, Ok(Completion::Flipped)) {
            cleanup::scrub_target(&preflight).await;
        }
        publication.cleanup(&preflight).await;

        result.map(|_| ())
    }
}

impl MoveKeysTask {
    /// Copy, catch-up, then the cutover loop: each pass waits for
    /// catch-up, parks for the operator (unless auto), and attempts
    /// the cutover; an aborted attempt re-enters at catch-up. The flip
    /// is followed by the source cleanup. Replication slots are
    /// cleaned up on any error.
    async fn copy_and_cutover(
        &self,
        ctx: &AsyncTaskContext<Self>,
        token: &tokio_util::sync::CancellationToken,
        orchestrator: Orchestrator,
        preflight: &Preflight,
    ) -> Result<Completion, MigrationError> {
        // From here the orchestrator may hold replication slots.
        let slots = orchestrator.publication_guard();
        let result: Result<Completion, MigrationError> = async {
            let orchestrator = copy::copy_data(self, ctx, orchestrator).await?;
            let mut waiter = copy::replicate(self, ctx, orchestrator).await?;

            // Catch up, then cut over: automatically, or when the
            // operator says so.
            loop {
                copy::wait_for_catch_up(self, token, &waiter).await?;

                // A cancellation during catch-up stops the task. It
                // must not fall through into the cutover: auto mode
                // has no other cancellation check before the flip.
                if token.is_cancelled() {
                    copy::stop_stream(&mut waiter).await?;
                    return Ok(Completion::Stopped);
                }

                if !self.auto_cutover {
                    match cutover::park(self, ctx, token, &mut waiter).await? {
                        cutover::Parked::CutoverRequested => {}
                        cutover::Parked::Ended => return Ok(Completion::Stopped),
                    }
                }

                match cutover::run(self, ctx, &mut waiter, preflight).await? {
                    CutoverOutcome::Done => break,
                    CutoverOutcome::Aborted => {
                        ctx.set_status(MoveKeysStatus::Replicating);
                    }
                }
            }

            // The flip stands: delete the moved rows from the source,
            // best effort with named recovery steps.
            ctx.set_status(MoveKeysStatus::CleaningUp);
            cleanup::source(self, preflight).await;

            Ok(Completion::Flipped)
        }
        .await;

        if result.is_err()
            && let Err(err) = slots.cleanup().await
        {
            tracing::warn!("failed to clean up replication slots after move keys: {err}");
        }

        result
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn test_status_labels_distinct() {
        let statuses = [
            MoveKeysStatus::Validating,
            MoveKeysStatus::SyncingData,
            MoveKeysStatus::Replicating,
            MoveKeysStatus::AwaitingCutover,
            MoveKeysStatus::Draining,
            MoveKeysStatus::Flipping,
            MoveKeysStatus::CleaningUp,
        ];
        let labels: HashSet<String> = statuses.iter().map(|s| s.to_string()).collect();
        assert_eq!(labels.len(), statuses.len());
        assert!(labels.iter().all(|label| !label.is_empty()));
    }
}
