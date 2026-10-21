//! Owns interruptible VM work and charges execution-thread CPU time.

use super::cpu_clock::CpuTime;
use ckb_script::{
    ChunkCommand, RunMode, Scheduler, SchedulerRunner, VM_INTERRUPTED_MESSAGE,
    types::{DebugPrinter, Machine, TerminatedResult},
};
use ckb_snapshot::Snapshot;
use ckb_store::data_loader_wrapper::DataLoaderWrapper;
use ckb_types::core::Cycle;
use ckb_vm::{Error, machine::Pause};
use futures_util::FutureExt;
use std::{future::Future, io, time::Duration};
use tokio::{
    sync::{oneshot, watch},
    task::JoinHandle,
};

#[cfg(test)]
mod tests;

pub(super) struct VmRunner<'a> {
    pub command: &'a mut watch::Receiver<ChunkCommand>,
    pub remaining: Option<Duration>,
}

impl SchedulerRunner<Scheduler<DataLoaderWrapper<Snapshot>, DebugPrinter, Machine>>
    for VmRunner<'_>
{
    async fn run(
        &mut self,
        mut scheduler: Scheduler<DataLoaderWrapper<Snapshot>, DebugPrinter, Machine>,
        max_cycles: Cycle,
    ) -> Result<Option<TerminatedResult>, Error> {
        self.run_vm(move |pause| {
            let remaining = max_cycles
                .checked_sub(scheduler.consumed_cycles())
                .ok_or(Error::CyclesExceeded)?;
            scheduler.run(RunMode::Pause(pause, remaining))
        })
        .await
    }
}

impl VmRunner<'_> {
    /// A slice returns the scheduler only after it stops. Joining it both
    /// acknowledges suspension and accounts for its CPU time, before any resume.
    async fn run_vm(
        &mut self,
        mut execute: impl FnMut(Pause) -> Result<TerminatedResult, Error> + Send + 'static,
    ) -> Result<Option<TerminatedResult>, Error> {
        let mut desired = self.command.borrow_and_update().clone();
        loop {
            if self.remaining.is_some_and(|time| time.is_zero()) {
                return Ok(None);
            }
            while desired != ChunkCommand::Resume {
                if desired == ChunkCommand::Stop {
                    return Err(Error::External(VM_INTERRUPTED_MESSAGE.into()));
                }
                desired = self
                    .command
                    .changed()
                    .await
                    .map_or(ChunkCommand::Stop, |_| {
                        self.command.borrow_and_update().clone()
                    });
            }

            let (returned, result, elapsed) = self.run_slice(execute, &mut desired).await?;
            execute = returned;
            let elapsed = match elapsed {
                Ok(elapsed) => elapsed,
                Err(error) => {
                    // This runner cannot safely grant another group or resume
                    // after losing its CPU receipt, even if failure is final.
                    if let Some(remaining) = &mut self.remaining {
                        *remaining = Duration::ZERO;
                    }
                    return unmeasured_result(result, error).map(Some);
                }
            };
            if let Some(remaining) = &mut self.remaining {
                *remaining = remaining.saturating_sub(elapsed);
                if remaining.is_zero() {
                    return Ok(None);
                }
            }
            if !matches!(result, Err(Error::Pause)) {
                return result.map(Some);
            }
        }
    }

    /// A wall deadline requests a checkpoint, not a refusal. Only the joined
    /// CPU receipt spends the budget; descheduled work resumes with what remains.
    async fn run_slice<F>(
        &mut self,
        mut execute: F,
        desired: &mut ChunkCommand,
    ) -> Result<(F, Result<TerminatedResult, Error>, io::Result<Duration>), Error>
    where
        F: FnMut(Pause) -> Result<TerminatedResult, Error> + Send + 'static,
    {
        let limit = self.remaining;
        let pause = Pause::new();
        let child_pause = pause.clone();
        let (started, start) = oneshot::channel();
        let child = VmTask {
            pause,
            handle: tokio::task::spawn_blocking(move || {
                // The blocking executor keeps controls runnable and keeps both
                // clock readings on the thread that actually executes the VM.
                let clock = if let Some(limit) = limit {
                    let clock = CpuTime::now().map_err(clock_failure)?;
                    let deadline =
                        tokio::time::Instant::now() + limit.max(Duration::from_millis(1));
                    let _ = started.send(deadline);
                    Some(clock)
                } else {
                    None
                };
                let result = execute(child_pause);
                let elapsed = clock.map_or(Ok(Duration::ZERO), |clock| clock.elapsed());
                Ok((execute, result, elapsed))
            }),
        };
        // Start the deadline on the execution thread, excluding queueing. It
        // survives command changes and can interrupt this slice only once.
        let checkpoint = async move {
            match start.await {
                Ok(deadline) => tokio::time::sleep_until(deadline).await,
                _ => std::future::pending().await,
            }
        };
        child.join(self.command, desired, checkpoint).await?
    }
}

/// Keep determined failures available for canonical attribution. An unmeasured
/// success or Pause is a local interruption, never a new script error.
fn unmeasured_result(
    result: Result<TerminatedResult, Error>,
    error: io::Error,
) -> Result<TerminatedResult, Error> {
    let interrupted = clock_failure(error);
    match result {
        Ok(result) if result.exit_code != 0 => Ok(result),
        Err(error) if error != Error::Pause => Err(error),
        _ => Err(interrupted),
    }
}

fn clock_failure(error: io::Error) -> Error {
    ckb_logger::warn!("VM CPU measurement failed: {error}");
    if let Some(metrics) = ckb_metrics::handle() {
        metrics.ckb_tx_pool_vm_cpu_clock_failures_total.inc();
    }
    Error::External(VM_INTERRUPTED_MESSAGE.into())
}

/// Dropping the caller interrupts and cancels the slice it owns.
struct VmTask<T> {
    pause: Pause,
    handle: JoinHandle<T>,
}

impl<T> VmTask<T> {
    /// Observe controls and the checkpoint until this owned task has joined.
    /// Desired commands survive the slice; its checkpoint does not.
    async fn join(
        mut self,
        command: &mut watch::Receiver<ChunkCommand>,
        desired: &mut ChunkCommand,
        checkpoint: impl Future<Output = ()>,
    ) -> Result<T, Error> {
        let checkpoint = checkpoint.fuse();
        tokio::pin!(checkpoint);
        let completed = loop {
            tokio::select! {
                biased;
                result = &mut self.handle => break result,
                () = &mut checkpoint => {
                    self.pause.interrupt();
                }
                changed = command.changed(), if *desired != ChunkCommand::Stop => {
                    *desired = changed.map_or(ChunkCommand::Stop, |_| {
                        command.borrow_and_update().clone()
                    });
                    if *desired != ChunkCommand::Resume {
                        self.pause.interrupt();
                    }
                }
            }
        };
        match completed {
            Ok(completed) => Ok(completed),
            Err(error) if error.is_panic() => std::panic::resume_unwind(error.into_panic()),
            Err(_) => Err(Error::External(VM_INTERRUPTED_MESSAGE.into())),
        }
    }
}

impl<T> Drop for VmTask<T> {
    fn drop(&mut self) {
        self.pause.interrupt();
        self.handle.abort();
    }
}
