//! Owns interruptible VM work and charges only time spent inside the scheduler.

use ckb_script::{
    ChunkCommand, RunMode, Scheduler, SchedulerRunner,
    types::{DebugPrinter, Machine, TerminatedResult},
};
use ckb_snapshot::Snapshot;
use ckb_store::data_loader_wrapper::DataLoaderWrapper;
use ckb_types::core::Cycle;
use ckb_vm::{Error, machine::Pause};
use futures_util::FutureExt;
use std::time::{Duration, Instant};
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
        self.run_vm(move |pause| scheduler.run(RunMode::Pause(pause, max_cycles)))
            .await
    }
}

impl VmRunner<'_> {
    /// A slice returns the scheduler only after it stops. Joining it both
    /// acknowledges suspension and accounts for its active time, before any resume.
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
                    return Err(Error::External("stopped".into()));
                }
                desired = self
                    .command
                    .changed()
                    .await
                    .map_or(ChunkCommand::Stop, |_| {
                        self.command.borrow_and_update().clone()
                    });
            }

            let limit = self.remaining;
            let pause = Pause::new();
            let child_pause = pause.clone();
            let (started, start) = oneshot::channel();
            let mut child = VmTask {
                pause,
                handle: tokio::spawn(async move {
                    let now = Instant::now();
                    let _ = started.send(now);
                    let run = || execute(child_pause);
                    // Leave the runtime free to poll network deadlines even
                    // when every async worker is executing a VM.
                    let result = if limit.is_some() {
                        tokio::task::block_in_place(run)
                    } else {
                        run()
                    };
                    (execute, result, now.elapsed())
                }),
            };
            // The deadline starts inside the task, excluding time in its queue.
            // It belongs to this slice and cannot interrupt a later resume.
            let deadline = async move {
                match start
                    .await
                    .ok()
                    .and_then(|started| limit.and_then(|limit| started.checked_add(limit)))
                {
                    Some(deadline) => tokio::time::sleep_until(deadline.into()).await,
                    None => std::future::pending().await,
                }
            }
            .fuse();
            tokio::pin!(deadline);
            let completed = loop {
                tokio::select! {
                    biased;
                    result = &mut child.handle => break result,
                    _ = &mut deadline => {
                        child.pause.interrupt();
                    }
                    changed = self.command.changed(), if desired != ChunkCommand::Stop => {
                        desired = changed.map_or(ChunkCommand::Stop, |_| {
                            self.command.borrow_and_update().clone()
                        });
                        if desired != ChunkCommand::Resume {
                            child.pause.interrupt();
                        }
                    }
                }
            };
            let (returned, result, elapsed) = match completed {
                Ok(completed) => completed,
                Err(error) if error.is_panic() => std::panic::resume_unwind(error.into_panic()),
                Err(_) => return Err(Error::External("stopped".into())),
            };
            execute = returned;
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
}

/// Dropping the caller interrupts and cancels the slice it owns.
struct VmTask<T> {
    pause: Pause,
    handle: JoinHandle<T>,
}

impl<T> Drop for VmTask<T> {
    fn drop(&mut self) {
        self.pause.interrupt();
        self.handle.abort();
    }
}
