//! Owns interruptible VM work and charges only time spent inside the scheduler.

use ckb_script::{
    ChunkCommand, RunMode, Scheduler, SchedulerRunner,
    types::{DebugPrinter, Machine, TerminatedResult},
};
use ckb_snapshot::Snapshot;
use ckb_store::data_loader_wrapper::DataLoaderWrapper;
use ckb_types::core::Cycle;
use ckb_vm::{Error, machine::Pause};
use std::time::{Duration, Instant};
use tokio::{sync::watch, task::JoinHandle};

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
    /// Run one group, joining it before charging its final receipt. Dropping
    /// this future interrupts and cancels its owned task, including during a pause.
    async fn run_vm(
        &mut self,
        execute: impl FnMut(Pause) -> Result<TerminatedResult, Error> + Send + 'static,
    ) -> Result<Option<TerminatedResult>, Error> {
        if self.remaining.is_some_and(|time| time.is_zero()) {
            return Ok(None);
        }
        let initial_command = self.command.borrow().clone();
        let (command, commands) = watch::channel(initial_command.clone());
        let (receipts, mut receipt) = watch::channel(Slice::default());
        let pause = Pause::new();
        let child_pause = pause.clone();
        let has_budget = self.remaining.is_some();
        let handle = tokio::spawn(execute_slices(
            execute,
            commands,
            child_pause,
            receipts,
            has_budget,
        ));
        let mut child = VmTask {
            command,
            pause,
            handle,
        };
        let mut desired = initial_command.clone();
        // Sending Suspend does not acknowledge quiescence. Only an Idle
        // receipt allows Resume to clear the VM's interrupt flag.
        let mut run_requested = initial_command == ChunkCommand::Resume;
        loop {
            let deadline = if child.pause.has_interrupted() || desired == ChunkCommand::Stop {
                None
            } else {
                self.remaining
                    .and_then(|limit| receipt.borrow().deadline(limit))
            };
            tokio::select! {
                biased;
                result = &mut child.handle => {
                    if let Some(remaining) = &mut self.remaining {
                        *remaining = remaining.saturating_sub(receipt.borrow().charged);
                    }
                    let result = match result {
                        Ok(result) => result,
                        Err(error) if error.is_panic() => std::panic::resume_unwind(error.into_panic()),
                        Err(_) => return Err(Error::External("stopped".into())),
                    };
                    return if self.remaining.is_some_and(|time| time.is_zero()) {
                        Ok(None)
                    } else {
                        result.map(Some)
                    };
                }
                Ok(()) = receipt.changed() => {
                    let slice = *receipt.borrow_and_update();
                    match slice.phase {
                        Phase::Running(_) => {}
                        Phase::Idle => {
                            run_requested = false;
                            if desired != ChunkCommand::Stop
                                && self.remaining.is_some_and(|limit| slice.charged >= limit)
                            {
                                desired = ChunkCommand::Stop;
                                let _ = child.command.send(ChunkCommand::Stop);
                            } else if desired == ChunkCommand::Resume {
                                child.pause.free();
                                let _ = child.command.send(ChunkCommand::Resume);
                                run_requested = true;
                            }
                        }
                        Phase::Finished => run_requested = false,
                    }
                }
                _ = async {
                    match deadline {
                        Some(deadline) => tokio::time::sleep_until(deadline.into()).await,
                        None => std::future::pending().await,
                    }
                } => {
                    // A timer can wake after a pause or resume. Recheck the
                    // latest slice; only the child's receipt decides exhaustion.
                    if self.remaining.and_then(|limit| receipt.borrow().deadline(limit))
                        .is_some_and(|deadline| deadline <= Instant::now())
                    {
                        child.pause.interrupt();
                    }
                }
                changed = self.command.changed(), if desired != ChunkCommand::Stop => {
                    desired = changed.map_or(ChunkCommand::Stop, |_| self.command.borrow().clone());
                    match desired {
                        ChunkCommand::Suspend => {
                            if run_requested { child.pause.interrupt(); }
                        }
                        ChunkCommand::Stop => {
                            if run_requested { child.pause.interrupt(); }
                            let _ = child.command.send(ChunkCommand::Stop);
                        }
                        ChunkCommand::Resume => {
                            if !run_requested && receipt.borrow().phase == Phase::Idle {
                                child.pause.free();
                                let _ = child.command.send(ChunkCommand::Resume);
                                run_requested = true;
                            }
                        }
                    }
                }
            }
        }
    }
}

async fn execute_slices(
    mut execute: impl FnMut(Pause) -> Result<TerminatedResult, Error> + Send + 'static,
    mut commands: watch::Receiver<ChunkCommand>,
    child_pause: Pause,
    receipts: watch::Sender<Slice>,
    has_budget: bool,
) -> Result<TerminatedResult, Error> {
    commands.mark_changed();
    let mut slice = Slice::default();
    loop {
        let next = commands
            .changed()
            .await
            .map_or(ChunkCommand::Stop, |_| commands.borrow().clone());
        match next {
            ChunkCommand::Stop => return Err(Error::External("stopped".into())),
            ChunkCommand::Suspend => continue,
            ChunkCommand::Resume => {}
        }
        let started = Instant::now();
        slice.phase = Phase::Running(started);
        receipts.send_replace(slice);
        let mut run = || execute(child_pause.clone());
        // Network verification must leave the runtime free to poll its
        // timer even when every async worker is executing a VM.
        let result = if has_budget {
            tokio::task::block_in_place(run)
        } else {
            run()
        };
        slice.charged = slice.charged.saturating_add(started.elapsed());
        slice.phase = if matches!(result, Err(Error::Pause)) {
            Phase::Idle
        } else {
            Phase::Finished
        };
        receipts.send_replace(slice);
        if !matches!(result, Err(Error::Pause)) {
            return result;
        }
    }
}

struct VmTask {
    command: watch::Sender<ChunkCommand>,
    pause: Pause,
    handle: JoinHandle<Result<TerminatedResult, Error>>,
}

impl Drop for VmTask {
    fn drop(&mut self) {
        self.pause.interrupt();
        let _ = self.command.send(ChunkCommand::Stop);
        self.handle.abort();
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
enum Phase {
    #[default]
    Idle,
    Running(Instant),
    Finished,
}

/// Cumulative receipts survive coalesced watch updates. Queueing, suspended
/// time and delays in polling the parent never spend the budget.
#[derive(Clone, Copy, Debug, Default)]
struct Slice {
    charged: Duration,
    phase: Phase,
}

impl Slice {
    fn deadline(self, limit: Duration) -> Option<Instant> {
        match self.phase {
            Phase::Running(started) => started.checked_add(limit.saturating_sub(self.charged)),
            Phase::Idle | Phase::Finished => None,
        }
    }
}
