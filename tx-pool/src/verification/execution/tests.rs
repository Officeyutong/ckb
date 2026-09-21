use super::*;
use futures_util::poll;
use std::sync::mpsc;
use tokio::sync::{mpsc as async_mpsc, oneshot};

const TEST_TIMEOUT: Duration = Duration::from_secs(3);

struct OnDrop(Option<oneshot::Sender<()>>);

impl Drop for OnDrop {
    fn drop(&mut self) {
        let _ = self.0.take().unwrap().send(());
    }
}

struct Invocation {
    pause: Pause,
    finish: mpsc::Sender<Result<TerminatedResult, Error>>,
}

// Each invocation returns only when the test acknowledges the VM's interrupt.
// This makes the Suspend/Resume race observable without hooks in production code.
fn controlled_vm() -> (
    impl FnMut(Pause) -> Result<TerminatedResult, Error> + Send,
    async_mpsc::UnboundedReceiver<Invocation>,
    oneshot::Receiver<()>,
) {
    let (started, invocations) = async_mpsc::unbounded_channel();
    let (dropped, finished) = oneshot::channel();
    let lifetime = OnDrop(Some(dropped));
    let execute = move |pause| {
        let _ = &lifetime;
        let (finish, result) = mpsc::channel();
        started.send(Invocation { pause, finish }).unwrap();
        result
            .recv_timeout(TEST_TIMEOUT)
            .expect("the test must release the VM")
    };
    (execute, invocations, finished)
}

fn success() -> TerminatedResult {
    TerminatedResult {
        exit_code: 0,
        consumed_cycles: 42,
    }
}

#[tokio::test]
async fn initial_suspend_and_stop_do_not_enter_the_scheduler() {
    let (commands, receiver) = watch::channel(ChunkCommand::Suspend);
    let (receipts, receipt) = watch::channel(Slice::default());
    let mut execution = Box::pin(execute_slices(
        |_| panic!("the scheduler must wait for Resume"),
        receiver,
        Pause::new(),
        receipts,
        false,
    ));
    assert!(poll!(&mut execution).is_pending());
    assert_eq!(receipt.borrow().phase, Phase::Idle);
    commands.send(ChunkCommand::Stop).unwrap();
    assert_eq!(
        execution.await.unwrap_err(),
        Error::External("stopped".into())
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn resume_waits_for_the_interrupted_slice_to_return() {
    let (commands, mut receiver) = watch::channel(ChunkCommand::Resume);
    let (execute, mut invocations, dropped) = controlled_vm();
    let mut runner = VmRunner {
        command: &mut receiver,
        remaining: Some(TEST_TIMEOUT),
    };
    let mut verification = Box::pin(runner.run_vm(execute));
    assert!(poll!(&mut verification).is_pending());
    let first = invocations.recv().await.unwrap();
    commands.send(ChunkCommand::Suspend).unwrap();
    assert!(poll!(&mut verification).is_pending());
    assert!(first.pause.has_interrupted());
    commands.send(ChunkCommand::Resume).unwrap();
    assert!(poll!(&mut verification).is_pending());
    assert!(
        first.pause.has_interrupted(),
        "Resume must wait for the Idle receipt"
    );
    assert!(invocations.try_recv().is_err());
    first.finish.send(Err(Error::Pause)).unwrap();
    let next = tokio::select! {
        result = &mut verification => panic!("verification ended before resuming: {result:?}"),
        invocation = invocations.recv() => invocation.unwrap(),
    };
    assert!(!next.pause.has_interrupted());
    next.finish.send(Ok(success())).unwrap();
    assert_eq!(verification.await.unwrap().unwrap().consumed_cycles, 42);
    dropped.await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn suspension_can_outlast_the_remaining_budget() {
    let (commands, mut receiver) = watch::channel(ChunkCommand::Resume);
    let (execute, mut invocations, dropped) = controlled_vm();
    let mut runner = VmRunner {
        command: &mut receiver,
        remaining: Some(Duration::from_millis(250)),
    };
    let mut verification = Box::pin(runner.run_vm(execute));
    assert!(poll!(&mut verification).is_pending());
    let first = invocations.recv().await.unwrap();
    commands.send(ChunkCommand::Suspend).unwrap();
    assert!(poll!(&mut verification).is_pending());
    assert!(first.pause.has_interrupted());
    first.finish.send(Err(Error::Pause)).unwrap();
    tokio::select! {
        result = &mut verification => panic!("suspension spent the budget: {result:?}"),
        _ = tokio::time::sleep(Duration::from_millis(350)) => {}
    }
    commands.send(ChunkCommand::Resume).unwrap();
    let resumed = tokio::select! {
        result = &mut verification => panic!("verification ended before resuming: {result:?}"),
        invocation = invocations.recv() => invocation.unwrap(),
    };
    resumed.finish.send(Ok(success())).unwrap();
    assert_eq!(verification.await.unwrap().unwrap().consumed_cycles, 42);
    dropped.await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn cancellation_interrupts_and_releases_running_vm_work() {
    let (_commands, mut receiver) = watch::channel(ChunkCommand::Resume);
    let (execute, mut invocations, dropped) = controlled_vm();
    let mut runner = VmRunner {
        command: &mut receiver,
        remaining: Some(TEST_TIMEOUT),
    };
    let mut verification = Box::pin(runner.run_vm(execute));
    assert!(poll!(&mut verification).is_pending());
    let running = invocations.recv().await.unwrap();
    drop(verification);
    assert!(running.pause.has_interrupted());
    running.finish.send(Err(Error::Pause)).unwrap();
    tokio::time::timeout(TEST_TIMEOUT, dropped)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn cancellation_and_closed_control_release_suspended_vm_work() {
    for cancel in [false, true] {
        let (commands, mut receiver) = watch::channel(ChunkCommand::Suspend);
        let (execute, mut invocations, dropped) = controlled_vm();
        let mut runner = VmRunner {
            command: &mut receiver,
            remaining: Some(TEST_TIMEOUT),
        };
        let mut verification = Box::pin(runner.run_vm(execute));
        assert!(poll!(&mut verification).is_pending());
        if cancel {
            drop(verification);
        } else {
            drop(commands);
            assert_eq!(
                verification.await.unwrap_err(),
                Error::External("stopped".into())
            );
        }
        tokio::time::timeout(TEST_TIMEOUT, dropped)
            .await
            .unwrap()
            .unwrap();
        assert!(invocations.recv().await.is_none());
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn deadline_survives_resume_flood_and_joins_vm_on_a_single_worker() {
    let (commands, mut receiver) = watch::channel(ChunkCommand::Resume);
    let (started, start) = mpsc::channel::<Pause>();
    let (stop_watchdog, stopped) = mpsc::channel();
    // An independent watchdog makes a runtime-starvation regression fail instead
    // of hanging the test process. It does not implement the verification deadline.
    let watchdog = std::thread::spawn(move || {
        if let Ok(pause) = start.recv_timeout(TEST_TIMEOUT)
            && stopped.recv_timeout(TEST_TIMEOUT).is_err()
        {
            pause.interrupt();
            return false;
        }
        true
    });
    let (dropped, finished) = oneshot::channel();
    let lifetime = OnDrop(Some(dropped));
    let execute = move |pause: Pause| {
        let _ = &lifetime;
        let _ = started.send(pause.clone());
        while !pause.has_interrupted() {
            std::thread::yield_now();
        }
        Err(Error::Pause)
    };
    let mut runner = VmRunner {
        command: &mut receiver,
        remaining: Some(Duration::from_millis(10)),
    };
    let verification = runner.run_vm(execute);
    tokio::pin!(verification);
    let result = loop {
        tokio::select! {
            biased;
            result = &mut verification => break result,
            _ = tokio::task::yield_now() => { commands.send(ChunkCommand::Resume).unwrap(); }
        }
    };
    let _ = stop_watchdog.send(());
    assert!(
        watchdog.join().unwrap(),
        "the runtime failed to poll its deadline"
    );
    assert!(result.unwrap().is_none());
    assert!(
        finished.await.is_ok(),
        "a budget refusal must join its VM task"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn groups_share_the_budget_and_exhaustion_precedes_completion() {
    let (_commands, mut receiver) = watch::channel(ChunkCommand::Resume);
    let mut runner = VmRunner {
        command: &mut receiver,
        remaining: Some(TEST_TIMEOUT),
    };
    assert!(runner.run_vm(|_| Ok(success())).await.unwrap().is_some());
    assert!(runner.remaining.unwrap() < TEST_TIMEOUT);
    // Even a normally completed slice is refused when its receipt exceeds the
    // remaining transaction budget. The next group must never start.
    runner.remaining = Some(Duration::from_nanos(1));
    assert!(runner.run_vm(|_| Ok(success())).await.unwrap().is_none());
    assert_eq!(runner.remaining, Some(Duration::ZERO));
    assert!(
        runner
            .run_vm(|_| panic!("the shared budget is exhausted"))
            .await
            .unwrap()
            .is_none()
    );
}

#[test]
fn cumulative_receipts_exclude_idle_time_and_replace_stale_deadlines() {
    let limit = Duration::from_millis(10);
    let started = Instant::now();
    let mut slice = Slice::default();
    assert_eq!(slice.deadline(limit), None);
    slice.phase = Phase::Running(started);
    let old_deadline = slice.deadline(limit).unwrap();
    assert_eq!(old_deadline, started + limit);
    slice.charged = Duration::from_millis(3);
    slice.phase = Phase::Idle;
    assert_eq!(slice.deadline(limit), None);
    let resumed = started + Duration::from_secs(60);
    slice.phase = Phase::Running(resumed);
    assert_eq!(
        slice.deadline(limit),
        Some(resumed + Duration::from_millis(7))
    );
    assert!(resumed > old_deadline);
    for spent in [9, 10, 11] {
        slice.charged = Duration::from_millis(spent);
        slice.phase = Phase::Finished;
        assert_eq!(slice.deadline(limit), None);
        assert_eq!(limit.saturating_sub(slice.charged).is_zero(), spent >= 10);
    }
}
