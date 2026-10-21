//! CPU receipts taken on the thread that runs the VM.
//!
//! Keeping both readings on their owner avoids remote thread handles and clock
//! observations that can lag execution or outlive the work being measured.

use std::{io, marker::PhantomData, rc::Rc, time::Duration};

/// A timestamp that cannot leave the thread it measures.
pub(super) struct CpuTime {
    total: Duration,
    _thread: PhantomData<Rc<()>>,
}

impl CpuTime {
    pub(super) fn now() -> io::Result<Self> {
        Ok(Self {
            total: thread_cpu_time()?,
            _thread: PhantomData,
        })
    }

    pub(super) fn elapsed(&self) -> io::Result<Duration> {
        thread_cpu_time()?
            .checked_sub(self.total)
            .ok_or_else(|| io::Error::other("thread CPU clock moved backwards"))
    }
}

// Use the existing safe POSIX wrapper wherever libc exposes the thread clock.
#[cfg(any(
    target_os = "linux",
    target_os = "android",
    target_vendor = "apple",
    target_os = "freebsd",
    target_os = "dragonfly",
    target_os = "openbsd",
    target_os = "netbsd",
    target_os = "aix",
    target_os = "haiku",
    target_os = "hurd",
    target_os = "nto",
    target_os = "fuchsia",
    target_os = "emscripten"
))]
fn thread_cpu_time() -> io::Result<Duration> {
    let clock = nix::time::ClockId::from_raw(libc::CLOCK_THREAD_CPUTIME_ID);
    let time = clock.now()?;
    Ok(Duration::new(
        time.tv_sec().try_into().map_err(io::Error::other)?,
        time.tv_nsec().try_into().map_err(io::Error::other)?,
    ))
}

#[cfg(windows)]
fn thread_cpu_time() -> io::Result<Duration> {
    use winapi::{
        shared::minwindef::FILETIME,
        um::processthreadsapi::{GetCurrentThread, GetThreadTimes},
    };

    let mut creation = FILETIME {
        dwLowDateTime: 0,
        dwHighDateTime: 0,
    };
    let mut exit = creation;
    let mut kernel = creation;
    let mut user = creation;
    // SAFETY: the pseudo handle is used synchronously on its own thread, and
    // all four output pointers refer to live FILETIME buffers.
    let result = unsafe {
        GetThreadTimes(
            GetCurrentThread(),
            &mut creation,
            &mut exit,
            &mut kernel,
            &mut user,
        )
    };
    if result == 0 {
        return Err(io::Error::last_os_error());
    }
    let duration = |time: FILETIME| {
        let ticks = (u64::from(time.dwHighDateTime) << 32) | u64::from(time.dwLowDateTime);
        Duration::new(ticks / 10_000_000, ((ticks % 10_000_000) as u32) * 100)
    };
    Ok(duration(kernel).saturating_add(duration(user)))
}

#[cfg(not(any(
    windows,
    target_os = "linux",
    target_os = "android",
    target_vendor = "apple",
    target_os = "freebsd",
    target_os = "dragonfly",
    target_os = "openbsd",
    target_os = "netbsd",
    target_os = "aix",
    target_os = "haiku",
    target_os = "hurd",
    target_os = "nto",
    target_os = "fuchsia",
    target_os = "emscripten"
)))]
fn thread_cpu_time() -> io::Result<Duration> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "thread CPU clock is unavailable",
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::mpsc;

    #[test]
    fn clock_is_thread_local_and_detects_backwards_time() {
        let mut clock = match CpuTime::now() {
            Ok(clock) => clock,
            Err(error) => {
                assert_eq!(error.kind(), io::ErrorKind::Unsupported);
                return;
            }
        };
        clock.total = Duration::MAX;
        assert!(clock.elapsed().is_err());

        let (started, start) = mpsc::channel();
        let (release, released) = mpsc::channel();
        let thread = std::thread::spawn(move || {
            let clock = CpuTime::now().unwrap();
            started.send(()).unwrap();
            released.recv_timeout(Duration::from_secs(3)).unwrap();
            clock.elapsed().unwrap()
        });
        start.recv().unwrap();
        let clock = CpuTime::now().unwrap();
        let limit = Duration::from_millis(50);
        while clock.elapsed().unwrap() < limit {
            std::hint::spin_loop();
        }
        release.send(()).unwrap();
        assert!(
            thread.join().unwrap() < limit,
            "another thread's CPU was charged"
        );
    }
}
