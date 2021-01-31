use std::{
    error::Error as StdError,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Condvar, Mutex,
    },
    thread::{spawn, JoinHandle},
    time::{Duration, Instant},
};

use crate::Result;

struct State<T: Task> {
    ask_time: Instant,
    killed: bool,
    thread: Option<JoinHandle<Result<(), T::Error>>>,
}

struct Shared<T: Task> {
    state: Mutex<State<T>>,
    failed: AtomicBool,
    cv: Condvar,
}

pub struct TaskCtl<T: Task> {
    shared: Arc<Shared<T>>,
}

pub enum ShouldRun {
    Yes,
    AskMeLater(Duration),
}

pub trait Task: Send + Sync + 'static {
    type Error: StdError + Send + Sync + 'static;
    fn should_run(&mut self) -> ShouldRun;
    fn run(&mut self) -> Result<(), Self::Error>;
}

impl<T: Task> Drop for TaskCtl<T> {
    fn drop(&mut self) {
        let mut state = self.shared.state.lock().unwrap();
        state.killed = true;
        self.shared.cv.notify_one();
        let thread = state.thread.take();

        // Unlock, then join thread.
        drop(state);
        drop(thread);
    }
}

impl<T: Task> TaskCtl<T> {
    pub fn new(task: T) -> Self {
        let shared = Arc::new(Shared {
            state: Mutex::new(State {
                ask_time: Instant::now(),
                killed: false,
                thread: None,
            }),
            failed: AtomicBool::new(false),
            cv: Condvar::new(),
        });

        let thread = spawn({
            let mut task = task;
            let shared = shared.clone();
            move || -> Result<(), T::Error> {
                loop {
                    let mut state = shared.state.lock().unwrap();
                    while !state.killed {
                        let now = Instant::now();
                        if now >= state.ask_time {
                            match task.should_run() {
                                ShouldRun::Yes => break,
                                ShouldRun::AskMeLater(dur) => {
                                    state.ask_time = Instant::now() + dur;
                                }
                            }
                        } else {
                            let wait_time = state.ask_time - now;
                            let (s, _) = shared.cv.wait_timeout(state, wait_time).unwrap();
                            state = s;
                        }
                    }

                    if state.killed {
                        return Ok(());
                    }

                    drop(state);

                    if let Err(e) = task.run() {
                        log::error!("Task failed: {}", e);
                        // The ordering here does not really matter. It only
                        // needs to be eventually visible to readers.
                        shared.failed.store(true, Ordering::Relaxed);
                        return Err(e);
                    }
                }
            }
        });

        shared.state.lock().unwrap().thread = Some(thread);

        Self { shared }
    }

    pub fn check_should_run(&self) {
        let mut state = self.shared.state.lock().unwrap();
        state.ask_time = Instant::now();
        self.shared.cv.notify_one();
    }

    pub fn is_failed(&self) -> bool {
        self.shared.failed.load(Ordering::Relaxed)
    }

    pub fn kill(&self) -> Result<(), T::Error> {
        let mut state = self.shared.state.lock().unwrap();
        assert!(!state.killed, "could only kill once");
        state.killed = true;
        self.shared.cv.notify_one();

        let thread = state.thread.take().unwrap();
        drop(state);

        thread.join().unwrap()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread::sleep;
    use thiserror::Error;

    struct TestTask {
        pending: Arc<Mutex<bool>>,
        err: bool,
    }
    #[derive(Error, Debug)]
    #[error("test error")]
    struct TestError;

    impl Task for TestTask {
        type Error = TestError;

        fn should_run(&mut self) -> ShouldRun {
            if *self.pending.lock().unwrap() {
                ShouldRun::Yes
            } else {
                ShouldRun::AskMeLater(Duration::from_millis(100))
            }
        }

        fn run(&mut self) -> Result<(), Self::Error> {
            *self.pending.lock().unwrap() = false;
            if self.err {
                Err(TestError)
            } else {
                Ok(())
            }
        }
    }

    #[test]
    fn task_ctl_basic() {
        let pending = Arc::new(Mutex::new(false));
        let task = TestTask {
            pending: pending.clone(),
            err: false,
        };
        let _ctl = TaskCtl::new(task);

        // Avoid cleared by the initial run.
        sleep(Duration::from_millis(30));
        *pending.lock().unwrap() = true;

        // Test not cleared.
        sleep(Duration::from_millis(30));
        assert!(*pending.lock().unwrap());

        // Test cleared.
        sleep(Duration::from_millis(50));
        assert!(!*pending.lock().unwrap());

        // Set pending second time.
        *pending.lock().unwrap() = true;

        // Test not cleared.
        sleep(Duration::from_millis(30));
        assert!(*pending.lock().unwrap());

        // Test cleared.
        sleep(Duration::from_millis(80));
        assert!(!*pending.lock().unwrap());
    }

    #[test]
    fn task_ctl_check_should_run() {
        let pending = Arc::new(Mutex::new(false));
        let task = TestTask {
            pending: pending.clone(),
            err: false,
        };
        let ctl = TaskCtl::new(task);

        // Avoid cleared by the initial run.
        sleep(Duration::from_millis(30));
        *pending.lock().unwrap() = true;

        // Test not cleared.
        sleep(Duration::from_millis(30));
        assert!(*pending.lock().unwrap());

        // Test cleared.
        ctl.check_should_run();
        sleep(Duration::from_millis(10));
        assert!(!*pending.lock().unwrap());
    }

    #[test]
    fn task_ctl_kill() {
        let pending = Arc::new(Mutex::new(false));
        let task = TestTask {
            pending: pending.clone(),
            err: false,
        };
        let ctl = TaskCtl::new(task);

        // Avoid cleared by the initial run.
        sleep(Duration::from_millis(30));
        *pending.lock().unwrap() = true;

        assert!(ctl.kill().is_ok());
        sleep(Duration::from_millis(120));
        assert!(*pending.lock().unwrap());
    }

    #[test]
    fn task_ctl_return_err() {
        let pending = Arc::new(Mutex::new(false));
        let task = TestTask {
            pending: pending.clone(),
            err: true,
        };
        let ctl = TaskCtl::new(task);

        // Avoid cleared by the initial run.
        sleep(Duration::from_millis(30));
        *pending.lock().unwrap() = true;

        sleep(Duration::from_millis(120));
        assert!(ctl.kill().is_err());
    }
}
