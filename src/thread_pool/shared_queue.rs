extern crate crossbeam_channel;
extern crate num_cpus;

use crossbeam_channel::{unbounded, Receiver as RX, Sender as TX};
use slog::Logger;

use std::sync::Arc;
use std::thread::{self, JoinHandle};

use super::ThreadPool;
use crate::{get_logger, Result};

type Task = Box<dyn FnOnce() + Send + 'static>;
type WorkerID = usize;

enum Message {
    Run(Task),
    Shutdown,
}

enum Control {
    Test,
    Bury(WorkerID),
    Stop,
}

struct QueuedThreadPool {
    log: Logger,
    size: u32,
    worker: TX<Message>,
    monitor: TX<Control>,
    monitor_handle: Option<JoinHandle<()>>,
}

#[derive(Clone)]
pub struct SharedQueueThreadPool(Arc<QueuedThreadPool>);

struct Monitor {
    log: Logger,
    size: u32,
    control: RX<Control>,
    worker_ctl: TX<Control>,
    worker_rx: RX<Message>,
    workers: Vec<Worker>,
}

struct Worker {
    log: Logger,
    id: WorkerID,
    handle: Option<JoinHandle<()>>,
}

struct Panicer {
    log: Logger,
    id: WorkerID,
    monitor: TX<Control>,
}

impl ThreadPool for SharedQueueThreadPool {
    fn new(size: u32) -> Result<Self> {
        Ok(SharedQueueThreadPool(Arc::new(QueuedThreadPool::with_log(size, None)?)))
    }

    fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static,
    {
        // Check monitor is alive.
        self.0.monitor.send(Control::Test).expect("monitor dead");
        self.0.worker.send(Message::Run(Box::new(job))).unwrap();
    }
}

impl QueuedThreadPool {
    pub fn with_log<LG>(mut size: u32, log: LG) -> Result<Self>
    where
        LG: Into<Option<Logger>>,
    {
        if size == 0 {
            size = num_cpus::get() as u32;
        }
        let (worker, worker_rx) = unbounded();
        let (monitor, monitor_rx) = unbounded();
        let worker_ctl = monitor.clone();
        let log = get_logger(&mut log.into());
        let m_log = log.new(o!("role" => "monitor"));
        let monitor_handle = Some(thread::spawn(move || {
            let mut monitor = Monitor::new(m_log, size, monitor_rx, worker_ctl, worker_rx);
            monitor.watch();
        }));
        Ok(QueuedThreadPool {
            size,
            worker,
            monitor,
            monitor_handle,
            log,
        })
    }
}

impl Drop for QueuedThreadPool {
    fn drop(&mut self) {
        self.monitor.send(Control::Stop).unwrap();
        for _ in 0..self.size {
            self.worker.send(Message::Shutdown).unwrap();
        }
        if let Err(e) = self.monitor_handle.take().unwrap().join() {
            error!(self.log, "monitor panicked: {:?}", e);
        }
    }
}

impl Monitor {
    fn new(
        log: Logger,
        size: u32,
        control: RX<Control>,
        worker_ctl: TX<Control>,
        worker_rx: RX<Message>,
    ) -> Monitor {
        let mut workers = Vec::with_capacity(size as usize);
        for i in 0..size as WorkerID {
            let w_log = log.new(o!("role" => format!("worker {}", i)));
            let worker = Worker::new(w_log, i, worker_rx.clone(), worker_ctl.clone());
            workers.push(worker);
        }
        Monitor {
            log,
            size,
            control,
            worker_ctl,
            worker_rx,
            workers,
        }
    }

    fn watch(&mut self) {
        while let Ok(ctl) = self.control.recv() {
            match ctl {
                Control::Test => continue,
                Control::Stop => break,
                Control::Bury(id) => {
                    error!(self.log, "found worker {} dead", id);
                    let id = id + self.size as WorkerID;
                    let w_log = self.log.new(o!("role" => format!("worker {}", id)));
                    let worker =
                        Worker::new(w_log, id, self.worker_rx.clone(), self.worker_ctl.clone());
                    self.workers[id % self.size as WorkerID] = worker;
                }
            }
        }
    }
}

impl Worker {
    fn new(log: Logger, id: WorkerID, rx: RX<Message>, monitor: TX<Control>) -> Worker {
        let tid = id;
        let p_log = log.clone();
        let handle = Some(thread::spawn(move || {
            // use to detect panic.
            let panicer = Panicer {
                log: p_log,
                id: tid,
                monitor,
            };
            while let Ok(Message::Run(job)) = rx.recv() {
                job();
            }
            drop(panicer);
        }));
        Worker { log, id, handle }
    }
}

impl Drop for Worker {
    fn drop(&mut self) {
        if let Err(e) = self.handle.take().unwrap().join() {
            error!(self.log, "thread {} panicked: {:?}", self.id, e);
        }
    }
}

impl Drop for Panicer {
    fn drop(&mut self) {
        if thread::panicking() {
            if self.monitor.send(Control::Bury(self.id)).is_err() {
                error!(self.log, "worker {} panicked after monitor dead", self.id);
            }
        }
    }
}
