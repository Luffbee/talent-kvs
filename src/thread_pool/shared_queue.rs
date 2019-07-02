extern crate crossbeam_channel;
extern crate num_cpus;

use crossbeam_channel::{unbounded, Receiver, Sender, TryRecvError};

use std::thread::{self, JoinHandle};
use std::sync::Arc;

use super::ThreadPool;
use crate::Result;

type Task = Box<dyn FnOnce() + Send + 'static>;
type WorkerID = usize;

enum Message {
    Run(Task),
    Shutdown,
}

enum Control {
    GoOn,
    Stop,
}

struct QueuedThreadPool {
    size: u32,
    worker: Sender<Message>,
    monitor: Sender<Control>,
    monitor_handle: Option<JoinHandle<()>>
}

#[derive(Clone)]
pub struct SharedQueueThreadPool(Arc<QueuedThreadPool>);

struct Monitor {
    size: u32,
    control: Receiver<Control>,
    worker_rx: Receiver<Message>,
    prx: Receiver<WorkerID>,
    psx: Sender<WorkerID>,
    workers: Vec<Worker>,
}

struct Worker {
    id: WorkerID,
    handle: Option<JoinHandle<()>>,
}

impl ThreadPool for SharedQueueThreadPool {
    fn new(mut size: u32) -> Result<Self> {
        if size == 0 {
            size = num_cpus::get() as u32;
        }
        let (worker, worker_rx) = unbounded();
        let (monitor, monitor_rx) = unbounded();
        let monitor_handle = Some(thread::spawn(move || {
            let mut monitor = Monitor::new(size, monitor_rx, worker_rx);
            monitor.watch();
        }));
        Ok(SharedQueueThreadPool(Arc::new(QueuedThreadPool {
            size,
            worker,
            monitor,
            monitor_handle,
        })))
    }

    fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static,
    {
        // Check monitor is alive.
        self.0.monitor.send(Control::GoOn).expect("monitor dead");
        self.0.worker.send(Message::Run(Box::new(job))).unwrap();
    }
}

impl Drop for QueuedThreadPool {
    fn drop(&mut self) {
        self.monitor.send(Control::Stop).unwrap();
        for _ in 0..self.size {
            self.worker.send(Message::Shutdown).unwrap();
        }
        if let Err(e) = self.monitor_handle.take().unwrap().join() {
            eprintln!("monitor panicked: {:?}", e);
        }
    }
}

impl Monitor {
    fn new(size: u32, control: Receiver<Control>, worker_rx: Receiver<Message>) -> Monitor {
        let (psx, prx) = unbounded();
        let mut workers = Vec::with_capacity(size as usize);
        for i in 0..size as WorkerID {
            let worker = Worker::new(i, worker_rx.clone(), psx.clone());
            workers.push(worker);
        }
        Monitor {
            size,
            control,
            worker_rx,
            prx,
            psx,
            workers,
        }
    }

    fn watch(&mut self) {
        while let Ok(id) = self.prx.recv() {
            match self.control.try_recv() {
                Ok(Control::GoOn) | Err(TryRecvError::Empty) => {}
                _ => break,
            }
            eprintln!("found worker {} panicked", id);
            let id = id + self.size as WorkerID;
            let worker = Worker::new(id, self.worker_rx.clone(), self.psx.clone());
            self.workers[id % self.size as WorkerID] = worker;
        }
    }
}

impl Worker {
    fn new(id: WorkerID, rx: Receiver<Message>, psx: Sender<WorkerID>) -> Worker {
        let tid = id;
        let handle = Some(thread::spawn(move || {
            // use to detect panic.
            let panicer = Panicer { id: tid, psx };
            while let Ok(Message::Run(job)) = rx.recv() {
                job();
            }
            drop(panicer);
        }));
        Worker { id, handle }
    }
}

impl Drop for Worker {
    fn drop(&mut self) {
        eprintln!("dropping worker {}", self.id);
        if let Err(e) = self.handle.take().unwrap().join() {
            eprintln!("thread {} panicked: {:?}", self.id, e);
        }
        eprintln!("dropped worker {}", self.id);
    }
}

struct Panicer {
    id: WorkerID,
    psx: Sender<WorkerID>,
}

impl Drop for Panicer {
    fn drop(&mut self) {
        if thread::panicking() {
            self.psx.send(self.id).unwrap();
        }
    }
}
