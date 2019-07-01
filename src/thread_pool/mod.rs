extern crate rayon;

use rayon::{ThreadPool as RayonTP, ThreadPoolBuilder};

mod naive;
mod shared_queue;
pub mod panic_control;

use crate::Result;
pub use naive::NaiveThreadPool;
pub use shared_queue::SharedQueueThreadPool;

pub trait ThreadPool: Send + 'static {
    fn new(threads: u32) -> Result<Self>
    where
        Self: Sized;
    fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static;
}

pub struct RayonThreadPool(RayonTP);

impl ThreadPool for RayonThreadPool {
    fn new(n: u32) -> Result<Self> {
        Ok(RayonThreadPool(ThreadPoolBuilder::new().num_threads(n as usize).build()?))
    }

    fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static,
    {
        self.0.spawn(job);
    }
}
