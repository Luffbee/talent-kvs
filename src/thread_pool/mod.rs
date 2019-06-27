mod naive;
pub mod panic_control;

use crate::Result;
pub use naive::NaiveThreadPool;

pub trait ThreadPool {
    fn new(threads: u32) -> Result<Self>
    where
        Self: Sized;
    fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static;
}

pub struct SharedQueueThreadPool {}

impl ThreadPool for SharedQueueThreadPool {
    fn new(_: u32) -> Result<Self> {
        Ok(SharedQueueThreadPool{})
    }

    fn spawn<F>(&self, _job: F)
    where
        F: FnOnce() + Send + 'static,
    {
    }
}

pub struct RayonThreadPool {}

impl ThreadPool for RayonThreadPool {
    fn new(_: u32) -> Result<Self> {
        Ok(RayonThreadPool{})
    }

    fn spawn<F>(&self, _job: F)
    where
        F: FnOnce() + Send + 'static,
    {
    }
}
