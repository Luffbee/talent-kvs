use std::thread;

use super::ThreadPool;
use crate::Result;

pub struct NaiveThreadPool {}

impl ThreadPool for NaiveThreadPool {
    fn new(_: u32) -> Result<Self> {
        Ok(NaiveThreadPool {})
    }

    fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static,
    {
        thread::spawn(job);
    }
}
