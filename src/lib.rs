//#![deny(missing_docs)]
//! A simple key-value store.

#[macro_use]
pub extern crate slog;
extern crate slog_stdlog;
extern crate failure;

pub use failure::Error;
use slog::{Logger, Drain};

mod client;
mod engine;
mod protocol;
mod server;
pub mod thread_pool;

pub type Result<T> = std::result::Result<T, Error>;

pub use client::KvsClient;
pub use engine::kvstore::{Error as KvsError, KvStore as RealKvStore};
pub use engine::sledkv::SledDb;
pub use engine::{KvStore, KvsEngine};
pub use server::KvsServer;

fn get_logger(opt: &mut Option<Logger>) -> Logger {
    opt.take().unwrap_or_else(|| Logger::root(slog_stdlog::StdLog.fuse(), o!()))
}
