//#![deny(missing_docs)]
//! A simple key-value store.

extern crate failure;
pub use failure::Error;

mod client;
mod engine;
mod protocol;
mod server;
pub mod thread_pool;

pub type Result<T> = std::result::Result<T, Error>;

pub use client::KvClient;
pub use engine::kvstore::{slog, Error as KvsError, KvStore as RealKvStore};
pub use engine::sledkv::SledDb;
pub use engine::{KvStore, KvsEngine};
pub use server::KvServer;
