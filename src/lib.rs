//#![deny(missing_docs)]
//! A simple key-value store.

extern crate failure;
pub use failure::Error;

mod engine;
mod server;
mod client;
mod protocol;
pub mod thread_pool;

pub type Result<T> = std::result::Result<T, Error>;

pub use engine::{KvsEngine, AKvStore};
pub use engine::kvstore::{KvStore, slog, Error as KvsError};
pub use engine::sledkv::SledDb;
pub use server::KvsServer;
pub use client::KvsClient;

