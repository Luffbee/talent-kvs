//#![deny(missing_docs)]
//! A simple key-value store.

extern crate failure;
pub use failure::Error;

mod engine;
pub mod protocol;
pub mod thread_pool;

pub type Result<T> = std::result::Result<T, Error>;

pub use engine::KvsEngine;
pub use engine::kvstore::{KvStore, slog, Error as KvsError};
pub use engine::sledkv::Db as SledDb;

