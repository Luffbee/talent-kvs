//#![deny(missing_docs)]
//! A simple key-value store.

extern crate failure;
pub use failure::Error;

mod error;
mod engine;
pub mod protocol;
pub mod thread_pool;

pub type Result<T> = std::result::Result<T, Error>;

pub use error::Error as KvsError;
pub use engine::KvsEngine;
pub use engine::kv::{KvStore, slog};
pub use engine::sledkv::Db as SledDb;

