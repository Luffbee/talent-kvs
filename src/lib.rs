#![deny(missing_docs)]
//! A simple key-value store.

extern crate failure;
pub use failure::Error;

mod kv;
mod error;
mod engine;
/// KvStore Result
pub type Result<T> = std::result::Result<T, Error>;

pub use kv::{KvStore, slog};
pub use error::Error as KvsError;
pub use engine::KvsEngine;

