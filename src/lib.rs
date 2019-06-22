#![deny(missing_docs)]
//! A simple key-value store.

extern crate failure;
use failure::Error as FailError;

mod kv;
mod error;
mod engine;
/// KvStore Result
pub type Result<T> = std::result::Result<T, FailError>;

pub use kv::KvStore;
pub use error::Error;
pub use engine::KvsEngine;

