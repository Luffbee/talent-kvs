#![deny(missing_docs)]
//! A simple key-value store.

mod kv;
pub use kv::Error;
pub use kv::KvStore;
pub use kv::Result;
