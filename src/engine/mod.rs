extern crate failure;

use std::path::Path;

pub mod kvstore;
pub mod sledkv;

use kvstore::KvStore;
use crate::Result;

/// KV server storage backend.
pub trait KvsEngine {
    /// Check meta then open database.
    fn open(path: impl AsRef<Path>) -> Result<Self>
    where
        Self: Sized;
    /// Set key-value.
    fn set(&mut self, key: String, value: String) -> Result<()>;
    /// Get key.
    fn get(&mut self, key: String) -> Result<Option<String>>;
    /// Remove key.
    fn remove(&mut self, key: String) -> Result<()>;
}

impl KvsEngine for KvStore {
    fn open(path: impl AsRef<Path>) -> Result<Self> {
        Self::open(path)
    }
    fn set(&mut self, key: String, value: String) -> Result<()> {
        self.set(key, value)
    }
    fn get(&mut self, key: String) -> Result<Option<String>> {
        self.get(key)
    }
    fn remove(&mut self, key: String) -> Result<()> {
        self.remove(key)
    }
}
