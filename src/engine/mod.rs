extern crate failure;

pub mod kvstore;
pub mod sledkv;

use crate::Result;
pub use kvstore::KvStore;

/// KV server storage backend.
pub trait KvsEngine: Clone + Send + 'static {
    /// Set key-value.
    fn set(&self, key: String, value: String) -> Result<()>;
    /// Get key.
    fn get(&self, key: String) -> Result<Option<String>>;
    /// Remove key.
    fn remove(&self, key: String) -> Result<()>;
}

impl KvsEngine for KvStore {
    fn set(&self, key: String, value: String) -> Result<()> {
        self.set(key, value)
    }
    fn get(&self, key: String) -> Result<Option<String>> {
        self.get(key)
    }
    fn remove(&self, key: String) -> Result<()> {
        self.remove(key)
    }
}
