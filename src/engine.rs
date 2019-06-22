use crate::Result;
use crate::KvStore;

/// KV server storage backend.
pub trait KvsEngine {
    /// Set key-value.
    fn set(&mut self, key: String, value: String) -> Result<()>;
    /// Get key.
    fn get(&mut self, key: String) -> Result<Option<String>>;
    /// Remove key.
    fn remove(&mut self, key: String) -> Result<()>;
}

impl KvsEngine for KvStore {
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
