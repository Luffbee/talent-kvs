use crate::Result;

/// KV server storage backend.
pub trait KvsEngine {
    /// Set key-value.
    fn set(&mut self, key: String, value: String) -> Result<()>;
    /// Get key.
    fn get(&mut self, key: String) -> Result<Option<String>>;
    /// Remove key.
    fn remove(&mut self, key: String) -> Result<()>;
}
