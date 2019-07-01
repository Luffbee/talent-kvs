extern crate failure;

use failure::format_err;

use std::path::Path;
use std::sync::{Arc, Mutex};

pub mod kvstore;
pub mod sledkv;

use crate::Result;
use kvstore::{slog::Logger, KvStore, KvStoreBuilder};

/// KV server storage backend.
pub trait KvsEngine: Clone + Send + 'static {
    /// Set key-value.
    fn set(&self, key: String, value: String) -> Result<()>;
    /// Get key.
    fn get(&self, key: String) -> Result<Option<String>>;
    /// Remove key.
    fn remove(&self, key: String) -> Result<()>;
}

#[derive(Clone)]
pub struct AKvStore(Arc<Mutex<KvStore>>);

impl AKvStore {
    pub fn with_logger(path: impl AsRef<Path>, log: Logger) -> Result<Self> {
        let kvs = KvStoreBuilder::new(path).logger(log).build()?;
        Ok(AKvStore(Arc::new(Mutex::new(kvs))))
    }
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        Ok(AKvStore(Arc::new(Mutex::new(KvStore::open(path)?))))
    }
}

impl KvsEngine for AKvStore {
    fn set(&self, key: String, value: String) -> Result<()> {
        let mut guard = self.0.lock().map_err(|e| format_err!("{:?}", e))?;
        guard.set(key, value)
    }
    fn get(&self, key: String) -> Result<Option<String>> {
        let mut guard = self.0.lock().map_err(|e| format_err!("{:?}", e))?;
        guard.get(key)
    }
    fn remove(&self, key: String) -> Result<()> {
        let mut guard = self.0.lock().map_err(|e| format_err!("{:?}", e))?;
        guard.remove(key)
    }
}
