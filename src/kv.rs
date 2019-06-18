use std::collections::HashMap;

/// Store key-value pairs in a `HashMap` in memory.
///
/// Example:
///
/// ``` rust
/// # use kvs::KvStore;
/// let mut store = KvStore::new();
/// store.set("key".to_owned(), "value".to_owned());
/// let val = store.get("key".to_owned());
/// assert_eq!(val, Some("value".to_owned()));
/// ```
#[derive(Default)]
pub struct KvStore {
    store: HashMap<String, String>,
}

impl KvStore {
    /// Return an empty key-value store.
    pub fn new() -> KvStore {
        KvStore {
            store: HashMap::new(),
        }
    }

    /// If the key already in the store, update the value.  
    /// Otherwise, insert the key-value pair into the store.
    pub fn set(&mut self, key: String, val: String) {
        if let Some(old) = self.store.insert(key, val) {
            eprintln!("Updated old value: {}", old);
        }
    }

    /// If the key already in the store, return the `Some(value)`.  
    /// Otherwise, return `None`.
    pub fn get(&self, key: String) -> Option<String> {
        self.store.get(&key).map(|sr| sr.clone())
    }

    /// If the key already in the store, remove it.  
    /// Otherwise, do nothing.
    pub fn remove(&mut self, key: String) {
        if let Some(old) = self.store.remove(&key) {
            eprintln!("Removed old value: {}", old);
        }
    }
}
