extern crate failure;
extern crate sled;

use failure::format_err;
pub use sled::{Db, Tree};

use std::fs;
use std::path::Path;
use std::string::String;

use crate::{KvsEngine, KvsError, Result};

#[derive(Clone)]
pub struct SledDb(Db);

impl SledDb {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let metapath = path.as_ref().join("meta");
        if metapath.is_dir() {
            return Err(format_err!("{:?} is dir", metapath));
        } else if metapath.is_file() {
            let meta = String::from_utf8_lossy(&fs::read(&metapath)?).into_owned();
            if meta != "sled" {
                return Err(format_err!("invalid metadata {:?}: {}", metapath, meta));
            }
        } else {
            fs::write(metapath, "sled")?;
        }
        Ok(Self(Db::start_default(path)?))
    }
}

impl KvsEngine for SledDb {
    fn open(path: impl AsRef<Path>) -> Result<Self> {
        Self::open(path)
    }
    /// Set key-value.
    fn set(&self, key: String, value: String) -> Result<()> {
        Tree::set(&self.0, key.as_bytes(), value.as_bytes())?;
        self.0.flush()?;
        Ok(())
    }
    /// Get key.
    fn get(&self, key: String) -> Result<Option<String>> {
        Ok(Tree::get(&self.0, key.as_bytes())?.map(|v| String::from_utf8_lossy(&v).to_string()))
    }

    /// Remove key.
    fn remove(&self, key: String) -> Result<()> {
        if None == self.0.del(key.clone())? {
            Err(KvsError::KeyNotFound(key))?;
        }
        self.0.flush()?;
        Ok(())
    }
}
