extern crate sled;
extern crate failure;

pub use sled::{Db, Tree};
use failure::format_err;

use std::string::String;
use std::path::Path;
use std::fs::{self};

use crate::{KvsEngine, Result, KvsError};

impl KvsEngine for Db {
    fn open(path: impl AsRef<Path>) -> Result<Self> {
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
        Ok(Self::start_default(path)?)
    }

    /// Set key-value.
    fn set(&mut self, key: String, value: String) -> Result<()> {
        Tree::set(self, key.as_bytes(), value.as_bytes())?;
        self.flush()?;
        Ok(())
    }
    /// Get key.
    fn get(&mut self, key: String) -> Result<Option<String>> {
        Ok(Tree::get(self, key.as_bytes())?.map(|v| String::from_utf8_lossy(&v).to_string()))
    }

    /// Remove key.
    fn remove(&mut self, key: String) -> Result<()> {
        if None == self.del(key.clone())? {
            Err(KvsError::KeyNotFound(key))?;
        }
        self.flush()?;
        Ok(())
    }
}
