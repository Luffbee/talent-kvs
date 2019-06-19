extern crate serde_json;
use failure::{format_err, Error};
use log::info;
use serde::Deserialize as SerdeDe;
use serde_derive::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};
use std::fs::{self, File, OpenOptions};
use std::io::{BufReader, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

const MAX_FILE_SIZE: u32 = 2 * 1024;

/// KvStore Result
pub type Result<T> = std::result::Result<T, Error>;

// (File id, offset)
#[derive(Debug, Clone)]
struct FileOffset(u32, u32);
type Index = HashMap<String, FileOffset>;

#[derive(Serialize, Deserialize, Debug)]
enum Command {
    #[serde(rename = "S")]
    Set(String, String),
    #[serde(rename = "R")]
    Rm(String),
}

/// Store key-value pairs in a `HashMap` in memory.
///
/// Example:
///
/// ``` rust
/// # use kvs::KvStore;
/// # use tempfile::TempDir;
/// let temp_dir = TempDir::new().unwrap();
/// let mut store = KvStore::open(temp_dir.path()).unwrap();
/// store.set("key".to_owned(), "value".to_owned()).unwrap();
/// let val = store.get("key".to_owned()).unwrap();
/// assert_eq!(val, Some("value".to_owned()));
/// ```
pub struct KvStore {
    index: Index,
    dir: PathBuf,
    fsz: u32,
    fids: VecDeque<u32>,
}

impl KvStore {
    /// Open a database at dir.
    pub fn open<P: AsRef<Path>>(dir: P) -> Result<KvStore> {
        let dir = dir.as_ref().to_owned();
        let mut fids = Self::load_file_ids(&dir)?;
        // Init if there is no data file.
        info!("Get fids: {:?}.", fids);
        if fids.back().is_none() {
            OpenOptions::new()
                .append(true)
                .create_new(true)
                .open(&dir.join("1.data"))?;
            fids.push_back(1);
        }
        Ok(KvStore {
            index: Self::load_index(&dir, &fids)?,
            fids,
            dir,
            fsz: MAX_FILE_SIZE,
        })
    }

    // Return sorted file ids.
    fn load_file_ids(dir: &PathBuf) -> Result<VecDeque<u32>> {
        info!("Loading file ids from dir {:?}.", dir);
        let mut ids = Vec::new();
        for entry in fs::read_dir(&dir)? {
            let path = entry?.path();
            if path.extension() != Some("data".as_ref()) {
                info!("Skipped file {:?}.", path);
                continue;
            }
            if path.is_dir() {
                info!("Skipped dir {:?}.", path);
                continue;
            }
            if let Some(stem) = path.file_stem() {
                if let Some(id) = stem.to_str() {
                    ids.push(u32::from_str_radix(id, 10)?);
                } else {
                    return Err(format_err!("bad file name: '{:?}'", path));
                }
            } else {
                return Err(format_err!("bad file name: '{:?}'", path));
            }
        }
        ids.sort();
        Ok(VecDeque::from(ids))
    }

    // Read the data files to generate a HashMap index.
    fn load_index(dir: &PathBuf, fids: &VecDeque<u32>) -> Result<Index> {
        info!("Loading data from dir {:?}.", dir);
        let mut index = Index::new();
        for id in fids.iter() {
            let fname = dir.join(format!("{}.data", id));
            info!("Loading data from file {:?}.", fname);

            let reader = BufReader::new(File::open(fname)?);
            let mut stream = serde_json::Deserializer::from_reader(reader).into_iter::<Command>();
            loop {
                let offset = stream.byte_offset();
                match stream.next() {
                    Some(Ok(Command::Set(key, _))) => {
                        index.insert(key, FileOffset(*id, offset as u32));
                    }
                    Some(Ok(Command::Rm(key))) => {
                        index.remove(&key);
                    }
                    Some(Err(e)) => {
                        if e.is_eof() {
                            break;
                        } else {
                            Err(e)?;
                        }
                    }
                    None => break,
                }
            }
        }
        Ok(index)
    }

    // Write command to the active data file.
    // Allocate a new active data file if readched threshold.
    fn append(&self, cmd: &Command) -> Result<FileOffset> {
        let cmd = serde_json::to_string(cmd)?;
        info!("Appending command: {}", cmd);

        let active_id = self
            .fids
            .back()
            .ok_or_else(|| format_err!("unknown error: no existing file ids"))?;
        let fname = self.dir.join(format!("{}.data", active_id));
        let flen = fs::metadata(&fname)?.len();

        let (mut file, offset) = if flen + cmd.len() as u64 > u64::from(self.fsz) {
            let active_id = 1 + active_id;
            let new_fname = self.dir.join(format!("{}.data", active_id));

            info!(
                "File {:?} reached threshold,
                  allocating new file: {:?}",
                fname, new_fname
            );
            (
                OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .open(&new_fname)?,
                0,
            )
        } else {
            (OpenOptions::new().append(true).open(&fname)?, flen as u32)
        };
        let n = file.write(cmd.as_ref())?;
        if n != cmd.len() {
            panic!(
                "Write {} bytes for {} length command: {}",
                n,
                cmd.len(),
                cmd
            );
        }

        Ok(FileOffset(*active_id, offset))
    }

    fn read(&self, loc: &FileOffset) -> Result<Command> {
        let fname = self.dir.join(format!("{}.data", loc.0));
        let flen = fs::metadata(&fname)?.len();

        // Check the file size first,
        // because seek() may accept a offset beyond the end.
        if flen < u64::from(loc.1) {
            return Err(format_err!(
                "read location {:?} in file {:?} with length {}",
                loc,
                fname,
                flen
            ));
        }

        let mut file = File::open(fname)?;
        file.seek(SeekFrom::Start(u64::from(loc.1)))?;

        let mut de = serde_json::Deserializer::from_reader(file);
        Ok(<Command as SerdeDe>::deserialize(&mut de)?)
    }

    /// If the key already in the store, update the value.  
    /// Otherwise, insert the key-value pair into the store.
    pub fn set(&mut self, key: String, val: String) -> Result<()> {
        let cmd = Command::Set(key, val);
        let nloc = self.append(&cmd)?;
        if let Command::Set(key, _) = cmd {
            if let Some(loc) = self.index.insert(key.clone(), nloc.clone()) {
                info!("Old location of key '{}': {:?}.", key, loc);
                info!("New location of key '{}': {:?}.", key, nloc);
            } else {
                info!("Insert new key '{}' at {:?}.", key, nloc);
            }
        }
        Ok(())
    }

    /// If the key already in the store, return the `Some(value)`.  
    /// Otherwise, return `None`.
    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        let loc = self.index.get(&key).cloned();
        if let Some(loc) = loc {
            let cmd = self.read(&loc)?;
            if let Command::Set(k, v) = cmd {
                if k == key {
                    Ok(Some(v))
                } else {
                    Err(format_err!("get wrong key '{}', expected '{}'", k, key))
                }
            } else {
                Err(format_err!("get wrong command: {:?}", cmd))
            }
        } else {
            Ok(None)
        }
    }

    /// If the key already in the store, remove it.  
    /// Otherwise, do nothing.
    pub fn remove(&mut self, key: String) -> Result<()> {
        if let Some(loc) = self.index.remove(&key) {
            info!("Old location of key {}: {:?}.", key, loc);
            let cmd = Command::Rm(key);
            let loc = self.append(&cmd)?;
            if let Command::Rm(key) = cmd {
                info!("New location of key {}: {:?}.", key, loc);
            }
            Ok(())
        } else {
            Err(format_err!("no such key: {}", key))
        }
    }
}
