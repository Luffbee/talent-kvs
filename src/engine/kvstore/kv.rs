extern crate chashmap;
pub extern crate slog;
extern crate slog_stdlog;

use chashmap::CHashMap;
use slog::{debug, error, info, o, warn, Drain, Logger};

use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap};
use std::ffi::OsStr;
use std::fs::{self, File};
use std::io::{BufWriter, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, TryLockError};

use super::command::Command;
use super::file::{self, Fdr, Fdw, Fid, Location};
use crate::{KvsError as Error, Result};

const ACTIVE_THRESHOLD: u64 = 1024 * 1024;
const COMPACT_THRESHOLD: usize = 2 * 1024 * 1024;

type Index = CHashMap<String, CmdInfo>;
type FdrMap = BTreeMap<Fid, Fdr>;
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct CmdInfo {
    loc: Location,
    len: usize,
}

impl CmdInfo {
    fn new(id: Fid, offset: u64, len: usize) -> CmdInfo {
        CmdInfo {
            loc: Location { id, offset },
            len,
        }
    }
}

/// Store key-value pairs.
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
    dir: PathBuf,
    log: Logger,
    cthreshold: usize,

    garbage_sz: Arc<AtomicUsize>,
    index: Arc<Index>,
    active: Arc<Mutex<Fdw>>,
    writer: Arc<Mutex<()>>,
    compact_lock: Arc<Mutex<()>>,
    lowest_id: Arc<AtomicUsize>,

    fds: RefCell<FdrMap>,
}

/// Use to costom KvStore.
pub struct KvStoreBuilder {
    dir: PathBuf,
    log: Option<Logger>,
    wthreshold: u64,
    cthreshold: usize,
}

impl KvStore {
    /// Open a database with default configuration.
    pub fn open(dir: impl AsRef<Path>) -> Result<KvStore> {
        KvStoreBuilder::new(dir).build()
    }

    pub fn with_logger(dir: impl AsRef<Path>, log: Logger) -> Result<KvStore> {
        KvStoreBuilder::new(dir).logger(log).build()
    }

    /// If the key already in the store, return the `Some(value)`.  
    /// Otherwise, return `None`.
    pub fn get(&self, key: String) -> Result<Option<String>> {
        let info = match self.index.get(&key) {
            Some(info) => info.clone(),
            None => return Ok(None),
        };
        let cmd = self.fetch(&info.loc)?;
        if let Command::Set(k, v) = cmd {
            if k == key {
                Ok(Some(v))
            } else {
                return Err(Error::UnexpectCmd {
                    found: format!("Set({:?}, {:?})", k, v),
                    expect: format!("Set({:?}, _)", key),
                })?;
            }
        } else {
            return Err(Error::UnexpectCmd {
                found: format!("{:?}", cmd),
                expect: format!("Set({:?}, _)", key),
            })?;
        }
    }

    /// If the key already in the store, update the value.  
    /// Otherwise, insert the key-value pair into the store.
    pub fn set(&self, key: String, val: String) -> Result<()> {
        let (info, writer) = self.append(&Command::Set(key.clone(), val.clone()))?;
        let new_gbg = match self.index.insert(key.clone(), info.clone()) {
            Some(old) => {
                debug!(self.log, "Old location of key '{}': {:?}.", key, old);
                debug!(self.log, "New location of key '{}': {:?}.", key, info);
                old.len
            }
            None => {
                debug!(self.log, "Insert new key '{}' at {:?}.", key, info);
                0
            }
        };
        if new_gbg == 0 {
            return Ok(());
        }
        let gbg_sz = self.garbage_sz.fetch_add(new_gbg, Ordering::SeqCst);
        drop(writer);
        if gbg_sz > self.cthreshold {
            self.compact()?;
        }
        Ok(())
    }

    /// If the key already in the store, remove it.  
    /// Otherwise, do nothing.
    pub fn remove(&self, key: String) -> Result<()> {
        if None == self.index.get(&key) {
            return Err(Error::KeyNotFound(key))?;
        }

        let (info, writer) = self.append(&Command::Rm(key.clone()))?;

        let new_gbg = match self.index.remove(&key) {
            Some(old) => info.len + old.len,
            None => info.len,
        };
        let gbg_sz = self.garbage_sz.fetch_add(new_gbg, Ordering::SeqCst);
        drop(writer);
        if gbg_sz > self.cthreshold {
            self.compact()?;
        }
        if new_gbg == info.len {
            Err(Error::KeyNotFound(key))?;
        }
        Ok(())
    }

    // Write command to the active data file.
    // Allocate a new active data file if readched threshold.
    fn append(&self, cmd: &Command) -> Result<(CmdInfo, MutexGuard<()>)> {
        let mut active = self.active.lock().unwrap();

        debug!(self.log, "Appending command: {:?}", cmd);
        let offset = active.wtr.seek(SeekFrom::End(0))?;
        let cmd = Command::ser(cmd)?;
        let len = cmd.len();
        active.wtr.write_all(cmd.as_ref())?;

        active.wtr.flush()?;

        let writer = self.writer.lock().unwrap();
        Ok((CmdInfo::new(active.id, offset, len), writer))
    }

    fn fetch(&self, loc: &Location) -> Result<Command> {
        debug!(self.log, "fetching location: {:?}", loc);
        let mut fds = self.fds.borrow_mut();
        let fd = match fds.get_mut(&loc.id) {
            Some(fd) => fd,
            None => {
                self.update_fds();
                fds.insert(loc.id, file::fdr(&self.dir, loc.id)?);
                fds.get_mut(&loc.id).unwrap()
            }
        };
        if fd.id != loc.id {
            let e = format!("get wrong fd: {:?}, expect: {:?}", fd.id, loc.id);
            error!(self.log, "{}", e);
            return Err(From::from(Error::UnknowErr(e)));
        }

        let file = &mut fd.rdr;
        file.seek(SeekFrom::Start(loc.offset))?;
        Command::from_reader(file)
    }

    /// remove old fds
    fn update_fds(&self) {
        let low = self.lowest_id.load(Ordering::SeqCst);
        let new_fds = self.fds.borrow_mut().split_off(&low);
        self.fds.replace(new_fds);
    }

    /// Read command from locations in vec, and write to tempfiles.
    /// Tempfiles' id is a range: `lowest .. active_id`.
    /// Return updated index and the `lowest`.
    fn merge(&self, merge_id: Fid, vec: Vec<CmdInfo>) -> Result<HashMap<String, CmdInfo>> {
        let mut index = HashMap::new();
        let mut merge_wtr = self.new_temp(merge_id)?;

        let mut data_id: Fid = vec[0].loc.id;
        let mut rdr = file::open_r(self.datafile(data_id))?;

        for CmdInfo {
            loc: Location { id: fid, offset },
            ..
        } in vec.iter()
        {
            if fid != &data_id {
                data_id = *fid;
                rdr = file::open_r(self.datafile(data_id))?;
            }

            rdr.seek(SeekFrom::Start(*offset))?;
            let cmd = Command::from_reader(&mut rdr)?;
            match cmd {
                Command::Set(ref key, _) => {
                    let s = cmd.ser()?;
                    let len = s.len();
                    let offset = merge_wtr.seek(SeekFrom::End(0))?;
                    merge_wtr.write_all(s.as_bytes())?;
                    index.insert(key.to_owned(), CmdInfo::new(merge_id, offset, len));
                }
                Command::Rm(ref key) => {
                    Err(Error::UnexpectCmd {
                        found: format!("Rm({:?})", key),
                        expect: "Set(_, _)".to_owned(),
                    })?;
                }
            }
        }

        fs::rename(self.tempfile(merge_id), self.datafile(merge_id))?;

        Ok(index)
    }

    /// Compact
    pub fn compact(&self) -> Result<()> {
        let lock = match self.compact_lock.try_lock() {
            Ok(mutex) => mutex,
            Err(TryLockError::WouldBlock) => return Ok(()),
            Err(e) => panic!("compact lock poisoned: {}", e),
        };
        let mut active = self.active.lock().unwrap();
        let merge_id = active.id + 1;
        let active_id = merge_id + 1;
        *active = file::fdw(&self.dir, active_id)?;
        let writer = self.writer.lock().unwrap();
        drop(active);
        self.garbage_sz.store(0, Ordering::SeqCst);
        let index = (*self.index).clone();
        let vec: Vec<_> = index
            .into_iter()
            .map(|(_, v)| v)
            .filter(|v| v.loc.id < merge_id)
            .collect();
        drop(writer);
        let index = if !(vec.is_empty()) {
            self.merge(merge_id, vec)?
        } else {
            HashMap::new()
        };

        let mut new_gbg = 0;
        for (key, val) in index.iter() {
            match self.index.get_mut(key) {
                // If file id >= active id, not compacted.
                Some(ref mut rval) if rval.loc.id < active_id => {
                    **rval = val.clone();
                }
                _ => {
                    new_gbg += val.len;
                }
            }
        }
        self.garbage_sz.fetch_add(new_gbg, Ordering::SeqCst);

        self.update_fds();
        self.lowest_id.store(merge_id, Ordering::SeqCst);
        drop(lock);

        let new_fds = self.fds.borrow_mut().split_off(&merge_id);
        for id in self.fds.borrow().keys() {
            let path = self.datafile(*id);
            info!(self.log, "delete file: {:?}", path);
            if let Err(e) = fs::remove_file(&path) {
                error!(self.log, "failed to delete file {:?}: {}", path, e);
            }
        }

        self.fds.replace(new_fds);
        self.fds
            .borrow_mut()
            .insert(merge_id, file::fdr(&self.dir, merge_id)?);

        Ok(())
    }

    fn new_temp(&self, id: Fid) -> Result<BufWriter<File>> {
        let path = self.tempfile(id);
        info!(self.log, "Creating new file: {:?}", path);
        file::new(path)
    }

    fn tempfile(&self, id: Fid) -> PathBuf {
        file::temp(&self.dir, id)
    }

    fn datafile(&self, id: Fid) -> PathBuf {
        file::data(&self.dir, id)
    }
}

impl Clone for KvStore {
    fn clone(&self) -> Self {
        let mut fds = FdrMap::new();
        for k in self.fds.borrow().keys() {
            if let Ok(fd) = file::fdr(&self.dir, *k) {
                fds.insert(*k, fd);
            }
        }

        Self {
            dir: self.dir.clone(),
            log: self.log.clone(),
            cthreshold: self.cthreshold,

            garbage_sz: self.garbage_sz.clone(),
            index: self.index.clone(),
            active: self.active.clone(),
            writer: self.writer.clone(),
            compact_lock: self.compact_lock.clone(),
            lowest_id: self.lowest_id.clone(),

            fds: RefCell::new(fds),
        }
    }
}

#[allow(dead_code)]
impl KvStoreBuilder {
    /// Return a builder.
    pub fn new(dir: impl AsRef<Path>) -> KvStoreBuilder {
        let dir = dir.as_ref().to_owned();
        KvStoreBuilder {
            dir,
            wthreshold: ACTIVE_THRESHOLD,
            cthreshold: COMPACT_THRESHOLD,
            log: None,
        }
    }

    /// Set logger.
    pub fn logger(mut self, log: Logger) -> Self {
        self.log = Some(log);
        self
    }

    /// Set the max size of active file.
    pub fn active_threshold(mut self, sz: u64) -> Self {
        self.wthreshold = sz;
        self
    }

    pub fn compact_threshold(mut self, sz: usize) -> Self {
        self.cthreshold = sz;
        self
    }

    fn metapath(&self) -> PathBuf {
        self.dir.join("meta")
    }

    fn read_meta(&self) -> Result<Option<String>> {
        let metapath = self.metapath();
        if metapath.is_file() {
            Ok(Some(
                String::from_utf8_lossy(&fs::read(&metapath)?).into_owned(),
            ))
        } else if metapath.is_dir() {
            Err(Error::InvalidMeta(metapath))?
        } else {
            Ok(None)
        }
    }

    /// Build the KvStore.
    pub fn build(mut self) -> Result<KvStore> {
        let log = self
            .log
            .take()
            .unwrap_or_else(|| Logger::root(slog_stdlog::StdLog.fuse(), o!()));

        let mut fds;
        let active;
        let index;
        let garbage_sz;
        let low;

        match self.read_meta()? {
            Some(ref meta) if meta != "kvs" => {
                return Err(Error::InvalidMeta(self.metapath()))?;
            }
            Some(_) => {
                fds = Self::file_list(&self.dir)?;
                low = *fds.keys().nth(0).unwrap();

                let active_id = *fds.keys().last().unwrap();
                active = Fdw {
                    id: active_id,
                    wtr: file::open_w(file::data(&self.dir, active_id))?,
                };

                let (idx, sz) = Self::load_index(&mut fds)?;
                index = idx;
                garbage_sz = sz;
            }
            None => {
                warn!(log, "initializing the dir: {:?}", self.dir);
                fs::write(self.metapath(), "kvs")?;

                active = file::fdw(&self.dir, 1)?;
                low = 1;

                fds = FdrMap::new();
                fds.insert(1, file::fdr(&self.dir, 1)?);

                index = Index::new();
                garbage_sz = 0;
            }
        }

        Ok(KvStore {
            log,
            dir: self.dir,
            cthreshold: self.cthreshold,
            index: Arc::new(index),
            garbage_sz: Arc::new(AtomicUsize::new(garbage_sz)),
            active: Arc::new(Mutex::new(active)),
            writer: Arc::new(Mutex::new(())),
            compact_lock: Arc::new(Mutex::new(())),
            lowest_id: Arc::new(AtomicUsize::new(low)),
            fds: RefCell::new(fds),
        })
    }

    /// Return sorted file ids.
    fn file_list(dir: &PathBuf) -> Result<FdrMap> {
        let mut ids: Vec<Fid> = fs::read_dir(dir)?
            .flat_map(|entry| -> Result<_> { Ok(entry?.path()) })
            .filter(|path| path.is_file())
            .filter(|path| path.extension() == Some("data".as_ref()))
            .flat_map(|path| {
                path.file_stem()
                    .and_then(OsStr::to_str)
                    .map(str::parse::<Fid>)
            })
            .flatten()
            .collect();
        ids.sort_unstable();
        let mut fds = FdrMap::new();
        for id in ids {
            fds.insert(id, file::fdr(dir, id)?);
        }
        Ok(fds)
    }

    /// Read the data files to generate a HashMap index.
    fn load_index(fds: &mut FdrMap) -> Result<(Index, usize)> {
        let index = Index::new();
        let mut sz = 0;

        for (_, Fdr { id, rdr }) in fds.iter_mut() {
            let mut stream = Command::deserializer(rdr).into_iter();
            let mut offset = stream.byte_offset();
            while let Some(cmd) = stream.next() {
                let next_offset = stream.byte_offset();
                match cmd? {
                    Command::Set(key, _) => {
                        let old = index
                            .insert(key, CmdInfo::new(*id, offset as u64, next_offset - offset));
                        sz += old.map_or(0, |i| i.len);
                    }
                    Command::Rm(key) => {
                        let old = index.remove(&key);
                        sz += old.map_or(0, |i| i.len);
                        sz += next_offset - offset;
                    }
                }
                offset = next_offset;
            }
        }
        Ok((index, sz))
    }
}
