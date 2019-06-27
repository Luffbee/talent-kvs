pub extern crate slog;
extern crate slog_stdlog;

use slog::{debug, info, o, warn, Drain, Logger};

use std::collections::{BTreeMap, HashMap};
use std::ffi::OsStr;
use std::fs::{self, File};
use std::io::{BufWriter, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use super::command::Command;
use super::file::{self, Fdr, Fdw, Fid, Location};
use crate::{KvsError as Error, Result};

const ACTIVE_THRESHOLD: u64 = 1024 * 1024;
const COMPACT_THRESHOLD: usize = 2 * 1024 * 1024;

type Index = HashMap<String, CmdInfo>;
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
    wthreshold: u64,
    cthreshold: usize,

    garbage_sz: usize,
    index: Index,
    fds: FdrMap,
    active: Fdw,
}

/// Use to costom KvStore.
pub struct KvStoreBuilder {
    dir: PathBuf,
    log: Option<Logger>,
    wthreshold: u64,
    cthreshold: usize,
}

impl KvStoreBuilder {
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

        match self.read_meta()? {
            Some(ref meta) if meta != "kvs" => {
                return Err(Error::InvalidMeta(self.metapath()))?;
            }
            Some(_) => {
                fds = Self::file_list(&self.dir)?;

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

                fds = FdrMap::new();
                fds.insert(1, file::fdr(&self.dir, 1)?);

                index = Index::new();
                garbage_sz = 0;
            }
        }

        Ok(KvStore {
            index,
            fds,
            dir: self.dir,
            active,
            wthreshold: self.wthreshold,
            cthreshold: self.cthreshold,
            garbage_sz,
            log: log,
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
        let mut index = Index::new();
        let mut sz = 0;

        for (_, Fdr { id, rdr }) in fds.iter_mut() {
            let mut stream = Command::deserializer(rdr).into_iter();
            let mut offset = stream.byte_offset();
            while let Some(cmd) = stream.next() {
                let next_offset = stream.byte_offset();
                match cmd? {
                    Command::Set(key, _) => {
                        let old = index.insert(key, CmdInfo::new(*id, offset as u64, next_offset - offset));
                        sz += old.map_or(0, |i| i.len);
                    }
                    Command::Rm(key) => {
                        let old = index.remove(&key);
                        sz += old.map_or(0, |i| i.len);
                    }
                }
                offset = next_offset;
            }
        }
        Ok((index, sz))
    }
}

impl KvStore {
    /// Open a database with default configuration.
    pub fn open(dir: impl AsRef<Path>) -> Result<KvStore> {
        Self::new(dir).build()
    }

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

    /// If the key already in the store, update the value.  
    /// Otherwise, insert the key-value pair into the store.
    pub fn set(&mut self, key: String, val: String) -> Result<()> {
        let cmd = Command::Set(key, val);
        let info = self.append(&cmd)?;
        if let Command::Set(key, _) = cmd {
            if let Some(old) = self.index.insert(key.clone(), info.clone()) {
                self.garbage_sz += old.len;
                debug!(self.log, "Old location of key '{}': {:?}.", key, old);
                debug!(self.log, "New location of key '{}': {:?}.", key, info);
            } else {
                debug!(self.log, "Insert new key '{}' at {:?}.", key, info);
            }
        }
        debug!(self.log, "Fds: {:?}.", self.fds);
        if self.garbage_sz > self.cthreshold {
            self.compact()?;
        }
        Ok(())
    }

    /// If the key already in the store, return the `Some(value)`.  
    /// Otherwise, return `None`.
    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        let info = self.index.get(&key).cloned();
        if let Some(info) = info {
            let cmd = self.fetch(&info.loc)?;
            if let Command::Set(k, v) = cmd {
                if k == key {
                    Ok(Some(v))
                } else {
                    return Err(Error::UnexpectCmd {
                        found: format!("Set({:?}, {:?})", k, v),
                        expect: format!("Set({:?}, {})", key, "_"),
                    })?;
                }
            } else {
                return Err(Error::UnexpectCmd {
                    found: format!("{:?}", cmd),
                    expect: format!("Set({:?}, {})", key, "_"),
                })?;
            }
        } else {
            Ok(None)
        }
    }

    /// If the key already in the store, remove it.  
    /// Otherwise, do nothing.
    pub fn remove(&mut self, key: String) -> Result<()> {
        if let Some(info) = self.index.remove(&key) {
            debug!(self.log, "Old location of key {}: {:?}.", key, info);
            let cmd = Command::Rm(key);
            self.garbage_sz += info.len;
            let info = self.append(&cmd)?;
            self.garbage_sz += info.len;
            if self.garbage_sz > self.cthreshold {
                self.compact()?;
            }
            Ok(())
        } else {
            return Err(Error::KeyNotFound(key))?;
        }
    }

    fn append_with_threshold<W, F>(
        wtr: &mut W,
        cmd: &Command,
        threshold: u64,
        mut gen_next: F,
    ) -> Result<(u64, usize)>
    where
        W: Write + Seek,
        F: FnMut() -> Result<W>,
    {
        // Ensure append, and get current size.
        let mut offset = wtr.seek(SeekFrom::End(0))?;
        let cmd = Command::ser(cmd)?;
        let len = cmd.len();
        if offset + cmd.len() as u64 > threshold as u64 {
            *wtr = gen_next()?;
            offset = 0;
        }
        let n = wtr.write(cmd.as_ref())?;
        if n != cmd.len() {
            panic!("Write {} bytes for {} length command: {}", n, len, cmd);
        }
        Ok((offset, len))
    }

    // Write command to the active data file.
    // Allocate a new active data file if readched threshold.
    fn append(&mut self, cmd: &Command) -> Result<CmdInfo> {
        let mut active_id = self.active.id;
        let dir = &self.dir;
        let log = &self.log;

        debug!(self.log, "Appending command: {:?}", cmd);
        let (offset, len) =
            Self::append_with_threshold(&mut self.active.wtr, cmd, self.wthreshold, || {
                active_id += 1;
                let fname = file::data(dir, active_id);
                info!(log, "Creating new file: {:?}", fname);
                Ok(file::new(&fname)?)
            })?;
        if active_id != self.active.id {
            self.active.id = active_id;
            self.fds.insert(active_id, file::fdr(&self.dir, active_id)?);
        }

        self.active.wtr.flush()?;

        Ok(CmdInfo::new(active_id, offset, len))
    }

    fn fetch(&mut self, loc: &Location) -> Result<Command> {
        debug!(self.log, "fetching location: {:?}", loc);
        let fd = self.fds.get_mut(&loc.id).expect("lost a data file");
        assert_eq!(fd.id, loc.id, "get wrong fd");

        let file = &mut fd.rdr;
        file.seek(SeekFrom::Start(loc.offset))?;
        Command::from_reader(file)
    }

    /// Read command from locations in vec, and write to tempfiles.
    /// Tempfiles' id is a range: `lowest .. active_id`.
    /// Return updated index and the `lowest`.
    fn merge(&self, merge_id: Fid, mut index: Index, vec: Vec<CmdInfo>) -> Result<Index> {
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

            rdr.seek(SeekFrom::Start(u64::from(*offset)))?;
            let cmd = Command::from_reader(&mut rdr)?;
            match cmd {
                Command::Set(ref key, _) => {
                    let s = cmd.ser()?;
                    let len = s.len();
                    let offset = merge_wtr.seek(SeekFrom::End(0))?;
                    merge_wtr.write(s.as_bytes())?;
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

    // Only compact data if file id < active id.
    // Return compacted index and the lowest merged data file id.
    // If no merged data file, return active_id.
    // Merged data file is the range: lowest_id .. active_id
    fn real_compact(&self, merge_id: Fid, mut index: Index) -> Result<Index> {
        let mut vec = Vec::new();
        for (_, val) in index.drain() {
            if val.loc.id >= merge_id {
                continue;
            }
            vec.push(val);
        }
        if vec.len() <= 0 {
            return Ok(index);
        }
        vec.sort_unstable();
        self.merge(merge_id, index, vec)
    }

    /// Compact
    pub fn compact(&mut self) -> Result<()> {
        let merge_id = self.active.id + 1;
        let active_id = merge_id + 1;
        self.active = file::fdw(&self.dir, active_id)?;
        self.garbage_sz = 0;

        let index = self.real_compact(merge_id, self.index.clone())?;

        for (key, val) in index.iter() {
            if let Some(rval) = self.index.get_mut(key) {
                // If file id >= active id, not compacted.
                if rval.loc.id < active_id {
                    assert_eq!(rval.len, val.len);
                    rval.loc = val.loc.clone();
                } else {
                    self.garbage_sz += val.len;
                }
            }
        }

        debug!(self.log, "Fids after real_compact: {:?}.", self.fds);

        let new_fds = self.fds.split_off(&merge_id);
        for id in self.fds.keys() {
            info!(self.log, "Delete file: {:?}", self.datafile(*id));
            fs::remove_file(self.datafile(*id))?;
        }
        self.fds = new_fds;
        self.fds.insert(merge_id, file::fdr(&self.dir, merge_id)?);

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
