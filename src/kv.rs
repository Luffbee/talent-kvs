extern crate serde;
extern crate serde_derive;
extern crate serde_json;
pub extern crate slog;
extern crate slog_stdlog;

use serde::Deserialize as SerdeDe;
use serde_derive::{Deserialize, Serialize};
use serde_json::{de::IoRead, StreamDeserializer};
use slog::{debug, info, o, warn, Drain, Logger};

use std::collections::{HashMap, VecDeque};
use std::convert::From;
use std::ffi::OsStr;
use std::fs::{self, File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use crate::{KvsError as Error, Result};

const MAX_ACTIVE_SIZE: u64 = 16 * 1024;

type Fid = usize;
// (File id, offset)
type Location = (Fid, u64);
type Index = HashMap<String, Location>;
type BufRFile = BufReader<File>;
type BufWFile = BufWriter<File>;
type Fd = (Fid, BufRFile);
type FdList = VecDeque<Fd>;

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
    fsz: u64,

    data_num: usize,
    index: Index,
    fds: FdList,
    active: (Fid, BufWFile),
}

/// Use to costom KvStore.
pub struct KvStoreBuilder {
    dir: PathBuf,
    log: Option<Logger>,
    fsz: u64,
}

impl KvStoreBuilder {
    /// Set logger.
    pub fn logger(mut self, log: Logger) -> Self {
        self.log = Some(log);
        self
    }

    /// Set the max size of active file.
    pub fn active_size(mut self, sz: u64) -> Self {
        self.fsz = sz;
        self
    }

    /// Build the KvStore.
    pub fn build(self) -> Result<KvStore> {
        let dir = self.dir;
        let log = match self.log {
            Some(logger) => logger,
            None => {
                Logger::root(slog_stdlog::StdLog.fuse(), o!())
            }
        };

        info!(log, "Loading fds from dir {:?}.", dir);
        let mut fds = Self::get_file_list(&dir)?;

        // Init if there is no data file.
        debug!(log, "Load fds: {:?}.", fds);
        let active = match fds.back() {
            None => {
                warn!(log, "No data file exists in dir {:?}, creating one.", dir);
                let path = dir.join("1.data");
                newfile(&path)?;
                fds.push_back(get_fd(&dir, 1)?);
                1
            }
            Some(id) => id.0,
        };
        let active = (
            active,
            BufWriter::new(
                OpenOptions::new()
                    .write(true)
                    .open(datafile(&dir, active))?,
            ),
        );

        info!(log, "Loading data from dir {:?}.", dir);
        let index = Self::load_index(&mut fds)?;

        Ok(KvStore {
            index,
            data_num: fds.len(),
            fds,
            dir,
            active,
            fsz: self.fsz,
            log,
        })
    }

    /// Return sorted file ids.
    fn get_file_list(dir: &PathBuf) -> Result<FdList> {
        let mut ids: Vec<_> = fs::read_dir(dir)?
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
        let mut fds = VecDeque::with_capacity(ids.len());
        for id in ids {
            let path = dir.join(format!("{}.data", id));
            fds.push_back((id, BufReader::new(File::open(path)?)));
        }
        Ok(fds)
    }

    /// Read the data files to generate a HashMap index.
    fn load_index(fds: &mut FdList) -> Result<Index> {
        let mut index = Index::new();

        for (id, rdr) in fds.iter_mut() {
            let mut stream = Command::iter_from_reader(rdr);
            let mut offset = stream.byte_offset();
            while let Some(cmd) = stream.next() {
                match cmd? {
                    Command::Set(key, _) => {
                        index.insert(key, (*id, offset as u64));
                    }
                    Command::Rm(key) => {
                        index.remove(&key);
                    }
                }
                offset = stream.byte_offset();
            }
        }
        Ok(index)
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
            fsz: MAX_ACTIVE_SIZE,
            log: None,
        }
    }

    /// If the key already in the store, update the value.  
    /// Otherwise, insert the key-value pair into the store.
    pub fn set(&mut self, key: String, val: String) -> Result<()> {
        let cmd = Command::Set(key, val);
        let nloc = self.append(&cmd)?;
        if let Command::Set(key, _) = cmd {
            if let Some(loc) = self.index.insert(key.clone(), nloc.clone()) {
                debug!(self.log, "Old location of key '{}': {:?}.", key, loc);
                debug!(self.log, "New location of key '{}': {:?}.", key, nloc);
            } else {
                debug!(self.log, "Insert new key '{}' at {:?}.", key, nloc);
            }
        }
        debug!(self.log, "Fds: {:?}.", self.fds);
        if self.fds.len() > self.data_num as usize {
            self.compact()?;
            self.data_num = self.fds.len() * 2;
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
                    Err(From::from(Error::UnexpectCmd {
                        found: format!("Set({:?}, {:?})", k, v),
                        expect: format!("Set({:?}, {})", key, "_"),
                    }))
                }
            } else {
                Err(From::from(Error::UnexpectCmd {
                    found: format!("{:?}", cmd),
                    expect: format!("Set({:?}, {})", key, "_"),
                }))
            }
        } else {
            Ok(None)
        }
    }

    /// If the key already in the store, remove it.  
    /// Otherwise, do nothing.
    pub fn remove(&mut self, key: String) -> Result<()> {
        if let Some(loc) = self.index.remove(&key) {
            debug!(self.log, "Old location of key {}: {:?}.", key, loc);
            let cmd = Command::Rm(key);
            let loc = self.append(&cmd)?;
            if let Command::Rm(key) = cmd {
                debug!(self.log, "New location of key {}: {:?}.", key, loc);
            }
            if self.fds.len() > self.data_num as usize {
                self.compact()?;
                self.data_num = self.fds.len() * 2;
            }
            Ok(())
        } else {
            Err(From::from(Error::KeyNotFound(key)))
        }
    }

    fn append_with_threshold<W, F>(
        wtr: &mut W,
        cmd: &Command,
        threshold: u64,
        mut gen_next: F,
    ) -> Result<u64>
    where
        W: Write + Seek,
        F: FnMut() -> Result<W>,
    {
        // Ensure append, and get current size.
        let mut flen = wtr.seek(SeekFrom::End(0))?;
        let cmd = Command::ser(cmd)?;
        if flen + cmd.len() as u64 > threshold as u64 {
            *wtr = gen_next()?;
            flen = 0;
        }
        let n = wtr.write(cmd.as_ref())?;
        if n != cmd.len() {
            panic!(
                "Write {} bytes for {} length command: {}",
                n,
                cmd.len(),
                cmd
            );
        }
        Ok(flen)
    }

    // Write command to the active data file.
    // Allocate a new active data file if readched threshold.
    fn append(&mut self, cmd: &Command) -> Result<Location> {
        let mut active_id = self.active.0;
        let dir = &self.dir;
        let log = &self.log;

        debug!(self.log, "Appending command: {:?}", cmd);
        let offset = Self::append_with_threshold(&mut self.active.1, cmd, self.fsz, || {
            active_id += 1;
            let fname = datafile(dir, active_id);
            info!(log, "Creating new file: {:?}", fname);
            Ok(newfile(&fname)?)
        })?;
        if active_id != self.active.0 {
            self.active.0 = active_id;
            self.fds.push_back(get_fd(&self.dir, active_id)?);
        }

        self.active.1.flush()?;

        Ok((active_id, offset))
    }

    fn read(&self, loc: &Location) -> Result<Command> {
        let fname = self.datafile(loc.0);
        let flen = fs::metadata(&fname)?.len();

        // Check the file size first,
        // because seek() may accept a offset beyond the end.
        if flen < u64::from(loc.1) {
            Err(Error::UnknowErr(format!(
                "read location {:?} in file {:?} with length {}",
                loc, fname, flen
            )))?;
        }

        let mut file = BufReader::new(File::open(fname)?);
        file.seek(SeekFrom::Start(u64::from(loc.1)))?;

        Command::one_from_reader(&mut file)
    }

    fn open_temp(&self, id: Fid) -> Result<BufWriter<File>> {
        let path = self.tempfile(id);
        info!(self.log, "Creating new file: {:?}", path);
        Ok(BufWriter::new(
            OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open(path)?,
        ))
    }

    /// Read command from locations in vec, and write to tempfiles.
    /// Tempfiles' id is a range: `lowest .. active_id`.
    /// Return updated index and the `lowest`.
    fn merge(
        &self,
        active_id: Fid,
        mut index: Index,
        mut vec: Vec<Location>,
    ) -> Result<(Index, Fid)> {
        if vec.len() <= 0 {
            return Ok((index, active_id));
        }
        vec.sort_unstable();

        let mut merge_id = active_id - 1;
        let mut merge_wtr = self.open_temp(merge_id)?;

        let mut data_id: Fid = vec[0].0;
        let mut rdr = BufReader::new(File::open(self.datafile(data_id))?);

        for (fid, offset) in vec.iter() {
            if fid != &data_id {
                data_id = *fid;
                rdr = BufReader::new(File::open(self.datafile(data_id))?);
            }

            rdr.seek(SeekFrom::Start(u64::from(*offset)))?;
            let cmd = Command::one_from_reader(&mut rdr)?;
            match cmd {
                Command::Set(ref key, _) => {
                    let offset =
                        Self::append_with_threshold(&mut merge_wtr, &cmd, self.fsz, || {
                            merge_id -= 1;
                            Ok(self.open_temp(merge_id)?)
                        })?;
                    index.insert(key.to_owned(), (merge_id, offset));
                }
                Command::Rm(ref key) => {
                    Err(Error::UnexpectCmd {
                        found: format!("Rm({:?})", key),
                        expect: "Set(_, _)".to_owned(),
                    })?;
                }
            }
        }

        Ok((index, merge_id))
    }

    // Only compact data if file id < active id.
    // Return compacted index and the lowest merged data file id.
    // If no merged data file, return active_id.
    // Merged data file is the range: lowest_id .. active_id
    fn real_compact(&self, active_id: Fid, mut index: Index) -> Result<(Index, Fid)> {
        let mut vec = Vec::new();
        for (_, val) in index.drain() {
            if val.0 >= active_id {
                continue;
            }
            vec.push(val);
        }
        self.merge(active_id, index, vec)
    }

    /// Compact
    pub fn compact(&mut self) -> Result<()> {
        let active_id = self.active.0;
        let (index, lowest_id) = self.real_compact(active_id, self.index.clone())?;
        // TODO: The process after can be asynchronous.
        assert!(lowest_id <= active_id);
        for (key, val) in index.iter() {
            if let Some(rval) = self.index.get_mut(key) {
                // If file id >= active id, not compacted.
                if rval.0 < active_id {
                    *rval = val.to_owned();
                }
            }
        }
        debug!(self.log, "Fids after real_compact: {:?}.", self.fds);
        loop {
            if let Some((id, _)) = self.fds.front() {
                if *id < active_id {
                    info!(self.log, "Delete file: {:?}", self.datafile(*id));
                    fs::remove_file(self.datafile(*id))?;
                    self.fds.pop_front().unwrap();
                } else if *id == active_id {
                    break;
                } else {
                    Err(Error::UnknowErr(format!("active id lost: {}", active_id)))?;
                }
            } else {
                Err(Error::UnknowErr(format!("active id lost: {}", active_id)))?;
            }
        }
        for i in lowest_id..active_id {
            info!(
                self.log,
                "Move file: {:?} -> {:?}.",
                self.tempfile(i),
                self.datafile(i)
            );
            fs::rename(self.tempfile(i), self.datafile(i))?;
            self.fds.push_front(get_fd(&self.dir, i)?);
        }
        Ok(())
    }

    fn tempfile(&self, id: Fid) -> PathBuf {
        self.dir.join(format!("{}.data.temp", id))
    }

    fn datafile(&self, id: Fid) -> PathBuf {
        datafile(&self.dir, id)
    }
}

#[derive(Serialize, Deserialize, Debug)]
enum Command {
    #[serde(rename = "S")]
    Set(String, String),
    #[serde(rename = "R")]
    Rm(String),
}

// Only serde_json support stream, that's the reason to choose it.
impl Command {
    fn ser(&self) -> Result<String> {
        Ok(serde_json::to_string(self)?)
    }
    fn one_from_reader<R: Read>(rdr: R) -> Result<Self> {
        let mut de = serde_json::Deserializer::from_reader(rdr);
        Ok(<Self as SerdeDe>::deserialize(&mut de)?)
    }
    fn iter_from_reader<R: Read>(rdr: R) -> StreamDeserializer<'static, IoRead<R>, Self> {
        serde_json::Deserializer::from_reader(rdr).into_iter::<Self>()
    }
}

fn newfile(path: impl AsRef<Path>) -> Result<BufWriter<File>> {
    Ok(BufWriter::new(
        OpenOptions::new().write(true).create_new(true).open(path)?,
    ))
}

fn datafile(dir: &PathBuf, id: Fid) -> PathBuf {
    dir.join(format!("{}.data", id))
}

fn get_fd(dir: &PathBuf, id: Fid) -> Result<Fd> {
    Ok((id, BufReader::new(File::open(datafile(dir, id))?)))
}
