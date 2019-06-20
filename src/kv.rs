extern crate failure;
extern crate log;
extern crate serde;
extern crate serde_derive;
extern crate serde_json;

use failure::Error as FailError;
use log::{debug, info, warn};
use serde::Deserialize as SerdeDe;
use serde_json::{StreamDeserializer, de::IoRead};
use serde_derive::{Deserialize, Serialize};

use std::collections::{HashMap, VecDeque};
use std::convert::From;
use std::error::Error as ErrorTrait;
use std::fmt::{self, Display, Formatter};
use std::fs::{self, File, OpenOptions};
use std::io::{BufReader, BufWriter, Seek, SeekFrom, Read, Write};
use std::path::{Path, PathBuf};

const MAX_FILE_SIZE: u32 = 16 * 1024;

/// KvStore Result
pub type Result<T> = std::result::Result<T, FailError>;

type Fid = u32;
// (File id, offset)
type Location = (Fid, u32);
type Index = HashMap<String, Location>;

/// KeyNotFound contains the key.
/// OtherErr contains lower level errors.
#[derive(Debug)]
pub enum Error {
    /// Contains the path with problem.
    BadPath(PathBuf),
    /// Found an unexpect command.
    UnexpectCmd {
        /// The found command.
        found: String,
        /// The expected command.
        expect: String,
    },
    /// Contains the key.
    KeyNotFound(String),
    /// No active file.
    NoActive,
    /// Some unknown error.
    UnknowErr(String),
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter) -> std::result::Result<(), fmt::Error> {
        match self {
            Error::BadPath(path) => write!(f, "bad path: {:?}", path),
            Error::UnexpectCmd { found, expect } => write!(
                f,
                "unexpect command: expect {:?}, but found {:?}",
                expect, found
            ),
            Error::KeyNotFound(key) => write!(f, "key not found: {}", key),
            Error::NoActive => write!(f, "no active file"),
            Error::UnknowErr(s) => write!(f, "unknown error: {}", s),
        }
    }
}

impl ErrorTrait for Error {
    fn source(&self) -> Option<&(dyn ErrorTrait + 'static)> {
        None
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
    fn ser(&self) ->Result<String> {
        Ok(serde_json::to_string(self)?)
    }
    fn one_from_reader<R: Read>(rdr: R) -> Result<Self> {
        let mut de = serde_json::Deserializer::from_reader(rdr);
        Ok(<Self as SerdeDe>::deserialize(&mut de)?)
    }
    fn iter_from_reader<R: Read>(rdr: R) -> StreamDeserializer<'static, IoRead<R>, Self> {
        serde_json::Deserializer::from_reader(rdr).into_iter::<Self>()
    }
    fn one_from_slice(bytes: &[u8]) -> Result<Self> {
        let mut de = serde_json::Deserializer::from_slice(bytes);
        Ok(<Self as SerdeDe>::deserialize(&mut de)?)
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
    index: Index,
    dir: PathBuf,
    fsz: u32,
    fids: VecDeque<Fid>,
}

impl KvStore {
    /// Open a database at dir.
    pub fn open<P: AsRef<Path>>(dir: P) -> Result<KvStore> {
        let dir = dir.as_ref().to_owned();
        let mut fids = Self::load_file_ids(&dir)?;
        // Init if there is no data file.
        info!("Get fids: {:?}.", fids);
        if fids.back().is_none() {
            warn!("No data file exists in dir {:?}, creating one.", dir);
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
    fn load_file_ids(dir: &PathBuf) -> Result<VecDeque<Fid>> {
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
                    ids.push(Fid::from_str_radix(id, 10)?);
                } else {
                    Err(Error::BadPath(path))?;
                }
            } else {
                Err(Error::BadPath(path))?;
            }
        }
        ids.sort();
        Ok(VecDeque::from(ids))
    }

    // Read the data files to generate a HashMap index.
    fn load_index(dir: &PathBuf, fids: &VecDeque<Fid>) -> Result<Index> {
        info!("Loading data from dir {:?}.", dir);
        let mut index = Index::new();
        for id in fids.iter() {
            let fname = dir.join(format!("{}.data", id));
            info!("Loading data from file {:?}.", fname);

            let reader = BufReader::new(File::open(fname)?);
            let mut stream = Command::iter_from_reader(reader);
            loop {
                let offset = stream.byte_offset();
                match stream.next() {
                    Some(Ok(Command::Set(key, _))) => {
                        index.insert(key, (*id, offset as u32));
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

    fn active_id(&self) -> Result<Fid> {
        self.fids
            .back()
            .cloned()
            .ok_or(FailError::from(Error::NoActive))
    }

    fn append_with_threshold<W, F>(
        wtr: &mut W,
        cmd: &Command,
        threshold: u32,
        mut gen_next: F,
    ) -> Result<u32>
    where
        W: Write + Seek,
        F: FnMut() -> Result<W>,
    {
        // Ensure append, and get current size.
        let mut flen = wtr.seek(SeekFrom::End(0))?;
        let cmd = Command::ser(cmd)?;
        info!("Appending command: {}", cmd);
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
        Ok(flen as u32)
    }

    // Write command to the active data file.
    // Allocate a new active data file if readched threshold.
    fn append(&mut self, cmd: &Command) -> Result<Location> {
        let mut active_id = self.active_id()?;
        let fname = self.dir.join(format!("{}.data", active_id));
        let mut file = OpenOptions::new().read(true).write(true).open(&fname)?;

        let offset = Self::append_with_threshold(&mut file, cmd, self.fsz, || {
            active_id += 1;
            let fname = self.dir.join(format!("{}.data", active_id));
            info!("Creating new file: {:?}", fname);
            self.fids.push_back(active_id);
            Ok(OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&fname)?)
        })?;

        Ok((active_id, offset))
    }

    fn read(&self, loc: &Location) -> Result<Command> {
        let fname = self.dir.join(format!("{}.data", loc.0));
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

        Command::one_from_reader(file)
    }

    fn merge(&self, active_id: Fid, mut index: Index, vec: Vec<Location>) -> Result<(Index, Fid)> {
        if vec.len() <= 0 {
            return Ok((index, active_id));
        }
        fn open_file_w(path: PathBuf) -> Result<BufWriter<File>> {
            Ok(BufWriter::new(
                OpenOptions::new()
                    .write(true)
                    .create(true)
                    .truncate(true)
                    .open(path)?,
            ))
        }

        let mut merge_id = active_id - 1;
        let merge_path = self.dir.join(format!("{}.data.temp", merge_id));
        let mut merge_wtr = open_file_w(merge_path)?;

        //let hint_path = self.dir.join(format!("{}.hint.temp", merge_id));
        //let mut hint_wtr = open_file_w(hint_path)?;

        let mut data_id: Fid = vec[0].0;
        let data_path = self.dir.join(format!("{}.data", data_id));
        let mut data = Vec::new();
        File::open(data_path)?.read_to_end(&mut data)?;

        for (fid, offset) in vec.iter() {
            if fid != &data_id {
                data_id = *fid;
                let data_path = self.dir.join(format!("{}.data", data_id));
                data.clear();
                File::open(data_path)?.read_to_end(&mut data)?;
            }
            let cmd = Command::one_from_slice(&data.as_slice()[(*offset) as usize..])?;
            if let Command::Rm(ref key) = cmd {
                return Err(FailError::from(Error::UnexpectCmd{
                    found: format!("Rm({:?})", key),
                    expect: "Set(_, _)".to_owned(),
                }));
            }
            let offset = Self::append_with_threshold(&mut merge_wtr, &cmd, self.fsz, || {
                merge_id -= 1;
                let path = self.dir.join(format!("{}.data.temp", merge_id));
                info!("Creating new file: {:?}", path);
                //    self.fids.push_back(active_id);
                Ok(open_file_w(path)?)
            })?;
            if let Command::Set(key, _) = cmd {
                index.insert(key, (merge_id, offset));
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
        vec.sort();
        self.merge(active_id, index, vec)
    }

    /// Compact
    pub fn compact(&mut self) -> Result<()> {
        let active_id = self.active_id()?;
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
        debug!("Fids after real_compact: {:?}.", self.fids);
        loop {
            if let Some(id) = self.fids.front() {
                if *id < active_id {
                    let path = self.dir.join(format!("{}.data", id));
                    fs::remove_file(path)?;
                    self.fids.pop_front().unwrap();
                } else if *id == active_id {
                    break;
                } else {
                    Err(Error::UnknowErr(format!("active id lost: {}", active_id)))?;
                }
            } else {
                Err(Error::UnknowErr(format!("active id lost: {}", active_id)))?;
            }
        }
        for i in lowest_id .. active_id {
            self.fids.push_front(i);
            let src = self.dir.join(format!("{}.data.temp", i));
            let dst = self.dir.join(format!("{}.data", i));
            fs::rename(src, dst)?;
        }
        Ok(())
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
        info!("Fids: {:?}.", self.fids);
        if self.fids.len() > 3 {
            self.compact()?;
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
            info!("Old location of key {}: {:?}.", key, loc);
            let cmd = Command::Rm(key);
            let loc = self.append(&cmd)?;
            if let Command::Rm(key) = cmd {
                info!("New location of key {}: {:?}.", key, loc);
            }
            if self.fids.len() > 3 {
                self.compact()?;
            }
            Ok(())
        } else {
            Err(From::from(Error::KeyNotFound(key)))
        }
    }
}
