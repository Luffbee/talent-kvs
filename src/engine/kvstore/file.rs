use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter};
use std::path::{Path, PathBuf};

use crate::Result;

pub type Fid = usize;

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Location {
    pub id: Fid,
    pub offset: u64,
}

#[derive(Debug)]
pub struct Fdr {
    pub id: Fid,
    pub rdr: BufReader<File>,
}

pub struct Fdw {
    pub id: Fid,
    pub wtr: BufWriter<File>,
}

pub fn new(path: impl AsRef<Path>) -> Result<BufWriter<File>> {
    Ok(BufWriter::new(
        OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)?,
    ))
}

pub fn data(dir: &PathBuf, id: Fid) -> PathBuf {
    dir.join(format!("{}.data", id))
}

pub fn temp(dir: &PathBuf, id: Fid) -> PathBuf {
    dir.join(format!("{}.data.temp", id))
}

pub fn open_r(path: impl AsRef<Path>) -> Result<BufReader<File>> {
    Ok(BufReader::new(File::open(path)?))
}

pub fn open_w(path: impl AsRef<Path>) -> Result<BufWriter<File>> {
    let wtr = OpenOptions::new().write(true).open(path)?;
    Ok(BufWriter::new(wtr))
}

pub fn fdr(dir: &PathBuf, id: Fid) -> Result<Fdr> {
    let rdr = open_r(&data(dir, id))?;
    Ok(Fdr { id, rdr })
}

pub fn fdw(dir: &PathBuf, id: Fid) -> Result<Fdw> {
    let wtr = new(&data(dir, id))?;
    Ok(Fdw { id, wtr })
}
