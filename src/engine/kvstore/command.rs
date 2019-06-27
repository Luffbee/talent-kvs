extern crate serde;
extern crate serde_derive;
extern crate serde_json;

use serde::Deserialize as SerdeDe;
use serde_derive::{Deserialize, Serialize};
use serde_json::{de::IoRead, Deserializer};

use std::io::Read;

use crate::Result;

#[derive(Serialize, Deserialize, Debug)]
pub enum Command {
    #[serde(rename = "S")]
    Set(String, String),
    #[serde(rename = "R")]
    Rm(String),
}

// Only serde_json support stream, that's the reason to choose it.
impl Command {
    pub fn ser(&self) -> Result<String> {
        Ok(serde_json::to_string(self)?)
    }
    pub fn deserializer<R: Read>(rdr: R) -> Deserializer<IoRead<R>> {
        Deserializer::from_reader(rdr)
    }
    pub fn from_reader<R: Read>(rdr: R) -> Result<Self> {
        let mut de = Self::deserializer(rdr);
        Ok(Self::deserialize(&mut de)?)
    }
}
