use std::error::Error as StdError;
use std::fmt::{self, Display, Formatter};
use std::path::PathBuf;

/// KeyNotFound contains the key.
/// OtherErr contains lower level errors.
#[derive(Debug)]
pub enum Error {
    /// Contains the path with problem.
    BadPath(PathBuf),
    /// Invalid metadata
    InvalidMeta(PathBuf),
    /// Found an unexpect command.
    UnexpectCmd {
        /// The found command.
        found: String,
        /// The expected command.
        expect: String,
    },
    /// Contains the key.
    KeyNotFound(String),
    /// Some unknown error.
    UnknowErr(String),
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter) -> std::result::Result<(), fmt::Error> {
        match self {
            Error::BadPath(path) => write!(f, "bad path: {:?}", path),
            Error::InvalidMeta(path) => write!(f, "invalid metadata: {:?}", path),
            Error::UnexpectCmd { found, expect } => write!(
                f,
                "unexpect command: expect {:?}, but found {:?}",
                expect, found
            ),
            Error::KeyNotFound(key) => write!(f, "key not found: {}", key),
            Error::UnknowErr(s) => write!(f, "unknown error: {}", s),
        }
    }
}

impl StdError for Error {
    fn source(&self) -> Option<&'static dyn StdError> {
        None
    }
}
