#![allow(dead_code)]
use std::error::Error as StdError;
use std::fmt::{self, Display, Formatter};
use std::io::BufRead;
use std::str;

use crate::Result;

/// Proto
#[derive(Debug)]
pub enum Proto {
    /// Sequence
    Seq(Vec<Proto>),
    /// String
    Str(String),
    /// Error
    Err(String),
    /// Binary
    Bulk(Vec<u8>),
    /// Null
    Null,
}

const CRLF: &[u8; 2] = b"\r\n";

impl Proto {
    /// Str and Err should not contain CR or LF.
    pub fn ser(&self) -> Vec<u8> {
        let mut res = Vec::new();
        match self {
            Proto::Str(s) => {
                res.push(b'+');
                res.extend_from_slice(s.as_bytes());
            }
            Proto::Err(e) => {
                res.push(b'-');
                res.extend_from_slice(e.to_string().as_bytes());
            }
            Proto::Bulk(s) => {
                res.push(b'$');
                let n = s.len();
                res.extend_from_slice(n.to_string().as_bytes());
                res.extend_from_slice(CRLF);
                res.extend_from_slice(s);
            }
            Proto::Null => {
                return Vec::from("$-1\r\n");
            }
            Proto::Seq(v) => {
                return v.iter().fold(Vec::new(), |mut acc, x| {
                    acc.append(&mut x.ser());
                    acc
                });
            }
        }
        res.extend_from_slice(CRLF);
        res
    }

    /// from BufRead
    pub fn from_bufread(rdr: &mut impl BufRead) -> Result<Proto> {
        let mut prefix = [0; 1];
        let mut buf: Vec<u8> = Vec::new();
        if let Err(e) = rdr.read_exact(&mut prefix) {
            //eprintln!("EXEXEXEXEXEXEX");
            Err(e)?;
        }
        match prefix[0] {
            b'+' => {
                rdr.read_until(b'\n', &mut buf)?;
                Ok(Proto::Str(str::from_utf8(&buf)?.trim().to_owned()))
            }
            b'-' => {
                rdr.read_until(b'\n', &mut buf)?;
                let s = str::from_utf8(&buf)?.trim().to_owned();
                Ok(Proto::Err(s))
            }
            b'$' => {
                rdr.read_until(b'\n', &mut buf)?;
                let n: isize = str::from_utf8(&buf)?.trim().parse()?;
                if n <= -1 {
                    return Ok(Proto::Null);
                }
                let n = n as usize;
                // n bytes bulk + 2 bytes CRLF
                buf.resize(n + 2, 0);
                rdr.read_exact(&mut buf)?;
                buf.truncate(n);
                Ok(Proto::Bulk(buf))
            }
            x => Err(ProtoError::InvalidPrefix(x))?,
        }
    }
}

/// ProtoError
#[derive(Debug)]
pub enum ProtoError {
    /// Invalid prefix
    InvalidPrefix(u8),
    /// Bad request
    BadRequest(String),
}

impl Display for ProtoError {
    fn fmt(&self, f: &mut Formatter) -> std::result::Result<(), fmt::Error> {
        match self {
            ProtoError::InvalidPrefix(x) => write!(f, "invalid prefix: {:x?}", x),
            ProtoError::BadRequest(s) => write!(f, "bad request: {}", s),
        }
    }
}

impl StdError for ProtoError {
    fn source(&self) -> Option<&'static dyn StdError> {
        None
    }
}
