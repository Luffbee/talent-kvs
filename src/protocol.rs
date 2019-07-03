extern crate bytes;
extern crate tokio;

use bytes::BytesMut;
use tokio::codec::{Decoder, Encoder};

use std::error::Error as StdError;
use std::fmt::{self, Display, Formatter};
use std::io::BufRead;
use std::str;

use crate::{Error, Result};

const CRLF: &[u8; 2] = b"\r\n";

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

pub enum ProtoCodec {
    Unknown,
    Str(usize),
    Err(usize),
    BulkOrNull(usize),
    Bulk(usize),
}

impl ProtoCodec {
    pub fn new() -> Self {
        ProtoCodec::Unknown
    }

    fn dispatch(&self, x: u8) -> Result<Self> {
        Ok(match x {
            b'+' => ProtoCodec::Str(0),
            b'-' => ProtoCodec::Err(0),
            b'$' => ProtoCodec::BulkOrNull(0),
            x => return Err(ProtoError::InvalidPrefix(x))?,
        })
    }
}

impl Decoder for ProtoCodec {
    type Item = Proto;
    type Error = Error;
    fn decode(&mut self, buf: &mut BytesMut) -> Result<Option<Proto>> {
        loop {
            if buf.is_empty() {
                return Ok(None);
            }
            match self {
                ProtoCodec::Unknown => {
                    *self = self.dispatch(buf.split_to(1)[0])?;
                },
                ProtoCodec::Str(ref mut offset) => {
                    if let Some(s) = until_crlf(offset, buf)? {
                        *self = ProtoCodec::Unknown;
                        return Ok(Some(Proto::Str(s)));
                    } else {
                        return Ok(None);
                    }
                },
                ProtoCodec::Err(ref mut offset) => {
                    if let Some(s) = until_crlf(offset, buf)? {
                        *self = ProtoCodec::Unknown;
                        return Ok(Some(Proto::Err(s)));
                    } else {
                        return Ok(None);
                    }
                },
                ProtoCodec::BulkOrNull(ref mut offset) => {
                    if let Some(s) = until_crlf(offset, buf)? {
                        let len: isize = s.parse()?;
                        if len <= -1 {
                            *self = ProtoCodec::Unknown;
                            return Ok(Some(Proto::Null));
                        }
                        *self = ProtoCodec::Bulk(len as usize);
                    } else {
                        return Ok(None);
                    }
                },
                &mut ProtoCodec::Bulk(len) => {
                    if let Some(v) = until_len_crlf(len, buf)? {
                        *self = ProtoCodec::Unknown;
                        return Ok(Some(Proto::Bulk(v)));
                    } else {
                        return Ok(None);
                    }
                }
            }
        }
    }
}

impl Encoder for ProtoCodec {
    type Item = Proto;
    type Error = Error;
    fn encode(&mut self, item: Proto, dst: &mut BytesMut) -> Result<()> {
        dst.extend_from_slice(&item.ser());
        Ok(())
    }
}

fn until_crlf(offset: &mut usize, buf: &mut BytesMut) -> Result<Option<String>> {
    if let Some(idx) = buf[*offset..].iter().position(|b| *b == b'\n') {
        let s = buf.split_to(idx+1);
        *offset = 0;
        if s.len() < 2 || s[idx-1] != b'\r' {
            return Err(ProtoError::UnexpectedLF)?;
        }
        let s = str::from_utf8(&s[..idx-1])?;
        Ok(Some(s.to_string()))
    } else {
        *offset = buf.len();
        Ok(None)
    }
}

fn until_len_crlf(len: usize, buf: &mut BytesMut) -> Result<Option<Vec<u8>>> {
    if buf.len() < len + 2 {
        Ok(None)
    } else {
        let mut v = Vec::from(&buf.split_to(len+2)[..]);
        if v[len..len+2] != CRLF[..] {
            Err(ProtoError::InvalidBulk(v))?
        } else {
            v.truncate(len);
            v.shrink_to_fit();
            Ok(Some(v))
        }
    }
}

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
    UnexpectedLF,
    InvalidBulk(Vec<u8>),
}

impl Display for ProtoError {
    fn fmt(&self, f: &mut Formatter) -> std::result::Result<(), fmt::Error> {
        match self {
            ProtoError::InvalidPrefix(x) => write!(f, "invalid prefix: {:x?}", x),
            ProtoError::UnexpectedLF => write!(f, "unexpected '\\n'"),
            ProtoError::InvalidBulk(u) => write!(f, "invalid bulk: {:?}", u),
        }
    }
}

impl StdError for ProtoError {
    fn source(&self) -> Option<&'static dyn StdError> {
        None
    }
}
