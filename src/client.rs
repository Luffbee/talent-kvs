extern crate slog;
extern crate slog_stdlog;

use slog::{o, crit, error, Drain, Logger};

use std::io::{prelude::*, BufReader};
use std::net::{SocketAddr, TcpStream};

use crate::protocol::Proto;

pub struct KvsClient {
    stream: TcpStream,
    log: Logger,
}

impl KvsClient {
    pub fn new(addr: SocketAddr, log: Option<Logger>) -> Result<Self, i32> {
        let log = log
            .unwrap_or_else(|| Logger::root(slog_stdlog::StdLog.fuse(), o!()));
        let stream = match TcpStream::connect(addr) {
            Ok(s) => s,
            Err(e) => {
                crit!(log, "Failed to connect to {}: {}.", addr, e);
                return Err(666);
            }
        };
        Ok(Self { stream, log })
    }

    pub fn set(&mut self, key: String, val: String) -> Result<(), i32> {
        let req = Proto::Seq(vec![
            Proto::Str("SET".to_owned()),
            Proto::Bulk(Vec::from(key)),
            Proto::Bulk(Vec::from(val)),
        ]);
        if let Err(e) = self.stream.write(&req.ser()) {
            crit!(self.log, "Failed to send command: {}.", e);
            return Err(2);
        }
        let mut rdr = BufReader::new(&mut self.stream);
        let resp = match Proto::from_bufread(&mut rdr) {
            Ok(reply) => reply,
            Err(e) => {
                eprintln!("{:?}", e);
                return Err(999);
            }
        };
        match resp {
            Proto::Str(_) => {}
            Proto::Err(e) => {
                error!(self.log, "server error: {}", e);
                return Err(3);
            }
            item => {
                error!(self.log, "unexpected item: {:?}", item);
                return Err(4);
            }
        }
        Ok(())
    }

    pub fn get(&mut self, key: String) -> Result<Option<String>, i32> {
        let req = Proto::Seq(vec![
            Proto::Str("GET".to_owned()),
            Proto::Bulk(Vec::from(key)),
        ]);
        if let Err(e) = self.stream.write(&req.ser()) {
            crit!(self.log, "Failed to send command: {}.", e);
            return Err(5);
        }
        let mut rdr = BufReader::new(&mut self.stream);
        let resp = Proto::from_bufread(&mut rdr).unwrap();
        match resp {
            Proto::Bulk(v) => {
                Ok(Some(String::from_utf8_lossy(&v).into_owned()))
            }
            Proto::Null => {
                Ok(None)
            }
            Proto::Err(e) => {
                error!(self.log, "server error: {}", e);
                Err(6)
            }
            item => {
                error!(self.log, "unexpected item: {:?}", item);
                Err(7)
            }
        }
    }

    pub fn rm(&mut self, key: String) -> Result<(), i32> {
        let req = Proto::Seq(vec![
            Proto::Str("RM".to_owned()),
            Proto::Bulk(Vec::from(key)),
        ]);
        if let Err(e) = self.stream.write(&req.ser()) {
            crit!(self.log, "Failed to send command: {}.", e);
            return Err(1);
        }
        let mut rdr = BufReader::new(&mut self.stream);
        let resp = Proto::from_bufread(&mut rdr).unwrap();
        match resp {
            Proto::Str(_) => {}
            Proto::Null => {
                error!(self.log, "Key not found");
                return Err(1);
            }
            Proto::Err(e) => {
                error!(self.log, "server error: {}", e);
                return Err(1);
            }
            item => {
                error!(self.log, "unexpected item: {:?}", item);
                return Err(1);
            }
        }
        Ok(())
    }
}
