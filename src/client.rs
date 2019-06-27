extern crate slog;

use slog::{crit, error, Logger};

use std::net::{SocketAddr, TcpStream};
use std::io::{prelude::*, BufReader};

use crate::protocol::Proto;

pub struct KvsClient {
    stream: TcpStream,
    log: Logger,
}

impl KvsClient {
    pub fn new(addr: SocketAddr, log: Logger) -> Result<KvsClient, i32> {
        let stream = match TcpStream::connect(addr) {
            Ok(s) => s,
            Err(e) => {
                crit!(log, "Failed to connect to {}: {}.", addr, e);
                return Err(1);
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
            return Err(1);
        }
        let mut rdr = BufReader::new(&mut self.stream);
        let resp = Proto::from_bufread(&mut rdr).unwrap();
        match resp {
            Proto::Str(_) => {},
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

    pub fn get(&mut self, key: String) -> Result<(), i32> {
        let req = Proto::Seq(vec![
            Proto::Str("GET".to_owned()),
            Proto::Bulk(Vec::from(key)),
        ]);
        if let Err(e) = self.stream.write(&req.ser()) {
            crit!(self.log, "Failed to send command: {}.", e);
            return Err(1);
        }
        let mut rdr = BufReader::new(&mut self.stream);
        let resp = Proto::from_bufread(&mut rdr).unwrap();
        match resp {
            Proto::Bulk(v) => {
                println!("{}", String::from_utf8_lossy(&v));
            }
            Proto::Null => {
                println!("Key not found");
            },
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
            },
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
