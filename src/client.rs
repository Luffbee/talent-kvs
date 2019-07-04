extern crate bytes;
extern crate tokio;

use slog::Logger;
use tokio::codec::Framed;
use tokio::net::TcpStream;
use tokio::prelude::*;

use std::net::SocketAddr;
use std::str;

use crate::protocol::{Proto, ProtoCodec};
use crate::get_logger;

pub struct KvsClient {
    addr: SocketAddr,
    log: Logger,
}

impl KvsClient {
    pub fn new<LG>(addr: SocketAddr, log: LG) -> Result<Self, i32>
    where
        LG: Into<Option<Logger>>,
    {
        let log = get_logger(&mut log.into());
        Ok(Self { addr, log })
    }

    fn request(&self, req: Proto) -> impl Future<Item = Proto, Error = i32> {
        let addr = self.addr;
        let log0 = self.log.clone();
        let log1 = self.log.clone();
        let log2 = self.log.clone();
        TcpStream::connect(&self.addr)
            .map_err(move |e| {
                crit!(log0, "failed to connect {}: {}", addr, e);
                666
            })
            .and_then(|sock| {
                Framed::new(sock, ProtoCodec::new())
                    .send(req)
                    .map_err(move |e| {
                        crit!(log1, "failed to send command: {}", e);
                        2
                    })
            })
            .and_then(move |frame| {
                let log = log2.clone();
                frame
                    .into_future()
                    .map_err(move |(e, _)| {
                        crit!(log2, "failed to decode reply: {:?}", e);
                        999
                    })
                    .and_then(move |(resp, _)| {
                        resp.ok_or_else(|| {
                            crit!(log, "no reply from server");
                            998
                        })
                    })
            })
    }

    pub fn set(&self, key: String, val: String) -> impl Future<Item = (), Error = i32> {
        let req = Proto::Seq(vec![
            Proto::Str("SET".to_owned()),
            Proto::Bulk(Vec::from(key)),
            Proto::Bulk(Vec::from(val)),
        ]);
        let log = self.log.clone();
        self.request(req).and_then(move |rep| match rep {
            Proto::Str(_) => Ok(()),
            Proto::Err(e) => {
                error!(log, "server error: {}", e);
                Err(3)
            }
            item => {
                crit!(log, "unexpected item: {:?}", item);
                Err(4)
            }
        })
    }

    pub fn get(&self, key: String) -> impl Future<Item = Option<String>, Error = i32> {
        let req = Proto::Seq(vec![
            Proto::Str("GET".to_owned()),
            Proto::Bulk(Vec::from(key)),
        ]);
        let log = self.log.clone();
        self.request(req).and_then(move |rep| match rep {
            Proto::Bulk(v) => match str::from_utf8(&v) {
                Ok(s) => Ok(Some(s.to_string())),
                Err(e) => {
                    crit!(log, "bad bulk: {}", e);
                    Err(5)
                }
            },
            Proto::Null => Ok(None),
            Proto::Err(e) => {
                error!(log, "server error: {}", e);
                Err(6)
            }
            item => {
                crit!(log, "unexpected item: {:?}", item);
                Err(7)
            }
        })
    }

    pub fn rm(&mut self, key: String) -> impl Future<Item = (), Error = i32> {
        let req = Proto::Seq(vec![
            Proto::Str("RM".to_owned()),
            Proto::Bulk(Vec::from(key)),
        ]);
        let log = self.log.clone();
        self.request(req).and_then(move |rep| match rep {
            Proto::Str(_) => Ok(()),
            Proto::Null => {
                error!(log, "Key not found");
                Err(8)
            }
            Proto::Err(e) => {
                error!(log, "server error: {}", e);
                Err(9)
            }
            item => {
                crit!(log, "unexpected item: {:?}", item);
                Err(10)
            }
        })
    }
}
