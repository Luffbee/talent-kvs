use failure::format_err;
use std::io::{prelude::*, BufRead, BufReader};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::string::String;

use crate::slog::{crit, error, info, o, Logger};
use crate::{Error, KvsError, KvsEngine};
use crate::protocol::{Proto, ProtoError};


pub struct KvsServer<E: KvsEngine>{
    store: E,
}

impl<E: KvsEngine> KvsServer<E> {
    pub fn new(store: E) -> Self {
        KvsServer { store }
    }

    pub fn start(&self, addr: SocketAddr, log: Logger) -> Result<(), i32> {
        let listener: TcpListener = match TcpListener::bind(addr) {
            Ok(listener) => listener,
            Err(e) => {
                crit!(log, "failed to listen the the {}: {}", addr, e);
                return Err(1);
            }
        };
        for res in listener.incoming() {
            match res {
                Ok(stream) => {
                    let peer = stream.peer_addr().unwrap();
                    info!(log, "connected to client {}.", peer);
                    let peer_log = log.new(o!("client" => peer.to_string()));
                    self.handle_client(stream, peer_log);
                }
                Err(e) => {
                    error!(log, "bad stream: {}", e);
                    return Err(1);
                }
            }
        }
        Ok(())
    }

    pub fn handle_client(&self, mut stream: TcpStream, log: Logger) {
        let mut wtr = match stream.try_clone() {
            Ok(stream) => stream,
            Err(e) => {
                error!(log, "failed to clone stream: {}", e);
                stream
                    .write(&Proto::Err("server internal error".to_owned()).ser())
                    .expect("failed to write stream");
                return;
            }
        };
        let mut rdr = BufReader::new(&mut stream);
        if let Err(e) = self.try_handle_client(&mut rdr, &mut wtr, log.clone()) {
            let err = ProtoError::BadRequest(e.to_string());
            error!(log, "{}", err);
            if let Err(e) = wtr.write(&Proto::Err(err.to_string()).ser()) {
                error!(log, "failed to write stream: {}", e);
            }
        }
    }

    fn try_handle_client(
        &self,
        rdr: &mut impl BufRead,
        wtr: &mut impl Write,
        log: Logger,
    ) -> Result<(), Error> {
        //let mut buffer = [0; 128];
        //let n = rdr.read(&mut buffer)?;
        //info!(log, "Read from client: {:?}", String::from_utf8_lossy(&buffer[..n]));
        //return Ok(());
        let head = Proto::from_bufread(rdr)?;
        if let Proto::Str(s) = head {
            match s.as_str() {
                "SET" => {
                    let key = match Proto::from_bufread(rdr)? {
                        Proto::Bulk(key) => String::from_utf8(key)?,
                        item => return unexpected_item(item, wtr, log),
                    };
                    let val = match Proto::from_bufread(rdr)? {
                        Proto::Bulk(val) => String::from_utf8(val)?,
                        item => return unexpected_item(item, wtr, log),
                    };
                    info!(log, "received command: SET {:?} {:?}", key, val);
                    if let Err(e) = self.store.set(key, val) {
                        wtr.write(&Proto::Err(e.to_string()).ser())?;
                    } else {
                        wtr.write(&Proto::Str("".to_owned()).ser())?;
                    }
                }
                "GET" => {
                    let key = match Proto::from_bufread(rdr)? {
                        Proto::Bulk(key) => String::from_utf8(key)?,
                        item => return unexpected_item(item, wtr, log),
                    };
                    info!(log, "received command: GET {:?}", key);
                    match self.store.get(key) {
                        Ok(Some(val)) => {
                            wtr.write(&Proto::Bulk(Vec::from(val)).ser())?;
                        }
                        Ok(None) => {
                            wtr.write(&Proto::Null.ser())?;
                        }
                        Err(e) => {
                            wtr.write(&Proto::Err(e.to_string()).ser())?;
                        }
                    }
                }
                "RM" => {
                    let key = match Proto::from_bufread(rdr)? {
                        Proto::Bulk(key) => String::from_utf8(key)?,
                        item => return unexpected_item(item, wtr, log),
                    };
                    info!(log, "received command: RM {:?}", key);
                    if let Err(e) = self.store.remove(key) {
                        if let Some(KvsError::KeyNotFound(_)) = e.downcast_ref() {
                            wtr.write(&Proto::Null.ser())?;
                        }
                        wtr.write(&Proto::Err(e.to_string()).ser())?;
                    } else {
                        wtr.write(&Proto::Str("".to_owned()).ser())?;
                    }
                }
                cmd => {
                    error!(log, "unknown command: {}", cmd);
                }
            }
        }
        Ok(())
    }
}

fn unexpected_item(item: Proto, wtr: &mut impl Write, log: Logger) -> Result<(), Error> {
    let err = format_err!("unexpected item: {:?}", item);
    error!(log, "{}", err);
    if let Err(e) = wtr.write(&Proto::Err(err.to_string()).ser()) {
        return Err(format_err!("failed to write stream: {}", e));
    }
    return Err(err);
}
