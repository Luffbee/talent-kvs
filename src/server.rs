extern crate failure;
extern crate num_cpus;
extern crate slog_stdlog;

use failure::format_err;

use std::io::{prelude::*, BufRead, BufReader};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::string::String;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{spawn, JoinHandle};
//use std::thread;
//use std::time::Duration;

use crate::protocol::{Proto, ProtoError};
use crate::slog::{crit, error, info, o, Drain, Logger};
use crate::thread_pool::ThreadPool;
use crate::{Error, KvsEngine, KvsError};

pub struct KvsServer<EG: KvsEngine, TP: ThreadPool> {
    store: EG,
    pool: Arc<Mutex<TP>>,
    stop: Arc<AtomicBool>,
    addr: SocketAddr,
    log: Logger,
}

impl<EG: KvsEngine, TP: ThreadPool> Clone for KvsServer<EG, TP> {
    fn clone(&self) -> Self {
        Self {
            store: self.store.clone(),
            pool: self.pool.clone(),
            stop: self.stop.clone(),
            addr: self.addr,
            log: self.log.clone(),
        }
    }
}

impl<EG: KvsEngine, TP: ThreadPool> KvsServer<EG, TP> {
    pub fn new<LOG>(store: EG, pool: TP, addr: SocketAddr, log: LOG) -> Self
    where
        LOG: Into<Option<Logger>>,
    {
        let log = log
            .into()
            .unwrap_or_else(|| Logger::root(slog_stdlog::StdLog.fuse(), o!()));
        Self {
            store,
            pool: Arc::new(Mutex::new(pool)),
            stop: Arc::new(AtomicBool::new(false)),
            addr,
            log,
        }
    }

    pub fn run(&self) -> Result<(), i32> {
        let handle = self.start()?;
        if let Err(e) = handle.join() {
            error!(self.log, "listener panicked: {:?}", e);
            return Err(1);
        }
        Ok(())
    }

    pub fn start(&self) -> Result<JoinHandle<()>, i32> {
        let listener: TcpListener = match TcpListener::bind(self.addr) {
            Ok(listener) => listener,
            Err(e) => {
                crit!(self.log, "failed to listen the the {}: {}", self.addr, e);
                return Err(1);
            }
        };
        let this: Self = self.clone();
        let handle = spawn(move || {
            for res in listener.incoming() {
                if this.stop.load(Ordering::SeqCst) {
                    break;
                }
                match res {
                    Ok(stream) => {
                        let peer = stream.peer_addr().unwrap();
                        info!(this.log, "connected to client {}.", peer);
                        let peer_log = this.log.new(o!("client" => peer.to_string()));
                        let store = this.store.clone();
                        this.pool.lock().unwrap().spawn(move || {
                            //thread::sleep(Duration::from_secs(2));
                            handle_client(store, stream, peer_log);
                        });
                    }
                    Err(e) => {
                        error!(this.log, "bad stream: {}", e);
                    }
                }
            }
        });
        Ok(handle)
    }

    pub fn shutdown(&self) {
        self.stop.store(true, Ordering::SeqCst);
        let _ = TcpStream::connect(self.addr);
    }
}

fn handle_client<EG: KvsEngine>(store: EG, mut stream: TcpStream, log: Logger) {
    let mut wtr = match stream.try_clone() {
        Ok(stream) => stream,
        Err(e) => {
            error!(log, "failed to clone stream: {}", e);
            stream
                .write_all(&Proto::Err("server internal error".to_owned()).ser())
                .expect("failed to write stream");
            return;
        }
    };
    let mut rdr = BufReader::new(&mut stream);
    if let Err(e) = try_handle_client(store, &mut rdr, &mut wtr, log.clone()) {
        let err = ProtoError::BadRequest(e.to_string());
        error!(log, "{}", err);
        if let Err(e) = wtr.write_all(&Proto::Err(err.to_string()).ser()) {
            error!(log, "failed to write stream: {}", e);
        }
    }
}

fn try_handle_client<EG: KvsEngine>(
    store: EG,
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
                if let Err(e) = store.set(key, val) {
                    wtr.write_all(&Proto::Err(e.to_string()).ser())?;
                } else {
                    wtr.write_all(&Proto::Str("".to_owned()).ser())?;
                }
            }
            "GET" => {
                let key = match Proto::from_bufread(rdr)? {
                    Proto::Bulk(key) => String::from_utf8(key)?,
                    item => return unexpected_item(item, wtr, log),
                };
                info!(log, "received command: GET {:?}", key);
                match store.get(key) {
                    Ok(Some(val)) => {
                        wtr.write_all(&Proto::Bulk(Vec::from(val)).ser())?;
                    }
                    Ok(None) => {
                        wtr.write_all(&Proto::Null.ser())?;
                    }
                    Err(e) => {
                        wtr.write_all(&Proto::Err(e.to_string()).ser())?;
                    }
                }
            }
            "RM" => {
                let key = match Proto::from_bufread(rdr)? {
                    Proto::Bulk(key) => String::from_utf8(key)?,
                    item => return unexpected_item(item, wtr, log),
                };
                info!(log, "received command: RM {:?}", key);
                if let Err(e) = store.remove(key) {
                    if let Some(KvsError::KeyNotFound(_)) = e.downcast_ref() {
                        wtr.write_all(&Proto::Null.ser())?;
                    }
                    wtr.write_all(&Proto::Err(e.to_string()).ser())?;
                } else {
                    wtr.write_all(&Proto::Str("".to_owned()).ser())?;
                }
            }
            cmd => {
                error!(log, "unknown command: {}", cmd);
            }
        }
    }
    Ok(())
}

fn unexpected_item(item: Proto, wtr: &mut impl Write, log: Logger) -> Result<(), Error> {
    let err = format_err!("unexpected item: {:?}", item);
    error!(log, "{}", err);
    if let Err(e) = wtr.write_all(&Proto::Err(err.to_string()).ser()) {
        return Err(format_err!("failed to write stream: {}", e));
    }
    Err(err)
}
