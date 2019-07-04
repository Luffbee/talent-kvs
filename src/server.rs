extern crate tokio;

use future::FutureResult;
use tokio::codec::{FramedRead, FramedWrite};
use tokio::io::ReadHalf;
use tokio::net::{TcpListener, TcpStream};
use tokio::prelude::*;
use tokio::runtime::Runtime;
use tokio::sync::oneshot;

use std::fmt::Display;
use std::net::{self, SocketAddr};
use std::str;
use std::string::String;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use crate::get_logger;
use crate::protocol::{Proto, ProtoCodec};
use crate::slog::Logger;
use crate::thread_pool::ThreadPool;
use crate::KvsEngine;

pub struct KvsServer<EG: KvsEngine, TP: ThreadPool> {
    store: EG,
    pool: TP,
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
        let log = get_logger(&mut log.into());
        Self {
            store,
            pool,
            stop: Arc::new(AtomicBool::new(false)),
            addr,
            log,
        }
    }

    pub fn run(&self) -> Result<(), i32> {
        let server = self.start();
        let mut rt = Runtime::new().unwrap();
        let res = rt.block_on(server);
        rt.shutdown_on_idle().wait().unwrap();
        res
    }

    pub fn start(&self) -> Box<dyn Future<Item = (), Error = i32> + Send + 'static> {
        let log1 = self.log.clone();
        let stop = self.stop.clone();
        let this = self.clone();
        let listener = match TcpListener::bind(&self.addr) {
            Ok(x) => x,
            Err(e) => {
                crit!(self.log, "failed to listen the the {}: {}", self.addr, e);
                return Box::new(future::err(1));
            }
        };
        Box::new(
            listener
                .incoming()
                .take_while(move |_| future::ok(!stop.load(Ordering::SeqCst)))
                .then(move |res| match res {
                    Ok(sock) => Ok(Some(sock)),
                    Err(e) => {
                        error!(log1, "bad stream: {}", e);
                        Ok(None)
                    }
                })
                .filter_map(|opt| opt)
                .for_each(move |sock: TcpStream| {
                    tokio::spawn(this.process(sock));
                    future::ok(())
                }),
        )
    }

    pub fn process(&self, sock: TcpStream) -> FutureResult<(), ()> {
        let peer = match sock.peer_addr() {
            Ok(addr) => addr,
            Err(e) => {
                error!(self.log, "failed to get peer address: {}", e);
                return future::ok(());
            }
        };

        let log = self.log.new(o!("client" => peer.to_string()));
        let store = self.store.clone();
        let pool = self.pool.clone();
        let (rdr, wtr) = sock.split();
        let wtr = FramedWrite::new(wtr, ProtoCodec::new());

        tokio::spawn(
            ReqFuture::new(rdr)
                .into_future()
                .map_err(|(e, _)| e)
                .and_then(|(req, _)| req.ok_or_else(|| "empty request".to_owned()))
                .and_then(move |req| {
                    EngineFuture::new(req.clone(), store, pool).map(|rep| (req, rep))
                })
                .and_then(|(_req, resp)| match resp {
                    Reply::SR(Ok(())) => Ok(Proto::Str("".to_owned())),
                    Reply::SR(Err(e)) => Ok(Proto::Err(e)),
                    Reply::G(Ok(Some(val))) => Ok(Proto::Bulk(Vec::from(val))),
                    Reply::G(Ok(None)) => Ok(Proto::Null),
                    Reply::G(Err(e)) => Ok(Proto::Err(e)),
                })
                .and_then(move |resp| {
                    wtr.send(resp)
                        .map_err(|e| format!("failed to send reply: {}", e))
                })
                .map_err(move |e| error!(log, "{}", e))
                .map(|_| ()),
        );

        future::ok(())
    }

    pub fn shutdown(&self) {
        self.stop.store(true, Ordering::SeqCst);
        let _ = net::TcpStream::connect(self.addr);
    }
}

type ClientR = FramedRead<ReadHalf<TcpStream>, ProtoCodec>;

#[derive(Clone)]
enum Request {
    Set(String, String),
    Get(String),
    Rm(String),
}

enum ReqState {
    Unknown,
    Get,
    Rm,
    Set0,
    Set1(String),
}

struct ReqFuture {
    rdr: ClientR,
    state: ReqState,
}

impl ReqFuture {
    fn new(rdr: ReadHalf<TcpStream>) -> Self {
        let rdr = FramedRead::new(rdr, ProtoCodec::new());
        ReqFuture {
            rdr,
            state: ReqState::Unknown,
        }
    }
}

impl Stream for ReqFuture {
    type Item = Request;
    type Error = String;

    fn poll(&mut self) -> Poll<Option<Request>, String> {
        loop {
            let proto = match self.rdr.poll() {
                Ok(Async::Ready(x)) => x,
                Ok(_) => return Ok(Async::NotReady),
                Err(e) => return Err(decode_err(e)),
            };
            match self.state {
                ReqState::Unknown => {
                    let head = match proto {
                        Some(Proto::Str(h)) => h,
                        Some(x) => return Err(wrong_item(x)),
                        None => return Ok(Async::Ready(None)),
                    };
                    self.state = match head.as_str() {
                        "SET" => ReqState::Set0,
                        "GET" => ReqState::Get,
                        "RM" => ReqState::Rm,
                        x => return Err(format!("unknown command: {}", x)),
                    }
                }
                ReqState::Get => {
                    let key = get_bulk_string(proto, &["GET"])?;
                    let cmd = Request::Get(key);
                    self.state = ReqState::Unknown;
                    return Ok(Async::Ready(Some(cmd)));
                }
                ReqState::Rm => {
                    let key = get_bulk_string(proto, &["RM"])?;
                    let cmd = Request::Rm(key);
                    self.state = ReqState::Unknown;
                    return Ok(Async::Ready(Some(cmd)));
                }
                ReqState::Set0 => {
                    let key = get_bulk_string(proto, &["SET"])?;
                    self.state = ReqState::Set1(key);
                }
                ReqState::Set1(ref key) => {
                    let val = get_bulk_string(proto, &["SET", key])?;
                    let cmd = Request::Set(key.to_owned(), val);
                    self.state = ReqState::Unknown;
                    return Ok(Async::Ready(Some(cmd)));
                }
            }
        }

        fn wrong_item(item: Proto) -> String {
            format!("unexpected item: {:?}", item)
        }

        fn incomplete(cmd: &[&str]) -> String {
            format!("incomplete command: {:?}", cmd)
        }

        fn decode_err(e: impl Display) -> String {
            format!("decode error: {}", e)
        }

        fn get_bulk_string(proto: Option<Proto>, cmd: &[&str]) -> Result<String, String> {
            let s = match proto {
                Some(Proto::Bulk(v)) => v,
                Some(x) => return Err(wrong_item(x)),
                None => return Err(incomplete(cmd)),
            };
            match str::from_utf8(&s) {
                Ok(s) => Ok(s.to_string()),
                Err(e) => Err(decode_err(e)),
            }
        }
    }
}

#[derive(Clone, Debug)]
enum Reply {
    SR(Result<(), String>),
    G(Result<Option<String>, String>),
}

struct EngineFuture {
    rep: oneshot::Receiver<Reply>,
}

impl EngineFuture {
    fn new<E, T>(cmd: Request, store: E, pool: T) -> Self
    where
        E: KvsEngine,
        T: ThreadPool,
    {
        let (res, rep) = oneshot::channel();

        pool.spawn(move || {
            let rep = match cmd {
                Request::Set(key, val) => Reply::SR(store.set(key, val).map_err(|e| e.to_string())),
                Request::Get(key) => Reply::G(store.get(key).map_err(|e| e.to_string())),
                Request::Rm(key) => Reply::SR(store.remove(key).map_err(|e| e.to_string())),
            };
            res.send(rep).unwrap();
        });

        Self { rep }
    }
}

impl Future for EngineFuture {
    type Item = Reply;
    type Error = String;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        self.rep.poll().map_err(|e| format!("engine error: {}", e))
    }
}
