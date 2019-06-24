extern crate failure;
extern crate slog_async;
extern crate slog_term;
extern crate structopt;

use failure::format_err;
use structopt::clap::arg_enum;
use structopt::StructOpt;

use std::io::{prelude::*, BufRead, BufReader};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::string::String;

use kvs::slog::{crit, error, info, o, Drain, Logger};
use kvs::{Error, KvsError, KvStore, SledDb, KvsEngine};

use kvs::protocol::{Proto, ProtoError};

const DB_DIR: &str = "./";

#[derive(Debug, StructOpt)]
#[structopt(
    name = "kvs-server",
    about = "A simple key-value store server.",
    raw(setting = "structopt::clap::AppSettings::ColoredHelp"),
    raw(setting = "structopt::clap::AppSettings::VersionlessSubcommands"),
    raw(setting = "structopt::clap::AppSettings::DisableHelpSubcommand")
)]
struct Opt {
    #[structopt(
        name = "IP:PORT",
        long = "addr",
        help = "Listen to address.",
        default_value = "127.0.0.1:4000"
    )]
    addr: SocketAddr,
    #[structopt(
        name = "ENGIN-NAME",
        short = "e",
        long = "engine",
        help = "The storage engine.",
        default_value = "kvs",
        raw(possible_values = "&Engine::variants()")
    )]
    eng: Engine,
}

arg_enum! {
    #[derive(Copy, Clone, PartialEq, Eq, Debug)]
    #[allow(non_camel_case_types)]
    enum Engine {
        kvs,
        sled,
    }
}

fn main() -> Result<(), i32> {
    let opt = Opt::from_args();

    let decorator = slog_term::TermDecorator::new().build();
    let drain = slog_term::CompactFormat::new(decorator).build().fuse();
    let drain = slog_async::Async::new(drain).build().fuse();
    let log = Logger::root(
        drain,
        o!(
            "name" => "kvs-server",
            "version" => env!("CARGO_PKG_VERSION"),
            "address" => opt.addr.to_string(),
        ),
    );

    let mut store: Box<dyn KvsEngine> = match opt.eng {
        Engine::kvs => {
            let eng_log = log.new(o!("engine" => "KvStore"));
            match KvStore::new(DB_DIR).logger(eng_log).build() {
                Ok(st) => Box::new(st),
                Err(e) => {
                    crit!(log, "failed to start KvStore in {}: {}", DB_DIR, e);
                    return Err(1);
                }
            }
        }
        Engine::sled => {
            match SledDb::open(DB_DIR) {
                Ok(st) => Box::new(st),
                Err(e) => {
                    crit!(log, "failed to start KvStore in {}: {}", DB_DIR, e);
                    return Err(1);
                }
            }
        }
    };

    let listener = match TcpListener::bind(opt.addr) {
        Ok(listener) => listener,
        Err(e) => {
            crit!(log, "failed to listen the the {}: {}", opt.addr, e);
            return Err(1);
        }
    };
    for res in listener.incoming() {
        match res {
            Ok(stream) => {
                let peer = stream.peer_addr().unwrap();
                info!(log, "connected to client {}.", peer);
                let peer_log = log.new(o!("client" => peer.to_string()));
                handle_client(stream, &mut store, peer_log);
            }
            Err(e) => {
                error!(log, "bad stream: {}", e);
                return Err(1);
            }
        }
    }
    Ok(())
}

fn handle_client(mut stream: TcpStream, store: &mut Box<dyn KvsEngine>, log: Logger) {
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
    if let Err(e) = try_handle_client(&mut rdr, &mut wtr, store, log.clone()) {
        let err = ProtoError::BadRequest(e.to_string());
        error!(log, "{}", err);
        if let Err(e) = wtr.write(&Proto::Err(err.to_string()).ser()) {
            error!(log, "failed to write stream: {}", e);
        }
    }
}

fn try_handle_client(
    rdr: &mut impl BufRead,
    wtr: &mut impl Write,
    store: &mut Box<dyn KvsEngine>,
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
                match store.get(key) {
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
                if let Err(e) = store.remove(key) {
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

fn unexpected_item(item: Proto, wtr: &mut impl Write, log: Logger) -> Result<(), Error> {
    let err = format_err!("unexpected item: {:?}", item);
    error!(log, "{}", err);
    if let Err(e) = wtr.write(&Proto::Err(err.to_string()).ser()) {
        return Err(format_err!("failed to write stream: {}", e));
    }
    return Err(err);
}
