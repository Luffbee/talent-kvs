extern crate slog_async;
extern crate slog_term;
extern crate structopt;

use structopt::clap::arg_enum;
use structopt::StructOpt;

use std::net::{SocketAddr, TcpListener, TcpStream};
use std::io::Read;
use std::string::String;

use kvs::slog::{o, crit, error, info, Drain, Logger};
use kvs::{KvsEngine, KvStore};

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

    
    let mut _store: Box<KvsEngine> = match opt.eng {
        Engine::kvs => {
            let eng_log = log.new(o!("engine" => "KvStore"));
            match KvStore::new(DB_DIR).logger(eng_log).build() {
                Ok(st) => Box::new(st),
                Err(e) => {
                    crit!(log, "Failed to start KvStore in {}: {}.", DB_DIR, e);
                    return Err(1);
                }
            }
        }
        Engine::sled => {
            error!(log, "Unimplemented!");
            return Err(1);
        }
    };

    let listener = match TcpListener::bind(opt.addr) {
        Ok(listener) => listener,
        Err(e) => {
            crit!(log, "Failed to listen the the {}: {}.", opt.addr, e);
            return Err(1);
        }
    };
    for res in listener.incoming() {
        match res {
            Ok(stream) => {
                let peer = stream.peer_addr().unwrap();
                info!(log, "connected to client {}.", peer);
                let peer_log = log.new(o!("client" => peer.to_string()));
                handle_client(stream, peer_log);
            }
            Err(e) => {
                error!(log, "bad stream: {}", e);
                return Err(1);
            }
        }
    }
    Ok(())
}

#[allow(unused_variables)]
fn handle_client(mut stream: TcpStream, log: Logger) {
    let mut buffer = [0; 128];
    let n = stream.read(&mut buffer).unwrap();
    info!(log, "Read from client: {:?}", String::from_utf8_lossy(&buffer[..n]));
}
