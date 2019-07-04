extern crate failure;
extern crate kvs;
extern crate slog_async;
extern crate slog_term;
extern crate structopt;

use structopt::clap::arg_enum;
use structopt::StructOpt;

use std::net::SocketAddr;
use std::string::String;

use kvs::slog::{crit, o, Drain, Logger};
use kvs::thread_pool::*;
use kvs::{KvStore, KvsServer, SledDb};

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
    let drain = slog_async::Async::new(drain)
        .chan_size(10240)
        .build()
        .fuse();
    let log = Logger::root(
        drain,
        o!(
            "name" => "kvs-server",
            "version" => env!("CARGO_PKG_VERSION"),
            "address" => opt.addr.to_string(),
        ),
    );
    let pool = match SharedQueueThreadPool::new(0) {
        Ok(pool) => pool,
        Err(e) => {
            crit!(log, "failed to create thread pool: {}", e);
            return Err(1);
        }
    };

    match opt.eng {
        Engine::kvs => {
            let eng_log = log.new(o!("engine" => "kvs"));
            match KvStore::with_logger(DB_DIR, eng_log) {
                Ok(st) => KvsServer::new(st, pool, opt.addr, log.clone()).run()?,
                Err(e) => {
                    crit!(log, "failed to start KvStore in {}: {}", DB_DIR, e);
                    return Err(1);
                }
            }
        }
        Engine::sled => match SledDb::open(DB_DIR) {
            Ok(st) => KvsServer::new(st, pool, opt.addr, log.clone()).run()?,
            Err(e) => {
                crit!(log, "failed to start SledDB in {}: {}", DB_DIR, e);
                return Err(1);
            }
        },
    };

    Ok(())
}
