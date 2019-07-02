extern crate slog;
extern crate slog_async;
extern crate slog_term;
extern crate structopt;
extern crate kvs;

use slog::{o, Drain, Logger};
use structopt::StructOpt;

use std::net::SocketAddr;

use kvs::KvsClient;

#[derive(StructOpt)]
#[structopt(
    name = "kvs-client",
    about = "A simple key-value store client.",
    raw(setting = "structopt::clap::AppSettings::ColoredHelp"),
    raw(setting = "structopt::clap::AppSettings::VersionlessSubcommands"),
    raw(setting = "structopt::clap::AppSettings::DisableHelpSubcommand")
)]
struct Opt {
    #[structopt(
        name = "IP:PORT",
        long = "addr",
        help = "Server address.",
        default_value = "127.0.0.1:4000",
        global = true
    )]
    addr: SocketAddr,
    #[structopt(subcommand)]
    op: Operation,
}

#[derive(StructOpt)]
enum Operation {
    #[structopt(name = "set", about = "Set the value of a string key to a string")]
    Set {
        #[structopt(name = "KEY", help = "The key you want to change.")]
        key: String,
        #[structopt(name = "VALUE", help = "The value you want to set to the key.")]
        val: String,
    },
    #[structopt(name = "get", about = "Get the string value of a given string key")]
    Get {
        #[structopt(name = "KEY", help = "The key you want to query.")]
        key: String,
    },
    #[structopt(name = "rm", about = "Remove a given key")]
    Rmv {
        #[structopt(name = "KEY", help = "The key you want to remove.")]
        key: String,
    },
}

fn main() -> Result<(), i32> {
    let opt = Opt::from_args();

    let decorator = slog_term::TermDecorator::new().build();
    let drain = slog_term::FullFormat::new(decorator).build().fuse();
    let drain = slog_async::Async::new(drain).build().fuse();
    let log = Logger::root(drain, o!());

    let mut client = KvsClient::new(opt.addr, Some(log))?;

    match opt.op {
        Operation::Set { key, val } => {
            client.set(key, val)?;
        }
        Operation::Get { key } => {
            match client.get(key)? {
                Some(s) => println!("{}", s),
                None => println!("Key not found"), 
            }
        }
        Operation::Rmv { key } => {
            client.rm(key)?;
        }
    }
    Ok(())
}
