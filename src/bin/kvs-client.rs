extern crate structopt;

use structopt::StructOpt;

use std::net::SocketAddr;
use std::process;

use kvs::Error as KvError;
use kvs::KvStore;

const DB_DIR: &str = "./";

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
            global = true,
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

fn main() {
    let opt = Opt::from_args();

    let mut store = match KvStore::open(DB_DIR) {
        Ok(st) => st,
        Err(e) => {
            eprintln!("Error: bad database dir {}: {}.", DB_DIR, e);
            process::exit(1)
        }
    };

    let addr = opt.addr;
    match opt.op {
        Operation::Set { key, val } => {
            eprintln!("{:?}", addr);
            if let Err(e) = store.set(key, val) {
                eprintln!("Error: {}.", e);
                process::exit(1);
            }
        }
        Operation::Get { key } => {
            eprintln!("{:?}", addr);
            match store.get(key) {
                Ok(Some(s)) => {
                    println!("{}", s);
                }
                Ok(None) => {
                    println!("Key not found");
                }
                Err(e) => {
                    eprintln!("Error: {}.", e);
                    process::exit(1);
                }
            }
        }
        Operation::Rmv { key } => {
            eprintln!("{:?}", addr);
            if let Err(e) = store.remove(key) {
                if let Some(KvError::KeyNotFound(_)) = e.downcast_ref() {
                    println!("Key not found");
                } else {
                    eprintln!("{}", e);
                }
                process::exit(1);
            }
        }
    }
}
