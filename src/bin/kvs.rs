extern crate structopt;

use std::process;
use structopt::StructOpt;
//use log::{error, warn, info, debug, trace};

use kvs::Error as KvError;
use kvs::KvStore;

const DB_DIR: &str = "./";

#[derive(Debug, StructOpt)]
#[structopt(
    name = "kvs-client",
    about = "A simple key-value store client.",
    raw(setting = "structopt::clap::AppSettings::ColoredHelp"),
    raw(setting = "structopt::clap::AppSettings::VersionlessSubcommands"),
    raw(setting = "structopt::clap::AppSettings::DisableHelpSubcommand"),
    )]
enum Opt {
    #[structopt(name = "set", about = "Set the value of a string key to a string")]
    Set {
        #[structopt(name = "KEY", help = "The key you want to change.")]
        key: String,
        #[structopt(name = "VALUE", help = "The value you want to set to the key.")]
        val: String },
    #[structopt(name = "get", about = "Get the string value of a given string key")]
    Get {
        #[structopt(name = "KEY", help = "The key you want to query.")]
        key: String
    },
    #[structopt(name = "rm", about = "Remove a given key")]
    Rmv {
        #[structopt(name = "KEY", help = "The key you want to remove.")]
        key: String
    },
}

fn main() {
    let mut store = match KvStore::open(DB_DIR) {
        Ok(st) => st,
        Err(e) => {
            eprintln!("Error: bad database dir {}: {}.", DB_DIR, e);
            process::exit(1)
        }
    };

    match Opt::from_args() {
        Opt::Set{key, val} => {
            if let Err(e) = store.set(key, val) {
                eprintln!("Error: {}.", e);
                process::exit(1);
            }
        }
        Opt::Get{key} => {
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
        Opt::Rmv{key} => {
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
