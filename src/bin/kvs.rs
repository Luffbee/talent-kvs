extern crate stderrlog;
//extern crate log;

use clap::{App, AppSettings, Arg, SubCommand};
use std::process;
//use log::{error, warn, info, debug, trace};

use kvs::Error as KvError;
use kvs::KvStore;

const DB_DIR: &str = "./";

fn main() {
    stderrlog::new()
        .module(module_path!())
        .verbosity(5)
        .timestamp(stderrlog::Timestamp::Microsecond)
        .init()
        .unwrap();

    let app_m = App::new(env!("CARGO_PKG_NAME"))
        .version(env!("CARGO_PKG_VERSION"))
        .author(env!("CARGO_PKG_AUTHORS"))
        .about(env!("CARGO_PKG_DESCRIPTION"))
        .setting(AppSettings::SubcommandRequiredElseHelp)
        .setting(AppSettings::VersionlessSubcommands)
        .subcommand(
            SubCommand::with_name("set")
                .about("Set the value of a string key to a string")
                .arg(
                    Arg::with_name("KEY")
                        .help("The key you want to change.")
                        .required(true)
                        .index(1),
                )
                .arg(
                    Arg::with_name("VALUE")
                        .help("The value you want to set to the key.")
                        .required(true)
                        .index(2),
                ),
        )
        .subcommand(
            SubCommand::with_name("get")
                .about("Get the string value of a given string key")
                .arg(
                    Arg::with_name("KEY")
                        .help("The key you want to query.")
                        .required(true)
                        .index(1),
                ),
        )
        .subcommand(
            SubCommand::with_name("rm").about("Remove a given key").arg(
                Arg::with_name("KEY")
                    .help("The key you want to remove.")
                    .required(true)
                    .index(1),
            ),
        )
        .get_matches();

    let mut store = match KvStore::open(DB_DIR) {
        Ok(st) => st,
        Err(e) => {
            eprintln!("Error: bad database dir {}: {}.", DB_DIR, e);
            process::exit(1)
        }
    };

    match app_m.subcommand() {
        ("set", Some(sub_m)) => {
            let key = sub_m.value_of("KEY").unwrap();
            let val = sub_m.value_of("VALUE").unwrap();
            if let Err(e) = store.set(key.to_owned(), val.to_owned()) {
                eprintln!("Error: {}.", e);
                process::exit(1);
            }
        }
        ("get", Some(sub_m)) => {
            let key = sub_m.value_of("KEY").unwrap();
            match store.get(key.to_owned()) {
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
        ("rm", Some(sub_m)) => {
            let key = sub_m.value_of("KEY").unwrap();
            if let Err(e) = store.remove(key.to_owned()) {
                if let Some(KvError::KeyNotFound(_)) = e.downcast_ref() {
                    println!("Key not found");
                } else {
                    eprintln!("{}", e);
                }
                process::exit(1);
            }
        }
        _ => unreachable!(),
    }
}
