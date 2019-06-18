extern crate clap;
use clap::{App, Arg, SubCommand};
//use kvs::KvStore;
use std::process;

fn main() {
    let app_m = App::new(env!("CARGO_PKG_NAME"))
        .version(env!("CARGO_PKG_VERSION"))
        .author(env!("CARGO_PKG_AUTHORS"))
        .about(env!("CARGO_PKG_DESCRIPTION"))
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
    match app_m.subcommand() {
        ("set", Some(_sub_m)) => {
            //let key = sub_m.value_of("KEY").unwrap();
            //let val = sub_m.value_of("VALUE").unwrap();
            //let mut store = KvStore::new();
            //store.set(key.to_owned(), val.to_owned());
            eprintln!("unimplemented");
            process::exit(1);
        }
        ("get", Some(_sub_m)) => {
            //let key = sub_m.value_of("KEY").unwrap();
            //let store = KvStore::new();
            //println!("{}", store.get(key.to_owned()).unwrap());
            eprintln!("unimplemented");
            process::exit(1);
        }
        ("rm", Some(_sub_m)) => {
            //let key = sub_m.value_of("KEY").unwrap();
            //let mut store = KvStore::new();
            //store.remove(key.to_owned());
            eprintln!("unimplemented");
            process::exit(1);
        }
        _ => unreachable!(),
    }
}
