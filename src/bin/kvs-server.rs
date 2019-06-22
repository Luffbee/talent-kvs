extern crate structopt;

use structopt::StructOpt;
use structopt::clap::arg_enum;

use std::net::SocketAddr;
//use std::process;

//use kvs::Error as KvError;
//use kvs::KvStore;

//const DB_DIR: &str = "./";

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
        default_value = "127.0.0.1:4000",
    )]
    addr: SocketAddr,
    #[structopt(
        name = "ENGIN-NAME",
        short = "e",
        long = "engine",
        help = "The storage engine.",
        default_value = "kvs",
        raw(possible_values = "&Engine::variants()"),
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


fn main() {
    let opt = Opt::from_args();
    eprintln!("{:?}", opt);
}
