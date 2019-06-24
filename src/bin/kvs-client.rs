extern crate structopt;
extern crate slog;
extern crate slog_async;
extern crate slog_term;

use structopt::StructOpt;
use slog::{o, crit, error, Drain, Logger};

use std::net::{SocketAddr, TcpStream};
use std::io::{prelude::*, BufReader};

use kvs::protocol::Proto;

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

    let mut stream = match TcpStream::connect(opt.addr) {
        Ok(s) => s,
        Err(e) => {
            crit!(log, "Failed to connect to {}: {}.", opt.addr, e);
            return Err(1);
        }
    };

    match opt.op {
        Operation::Set { key, val } => {
            let req = Proto::Seq(vec![
                Proto::Str("SET".to_owned()),
                Proto::Bulk(Vec::from(key)), 
                Proto::Bulk(Vec::from(val)),
            ]);
            if let Err(e) = stream.write(&req.ser()) {
                crit!(log, "Failed to send command: {}.", e);
                return Err(1);
            }
            let mut rdr = BufReader::new(stream);
            let resp = Proto::from_bufread(&mut rdr).unwrap();
            match resp {
                Proto::Str(_) => {},
                Proto::Err(e) => {
                    error!(log, "server error: {}", e);
                    return Err(1);
                }
                item => {
                    error!(log, "unexpected item: {:?}", item);
                    return Err(1);
                }
            }
        }
        Operation::Get { key } => {
            let req = Proto::Seq(vec![
                Proto::Str("GET".to_owned()),
                Proto::Bulk(Vec::from(key)),
            ]);
            if let Err(e) = stream.write(&req.ser()) {
                crit!(log, "Failed to send command: {}.", e);
                return Err(1);
            }
            let mut rdr = BufReader::new(stream);
            let resp = Proto::from_bufread(&mut rdr).unwrap();
            match resp {
                Proto::Bulk(v) => {
                    println!("{}", String::from_utf8_lossy(&v));
                }
                Proto::Null => {
                    println!("Key not found");
                },
                Proto::Err(e) => {
                    error!(log, "server error: {}", e);
                    return Err(1);
                }
                item => {
                    error!(log, "unexpected item: {:?}", item);
                    return Err(1);
                }
            }
        }
        Operation::Rmv { key } => {
            let req = Proto::Seq(vec![
                Proto::Str("RM".to_owned()),
                Proto::Bulk(Vec::from(key)),
            ]);
            if let Err(e) = stream.write(&req.ser()) {
                crit!(log, "Failed to send command: {}.", e);
                return Err(1);
            }
            let mut rdr = BufReader::new(stream);
            let resp = Proto::from_bufread(&mut rdr).unwrap();
            match resp {
                Proto::Str(_) => {}
                Proto::Null => {
                    error!(log, "Key not found");
                    return Err(1);
                },
                Proto::Err(e) => {
                    error!(log, "server error: {}", e);
                    return Err(1);
                }
                item => {
                    error!(log, "unexpected item: {:?}", item);
                    return Err(1);
                }
            }
        }
    }
    Ok(())
}
