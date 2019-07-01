use kvs::KvClient;

use std::net::SocketAddr;
use std::str::FromStr;
use std::thread;

fn main() {
    let addr = SocketAddr::from_str("127.0.0.1:4000").unwrap();
    let mut v = Vec::new();
    for i in 0..1000 {
        let adr = addr.clone();
        let handle = thread::spawn(move || {
            match KvClient::new(adr, None) {
                Ok(mut c) => {
                    c.set("key1".to_owned(), "value1".to_owned()).unwrap();
                },
                Err(e) => {
                    println!("{}: {}", i, e);
                }
            };
        });
        v.push(Some(handle));
    }
    for mut i in v {
        i.take().unwrap().join().unwrap();
    }
    println!("pass");
}

