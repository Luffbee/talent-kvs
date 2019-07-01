extern crate criterion;
extern crate crossbeam;
extern crate slog_async;
extern crate slog_term;
extern crate tempfile;

use criterion::*;
use crossbeam::sync::WaitGroup;
use tempfile::TempDir;
//use rand::distributions::{Alphanumeric, Uniform};
//use rand::{thread_rng, Rng};

use std::net::SocketAddr;
use std::str::FromStr;
use std::thread;
use std::time::Duration;
//use std::sync::Arc;

use kvs::slog::{o, Drain, Logger};
use kvs::thread_pool::{SharedQueueThreadPool, ThreadPool, NaiveThreadPool};
use kvs::{KvClient, KvServer, KvStore};

fn write_queue_kvstore(c: &mut Criterion) {
    let inputs = &[4]; //, 2, 4, 6, 8];
    c.bench(
        "write_queue",
        ParameterizedBenchmark::new(
            "kvstore",
            move |b, &&num| {
                let decorator = slog_term::TermDecorator::new().build();
                let drain = slog_term::CompactFormat::new(decorator).build().fuse();
                let drain = slog_async::Async::new(drain).chan_size(1024 * 5).build().fuse();
                let log = Logger::root(
                    drain,
                    o!(
                        "name" => "kvs-server",
                        "version" => env!("CARGO_PKG_VERSION"),
                    ),
                );
                let sz: usize = 150;
                let addr = SocketAddr::from_str("127.0.0.1:4979").unwrap();

                let dir = TempDir::new().unwrap();
                let eng = KvStore::open(dir.path()).unwrap();
                let pool = SharedQueueThreadPool::new(num).unwrap();
                let adr = addr.clone();
                let server = KvServer::new(eng, pool, adr, log.clone());
                let handle = server.start().unwrap();

                let value = "the-value".to_owned();
                let keys: Vec<String> = (0..sz).map(|x| format!("key{:04}", x)).collect();
                let pool = NaiveThreadPool::new(sz as u32).unwrap();
                // wait for server
                thread::sleep(Duration::from_secs(1));

                b.iter(|| {
                    let wg = WaitGroup::new();
                    for i in 0..sz {
                        let adr = addr.clone();
                        let k = keys[i].clone();
                        let v = value.clone();
                        let wg = wg.clone();
                        let log = log.clone();
                        pool.spawn(move || {
                            match KvClient::new(adr, Some(log)) {
                                Ok(mut cli) => {
                                    if let Err(e) = cli.set(k, v) {
                                        eprintln!("11111111111EEEEEEEEEEEEEEE {}", e);
                                    }
                                }
                                Err(e) => {
                                    eprintln!("11111111111111111CCCCCCCCCCCCCCCCC {}", e);
                                }
                            }
                            drop(wg);
                        });
                    }
                    wg.wait();
                });

                server.shutdown();
                if let Err(e) = handle.join() {
                    eprintln!("*******************listener panicked: {:?}", e);
                }
            },
            inputs,
        )
        .sample_size(5),
    );
}

criterion_group!(benches, write_queue_kvstore,);
criterion_main!(benches);
