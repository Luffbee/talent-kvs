extern crate criterion;
extern crate crossbeam;
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

use kvs::thread_pool::{RayonThreadPool, SharedQueueThreadPool, ThreadPool};
use kvs::{KvClient, KvServer, KvStore, SledDb};

fn write_queued_kvstore(c: &mut Criterion) {
    let inputs = &[1, 2, 4, 6, 8];
    c.bench(
        "write",
        ParameterizedBenchmark::new(
            "queued_kvstore",
            move |b, &&num| {
                let sz: usize = 1000;
                let addr = SocketAddr::from_str("127.0.0.1:5979").unwrap();

                let dir = TempDir::new().unwrap();
                let eng = KvStore::open(dir.path()).unwrap();
                let pool = SharedQueueThreadPool::new(num).unwrap();
                let adr = addr.clone();
                let server = KvServer::new(eng, pool, adr, None);
                let handle = server.start().unwrap();

                let value = "the-value".to_owned();
                let keys: Vec<String> = (0..sz).map(|x| format!("key{:04}", x)).collect();
                let pool = SharedQueueThreadPool::new(50).unwrap();
                // wait for server
                thread::sleep(Duration::from_secs(1));

                b.iter(|| {
                    let wg = WaitGroup::new();
                    for i in 0..sz {
                        let adr = addr.clone();
                        let k = keys[i].clone();
                        let v = value.clone();
                        let wg = wg.clone();
                        pool.spawn(move || {
                            match KvClient::new(adr, None) {
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

fn read_queued_kvstore(c: &mut Criterion) {
    let inputs = &[1, 2, 4, 6, 8];
    c.bench(
        "read",
        ParameterizedBenchmark::new(
            "queued_kvstore",
            move |b, &&num| {
                let sz: usize = 1000;
                let addr = SocketAddr::from_str("127.0.0.1:5979").unwrap();

                let dir = TempDir::new().unwrap();
                let eng = KvStore::open(dir.path()).unwrap();
                let pool = SharedQueueThreadPool::new(num).unwrap();
                let adr = addr.clone();
                let server = KvServer::new(eng, pool, adr, None);
                let handle = server.start().unwrap();

                let value = "the-value".to_owned();
                let keys: Vec<String> = (0..sz).map(|x| format!("key{:04}", x)).collect();

                // wait for server
                thread::sleep(Duration::from_secs(1));

                for k in keys.iter() {
                    KvClient::new(addr.clone(), None)
                        .unwrap()
                        .set(k.clone(), value.clone())
                        .unwrap();
                }

                let pool = SharedQueueThreadPool::new(50).unwrap();

                b.iter(|| {
                    let wg = WaitGroup::new();
                    for i in 0..sz {
                        let adr = addr.clone();
                        let k = keys[i].clone();
                        let wg = wg.clone();
                        pool.spawn(move || {
                            match KvClient::new(adr, None) {
                                Ok(mut cli) => {
                                    if let Err(e) = cli.get(k) {
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

criterion_group!(benches, write_queued_kvstore, read_queued_kvstore);
criterion_main!(benches);
