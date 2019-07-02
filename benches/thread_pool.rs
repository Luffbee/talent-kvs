extern crate criterion;
extern crate crossbeam;
extern crate tempfile;
extern crate kvs;

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

use kvs::thread_pool::{SharedQueueThreadPool, RayonThreadPool, ThreadPool};
use kvs::{KvsClient, KvsServer, KvStore, SledDb};

const SZ: usize = 1000;
const NUMS: [u32; 5] = [1, 2, 4, 6, 8];

fn write_rayon_sled(c: &mut Criterion) {
    let inputs = &NUMS;
    c.bench(
        "write",
        ParameterizedBenchmark::new(
            "rayon_sled",
            move |b, &&num| {
                let addr = SocketAddr::from_str("127.0.0.1:5979").unwrap();

                let dir = TempDir::new().unwrap();
                let eng = SledDb::open(dir.path()).unwrap();
                let pool = RayonThreadPool::new(num).unwrap();
                let adr = addr.clone();
                let server = KvsServer::new(eng, pool, adr, None);
                let handle = server.start().unwrap();

                let value = "the-value".to_owned();
                let keys: Vec<String> = (0..SZ).map(|x| format!("key{:04}", x)).collect();
                let pool = RayonThreadPool::new(SZ as u32).unwrap();
                // wait for server
                thread::sleep(Duration::from_secs(1));

                b.iter(|| {
                    let wg = WaitGroup::new();
                    for i in 0..SZ {
                        let adr = addr.clone();
                        let k = keys[i].clone();
                        let v = value.clone();
                        let wg = wg.clone();
                        pool.spawn(move || {
                            match KvsClient::new(adr, None) {
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

fn read_rayon_sled(c: &mut Criterion) {
    let inputs = &NUMS;
    c.bench(
        "read",
        ParameterizedBenchmark::new(
            "rayon_sled",
            move |b, &&num| {
                let addr = SocketAddr::from_str("127.0.0.1:5979").unwrap();

                let dir = TempDir::new().unwrap();
                let eng = SledDb::open(dir.path()).unwrap();
                let pool = RayonThreadPool::new(num).unwrap();
                let adr = addr.clone();
                let server = KvsServer::new(eng, pool, adr, None);
                let handle = server.start().unwrap();

                let value = "the-value".to_owned();
                let keys: Vec<String> = (0..SZ).map(|x| format!("key{:04}", x)).collect();

                // wait for server
                thread::sleep(Duration::from_secs(1));

                for k in keys.iter() {
                    KvsClient::new(addr.clone(), None)
                        .unwrap()
                        .set(k.clone(), value.clone())
                        .unwrap();
                }

                let pool = RayonThreadPool::new(SZ as u32).unwrap();

                b.iter(|| {
                    let wg = WaitGroup::new();
                    for i in 0..SZ {
                        let adr = addr.clone();
                        let k = keys[i].clone();
                        let wg = wg.clone();
                        pool.spawn(move || {
                            match KvsClient::new(adr, None) {
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

fn write_rayon_kvstore(c: &mut Criterion) {
    let inputs = &NUMS;
    c.bench(
        "write",
        ParameterizedBenchmark::new(
            "rayon_kvstore",
            move |b, &&num| {
                let addr = SocketAddr::from_str("127.0.0.1:5979").unwrap();

                let dir = TempDir::new().unwrap();
                let eng = KvStore::open(dir.path()).unwrap();
                let pool = RayonThreadPool::new(num).unwrap();
                let adr = addr.clone();
                let server = KvsServer::new(eng, pool, adr, None);
                let handle = server.start().unwrap();

                let value = "the-value".to_owned();
                let keys: Vec<String> = (0..SZ).map(|x| format!("key{:04}", x)).collect();
                let pool = RayonThreadPool::new(SZ as u32).unwrap();
                // wait for server
                thread::sleep(Duration::from_secs(1));

                b.iter(|| {
                    let wg = WaitGroup::new();
                    for i in 0..SZ {
                        let adr = addr.clone();
                        let k = keys[i].clone();
                        let v = value.clone();
                        let wg = wg.clone();
                        pool.spawn(move || {
                            match KvsClient::new(adr, None) {
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

fn read_rayon_kvstore(c: &mut Criterion) {
    let inputs = &NUMS;
    c.bench(
        "read",
        ParameterizedBenchmark::new(
            "rayon_kvstore",
            move |b, &&num| {
                let addr = SocketAddr::from_str("127.0.0.1:5979").unwrap();

                let dir = TempDir::new().unwrap();
                let eng = KvStore::open(dir.path()).unwrap();
                let pool = RayonThreadPool::new(num).unwrap();
                let adr = addr.clone();
                let server = KvsServer::new(eng, pool, adr, None);
                let handle = server.start().unwrap();

                let value = "the-value".to_owned();
                let keys: Vec<String> = (0..SZ).map(|x| format!("key{:04}", x)).collect();

                // wait for server
                thread::sleep(Duration::from_secs(1));

                for k in keys.iter() {
                    KvsClient::new(addr.clone(), None)
                        .unwrap()
                        .set(k.clone(), value.clone())
                        .unwrap();
                }

                let pool = RayonThreadPool::new(SZ as u32).unwrap();

                b.iter(|| {
                    let wg = WaitGroup::new();
                    for i in 0..SZ {
                        let adr = addr.clone();
                        let k = keys[i].clone();
                        let wg = wg.clone();
                        pool.spawn(move || {
                            match KvsClient::new(adr, None) {
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

fn write_queued_kvstore(c: &mut Criterion) {
    let inputs = &NUMS;
    c.bench(
        "write",
        ParameterizedBenchmark::new(
            "queued_kvstore",
            move |b, &&num| {
                let addr = SocketAddr::from_str("127.0.0.1:5979").unwrap();

                let dir = TempDir::new().unwrap();
                let eng = KvStore::open(dir.path()).unwrap();
                let pool = SharedQueueThreadPool::new(num).unwrap();
                let adr = addr.clone();
                let server = KvsServer::new(eng, pool, adr, None);
                let handle = server.start().unwrap();

                let value = "the-value".to_owned();
                let keys: Vec<String> = (0..SZ).map(|x| format!("key{:04}", x)).collect();
                let pool = SharedQueueThreadPool::new(SZ as u32).unwrap();
                // wait for server
                thread::sleep(Duration::from_secs(1));

                b.iter(|| {
                    let wg = WaitGroup::new();
                    for i in 0..SZ {
                        let adr = addr.clone();
                        let k = keys[i].clone();
                        let v = value.clone();
                        let wg = wg.clone();
                        pool.spawn(move || {
                            match KvsClient::new(adr, None) {
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
    let inputs = &NUMS;
    c.bench(
        "read",
        ParameterizedBenchmark::new(
            "queued_kvstore",
            move |b, &&num| {
                let addr = SocketAddr::from_str("127.0.0.1:5979").unwrap();

                let dir = TempDir::new().unwrap();
                let eng = KvStore::open(dir.path()).unwrap();
                let pool = SharedQueueThreadPool::new(num).unwrap();
                let adr = addr.clone();
                let server = KvsServer::new(eng, pool, adr, None);
                let handle = server.start().unwrap();

                let value = "the-value".to_owned();
                let keys: Vec<String> = (0..SZ).map(|x| format!("key{:04}", x)).collect();

                // wait for server
                thread::sleep(Duration::from_secs(1));

                for k in keys.iter() {
                    KvsClient::new(addr.clone(), None)
                        .unwrap()
                        .set(k.clone(), value.clone())
                        .unwrap();
                }

                let pool = SharedQueueThreadPool::new(SZ as u32).unwrap();

                b.iter(|| {
                    let wg = WaitGroup::new();
                    for i in 0..SZ {
                        let adr = addr.clone();
                        let k = keys[i].clone();
                        let wg = wg.clone();
                        pool.spawn(move || {
                            match KvsClient::new(adr, None) {
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

criterion_group!(
    benches,
    write_queued_kvstore,
    read_queued_kvstore,
    write_rayon_kvstore,
    read_rayon_kvstore,
    write_rayon_sled,
    read_rayon_sled,
);
criterion_main!(benches);
