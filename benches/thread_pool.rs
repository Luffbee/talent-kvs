extern crate criterion;
extern crate crossbeam;
extern crate kvs;
extern crate tempfile;
extern crate tokio;

use criterion::*;
use crossbeam::sync::WaitGroup;
use tempfile::TempDir;
use tokio::prelude::*;

use std::thread;
use std::time::Duration;

use kvs::thread_pool::{RayonThreadPool, SharedQueueThreadPool, ThreadPool};
use kvs::{KvStore, KvsClient, KvsEngine, KvsServer, SledDb};

const SZ: usize = 100;
const NUMS: &[u32] = &[1, 2, 4];

fn write<E, T>(c: &mut Criterion, tp: &str, eg: &str)
where
    E: KvsEngine,
    T: ThreadPool,
{
    let inputs = NUMS;
    c.bench(
        "write",
        ParameterizedBenchmark::new(
            format!("{}_{}", tp, eg),
            move |b, &&num| {
                let addr = "127.0.0.1:5979".parse().unwrap();

                let dir = TempDir::new().unwrap();
                let eng = E::open(dir.path()).unwrap();
                let pool = T::new(num).unwrap();
                let server = KvsServer::new(eng, pool, addr, None);
                let mut rt = server.start();

                let value = "the-value".to_owned();
                let keys: Vec<String> = (0..SZ).map(|x| format!("key{:04}", x)).collect();
                // wait for server
                thread::sleep(Duration::from_secs(1));

                b.iter(|| {
                    let wg = WaitGroup::new();
                    for k in keys.iter().take(SZ) {
                        let k = k.clone();
                        let v = value.clone();
                        let wg = wg.clone();
                        rt.spawn(KvsClient::new(addr, None).unwrap().set(k, v).then(|res| {
                            match res {
                                Ok(_) => {}
                                Err(e) => eprintln!("client error: {}", e),
                            };
                            drop(wg);
                            Ok(())
                        }));
                    }
                    wg.wait();
                });

                server.shutdown();
                rt.shutdown_now().wait().unwrap();
            },
            inputs,
        )
        .sample_size(5),
    );
}

fn read<E, T>(c: &mut Criterion, tp: &str, eg: &str)
where
    E: KvsEngine,
    T: ThreadPool,
{
    let inputs = NUMS;
    c.bench(
        "read",
        ParameterizedBenchmark::new(
            format!("{}_{}", tp, eg),
            move |b, &&num| {
                let addr = "127.0.0.1:5979".parse().unwrap();

                let dir = TempDir::new().unwrap();
                let eng = E::open(dir.path()).unwrap();
                let pool = T::new(num).unwrap();
                let server = KvsServer::new(eng, pool, addr, None);
                let mut rt = server.start();

                let value = "the-value".to_owned();
                let keys: Vec<String> = (0..SZ).map(|x| format!("key{:04}", x)).collect();

                // wait for server
                thread::sleep(Duration::from_secs(1));

                for k in keys.iter() {
                    rt.block_on(
                        KvsClient::new(addr, None)
                            .unwrap()
                            .set(k.clone(), value.clone()),
                    )
                    .unwrap()
                }

                b.iter(|| {
                    let wg = WaitGroup::new();
                    for k in keys.iter().take(SZ) {
                        let k = k.clone();
                        let wg = wg.clone();
                        let val = value.clone();
                        rt.spawn(KvsClient::new(addr, None).unwrap().get(k).then(move |res| {
                            match res {
                                Ok(Some(ref v)) if v == &val => {}
                                Ok(_) => eprintln!("wrong value"),
                                Err(e) => eprintln!("client error: {}", e),
                            }
                            drop(wg);
                            Ok(())
                        }));
                    }
                    wg.wait();
                });

                server.shutdown();
                rt.shutdown_now().wait().unwrap();
            },
            inputs,
        )
        .sample_size(5),
    );
}

fn write_queued_kvstore(c: &mut Criterion) {
    write::<KvStore, SharedQueueThreadPool>(c, "queued", "kvs");
}

fn read_queued_kvstore(c: &mut Criterion) {
    read::<KvStore, SharedQueueThreadPool>(c, "queued", "kvs");
}

fn write_rayon_kvstore(c: &mut Criterion) {
    write::<KvStore, RayonThreadPool>(c, "rayon", "kvs");
}

fn read_rayon_kvstore(c: &mut Criterion) {
    read::<KvStore, RayonThreadPool>(c, "rayon", "kvs");
}

fn write_rayon_sled(c: &mut Criterion) {
    write::<SledDb, RayonThreadPool>(c, "rayon", "sled");
}

fn read_rayon_sled(c: &mut Criterion) {
    read::<SledDb, RayonThreadPool>(c, "rayon", "sled");
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
