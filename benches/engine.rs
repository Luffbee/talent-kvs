extern crate kvs;

use criterion::*;
use rand::distributions::{Alphanumeric, Uniform};
use rand::{thread_rng, Rng};
use tempfile::TempDir;

use kvs::{KvStore, KvsEngine, SledDb};

fn write100_unique(c: &mut Criterion) {
    let mut rng = thread_rng();
    let mut data = Vec::with_capacity(100);
    for _ in 0..100 {
        let len = rng.gen_range(1, 100001);
        let key: String = rng.sample_iter(&Alphanumeric).take(len).collect();
        let len = rng.gen_range(1, 100001);
        let val: String = rng.sample_iter(&Alphanumeric).take(len).collect();
        data.push((key, val));
    }
    bench_write(c, "write100_unique", data);
}

fn write100_repeat(c: &mut Criterion) {
    let keys_sz = 20;
    let mut rng = thread_rng();
    let mut keys = Vec::with_capacity(keys_sz);
    for _ in 0..keys_sz {
        let len = rng.gen_range(1, 100001);
        let key: String = rng.sample_iter(&Alphanumeric).take(len).collect();
        keys.push(key);
    }
    let mut data = Vec::with_capacity(100);
    for _ in 0..100 {
        let k = rng.gen_range(0, 20);
        let key = keys[k].clone();
        let len = rng.gen_range(1, 100001);
        let val: String = rng.sample_iter(&Alphanumeric).take(len).collect();
        data.push((key, val));
    }
    bench_write(c, "write100_repeat", data);
}

fn iter_write<E: KvsEngine>(eng: E, data: &Vec<(String, String)>) {
    for kv in data.iter() {
        eng.set(kv.0.clone(), kv.1.clone()).expect("failed to set");
    }
}

fn bench_write(c: &mut Criterion, name: &str, data: Vec<(String, String)>) {
    let data1 = data.clone();
    let data2 = data;
    c.bench(
        name,
        ParameterizedBenchmark::new(
            "sled_kvs",
            |b, (kvs, data)| {
                b.iter(|| {
                    let dir = TempDir::new().expect("failed to create temporary dir");
                    if *kvs {
                        let eng = KvStore::open(dir.path()).expect("failed to open kvs");
                        iter_write(eng, data);
                    } else {
                        let eng = SledDb::open(dir.path()).expect("failed to open sled");
                        iter_write(eng, data);
                    }
                })
            },
            vec![(false, data1), (true, data2)],
        )
        .sample_size(5),
    );
}

fn repeat_read(c: &mut Criterion) {
    bench_read(c, "repeat_read_500_1000", 250, 1000);
}

fn nonrepeat_read(c: &mut Criterion) {
    bench_read(c, "nonrepeat_read_1000_250", 1000, 250);
}

fn iter_read<E: KvsEngine>(eng: E, data: &Vec<(String, String)>, ord: &Vec<usize>) {
    for i in ord.iter() {
        let (key, val) = &data[*i];
        assert_eq!(
            Some(val.clone()),
            eng.get(key.clone()).expect("failed to get")
        );
    }
}

fn bench_read(c: &mut Criterion, name: &str, data_sz: usize, ord_sz: usize) {
    let mut rng = thread_rng();
    let mut data = Vec::with_capacity(data_sz);
    for _ in 0..data_sz {
        let len = rng.gen_range(1, 100001);
        let key: String = rng.sample_iter(&Alphanumeric).take(len).collect();
        let len = rng.gen_range(1, 100001);
        let val: String = rng.sample_iter(&Alphanumeric).take(len).collect();
        data.push((key, val));
    }
    let sled_dir = TempDir::new().expect("failed to create temporary dir");
    let sled = SledDb::open(sled_dir.path()).expect("failed to open kvs");
    for kv in data.iter() {
        sled.set(kv.0.clone(), kv.1.clone())
            .expect("sled failed to set");
    }
    drop(sled);
    let kvs_dir = TempDir::new().expect("failed to create temporary dir");
    let kvs = KvStore::open(kvs_dir.path()).expect("failed to open kvs");
    for kv in data.iter() {
        kvs.set(kv.0.clone(), kv.1.clone())
            .expect("kvs failed to set");
    }
    drop(kvs);
    let ord: Vec<usize> = rng
        .sample_iter(&Uniform::new(0, data_sz))
        .take(ord_sz)
        .collect();

    c.bench(
        name,
        ParameterizedBenchmark::new(
            "sled_kvs",
            |b, (kvs, dir, ord, data)| {
                b.iter(|| {
                    if *kvs {
                        let eng = KvStore::open(dir).expect("failed to reopen kvs");
                        iter_read(eng, data, ord);
                    } else {
                        let eng = SledDb::open(dir).expect("failed to reopen sled");
                        iter_read(eng, data, ord);
                    }
                })
            },
            vec![
                (false, sled_dir.path().to_owned(), ord.clone(), data.clone()),
                (true, kvs_dir.path().to_owned(), ord.clone(), data.clone()),
            ],
        )
        .sample_size(5),
    );
}

criterion_group!(
    benches,
    write100_repeat,
    write100_unique,
    repeat_read,
    nonrepeat_read
);
criterion_main!(benches);
