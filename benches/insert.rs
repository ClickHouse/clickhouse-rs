use std::{future::Future, mem, time::Duration};

use bytes::Bytes;
use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use http_body_util::Empty;
use hyper::{body::Incoming, Request, Response};
use serde::Serialize;
use tokio::{runtime::Runtime, time::Instant};

use clickhouse::{error::Result, Client, Compression, Row};

mod common;

async fn serve(request: Request<Incoming>) -> Response<Empty<Bytes>> {
    common::skip_incoming(request).await;
    Response::new(Empty::new())
}

#[derive(Row, Serialize)]
struct SomeRow {
    a: u64,
    b: i64,
    c: i32,
    d: u32,
    e: u64,
    f: u32,
    g: u64,
    h: i64,
}

impl SomeRow {
    fn sample() -> Self {
        black_box(Self {
            a: 42,
            b: 42,
            c: 42,
            d: 42,
            e: 42,
            f: 42,
            g: 42,
            h: 42,
        })
    }
}

async fn run_insert(client: Client, iters: u64) -> Result<Duration> {
    let start = Instant::now();
    let mut insert = client.insert("table")?;

    for _ in 0..iters {
        insert.write(&SomeRow::sample()).await?;
    }

    insert.end().await?;
    Ok(start.elapsed())
}

#[cfg(feature = "inserter")]
async fn run_inserter<const WITH_PERIOD: bool>(client: Client, iters: u64) -> Result<Duration> {
    let start = Instant::now();
    let mut inserter = client.inserter("table")?.with_max_rows(iters);

    if WITH_PERIOD {
        // Just to measure overhead, not to actually use it.
        inserter = inserter.with_period(Some(Duration::from_secs(1000)));
    }

    for _ in 0..iters {
        inserter.write(&SomeRow::sample())?;
        inserter.commit().await?;
    }

    inserter.end().await?;
    Ok(start.elapsed())
}

fn run<F>(c: &mut Criterion, name: &str, port: u16, f: impl Fn(Client, u64) -> F)
where
    F: Future<Output = Result<Duration>>,
{
    let addr = format!("127.0.0.1:{port}").parse().unwrap();
    let _server = common::start_server(addr, serve);

    let mut group = c.benchmark_group(name);
    group.throughput(Throughput::Bytes(mem::size_of::<SomeRow>() as u64));
    group.bench_function("no compression", |b| {
        b.iter_custom(|iters| {
            let rt = Runtime::new().unwrap();
            let client = Client::default()
                .with_url(format!("http://{addr}"))
                .with_compression(Compression::None);
            rt.block_on((f)(client, iters)).unwrap()
        })
    });
    #[cfg(feature = "lz4")]
    group.bench_function("lz4", |b| {
        b.iter_custom(|iters| {
            let rt = Runtime::new().unwrap();
            let client = Client::default()
                .with_url(format!("http://{addr}"))
                .with_compression(Compression::Lz4);
            rt.block_on((f)(client, iters)).unwrap()
        })
    });
    #[cfg(feature = "lz4")]
    group.bench_function("lz4hc(4)", |b| {
        b.iter_custom(|iters| {
            let rt = Runtime::new().unwrap();
            let client = Client::default()
                .with_url(format!("http://{addr}"))
                .with_compression(Compression::Lz4Hc(4));
            rt.block_on((f)(client, iters)).unwrap()
        })
    });
    group.finish();
}

fn insert(c: &mut Criterion) {
    run(c, "insert", 6543, run_insert);
}

#[cfg(feature = "inserter")]
fn inserter(c: &mut Criterion) {
    run(c, "inserter", 6544, run_inserter::<false>);
    run(c, "inserter-period", 6545, run_inserter::<true>);
}

#[cfg(not(feature = "inserter"))]
criterion_group!(benches, insert);
#[cfg(feature = "inserter")]
criterion_group!(benches, insert, inserter);
criterion_main!(benches);
