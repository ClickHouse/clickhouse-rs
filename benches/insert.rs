use std::{future::Future, mem, time::Duration};

use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use serde::Serialize;
use tokio::{runtime::Runtime, time::Instant};

use clickhouse::{error::Result, Client, Compression, Row};

mod server {
    use std::{convert::Infallible, net::SocketAddr, thread};

    use futures::stream::StreamExt;
    use hyper::service::{make_service_fn, service_fn};
    use hyper::{Body, Request, Response, Server};
    use tokio::runtime;

    async fn handle(req: Request<Body>) -> Result<Response<Body>, Infallible> {
        let mut body = req.into_body();

        while let Some(res) = body.next().await {
            res.unwrap();
        }

        Ok(Response::new(Body::empty()))
    }

    pub fn start(addr: SocketAddr) {
        thread::spawn(move || {
            runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(async {
                    let make_svc =
                        make_service_fn(|_| async { Ok::<_, Infallible>(service_fn(handle)) });
                    Server::bind(&addr).serve(make_svc).await.unwrap();
                });
        });
    }
}

#[derive(Row, Serialize)]
struct SomeRow {
    a: u64,
    b: i64,
    c: i32,
    d: u32,
}

async fn run_insert(client: Client, iters: u64) -> Result<Duration> {
    let start = Instant::now();
    let mut insert = client.insert("table")?;

    for _ in 0..iters {
        insert
            .write(&black_box(SomeRow {
                a: 42,
                b: 42,
                c: 42,
                d: 42,
            }))
            .await?;
    }

    insert.end().await?;
    Ok(start.elapsed())
}

async fn run_inserter(client: Client, iters: u64) -> Result<Duration> {
    let start = Instant::now();
    let mut inserter = client.inserter("table")?.with_max_rows(iters);

    for _ in 0..iters {
        inserter.write(&black_box(SomeRow {
            a: 42,
            b: 42,
            c: 42,
            d: 42,
        }))?;
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
    server::start(addr);

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

fn inserter(c: &mut Criterion) {
    run(c, "inserter", 6544, run_inserter);
}

criterion_group!(benches, insert, inserter);
criterion_main!(benches);
