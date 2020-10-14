use std::mem;

use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use serde::Serialize;
use tokio::{runtime::Runtime, time::Instant};

use typed_clickhouse::{error::Result, Client, Reflection};

mod server {
    use std::{convert::Infallible, net::SocketAddr, thread};

    use hyper::service::{make_service_fn, service_fn};
    use hyper::{Body, Request, Response, Server};
    use tokio::{runtime, stream::StreamExt};

    async fn handle(req: Request<Body>) -> Result<Response<Body>, Infallible> {
        let mut body = req.into_body();

        while let Some(res) = body.next().await {
            res.unwrap();
        }

        Ok(Response::new(Body::empty()))
    }

    pub fn start(addr: SocketAddr) {
        thread::spawn(move || {
            runtime::Builder::new()
                .basic_scheduler()
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

fn insert(c: &mut Criterion) {
    let addr = "127.0.0.1:6543".parse().unwrap();
    server::start(addr);

    #[derive(Reflection, Serialize)]
    struct Row {
        a: u64,
        b: i64,
        c: i32,
        d: u32,
    }

    async fn run(client: Client, iters: u64) -> Result<()> {
        let mut insert = client.insert("table")?;

        for _ in 0..iters {
            insert
                .write(&black_box(Row {
                    a: 42,
                    b: 42,
                    c: 42,
                    d: 42,
                }))
                .await?;
        }

        insert.end().await
    }

    let mut group = c.benchmark_group("insert");
    group.throughput(Throughput::Bytes(mem::size_of::<Row>() as u64));
    group.bench_function("insert", |b| {
        b.iter_custom(|iters| {
            let mut rt = Runtime::new().unwrap();
            let client = Client::default().with_url(format!("http://{}", addr));
            let start = Instant::now();
            rt.block_on(run(client, iters)).unwrap();
            start.elapsed()
        })
    });
    group.finish();
}

criterion_group!(benches, insert);
criterion_main!(benches);
