use std::mem;

use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use serde::Deserialize;
use tokio::{runtime::Runtime, time::Instant};

use clickhouse::{error::Result, Client, Compression, Row};

mod server {
    use std::{convert::Infallible, net::SocketAddr, thread};

    use bytes::Bytes;
    use futures::stream;
    use hyper::service::{make_service_fn, service_fn};
    use hyper::{body, Body, Request, Response, Server};
    use tokio::runtime;

    async fn handle(req: Request<Body>) -> Result<Response<Body>, Infallible> {
        let _ = body::aggregate(req.into_body()).await;
        let chunk = Bytes::from_static(&[15; 128 * 1024]);
        let stream = stream::repeat(Ok::<Bytes, &'static str>(chunk));
        let body = Body::wrap_stream(stream);

        Ok(Response::new(body))
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

fn select(c: &mut Criterion) {
    let addr = "127.0.0.1:6543".parse().unwrap();
    server::start(addr);

    #[allow(dead_code)]
    #[derive(Debug, Row, Deserialize)]
    struct SomeRow {
        a: u64,
        b: i64,
        c: i32,
        d: u32,
    }

    async fn run(client: Client, iters: u64) -> Result<()> {
        let mut cursor = client
            .query("SELECT ?fields FROM some")
            .fetch::<SomeRow>()?;

        for _ in 0..iters {
            black_box(cursor.next().await?);
        }

        Ok(())
    }

    let mut group = c.benchmark_group("select");
    group.throughput(Throughput::Bytes(mem::size_of::<SomeRow>() as u64));
    group.bench_function("select", |b| {
        b.iter_custom(|iters| {
            let rt = Runtime::new().unwrap();
            let client = Client::default()
                .with_url(format!("http://{addr}"))
                .with_compression(Compression::None);
            let start = Instant::now();
            rt.block_on(run(client, iters)).unwrap();
            start.elapsed()
        })
    });
    group.finish();
}

criterion_group!(benches, select);
criterion_main!(benches);
