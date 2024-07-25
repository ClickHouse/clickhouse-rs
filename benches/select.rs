use std::{convert::Infallible, mem};

use bytes::Bytes;
use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use futures::stream::{self, StreamExt as _};
use http_body_util::StreamBody;
use hyper::{
    body::{Body, Frame, Incoming},
    Request, Response,
};
use serde::Deserialize;
use tokio::{runtime::Runtime, time::Instant};

use clickhouse::{error::Result, Client, Compression, Row};

mod common;

async fn serve(
    request: Request<Incoming>,
) -> Response<impl Body<Data = Bytes, Error = Infallible>> {
    common::skip_incoming(request).await;

    let chunk = Bytes::from_static(&[15; 128 * 1024]);
    let stream = stream::repeat(chunk).map(|chunk| Ok(Frame::data(chunk)));

    Response::new(StreamBody::new(stream))
}

fn select(c: &mut Criterion) {
    let addr = "127.0.0.1:6543".parse().unwrap();
    let _server = common::start_server(addr, serve);

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
