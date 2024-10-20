use std::{
    convert::Infallible,
    mem,
    time::{Duration, Instant},
};

use bytes::Bytes;
use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use futures::stream::{self, StreamExt as _};
use http_body_util::StreamBody;
use hyper::{
    body::{Body, Frame, Incoming},
    Request, Response,
};
use serde::Deserialize;

use clickhouse::{error::Result, Client, Compression, Row};

mod common;

async fn serve(
    request: Request<Incoming>,
    chunk: Bytes,
) -> Response<impl Body<Data = Bytes, Error = Infallible>> {
    common::skip_incoming(request).await;

    let stream = stream::repeat(chunk).map(|chunk| Ok(Frame::data(chunk)));
    Response::new(StreamBody::new(stream))
}

fn prepare_chunk() -> Bytes {
    use rand::{distributions::Standard, rngs::SmallRng, Rng, SeedableRng};

    // Generate random data to avoid _real_ compression.
    // TODO: It would be more useful to generate real data.
    let mut rng = SmallRng::seed_from_u64(0xBA5E_FEED);
    let raw: Vec<_> = (&mut rng).sample_iter(Standard).take(128 * 1024).collect();

    // If the feature is enabled, compress the data even if we use the `None`
    // compression. The compression ratio is low anyway due to random data.
    #[cfg(feature = "lz4")]
    let chunk = clickhouse::_priv::lz4_compress(&raw).unwrap();
    #[cfg(not(feature = "lz4"))]
    let chunk = Bytes::from(raw);

    chunk
}

fn select(c: &mut Criterion) {
    let addr = "127.0.0.1:6543".parse().unwrap();
    let chunk = prepare_chunk();
    let _server = common::start_server(addr, move |req| serve(req, chunk.clone()));
    let runner = common::start_runner();

    #[allow(dead_code)]
    #[derive(Debug, Row, Deserialize)]
    struct SomeRow {
        a: u64,
        b: i64,
        c: i32,
        d: u32,
    }

    async fn run(client: Client, iters: u64) -> Result<Duration> {
        let start = Instant::now();
        let mut cursor = client
            .query("SELECT ?fields FROM some")
            .fetch::<SomeRow>()?;

        for _ in 0..iters {
            black_box(cursor.next().await?);
        }

        Ok(start.elapsed())
    }

    let mut group = c.benchmark_group("select");
    group.throughput(Throughput::Bytes(mem::size_of::<SomeRow>() as u64));
    group.bench_function("no compression", |b| {
        b.iter_custom(|iters| {
            let client = Client::default()
                .with_url(format!("http://{addr}"))
                .with_compression(Compression::None);
            runner.run(run(client, iters))
        })
    });
    #[cfg(feature = "lz4")]
    group.bench_function("lz4", |b| {
        b.iter_custom(|iters| {
            let client = Client::default()
                .with_url(format!("http://{addr}"))
                .with_compression(Compression::Lz4);
            runner.run(run(client, iters))
        })
    });
    group.finish();
}

criterion_group!(benches, select);
criterion_main!(benches);
