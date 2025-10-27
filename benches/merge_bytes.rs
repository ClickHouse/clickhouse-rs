use bytes::Bytes;
use clickhouse::{Client, Compression, error::Result};
use criterion::{Criterion, criterion_group, criterion_main};
use futures_util::FutureExt;
use futures_util::stream::{self, StreamExt as _};
use http_body_util::StreamBody;
use hyper::{
    Request, Response,
    body::{Body, Frame, Incoming},
};
use std::convert::Infallible;
use std::fs;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::time::{Duration, Instant};
mod common;

async fn serve(
    request: Request<Incoming>,
) -> Response<impl Body<Data = Bytes, Error = Infallible>> {
    common::skip_incoming(request).await;

    let mut data = Vec::new();
    let pp = format!("data.bin").to_string();
    let p = std::path::Path::new(pp.as_str());
    let bytes = Bytes::from(fs::read(p).unwrap());
    data.push(bytes);

    let stream = stream::iter(data).map(|chunk| Ok(Frame::data(chunk)));
    Response::new(StreamBody::new(stream))
}

const ADDR: SocketAddr = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 6523));

fn select(c: &mut Criterion) {
    async fn start_server() -> common::ServerHandle {
        common::start_server(ADDR, move |req| {
            Box::pin(serve(req).map(|resp| resp.map(|body| body)))
        })
        .await
    }

    let runner = common::start_runner();

    async fn select_bytes(client: Client, compression: Compression) -> Result<Duration> {
        let client = client.with_compression(compression);
        let _server = start_server().await;

        let start = Instant::now();
        let mut cursor = client
            .query("SELECT value FROM some")
            .fetch_bytes("RowBinary")?;

        let mut size = 0;
        loop {
            let buf = std::hint::black_box(cursor.next().await?);
            match buf {
                Some(buf) => size += buf.len() as u64,
                None => break,
            }
        }
        std::hint::black_box(size);

        let elapsed = start.elapsed();
        Ok(elapsed)
    }

    let mut group = c.benchmark_group("merge_bytes");
    group.bench_function("select_bytes", |b| {
        b.iter(|| {
            let client = Client::default().with_url(format!("http://{ADDR}"));
            let d = runner.run(select_bytes(client, Compression::Lz4));
            d
        })
    });
    group.finish();
}

criterion_group!(benches, select);
criterion_main!(benches);
