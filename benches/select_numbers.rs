use serde::Deserialize;

use clickhouse::{Client, Compression, Row};

#[derive(Row, Deserialize)]
struct Data {
    no: u64,
}

async fn bench(name: &str, compression: Compression) {
    let start = std::time::Instant::now();
    let (sum, dec_mbytes, rec_mbytes) = tokio::spawn(do_bench(compression)).await.unwrap();
    assert_eq!(sum, 124999999750000000);
    let elapsed = start.elapsed();
    let throughput = dec_mbytes / elapsed.as_secs_f64();
    println!("{name:>8}  {elapsed:>7.3?}  {throughput:>4.0} MiB/s  {rec_mbytes:>4.0} MiB");
}

async fn do_bench(compression: Compression) -> (u64, f64, f64) {
    let client = Client::default()
        .with_compression(compression)
        .with_url("http://localhost:8123");

    let mut cursor = client
        .query("SELECT number FROM system.numbers_mt LIMIT 500000000")
        .fetch::<Data>()
        .unwrap();

    let mut sum = 0;
    while let Some(row) = cursor.next().await.unwrap() {
        sum += row.no;
    }

    let dec_bytes = cursor.decoded_bytes();
    let dec_mbytes = dec_bytes as f64 / 1024.0 / 1024.0;
    let recv_bytes = cursor.received_bytes();
    let recv_mbytes = recv_bytes as f64 / 1024.0 / 1024.0;
    (sum, dec_mbytes, recv_mbytes)
}

#[tokio::main]
async fn main() {
    println!("compress  elapsed  throughput  received");
    bench("none", Compression::None).await;
    #[cfg(feature = "lz4")]
    bench("lz4", Compression::Lz4).await;
}
