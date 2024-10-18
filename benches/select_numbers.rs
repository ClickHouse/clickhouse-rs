use serde::Deserialize;

use clickhouse::{Client, Compression, Row};

#[derive(Row, Deserialize)]
struct Data {
    no: u64,
}

async fn bench(name: &str, compression: Compression) {
    let start = std::time::Instant::now();
    let sum = tokio::spawn(do_bench(compression)).await.unwrap();
    assert_eq!(sum, 124999999750000000);
    let elapsed = start.elapsed();
    println!("{name:>8}  {elapsed:>7.3?}");
}

async fn do_bench(compression: Compression) -> u64 {
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

    sum
}

#[tokio::main]
async fn main() {
    println!("compress  elapsed");
    bench("none", Compression::None).await;
    #[cfg(feature = "lz4")]
    bench("lz4", Compression::Lz4).await;
}
