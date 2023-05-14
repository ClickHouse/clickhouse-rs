use serde::Deserialize;

use clickhouse::{error::Result, Client, Compression, Row};

#[derive(Row, Deserialize)]
struct Data {
    no: u64,
}

async fn bench() -> u64 {
    let client = Client::default()
        .with_compression(Compression::None)
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
async fn main() -> Result<()> {
    println!("Started");
    let start = std::time::Instant::now();
    let sum = tokio::spawn(bench()).await.unwrap();
    let elapsed = start.elapsed();
    println!("Done: elapsed={elapsed:?} sum={sum}");
    Ok(())
}
