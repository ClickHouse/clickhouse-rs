use clickhouse::Client;
use tokio::fs::File;
use tokio::io::AsyncWriteExt;

#[tokio::main]
async fn main() -> clickhouse::error::Result<()> {
    let client = Client::default().with_url("http://localhost:8123");

    let mut raw_cursor = client
        .query(
            "
                SELECT number, randomPrintableASCII(20)
                FROM system.numbers
                LIMIT 100000
                FORMAT CSV
            ",
        )
        .fetch_raw()?;

    let mut file = File::create("output_async.txt").await.unwrap();

    loop {
        match raw_cursor.next().await {
            Ok(None) => break,
            Err(err) => return Err(err),
            Ok(Some(bytes)) => {
                println!("Bytes read: {}", bytes.len());
                file.write_all(&bytes).await.unwrap()
            }
        }
    }

    println!("Bytes written to output_async.txt");
    Ok(())
}
