use clickhouse::format::OutputFormat;
use clickhouse::Client;
use tokio::fs::File;
use tokio::io::AsyncWriteExt;

/// An example of streaming the result of a query in an arbitrary format to a file.

#[tokio::main]
async fn main() -> clickhouse::error::Result<()> {
    let filename = "output.csv";
    let client = Client::default().with_url("http://localhost:8123");

    let mut raw_cursor = client
        .query(
            "
                SELECT number, randomPrintableASCII(20)
                FROM system.numbers
                LIMIT 100000
            ",
        )
        .fetch_raw(OutputFormat::CSV)?;

    let mut file = File::create(filename).await.unwrap();

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

    println!("Bytes written to {filename}");
    Ok(())
}
