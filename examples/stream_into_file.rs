use clickhouse::Client;
use futures::StreamExt;
use tokio::fs::File;
use tokio::io::AsyncWriteExt;

/// An example of streaming the result of a query in an arbitrary format into a file
/// leveraging the [`StreamExt`] helper. In this case, `CSVWithNamesAndTypes` format is used.
/// See also: https://clickhouse.com/docs/en/interfaces/formats

#[tokio::main]
async fn main() -> clickhouse::error::Result<()> {
    let client = Client::default().with_url("http://localhost:8123");
    let filename = "output.csv";
    let mut bytes_cursor = client
        .query(
            "
                SELECT number, hex(randomPrintableASCII(20)) AS hex_str
                FROM system.numbers
                LIMIT 10000
            ",
        )
        .fetch_bytes("CSVWithNamesAndTypes")?;

    let mut file = File::create(filename).await.unwrap();
    while let Some(data) = bytes_cursor.next().await {
        match data {
            Ok(bytes) => {
                file.write_all(&bytes).await.unwrap();
                println!("Bytes written: {}", bytes.len());
            }
            Err(err) => return Err(err),
        }
    }

    println!("Raw data is written to {filename}");
    Ok(())
}
