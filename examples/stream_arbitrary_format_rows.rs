use futures::{AsyncBufReadExt, StreamExt};

use clickhouse::Client;

/// An example of streaming raw data row-by-row in an arbitrary format
/// leveraging the [`AsyncBufReadExt`] and [`StreamExt`] helpers.
/// In this case, the format is JSONEachRow.
/// Similarly, it can be used with other formats such as CSV, TSV,
/// and others that produce each row on a new line;
/// the only difference will be in how the data is parsed.
/// See also: https://clickhouse.com/docs/en/interfaces/formats

#[tokio::main]
async fn main() -> clickhouse::error::Result<()> {
    let client = Client::default().with_url("http://localhost:8123");
    let mut lines_cursor = client
        .query(
            "
                SELECT number, hex(randomPrintableASCII(20)) AS hex_str
                FROM system.numbers
                LIMIT 100
            ",
        )
        .fetch_bytes("JSONEachRow")?
        .lines();

    while let Some(data) = lines_cursor.next().await {
        match data {
            Ok(line_bytes) => {
                let value = serde_json::json!(&line_bytes);
                println!("JSONEachRow value: {}", value);
            }
            Err(err) => return Err(clickhouse::error::Error::Custom(err.to_string())),
        }
    }

    Ok(())
}
