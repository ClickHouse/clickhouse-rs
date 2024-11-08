use std::str::from_utf8;

use serde::Deserialize;

use clickhouse::Client;

/// An example of streaming raw data row-by-row in an arbitrary format.
/// In this case, it's JSONEachRow. Similarly, it can be used with CSV, TSV, and others;
/// the only difference will be in how the data is parsed.

#[tokio::main]
async fn main() -> clickhouse::error::Result<()> {
    let client = Client::default().with_url("http://localhost:8123");

    let mut raw_cursor_newline = client
        .query(
            "
                SELECT number, hex(randomPrintableASCII(20)) AS hex_str
                FROM system.numbers
                LIMIT 10
                FORMAT JSONEachRow
            ",
        )
        // By default, ClickHouse quotes (U)Int64 in JSON* family formats;
        // disable it to simplify this example.
        .with_option("output_format_json_quote_64bit_integers", "0")
        .fetch_raw()?
        .newline();

    loop {
        match raw_cursor_newline.next().await {
            Ok(None) => break,
            Err(err) => return Err(err),
            Ok(Some(row_bytes)) => {
                let json_str = from_utf8(row_bytes).unwrap();
                let parsed_json =
                    serde_json::from_str::<MyRowInJSONEachRowFormat>(json_str).unwrap();
                println!("Number: {}", parsed_json.number);
                println!("HexStr: {}", parsed_json.hex_str);
                println!("=================================================");
            }
        }
    }

    println!("Done!");
    Ok(())
}

// NB: there is no `Row` derive here
#[derive(Debug, Deserialize)]
struct MyRowInJSONEachRowFormat {
    number: i64,
    hex_str: String,
}
