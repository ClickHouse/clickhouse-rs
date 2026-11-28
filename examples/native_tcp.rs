use clickhouse::{Client, Row, error::Result, sql::Identifier};
use serde::{Deserialize, Serialize};

/// Using the native TCP protocol instead of HTTP.
///
/// Requires the `tcp` feature:
/// `cargo run --example native_tcp --features tcp`
///
/// The URL schema selects the transport: `tcp://` (or `tcps://` with TLS) uses the native protocol.
/// The rest of the API is the same as with HTTP.
#[tokio::main]
async fn main() -> Result<()> {
    let table_name = "chrs_native_tcp";
    let client = Client::default().with_url("tcp://localhost:9000");

    client
        .query("CREATE OR REPLACE TABLE ? (id UInt32, name LowCardinality(String), tags Array(String)) ENGINE = MergeTree ORDER BY id")
        .bind(Identifier(table_name))
        .execute()
        .await?;

    // The rows are converted to columnar blocks with the column types of the table.
    let mut insert = client.insert::<MyRow>(table_name).await?;
    for id in 0..10 {
        insert
            .write(&MyRow {
                id,
                name: format!("name-{}", id % 3),
                tags: vec!["a".to_string(); id as usize % 3],
            })
            .await?;
    }
    insert.end().await?;

    // Server-side parameters work as with HTTP.
    let mut cursor = client
        .query("SELECT ?fields FROM ? WHERE id >= {min_id: UInt32} ORDER BY id")
        .bind(Identifier(table_name))
        .param("min_id", 5)
        .fetch::<MyRow>()?;

    while let Some(row) = cursor.next().await? {
        println!("{row:?}");
    }

    // The native protocol returns blocks, which `fetch_native()` gives directly.
    let mut blocks = client
        .query("SELECT id, name FROM ? ORDER BY id")
        .bind(Identifier(table_name))
        .fetch_native()?;

    while let Some(block) = blocks.next().await? {
        let ids = block["id"].iter::<u32>()?.collect::<Result<Vec<_>>>()?;
        println!("block with {} rows: {ids:?}", block.num_rows());
    }

    Ok(())
}

#[derive(Debug, Row, Serialize, Deserialize)]
struct MyRow {
    id: u32,
    name: String,
    tags: Vec<String>,
}
