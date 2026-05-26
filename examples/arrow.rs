use std::sync::Arc;

use arrow::array::{Int32Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};

use clickhouse::{Client, error::Result};
use clickhouse_ext_arrow::{ArrowClientExt, ArrowQueryExt};

// This example demonstrates the `clickhouse-ext-arrow` extension traits:
//
//   - `ArrowClientExt::insert_arrow()` — write Arrow `RecordBatch`es to a table.
//   - `ArrowQueryExt::fetch_arrow()`   — stream the result of a `SELECT` as Arrow.
//
// See also: tests/it/arrow.rs

const TABLE_NAME: &str = "chrs_arrow_example";

async fn ddl(client: &Client) -> Result<()> {
    client
        .query(&format!(
            "CREATE OR REPLACE TABLE {TABLE_NAME}(bar Int32, baz String) \
             ENGINE = MergeTree ORDER BY bar"
        ))
        .execute()
        .await
}

// `insert_arrow()` writes one or more `RecordBatch`es to a target table.
//
// The Arrow field names are used as the target table's column list for the
// generated `INSERT ... FORMAT ArrowStream` query.
async fn insert(client: &Client) -> Result<()> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("bar", DataType::Int32, false),
        Field::new("baz", DataType::Utf8, false),
    ]));

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["one", "two", "three"])),
        ],
    )
    .unwrap();

    let mut insert = client.insert_arrow(TABLE_NAME)?;
    insert.write(&batch).await?;
    // For a multi-batch insert, call `insert.write(&batch).await?` repeatedly,
    // then `insert.end().await?` to flush and finish the request.
    insert.end().await?;

    Ok(())
}

// `fetch_arrow()` streams the response as a sequence of Arrow `RecordBatch`es.
async fn select(client: &Client) -> Result<()> {
    let mut cursor = client
        .query(&format!("SELECT bar, baz FROM {TABLE_NAME} ORDER BY bar"))
        .fetch_arrow()?;

    while let Some(batch) = cursor.next().await? {
        println!("batch with {} row(s):\n{batch:?}", batch.num_rows());
    }

    Ok(())
}

// Convenience helper: collect the whole response into a single `RecordBatch`.
// Allocates a fresh batch to concatenate the streamed pieces; use `select()`
// above when you want to process batches as they arrive instead.
async fn select_merged(client: &Client) -> Result<()> {
    let batch = client
        .query(&format!("SELECT bar, baz FROM {TABLE_NAME} ORDER BY bar"))
        .fetch_arrow()?
        .collect_merged()
        .await?;

    println!("merged batch with {} row(s):\n{batch:?}", batch.num_rows());

    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let client = Client::default().with_url("http://localhost:8123");

    ddl(&client).await?;
    insert(&client).await?;
    select(&client).await?;
    select_merged(&client).await?;

    Ok(())
}
