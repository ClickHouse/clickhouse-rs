#![cfg(feature = "wa-37420")]

use serde::{Deserialize, Serialize};

use clickhouse::Row;

mod common;

#[common::named]
#[tokio::test]
async fn smoke() {
    let client = common::prepare_database!();

    #[derive(Debug, Row, Serialize, Deserialize)]
    pub struct Row {
        time: u64,
        name: String,
    }

    #[derive(Debug, Row, Serialize, Deserialize)]
    pub struct Row2 {
        name: String,
        time: u64,
    }

    // Create a table.
    client
        .query("CREATE TABLE test(name String, time UInt64) ENGINE = MergeTree ORDER BY time")
        .execute()
        .await
        .unwrap();

    let row = Row {
        time: 1651760768976141295,
        name: "first".into(),
    };

    // Write to the table.
    let mut insert = client.insert("test").unwrap();
    insert.write(&row).await.unwrap();
    insert.end().await.unwrap();

    // Read from the table.
    let inserted = client
        .query("SELECT ?fields FROM test WHERE name = 'first'")
        .fetch_one::<Row>()
        .await
        .unwrap();

    assert_eq!(inserted.time, row.time);

    // -------

    let row = Row2 {
        name: "second".into(),
        time: 1651760768976141295,
    };

    // Write to the table.
    let mut insert = client.insert("test").unwrap();
    insert.write(&row).await.unwrap();
    insert.end().await.unwrap();

    // Read from the table.
    let inserted = client
        .query("SELECT ?fields FROM test WHERE name = 'second'")
        .fetch_one::<Row>()
        .await
        .unwrap();

    assert_eq!(inserted.time, row.time);
}
