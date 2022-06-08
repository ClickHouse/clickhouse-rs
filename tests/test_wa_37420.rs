#![cfg(feature = "wa-37420")]

use serde::{Deserialize, Serialize};

use clickhouse::Row;

mod common;

#[tokio::test]
async fn wa_37420() {
    let client = common::prepare_database("wa_37420").await;

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
        .query("CREATE TABLE some(name String, time UInt64) ENGINE = MergeTree ORDER BY time")
        .execute()
        .await
        .unwrap();

    let row = Row {
        time: 1651760768976141295,
        name: "first".into(),
    };

    // Write to the table.
    let mut insert = client.insert("some").unwrap();
    insert.write(&row).await.unwrap();
    insert.end().await.unwrap();

    // Read from the table.
    let inserted = client
        .query("SELECT ?fields FROM some WHERE name = 'first'")
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
    let mut insert = client.insert("some").unwrap();
    insert.write(&row).await.unwrap();
    insert.end().await.unwrap();

    // Read from the table.
    let inserted = client
        .query("SELECT ?fields FROM some WHERE name = 'second'")
        .fetch_one::<Row>()
        .await
        .unwrap();

    assert_eq!(inserted.time, row.time);
}
