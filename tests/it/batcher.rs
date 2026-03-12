use serde::{Deserialize, Serialize};

use clickhouse::batcher::{BatchConfig, TableBatcher};
use clickhouse::{Client, Row};

#[derive(Debug, PartialEq, Eq, Row, Serialize, Deserialize)]
struct MyRow {
    id: u32,
    data: String,
}

async fn create_table(client: &Client) {
    client
        .query(
            "CREATE TABLE test(id UInt32, data String) \
             ENGINE = MergeTree ORDER BY id",
        )
        .execute()
        .await
        .unwrap();
}

async fn count_rows(client: &Client) -> u64 {
    client
        .query("SELECT count() FROM test")
        .fetch_one::<u64>()
        .await
        .unwrap()
}

// ── Basic append + send ───────────────────────────────────────────────────────

#[tokio::test]
async fn batcher_basic() {
    let client = prepare_database!();
    create_table(&client).await;

    let batcher = TableBatcher::<MyRow>::new(
        &client,
        "test",
        BatchConfig::default().without_period(),
    );

    for i in 0..100u32 {
        batcher
            .append(MyRow { id: i, data: i.to_string() })
            .await
            .unwrap();
    }

    batcher.send().await.unwrap();

    assert_eq!(count_rows(&client).await, 100);
}

// ── flush() mid-stream ────────────────────────────────────────────────────────

#[tokio::test]
async fn batcher_explicit_flush() {
    let client = prepare_database!();
    create_table(&client).await;

    let batcher = TableBatcher::<MyRow>::new(
        &client,
        "test",
        BatchConfig::default().without_period(),
    );

    for i in 0..50u32 {
        batcher
            .append(MyRow { id: i, data: i.to_string() })
            .await
            .unwrap();
    }

    let q = batcher.flush().await.unwrap();
    assert_eq!(q.rows, 50);
    assert_eq!(count_rows(&client).await, 50);

    for i in 50..100u32 {
        batcher
            .append(MyRow { id: i, data: i.to_string() })
            .await
            .unwrap();
    }

    batcher.send().await.unwrap();
    assert_eq!(count_rows(&client).await, 100);
}

// ── max_rows threshold ────────────────────────────────────────────────────────

#[tokio::test]
async fn batcher_max_rows_flush() {
    let client = prepare_database!();
    create_table(&client).await;

    // Flush every 10 rows.
    let batcher = TableBatcher::<MyRow>::new(
        &client,
        "test",
        BatchConfig::default().with_max_rows(10).without_period(),
    );

    for i in 0..35u32 {
        batcher
            .append(MyRow { id: i, data: i.to_string() })
            .await
            .unwrap();
    }

    // 3 automatic flushes of 10 rows = 30 committed; 5 still buffered.
    // send() flushes the remaining 5.
    batcher.send().await.unwrap();

    assert_eq!(count_rows(&client).await, 35);
}

// ── period-based background flush ─────────────────────────────────────────────

#[tokio::test]
async fn batcher_period_flush() {
    let client = prepare_database!();
    create_table(&client).await;

    let batcher = TableBatcher::<MyRow>::new(
        &client,
        "test",
        BatchConfig::default()
            .with_max_rows(u64::MAX)
            .with_max_bytes(u64::MAX)
            .with_max_period(tokio::time::Duration::from_millis(200)),
    );

    for i in 0..20u32 {
        batcher
            .append(MyRow { id: i, data: i.to_string() })
            .await
            .unwrap();
    }

    // Wait long enough for two background flush ticks.
    tokio::time::sleep(tokio::time::Duration::from_millis(600)).await;

    // Data should already be in ClickHouse from the background task.
    assert_eq!(count_rows(&client).await, 20);

    batcher.send().await.unwrap();
    assert_eq!(count_rows(&client).await, 20);
}

// ── empty send() is safe ──────────────────────────────────────────────────────

#[tokio::test]
async fn batcher_empty_send() {
    let client = prepare_database!();
    create_table(&client).await;

    let batcher = TableBatcher::<MyRow>::new(
        &client,
        "test",
        BatchConfig::default().without_period(),
    );

    // No appends — send() must not panic or error.
    batcher.send().await.unwrap();

    assert_eq!(count_rows(&client).await, 0);
}
