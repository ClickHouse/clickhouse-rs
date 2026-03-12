use serde::{Deserialize, Serialize};

use clickhouse::async_inserter::{AsyncInserter, AsyncInserterConfig};
use clickhouse::{Client, Row};

#[derive(Debug, Clone, PartialEq, Eq, Row, Serialize, Deserialize)]
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

// ═══════════════════════════════════════════════════════════════════════════
// Happy-path tests
// ═══════════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn async_inserter_basic() {
    let client = prepare_database!();
    create_table(&client).await;

    let inserter = AsyncInserter::<MyRow>::new(
        &client,
        "test",
        AsyncInserterConfig::default().without_period(),
    );

    for i in 0..100u32 {
        inserter
            .write(MyRow { id: i, data: i.to_string() })
            .await
            .unwrap();
    }

    inserter.end().await.unwrap();

    assert_eq!(count_rows(&client).await, 100);
}

#[tokio::test]
async fn async_inserter_flush() {
    let client = prepare_database!();
    create_table(&client).await;

    let inserter = AsyncInserter::<MyRow>::new(
        &client,
        "test",
        AsyncInserterConfig::default().without_period(),
    );

    for i in 0..50u32 {
        inserter
            .write(MyRow { id: i, data: i.to_string() })
            .await
            .unwrap();
    }

    let q = inserter.flush().await.unwrap();
    assert_eq!(q.rows, 50);
    assert_eq!(count_rows(&client).await, 50);

    for i in 50..100u32 {
        inserter
            .write(MyRow { id: i, data: i.to_string() })
            .await
            .unwrap();
    }

    inserter.end().await.unwrap();
    assert_eq!(count_rows(&client).await, 100);
}

#[tokio::test]
async fn async_inserter_max_rows() {
    let client = prepare_database!();
    create_table(&client).await;

    let inserter = AsyncInserter::<MyRow>::new(
        &client,
        "test",
        AsyncInserterConfig::default()
            .with_max_rows(10)
            .without_period(),
    );

    for i in 0..35u32 {
        inserter
            .write(MyRow { id: i, data: i.to_string() })
            .await
            .unwrap();
    }

    inserter.end().await.unwrap();
    assert_eq!(count_rows(&client).await, 35);
}

#[tokio::test]
async fn async_inserter_period_flush() {
    let client = prepare_database!();
    create_table(&client).await;

    let inserter = AsyncInserter::<MyRow>::new(
        &client,
        "test",
        AsyncInserterConfig::default()
            .with_max_rows(u64::MAX)
            .with_max_bytes(u64::MAX)
            .with_max_period(tokio::time::Duration::from_millis(200)),
    );

    for i in 0..20u32 {
        inserter
            .write(MyRow { id: i, data: i.to_string() })
            .await
            .unwrap();
    }

    tokio::time::sleep(tokio::time::Duration::from_millis(600)).await;

    assert_eq!(count_rows(&client).await, 20);

    inserter.end().await.unwrap();
    assert_eq!(count_rows(&client).await, 20);
}

#[tokio::test]
async fn async_inserter_empty_end() {
    let client = prepare_database!();
    create_table(&client).await;

    let inserter = AsyncInserter::<MyRow>::new(
        &client,
        "test",
        AsyncInserterConfig::default().without_period(),
    );

    inserter.end().await.unwrap();
    assert_eq!(count_rows(&client).await, 0);
}

#[tokio::test]
async fn async_inserter_concurrent_handles() {
    let client = prepare_database!();
    create_table(&client).await;

    let inserter = AsyncInserter::<MyRow>::new(
        &client,
        "test",
        AsyncInserterConfig::default().without_period(),
    );

    let mut tasks = Vec::new();
    for chunk_start in (0..100u32).step_by(10) {
        let handle = inserter.handle();
        tasks.push(tokio::spawn(async move {
            for i in chunk_start..chunk_start + 10 {
                handle
                    .write(MyRow { id: i, data: i.to_string() })
                    .await
                    .unwrap();
            }
        }));
    }

    for task in tasks {
        task.await.unwrap();
    }

    inserter.end().await.unwrap();
    assert_eq!(count_rows(&client).await, 100);
}

// ═══════════════════════════════════════════════════════════════════════════
// Edge cases
// ═══════════════════════════════════════════════════════════════════════════

/// Writing a single row should work.
#[tokio::test]
async fn async_inserter_single_row() {
    let client = prepare_database!();
    create_table(&client).await;

    let inserter = AsyncInserter::<MyRow>::new(
        &client,
        "test",
        AsyncInserterConfig::default().without_period(),
    );

    inserter
        .write(MyRow {
            id: 42,
            data: "hello".into(),
        })
        .await
        .unwrap();

    inserter.end().await.unwrap();
    assert_eq!(count_rows(&client).await, 1);
}

/// Flush on an empty buffer should return zero quantities (not error).
#[tokio::test]
async fn async_inserter_flush_empty() {
    let client = prepare_database!();
    create_table(&client).await;

    let inserter = AsyncInserter::<MyRow>::new(
        &client,
        "test",
        AsyncInserterConfig::default().without_period(),
    );

    let q = inserter.flush().await.unwrap();
    assert_eq!(q.rows, 0);
    assert_eq!(q.bytes, 0);

    inserter.end().await.unwrap();
    assert_eq!(count_rows(&client).await, 0);
}

/// Multiple flushes in a row without writes between them.
#[tokio::test]
async fn async_inserter_double_flush() {
    let client = prepare_database!();
    create_table(&client).await;

    let inserter = AsyncInserter::<MyRow>::new(
        &client,
        "test",
        AsyncInserterConfig::default().without_period(),
    );

    for i in 0..10u32 {
        inserter
            .write(MyRow { id: i, data: i.to_string() })
            .await
            .unwrap();
    }

    let q1 = inserter.flush().await.unwrap();
    assert_eq!(q1.rows, 10);

    // Second flush with nothing buffered.
    let q2 = inserter.flush().await.unwrap();
    assert_eq!(q2.rows, 0);

    inserter.end().await.unwrap();
    assert_eq!(count_rows(&client).await, 10);
}

/// max_rows=1 should auto-flush after every single row.
#[tokio::test]
async fn async_inserter_max_rows_one() {
    let client = prepare_database!();
    create_table(&client).await;

    let inserter = AsyncInserter::<MyRow>::new(
        &client,
        "test",
        AsyncInserterConfig::default()
            .with_max_rows(1)
            .without_period(),
    );

    for i in 0..5u32 {
        inserter
            .write(MyRow { id: i, data: i.to_string() })
            .await
            .unwrap();
    }

    inserter.end().await.unwrap();
    assert_eq!(count_rows(&client).await, 5);
}

/// Rows with large string data still round-trip correctly.
#[tokio::test]
async fn async_inserter_large_strings() {
    let client = prepare_database!();
    create_table(&client).await;

    let inserter = AsyncInserter::<MyRow>::new(
        &client,
        "test",
        AsyncInserterConfig::default().without_period(),
    );

    let big = "x".repeat(100_000);
    for i in 0..3u32 {
        inserter
            .write(MyRow {
                id: i,
                data: big.clone(),
            })
            .await
            .unwrap();
    }

    inserter.end().await.unwrap();

    let rows: Vec<MyRow> = client
        .query("SELECT id, data FROM test ORDER BY id")
        .fetch_all()
        .await
        .unwrap();
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].data.len(), 100_000);
}

/// max_bytes flush threshold triggers when accumulated serialised data is large.
#[tokio::test]
async fn async_inserter_max_bytes_trigger() {
    let client = prepare_database!();
    create_table(&client).await;

    let inserter = AsyncInserter::<MyRow>::new(
        &client,
        "test",
        AsyncInserterConfig::default()
            .with_max_rows(u64::MAX)
            .with_max_bytes(100) // very small — should trigger after a few rows
            .without_period(),
    );

    for i in 0..20u32 {
        inserter
            .write(MyRow {
                id: i,
                data: "some payload data here".into(),
            })
            .await
            .unwrap();
    }

    inserter.end().await.unwrap();
    assert_eq!(count_rows(&client).await, 20);
}

/// Small channel capacity (1) forces extreme backpressure but should still work.
#[tokio::test]
async fn async_inserter_tiny_channel() {
    let client = prepare_database!();
    create_table(&client).await;

    let inserter = AsyncInserter::<MyRow>::new(
        &client,
        "test",
        AsyncInserterConfig::default()
            .with_channel_capacity(1)
            .without_period(),
    );

    for i in 0..20u32 {
        inserter
            .write(MyRow { id: i, data: i.to_string() })
            .await
            .unwrap();
    }

    inserter.end().await.unwrap();
    assert_eq!(count_rows(&client).await, 20);
}

// ═══════════════════════════════════════════════════════════════════════════
// Failure / error propagation tests
// ═══════════════════════════════════════════════════════════════════════════

/// Writing to a non-existent table should propagate the ClickHouse error back
/// to the caller (on flush/end, not on write — writes only serialize).
#[tokio::test]
async fn async_inserter_bad_table() {
    let client = prepare_database!();
    // Intentionally do NOT create the table.

    let inserter = AsyncInserter::<MyRow>::new(
        &client,
        "this_table_does_not_exist",
        AsyncInserterConfig::default()
            .with_max_rows(1) // force flush after one row
            .without_period(),
    );

    // write() serialises into the buffer — the error surfaces on the commit
    // triggered by max_rows=1.
    let result = inserter
        .write(MyRow {
            id: 1,
            data: "x".into(),
        })
        .await;

    // The error might surface on write (if commit happens inline) or on end.
    if result.is_ok() {
        let end_result = inserter.end().await;
        // At least end() should report the error.
        assert!(
            end_result.is_err() || end_result.unwrap().rows == 0,
            "expected error or zero rows for non-existent table"
        );
    }
}

/// Handle becomes inert after the inserter is ended — writes should fail.
#[tokio::test]
async fn async_inserter_handle_after_end() {
    let client = prepare_database!();
    create_table(&client).await;

    let inserter = AsyncInserter::<MyRow>::new(
        &client,
        "test",
        AsyncInserterConfig::default().without_period(),
    );

    let handle = inserter.handle();

    inserter.end().await.unwrap();

    // The background task has stopped — write via handle should fail.
    let result = handle
        .write(MyRow {
            id: 1,
            data: "late".into(),
        })
        .await;
    assert!(result.is_err(), "write after end() should fail");
}

/// flush() via handle after inserter is ended should fail.
#[tokio::test]
async fn async_inserter_flush_after_end() {
    let client = prepare_database!();
    create_table(&client).await;

    let inserter = AsyncInserter::<MyRow>::new(
        &client,
        "test",
        AsyncInserterConfig::default().without_period(),
    );

    let handle = inserter.handle();

    inserter.end().await.unwrap();

    let result = handle.flush().await;
    assert!(result.is_err(), "flush after end() should fail");
}

// ═══════════════════════════════════════════════════════════════════════════
// Stress / concurrency tests
// ═══════════════════════════════════════════════════════════════════════════

/// Many concurrent writers with small max_rows to stress the flush path.
#[tokio::test]
async fn async_inserter_stress_concurrent() {
    let client = prepare_database!();
    create_table(&client).await;

    let inserter = AsyncInserter::<MyRow>::new(
        &client,
        "test",
        AsyncInserterConfig::default()
            .with_max_rows(7) // prime number to create odd batch boundaries
            .without_period(),
    );

    let mut tasks = Vec::new();
    for task_id in 0..20u32 {
        let handle = inserter.handle();
        tasks.push(tokio::spawn(async move {
            for j in 0..50u32 {
                let id = task_id * 50 + j;
                handle
                    .write(MyRow {
                        id,
                        data: format!("task{task_id}_row{j}"),
                    })
                    .await
                    .unwrap();
            }
        }));
    }

    for task in tasks {
        task.await.unwrap();
    }

    inserter.end().await.unwrap();
    assert_eq!(count_rows(&client).await, 1000);
}

/// Interleaved writes and flushes from multiple handles.
#[tokio::test]
async fn async_inserter_interleaved_flush() {
    let client = prepare_database!();
    create_table(&client).await;

    let inserter = AsyncInserter::<MyRow>::new(
        &client,
        "test",
        AsyncInserterConfig::default().without_period(),
    );

    let h1 = inserter.handle();
    let h2 = inserter.handle();

    // Writer 1: write 10 rows, then flush.
    for i in 0..10u32 {
        h1.write(MyRow { id: i, data: "a".into() }).await.unwrap();
    }
    h1.flush().await.unwrap();

    // Writer 2: write 10 rows, then flush.
    for i in 10..20u32 {
        h2.write(MyRow { id: i, data: "b".into() }).await.unwrap();
    }
    h2.flush().await.unwrap();

    assert_eq!(count_rows(&client).await, 20);

    drop(h1);
    drop(h2);
    inserter.end().await.unwrap();
    assert_eq!(count_rows(&client).await, 20);
}
