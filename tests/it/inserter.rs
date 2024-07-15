#![cfg(feature = "inserter")]

use std::string::ToString;

use serde::Serialize;

use clickhouse::{inserter::Quantities, Client, Row};

#[derive(Debug, Row, Serialize)]
struct MyRow {
    data: String,
}

impl MyRow {
    fn new(data: impl ToString) -> Self {
        Self {
            data: data.to_string(),
        }
    }
}

async fn create_table(client: &Client) {
    client
        .query("CREATE TABLE test(data String) ENGINE = MergeTree ORDER BY data")
        .execute()
        .await
        .unwrap();
}

#[tokio::test]
async fn force_commit() {
    let client = prepare_database!();
    create_table(&client).await;

    let mut inserter = client.inserter("test").unwrap();
    let rows = 100;

    for i in 1..=rows {
        inserter.write(&MyRow::new(i)).unwrap();
        assert_eq!(inserter.commit().await.unwrap(), Quantities::ZERO);

        if i % 10 == 0 {
            assert_eq!(inserter.force_commit().await.unwrap().rows, 10);
        }
    }

    assert_eq!(inserter.end().await.unwrap(), Quantities::ZERO);

    let (count, sum) = client
        .query("SELECT count(), sum(toUInt64(data)) FROM test")
        .fetch_one::<(u64, u64)>()
        .await
        .unwrap();

    assert_eq!(count, rows);
    assert_eq!(sum, (1..=rows).sum::<u64>());
}

#[tokio::test]
async fn limited_by_rows() {
    let client = prepare_database!();
    create_table(&client).await;

    let mut inserter = client.inserter("test").unwrap().with_max_rows(10);
    let rows = 100;

    for i in (2..=rows).step_by(2) {
        let row = MyRow::new(i - 1);
        inserter.write(&row).unwrap();
        let row = MyRow::new(i);
        inserter.write(&row).unwrap();

        let inserted = inserter.commit().await.unwrap();
        let pending = inserter.pending();

        if i % 10 == 0 {
            assert_ne!(inserted.bytes, 0);
            assert_eq!(inserted.rows, 10);
            assert_eq!(inserted.transactions, 5);
            assert_eq!(pending, &Quantities::ZERO);
        } else {
            assert_eq!(inserted, Quantities::ZERO);
            assert_ne!(pending.bytes, 0);
            assert_eq!(pending.rows, i % 10);
            assert_eq!(pending.transactions, (i % 10) / 2);
        }
    }

    assert_eq!(inserter.end().await.unwrap(), Quantities::ZERO);

    let (count, sum) = client
        .query("SELECT count(), sum(toUInt64(data)) FROM test")
        .fetch_one::<(u64, u64)>()
        .await
        .unwrap();

    assert_eq!(count, rows);
    assert_eq!(sum, (1..=rows).sum::<u64>());
}

#[tokio::test]
async fn limited_by_bytes() {
    let client = prepare_database!();
    create_table(&client).await;

    let mut inserter = client.inserter("test").unwrap().with_max_bytes(100);
    let rows = 100;

    let row = MyRow::new("x".repeat(9));

    for i in 1..=rows {
        inserter.write(&row).unwrap();

        let inserted = inserter.commit().await.unwrap();
        let pending = inserter.pending();

        if i % 10 == 0 {
            assert_eq!(inserted.bytes, 100);
            assert_eq!(inserted.rows, 10);
            assert_eq!(inserted.transactions, 10);
            assert_eq!(pending, &Quantities::ZERO);
        } else {
            assert_eq!(inserted, Quantities::ZERO);
            assert_eq!(pending.bytes, (i % 10) * 10);
            assert_eq!(pending.rows, i % 10);
            assert_eq!(pending.transactions, i % 10);
        }
    }

    assert_eq!(inserter.end().await.unwrap(), Quantities::ZERO);

    let count = client
        .query("SELECT count() FROM test")
        .fetch_one::<u64>()
        .await
        .unwrap();

    assert_eq!(count, rows);
}

#[cfg(feature = "test-util")] // only with `tokio::time::Instant`
#[tokio::test(start_paused = true)]
async fn limited_by_time() {
    use std::time::Duration;

    let client = prepare_database!();
    create_table(&client).await;

    let period = Duration::from_secs(1);
    let mut inserter = client.inserter("test").unwrap().with_period(Some(period));
    let rows = 100;

    for i in 1..=rows {
        let row = MyRow::new(i);
        inserter.write(&row).unwrap();

        tokio::time::sleep(period / 10).await;

        let inserted = inserter.commit().await.unwrap();
        let pending = inserter.pending();

        if i % 10 == 0 {
            assert_ne!(inserted.bytes, 0);
            assert_eq!(inserted.rows, 10);
            assert_eq!(inserted.transactions, 10);
            assert_eq!(pending, &Quantities::ZERO);
        } else {
            assert_eq!(inserted, Quantities::ZERO);
            assert_ne!(pending.bytes, 0);
            assert_eq!(pending.rows, i % 10);
            assert_eq!(pending.transactions, i % 10);
        }
    }

    assert_eq!(inserter.end().await.unwrap(), Quantities::ZERO);

    let (count, sum) = client
        .query("SELECT count(), sum(toUInt64(data)) FROM test")
        .fetch_one::<(u64, u64)>()
        .await
        .unwrap();

    assert_eq!(count, rows);
    assert_eq!(sum, (1..=rows).sum::<u64>());
}
