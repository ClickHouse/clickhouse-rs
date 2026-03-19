// Tests for the unsafe reborrow in RowCursor::poll_next and Next::poll.
//
// These specifically exercise the code paths that previously used
// polonius-the-crab and now use a manual unsafe reborrow. The key
// scenarios are the get-or-retry loop (NotEnoughData -> extend -> retry)
// and borrowed deserialization (T::Value<'_> borrowing from the buffer).

#![cfg(feature = "test-util")]

use clickhouse::{Client, Row, test};
use serde::{Deserialize, Serialize};

// -- Mock-based tests (no ClickHouse needed) --------------------------------

#[tokio::test]
async fn cursor_single_row() {
    #[derive(Debug, Clone, Row, Serialize, Deserialize, PartialEq)]
    struct R {
        x: u32,
    }

    let mock = test::Mock::new();
    let client = Client::default().with_mock(&mock);
    mock.add(test::handlers::provide([R { x: 42 }]));

    let mut cursor = client.query("SELECT x").fetch::<R>().unwrap();
    assert_eq!(cursor.next().await.unwrap(), Some(R { x: 42 }));
    assert_eq!(cursor.next().await.unwrap(), None);
}

#[tokio::test]
async fn cursor_multiple_rows() {
    // The loop in poll_next is the bit that needs the reborrow. Multiple
    // rows means the loop iterates, which exercises extend() after a
    // successful deserialisation on the previous iteration.
    #[derive(Debug, Clone, Row, Serialize, Deserialize, PartialEq)]
    struct R {
        id: u64,
        data: String,
    }

    let mock = test::Mock::new();
    let client = Client::default().with_mock(&mock);
    let rows: Vec<R> = (0..100)
        .map(|i| R {
            id: i,
            data: format!("row-{i}"),
        })
        .collect();
    mock.add(test::handlers::provide(rows.clone()));

    let mut cursor = client.query("SELECT id, data").fetch::<R>().unwrap();
    let mut got = Vec::new();
    while let Some(row) = cursor.next().await.unwrap() {
        got.push(row);
    }
    assert_eq!(got, rows);
}

#[tokio::test]
async fn cursor_empty_result() {
    #[derive(Debug, Clone, Row, Serialize, Deserialize, PartialEq)]
    struct R {
        x: u32,
    }

    let mock = test::Mock::new();
    let client = Client::default().with_mock(&mock);
    mock.add(test::handlers::provide(Vec::<R>::new()));

    let mut cursor = client.query("SELECT x").fetch::<R>().unwrap();
    assert_eq!(cursor.next().await.unwrap(), None);
}

#[tokio::test]
async fn cursor_fetch_all_and_fetch_one() {
    // fetch_all and fetch_one both go through poll_next internally.
    #[derive(Debug, Clone, Row, Serialize, Deserialize, PartialEq)]
    struct R {
        v: String,
    }

    let mock = test::Mock::new();
    let client = Client::default().with_mock(&mock);

    let rows = vec![
        R {
            v: "aaa".to_string(),
        },
        R {
            v: "bbb".to_string(),
        },
        R {
            v: "ccc".to_string(),
        },
    ];
    mock.add(test::handlers::provide(rows.clone()));
    let got = client
        .query("SELECT v")
        .fetch_all::<R>()
        .await
        .unwrap();
    assert_eq!(got, rows);

    mock.add(test::handlers::provide([R {
        v: "one".to_string(),
    }]));
    let got = client
        .query("SELECT v")
        .fetch_one::<R>()
        .await
        .unwrap();
    assert_eq!(got, R {
        v: "one".to_string(),
    });
}

// -- Integration tests (need a real ClickHouse) -----------------------------

#[tokio::test]
async fn cursor_large_result_spanning_chunks() {
    // Large enough to span multiple HTTP response chunks, exercising the
    // NotEnoughData -> raw.poll_next -> extend -> retry path in the loop.
    // This is the core path the unsafe reborrow protects.
    #[derive(Debug, Clone, Row, Serialize, Deserialize, PartialEq)]
    struct R {
        id: u64,
        payload: String,
    }

    let client = prepare_database!();
    client
        .query(
            "CREATE TABLE test (id UInt64, payload String) \
             ENGINE = MergeTree ORDER BY id",
        )
        .execute()
        .await
        .unwrap();

    // 500 rows with ~200 bytes each = ~100KB, enough to span chunks.
    let expected: Vec<R> = (0..500)
        .map(|i| R {
            id: i,
            payload: format!("{i:0>200}"),
        })
        .collect();

    let mut insert = client.insert::<R>("test").await.unwrap();
    for row in &expected {
        insert.write(row).await.unwrap();
    }
    insert.end().await.unwrap();

    let mut cursor = client
        .query("SELECT id, payload FROM test ORDER BY id")
        .fetch::<R>()
        .unwrap();

    let mut got = Vec::new();
    while let Some(row) = cursor.next().await.unwrap() {
        got.push(row);
    }
    assert_eq!(got.len(), expected.len());
    assert_eq!(got, expected);
}

#[tokio::test]
async fn cursor_borrowed_rows() {
    // Borrowed deserialization is the reason the unsafe exists — the
    // returned T::Value<'_> borrows from the cursor's internal buffer.
    #[derive(Debug, Row, Serialize, Deserialize, PartialEq)]
    struct Borrowed<'a> {
        id: u64,
        data: &'a str,
    }

    let client = prepare_database!();
    crate::create_simple_table(&client, "test").await;

    let mut insert = client.insert::<Borrowed<'_>>("test").await.unwrap();
    insert
        .write(&Borrowed { id: 1, data: "one" })
        .await
        .unwrap();
    insert
        .write(&Borrowed {
            id: 2,
            data: "two",
        })
        .await
        .unwrap();
    insert
        .write(&Borrowed {
            id: 3,
            data: "three",
        })
        .await
        .unwrap();
    insert.end().await.unwrap();

    let mut cursor = client
        .query("SELECT id, data FROM test ORDER BY id")
        .fetch::<Borrowed<'_>>()
        .unwrap();

    let row = cursor.next().await.unwrap().unwrap();
    assert_eq!(row, Borrowed { id: 1, data: "one" });
    let row = cursor.next().await.unwrap().unwrap();
    assert_eq!(row, Borrowed {
        id: 2,
        data: "two",
    });
    let row = cursor.next().await.unwrap().unwrap();
    assert_eq!(
        row,
        Borrowed {
            id: 3,
            data: "three"
        }
    );
    assert_eq!(cursor.next().await.unwrap(), None);
}

#[tokio::test]
async fn cursor_small_block_size() {
    // Force ClickHouse to send one row per chunk. This maximises the
    // number of extend() calls per row, hammering the reborrow path.
    let client = prepare_database!();
    crate::create_simple_table(&client, "test").await;

    let mut insert = client.insert::<crate::SimpleRow>("test").await.unwrap();
    for i in 0..50 {
        insert
            .write(&crate::SimpleRow::new(i, format!("val-{i}")))
            .await
            .unwrap();
    }
    insert.end().await.unwrap();

    let mut cursor = client
        .with_option("max_block_size", "1")
        .query("SELECT ?fields FROM test ORDER BY id")
        .fetch::<crate::SimpleRow>()
        .unwrap();

    let mut count = 0u64;
    while cursor.next().await.unwrap().is_some() {
        count += 1;
    }
    assert_eq!(count, 50);
}
