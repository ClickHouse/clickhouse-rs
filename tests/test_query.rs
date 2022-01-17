use serde::{Deserialize, Serialize};

use clickhouse::Row;

mod common;

#[tokio::test]
async fn it_writes_then_reads() {
    let client = common::prepare_database("it_writes_then_reads").await;

    #[derive(Debug, Row, Serialize, Deserialize)]
    struct MyRow<'a> {
        no: u32,
        name: &'a str,
    }

    // Create a table.
    client
        .query(
            "
            CREATE TABLE some(no UInt32, name LowCardinality(String))
            ENGINE = MergeTree
            ORDER BY no
        ",
        )
        .execute()
        .await
        .unwrap();

    // Write to the table.
    let mut insert = client.insert("some").unwrap();
    for i in 0..1000 {
        insert.write(&MyRow { no: i, name: "foo" }).await.unwrap();
    }

    insert.end().await.unwrap();

    // Read from the table.
    let mut cursor = client
        .query("SELECT ?fields FROM some WHERE name = ? AND no BETWEEN ? AND ?.2")
        .bind("foo")
        .bind(500)
        .bind((42, 504))
        .fetch::<MyRow<'_>>()
        .unwrap();

    let mut i = 500;

    while let Some(row) = cursor.next().await.unwrap() {
        assert_eq!(row.no, i);
        assert_eq!(row.name, "foo");
        i += 1;
    }
}

// See #19.
#[tokio::test]
async fn it_requests_long_query() {
    let client = common::prepare_database("it_requests_long_query").await;

    client
        .query("CREATE TABLE test(n String) ENGINE = MergeTree ORDER BY n")
        .execute()
        .await
        .unwrap();

    let long_string = "A".repeat(100_000);

    let got_string = client
        .query("select ?")
        .bind(&long_string)
        .fetch_one::<String>()
        .await
        .unwrap();

    assert_eq!(got_string, long_string);
}

// See #22.
#[tokio::test]
async fn it_works_with_big_borrowed_str() {
    let client = common::prepare_database("it_works_with_big_borrowed_str").await;

    #[derive(Debug, Row, Serialize, Deserialize)]
    struct MyRow<'a> {
        no: u32,
        body: &'a str,
    }

    client
        .query("CREATE TABLE test(no UInt32, body String) ENGINE = MergeTree ORDER BY no")
        .execute()
        .await
        .unwrap();

    let long_string = "A".repeat(10000);

    let mut insert = client.insert("test").unwrap();
    insert
        .write(&MyRow {
            no: 0,
            body: &long_string,
        })
        .await
        .unwrap();
    insert.end().await.unwrap();

    let mut cursor = client
        .query("SELECT ?fields FROM test")
        .fetch::<MyRow<'_>>()
        .unwrap();

    let row = cursor.next().await.unwrap().unwrap();
    assert_eq!(row.body, long_string);
}
