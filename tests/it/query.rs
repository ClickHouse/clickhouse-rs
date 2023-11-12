use serde::{Deserialize, Serialize};

use clickhouse::{error::Error, Row};

#[tokio::test]
async fn smoke() {
    let client = prepare_database!();

    #[derive(Debug, Row, Serialize, Deserialize)]
    struct MyRow<'a> {
        no: u32,
        name: &'a str,
    }

    // Create a table.
    client
        .query(
            "
            CREATE TABLE test(no UInt32, name LowCardinality(String))
            ENGINE = MergeTree
            ORDER BY no
        ",
        )
        .execute()
        .await
        .unwrap();

    // Write to the table.
    let mut insert = client.insert("test").unwrap();
    for i in 0..1000 {
        insert.write(&MyRow { no: i, name: "foo" }).await.unwrap();
    }

    insert.end().await.unwrap();

    // Read from the table.
    let mut cursor = client
        .query("SELECT ?fields FROM test WHERE name = ? AND no BETWEEN ? AND ?.2")
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

#[tokio::test]
async fn fetch_one_and_optional() {
    let client = prepare_database!();

    client
        .query("CREATE TABLE test(n String) ENGINE = MergeTree ORDER BY n")
        .execute()
        .await
        .unwrap();

    let q = "SELECT * FROM test";
    let got_string = client.query(q).fetch_optional::<String>().await.unwrap();
    assert_eq!(got_string, None);

    let got_string = client.query(q).fetch_one::<String>().await;
    assert!(matches!(got_string, Err(Error::RowNotFound)));

    #[derive(Serialize, Row)]
    struct Row {
        n: String,
    }

    let mut insert = client.insert("test").unwrap();
    insert.write(&Row { n: "foo".into() }).await.unwrap();
    insert.write(&Row { n: "bar".into() }).await.unwrap();
    insert.end().await.unwrap();

    let got_string = client.query(q).fetch_optional::<String>().await.unwrap();
    assert_eq!(got_string, Some("bar".into()));

    let got_string = client.query(q).fetch_one::<String>().await.unwrap();
    assert_eq!(got_string, "bar");
}

// See #19.
#[tokio::test]
async fn long_query() {
    let client = prepare_database!();

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
async fn big_borrowed_str() {
    let client = prepare_database!();

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

// See #31.
#[tokio::test]
async fn all_floats() {
    let client = prepare_database!();

    client
        .query("CREATE TABLE test(no UInt32, f Float64) ENGINE = MergeTree ORDER BY no")
        .execute()
        .await
        .unwrap();

    #[derive(Row, Serialize)]
    struct Row {
        no: u32,
        f: f64,
    }

    let mut insert = client.insert("test").unwrap();
    insert.write(&Row { no: 0, f: 42.5 }).await.unwrap();
    insert.write(&Row { no: 1, f: 43.5 }).await.unwrap();
    insert.end().await.unwrap();

    let vec = client
        .query("SELECT f FROM test")
        .fetch_all::<f64>()
        .await
        .unwrap();

    assert_eq!(vec, &[42.5, 43.5]);
}
