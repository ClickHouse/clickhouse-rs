use serde::{Deserialize, Serialize};

use clickhouse::Row;

#[tokio::test]
async fn smoke() {
    let client = prepare_database!();

    #[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Row)]
    struct MyRow {
        no: i32,
        #[serde(rename = "items.name")]
        items_name: Vec<String>,
        #[serde(rename = "items.count")]
        items_count: Vec<u32>,
    }

    client
        .query(
            "
        CREATE TABLE test(
            no      Int32,
            items   Nested(
                name    String,
                count   UInt32
            )
        )
        ENGINE = MergeTree ORDER BY no
    ",
        )
        .execute()
        .await
        .unwrap();

    let original_row = MyRow {
        no: 42,
        items_name: vec!["foo".into(), "bar".into()],
        items_count: vec![1, 5],
    };

    let mut insert = client.insert::<MyRow>("test").await.unwrap();
    insert.write(&original_row).await.unwrap();
    insert.end().await.unwrap();

    let row = client
        .query("SELECT ?fields FROM test")
        .fetch_one::<MyRow>()
        .await
        .unwrap();

    assert_eq!(row, original_row);
}

#[tokio::test]
async fn no_flatten() {
    let client = prepare_database!().with_option("flatten_nested", "0");

    #[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Row)]
    struct MyRow {
        no: i32,
        items: Vec<(String, u32)>,
    }

    // `flatten_nested = 0` prevents flattening of nested columns, causing them to be stored as a
    // single array of tuples instead of as separate arrays

    client
        .query(
            "
            CREATE TABLE test(
                no      Int32,
                items   Nested(
                    name    String,
                    count   UInt32
                )
            )
            ENGINE = MergeTree ORDER BY no
            ",
        )
        .execute()
        .await
        .unwrap();

    let original_row = MyRow {
        no: 42,
        items: vec![("foo".into(), 1), ("bar".into(), 5)],
    };

    let mut insert = client.insert::<MyRow>("test").await.unwrap();
    insert.write(&original_row).await.unwrap();
    insert.end().await.unwrap();

    let row = client
        .query("SELECT ?fields FROM test")
        .fetch_one::<MyRow>()
        .await
        .unwrap();

    assert_eq!(row, original_row);
}

#[tokio::test]
async fn doubly_flattened() {
    let client = prepare_database!();

    #[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Row)]
    struct MyRow {
        no: i32,
        #[serde(rename = "items.names")]
        items_names: Vec<Vec<(String, String)>>,
        #[serde(rename = "items.count")]
        items_count: Vec<u32>,
    }

    // Only the first level is flattened and any more deeply nested columns are stored as an array
    // of tuples, so the table ends up with columns
    // - `no Int32`
    // - `items.names Array(Nested(first String, last String))`
    //   (i.e. `Array(Array(Tuple(first String, last String)))`)
    // - `items.count Array(UInt32)`

    client
        .query(
            "
            CREATE TABLE test(
                no      Int32,
                items   Nested(
                    names   Nested(first String, last String),
                    count   UInt32
                )
            )
            ENGINE = MergeTree ORDER BY no
            ",
        )
        .execute()
        .await
        .unwrap();

    let original_row = MyRow {
        no: 42,
        items_names: vec![
            vec![("foo".into(), "foo".into())],
            vec![("bar".into(), "bar".into())],
        ],
        items_count: vec![1, 5],
    };

    let mut insert = client.insert::<MyRow>("test").await.unwrap();
    insert.write(&original_row).await.unwrap();
    insert.end().await.unwrap();

    let row = client
        .query("SELECT ?fields FROM test")
        .fetch_one::<MyRow>()
        .await
        .unwrap();

    assert_eq!(row, original_row);
}
