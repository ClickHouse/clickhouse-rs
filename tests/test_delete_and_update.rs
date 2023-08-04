use core::time::Duration;
use std::thread::sleep;

use serde::{Deserialize, Serialize};

use clickhouse::Row;

mod common;

#[common::named]
#[tokio::test]
async fn test_update_delete() {
    let client = common::prepare_database!();

    #[derive(Debug, Row, Serialize, Deserialize)]
    struct MyRow<'a> {
        no: u32,
        no2: u32,
        name: &'a str,
        list: Vec<i32>,
    }

    // Create a table.
    client
        .query(
            "
            CREATE TABLE test(no UInt32, no2 UInt32, name LowCardinality(String) , list Array(UInt32))
            ENGINE = MergeTree
            PRIMARY KEY (no,no2)
        ",
        )
        .execute()
        .await
        .unwrap();

    // Write to the table.
    let mut insert = client.insert("test").unwrap();
    for i in 0..5 {
        insert
            .write(&MyRow {
                no: i,
                no2: 1,
                name: "foo",
                list: vec![1, 2, 3],
            })
            .await
            .unwrap();
    }
    for i in 0..5 {
        insert
            .write(&MyRow {
                no: i,
                no2: 2,
                name: "foo",
                list: vec![1, 2, 3],
            })
            .await
            .unwrap();
    }

    insert.end().await.unwrap();

    let delete = client.delete("test", vec!["no".to_string(), "no2".to_string()]);
    delete.delete(vec![1, 1]).await.unwrap();
    sleep(Duration::from_secs(1));

    let update = client.update(
        "test",
        vec!["no".to_string(), "no2".to_string()],
        vec![format!("name"), format!("list")],
    );
    let vec = vec!["name1", "[2,5,8]"];
    update.update_fields(vec, vec![2, 2]).await.unwrap();

    sleep(Duration::from_secs(2));

    let mut cursor = client
        .query("SELECT ?fields FROM test")
        .fetch::<MyRow<'_>>()
        .unwrap();

    while let Some(row) = cursor.next().await.unwrap() {
        if row.no == 1 {
            assert_ne!(row.no2, 1);
        } else if row.no == 2 && row.no2 == 2 {
            assert_eq!(row.name, "name1");
            assert_eq!(row.list, vec![2, 5, 8]);
        } else {
            assert_eq!(row.name, "foo");
            assert_eq!(row.list, vec![1, 2, 3]);
        }
    }
}
