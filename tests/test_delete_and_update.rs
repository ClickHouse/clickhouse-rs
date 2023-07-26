use core::time::Duration;
use std::{collections::HashSet, thread::sleep};

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
        name: &'a str,
        list: Vec<i32>,
    }

    // Create a table.
    client
        .query(
            "
            CREATE TABLE test(no UInt32, name LowCardinality(String) , list Array(UInt32))
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
        insert
            .write(&MyRow {
                no: i,
                name: "foo",
                list: vec![1, 2, 3],
            })
            .await
            .unwrap();
    }

    insert.end().await.unwrap();

    let mut pk_vec = vec![];
    let mut set = HashSet::new();
    for i in 0..100 {
        set.insert(i);
        pk_vec.push(i as u64)
    }
    pk_vec.push(567 as u64);
    set.insert(567 as u64);
    pk_vec.push(545 as u64);
    set.insert(545 as u64);
    pk_vec.push(674 as u64);
    set.insert(674 as u64);
    pk_vec.push(873 as u64);
    set.insert(873 as u64);

    let delete = client.delete("test", "no", pk_vec.len());
    delete.delete(pk_vec).await.unwrap();
    sleep(Duration::from_secs(1));
    let mut cursor = client
        .query("SELECT ?fields FROM test")
        .fetch::<MyRow<'_>>()
        .unwrap();

    while let Some(row) = cursor.next().await.unwrap() {
        assert!(!set.contains(&(row.no as u64)));
    }

    for i in 700..750 {
        let update = client.update("test", "no", vec![format!("name"), format!("list")]);
        let vec = vec!["name1", "[2,5,8]"];
        update.update_fields(vec, i as u64).await.unwrap();
    }
    sleep(Duration::from_secs(2));

    let mut cursor = client
        .query("SELECT ?fields FROM test")
        .fetch::<MyRow<'_>>()
        .unwrap();

    while let Some(row) = cursor.next().await.unwrap() {
        if row.no >= 700 && row.no < 750 {
            assert_eq!(row.name, "name1");
            assert_eq!(row.list, vec![2, 5, 8]);
        } else {
            assert_eq!(row.name, "foo");
            assert_eq!(row.list, vec![1, 2, 3]);
        }
    }
}
