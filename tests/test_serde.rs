#![allow(unused_imports)] // TODO

use serde::{Deserialize, Serialize};

use clickhouse::Row;

mod common;

#[cfg(feature = "uuid")]
#[common::named]
#[tokio::test]
async fn uuid() {
    use uuid::Uuid;

    let client = common::prepare_database!();

    #[derive(Debug, Row, Serialize, Deserialize)]
    struct MyRow {
        #[serde(with = "clickhouse::serde::uuid")]
        uuid: Uuid,
    }

    client
        .query("CREATE TABLE test(uuid UUID) ENGINE = MergeTree ORDER BY uuid")
        .execute()
        .await
        .unwrap();

    let uuid = Uuid::new_v4();
    println!("uuid: {}", uuid);

    let mut insert = client.insert("test").unwrap();
    insert.write(&MyRow { uuid }).await.unwrap();
    insert.end().await.unwrap();

    let (row, row_uuid_str) = client
        .query("SELECT ?fields, toString(uuid) FROM test")
        .fetch_one::<(MyRow, String)>()
        .await
        .unwrap();

    assert_eq!(row.uuid, uuid);
    assert_eq!(row_uuid_str, uuid.to_string());
}
