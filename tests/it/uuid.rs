#![cfg(feature = "uuid")]

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use clickhouse::Row;

#[tokio::test]
async fn smoke() {
    let client = prepare_database!();

    #[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Row)]
    struct MyRow {
        #[serde(with = "clickhouse::serde::uuid")]
        uuid: Uuid,
        #[serde(with = "clickhouse::serde::uuid::option")]
        uuid_opt: Option<Uuid>,
    }

    client
        .query(
            "
            CREATE TABLE test(
                uuid UUID,
                uuid_opt Nullable(UUID)
            ) ENGINE = MergeTree ORDER BY uuid
        ",
        )
        .execute()
        .await
        .unwrap();

    let uuid = Uuid::new_v4();
    println!("uuid: {uuid}");

    let original_row = MyRow {
        uuid,
        uuid_opt: Some(uuid),
    };

    let mut insert = client.insert("test").unwrap();
    insert.write(&original_row).await.unwrap();
    insert.end().await.unwrap();

    let (row, row_uuid_str) = client
        .query("SELECT ?fields, toString(uuid) FROM test")
        .fetch_one::<(MyRow, String)>()
        .await
        .unwrap();

    assert_eq!(row, original_row);
    assert_eq!(row_uuid_str, original_row.uuid.to_string());
}
