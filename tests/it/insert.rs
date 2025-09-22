use crate::{create_simple_table, fetch_rows, flush_query_log, SimpleRow};
use clickhouse::{sql::Identifier, Row};
use serde::{Deserialize, Serialize};

#[tokio::test]
async fn keeps_client_options() {
    let table_name = "insert_keeps_client_options";
    let query_id = uuid::Uuid::new_v4().to_string();
    let (client_setting_name, client_setting_value) = ("max_block_size", "1000");
    let (insert_setting_name, insert_setting_value) = ("async_insert", "1");

    let client = prepare_database!().with_option(client_setting_name, client_setting_value);
    create_simple_table(&client, table_name).await;

    let row = SimpleRow::new(42, "foo");

    let mut insert = client
        .insert::<SimpleRow>(table_name)
        .await
        .unwrap()
        .with_option(insert_setting_name, insert_setting_value)
        .with_option("query_id", &query_id);

    insert.write(&row).await.unwrap();
    insert.end().await.unwrap();

    flush_query_log(&client).await;

    let (has_insert_setting, has_client_setting) = client
        .query(&format!(
            "
            SELECT
              Settings['{insert_setting_name}'] = '{insert_setting_value}',
              Settings['{client_setting_name}'] = '{client_setting_value}'
            FROM system.query_log
            WHERE query_id = ?
            AND type = 'QueryFinish'
            AND query_kind = 'Insert'
            "
        ))
        .bind(&query_id)
        .fetch_one::<(bool, bool)>()
        .await
        .unwrap();

    assert!(
        has_insert_setting, "{}",
        format!("should contain {insert_setting_name} = {insert_setting_value} (from the insert options)")
    );
    assert!(
        has_client_setting, "{}",
        format!("should contain {client_setting_name} = {client_setting_value} (from the client options)")
    );

    let rows = fetch_rows::<SimpleRow>(&client, table_name).await;
    assert_eq!(rows, vec!(row))
}

#[tokio::test]
async fn overrides_client_options() {
    let table_name = "insert_overrides_client_options";
    let query_id = uuid::Uuid::new_v4().to_string();
    let (setting_name, setting_value, override_value) = ("async_insert", "0", "1");

    let client = prepare_database!().with_option(setting_name, setting_value);
    create_simple_table(&client, table_name).await;

    let row = SimpleRow::new(42, "foo");

    let mut insert = client
        .insert::<SimpleRow>(table_name)
        .await
        .unwrap()
        .with_option(setting_name, override_value)
        .with_option("query_id", &query_id);

    insert.write(&row).await.unwrap();
    insert.end().await.unwrap();

    flush_query_log(&client).await;

    let has_setting_override = client
        .query(&format!(
            "
            SELECT Settings['{setting_name}'] = '{override_value}'
            FROM system.query_log
            WHERE query_id = ?
            AND type = 'QueryFinish'
            AND query_kind = 'Insert'
            "
        ))
        .bind(&query_id)
        .fetch_one::<bool>()
        .await
        .unwrap();

    assert!(
        has_setting_override,
        "{}",
        format!("should contain {setting_name} = {override_value} (from the insert options)")
    );

    let rows = fetch_rows::<SimpleRow>(&client, table_name).await;
    assert_eq!(rows, vec!(row))
}

#[tokio::test]
async fn empty_insert() {
    // https://github.com/ClickHouse/clickhouse-rs/issues/137

    let table_name = "insert_empty";
    let query_id = uuid::Uuid::new_v4().to_string();

    let client = prepare_database!();
    create_simple_table(&client, table_name).await;

    let insert = client
        .insert::<SimpleRow>(table_name)
        .await
        .unwrap()
        .with_option("query_id", &query_id);
    insert.end().await.unwrap();

    let rows = fetch_rows::<SimpleRow>(&client, table_name).await;
    assert!(rows.is_empty())
}

#[tokio::test]
async fn rename_insert() {
    #[derive(Debug, Row, Serialize, Deserialize, PartialEq)]
    #[serde(rename_all = "camelCase")]
    struct RenameRow {
        #[serde(rename = "fixId")]
        pub(crate) fix_id: u64,
        #[serde(rename = "extComplexId")]
        pub(crate) complex_id: String,
        pub(crate) ext_float: f64,
    }

    let table_name = "insert_rename";
    let query_id = uuid::Uuid::new_v4().to_string();

    let client = prepare_database!();
    client
        .query(
            "
            CREATE TABLE ?(
              fixId UInt64,
              extComplexId String,
              extFloat Float64
            )
            ENGINE = MergeTree
            ORDER BY fixId
            ",
        )
        .bind(Identifier(table_name))
        .execute()
        .await
        .unwrap();

    let row = RenameRow {
        fix_id: 42,
        complex_id: String::from("foo"),
        ext_float: 0.5,
    };

    let mut insert = client
        .insert::<RenameRow>(table_name)
        .await
        .unwrap()
        .with_option("query_id", &query_id);

    insert.write(&row).await.unwrap();
    insert.end().await.unwrap();

    flush_query_log(&client).await;

    let rows = fetch_rows::<RenameRow>(&client, table_name).await;
    assert_eq!(rows, vec!(row))
}

#[tokio::test]
async fn insert_from_cursor() {
    #[derive(Debug, Row, Serialize, Deserialize, PartialEq)]
    struct BorrowedRow<'a> {
        id: u64,
        data: &'a str,
    }

    let client = prepare_database!();
    create_simple_table(&client, "test").await;

    // Fill the table with initial data.
    let mut insert = client.insert::<BorrowedRow<'_>>("test").await.unwrap();
    for (i, data) in ["foo", "bar"].iter().enumerate() {
        let row = BorrowedRow { id: i as _, data };
        insert.write(&row).await.unwrap();
    }
    insert.end().await.unwrap();

    // Fetch the rows and insert them back.
    let mut cursor = client
        .query("SELECT id, data FROM test")
        .fetch::<BorrowedRow<'_>>()
        .unwrap();

    let mut insert = client.insert::<BorrowedRow<'_>>("test").await.unwrap();
    while let Some(row) = cursor.next().await.unwrap() {
        insert.write(&row).await.unwrap();
    }
    insert.end().await.unwrap();

    // Verify that the rows were inserted correctly.
    let mut cursor = client
        .query("SELECT id, data FROM test ORDER BY id")
        .fetch::<BorrowedRow<'_>>()
        .unwrap();
    assert_eq!(
        cursor.next().await.unwrap().as_ref(),
        Some(&BorrowedRow { id: 0, data: "foo" })
    );
    assert_eq!(
        cursor.next().await.unwrap().as_ref(),
        Some(&BorrowedRow { id: 0, data: "foo" })
    );
    assert_eq!(
        cursor.next().await.unwrap().as_ref(),
        Some(&BorrowedRow { id: 1, data: "bar" })
    );
    assert_eq!(
        cursor.next().await.unwrap().as_ref(),
        Some(&BorrowedRow { id: 1, data: "bar" })
    );
    assert_eq!(cursor.next().await.unwrap(), None);
}
