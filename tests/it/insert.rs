use crate::{SimpleRow, create_simple_table, fetch_rows, flush_query_log};
use clickhouse::{Row, sql::Identifier};
use serde::{Deserialize, Serialize};
use std::panic::AssertUnwindSafe;

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
        has_insert_setting,
        "{}",
        format!(
            "should contain {insert_setting_name} = {insert_setting_value} (from the insert options)"
        )
    );
    assert!(
        has_client_setting,
        "{}",
        format!(
            "should contain {client_setting_name} = {client_setting_value} (from the client options)"
        )
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

#[tokio::test]
async fn clear_cached_metadata() {
    #[derive(clickhouse::Row, serde::Serialize)]
    struct Foo {
        bar: i32,
        baz: String,
    }

    #[derive(
        clickhouse::Row,
        serde::Serialize,
        serde::Deserialize,
        PartialEq,
        Eq,
        Debug
    )]
    struct Foo2 {
        bar: i32,
    }

    let client = prepare_database!().with_validation(true);

    client
        .query("CREATE TABLE foo(bar Int32, baz String) ENGINE = MergeTree PRIMARY KEY(bar)")
        .execute()
        .await
        .unwrap();

    let mut insert = client.insert::<Foo>("foo").await.unwrap();

    insert
        .write(&Foo {
            bar: 1,
            baz: "Hello, world!".to_string(),
        })
        .await
        .unwrap();

    insert.end().await.unwrap();

    client
        .query("ALTER TABLE foo DROP COLUMN baz")
        .execute()
        .await
        .unwrap();

    let mut insert = client.insert::<Foo>("foo").await.unwrap();

    insert
        .write(&Foo {
            bar: 2,
            baz: "Hello, ClickHouse!".to_string(),
        })
        .await
        .unwrap();

    dbg!(
        insert
            .end()
            .await
            .expect_err("Insert metadata is invalid; this should error!")
    );

    client.clear_cached_metadata().await;

    let write_invalid = AssertUnwindSafe(async {
        let mut insert = client.insert::<Foo>("foo").await.unwrap();

        insert
            .write(&Foo {
                bar: 2,
                baz: "Hello, ClickHouse!".to_string(),
            })
            .await
            .expect_err("`Foo` should no longer be valid for the table");
    });

    assert_panic_msg!(write_invalid, ["1 columns", "2 fields", "bar", "baz"]);

    let mut insert = client.insert::<Foo2>("foo").await.unwrap();

    insert.write(&Foo2 { bar: 3 }).await.unwrap();

    insert.end().await.unwrap();

    let rows = client
        .query("SELECT * FROM foo")
        .fetch_all::<Foo2>()
        .await
        .unwrap();

    assert_eq!(*rows, [Foo2 { bar: 1 }, Foo2 { bar: 3 }]);
}
