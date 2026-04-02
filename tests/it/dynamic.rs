//! Integration tests for dynamic (schema-driven) inserts.
//!
//! These tests require a running ClickHouse instance.

use clickhouse::sql::Identifier;
use serde_json::json;

#[tokio::test]
async fn inserts_simple_types() {
    let client = prepare_database!();
    let table = "dynamic_simple";

    client
        .query(
            "CREATE TABLE ?(id UInt64, name String, score Float64, active UInt8) \
             ENGINE = MergeTree ORDER BY id",
        )
        .with_option("wait_end_of_query", "1")
        .bind(Identifier(table))
        .execute()
        .await
        .unwrap();

    let mut insert = client.dynamic_insert(&test_database_name!(), table);
    insert
        .write_map(json!({"id": 1, "name": "alice", "score": 9.5, "active": 1}).as_object().unwrap())
        .await
        .unwrap();
    insert
        .write_map(json!({"id": 2, "name": "bob", "score": 7.2, "active": 0}).as_object().unwrap())
        .await
        .unwrap();
    let rows = insert.end().await.unwrap();
    assert_eq!(rows, 2);

    // Query back
    let result = client
        .query(&format!("SELECT id, name, score FROM {table} ORDER BY id"))
        .fetch_all::<(u64, String, f64)>()
        .await
        .unwrap();

    assert_eq!(result.len(), 2);
    assert_eq!(result[0], (1, "alice".to_string(), 9.5));
    assert_eq!(result[1], (2, "bob".to_string(), 7.2));
}

#[tokio::test]
async fn inserts_nullable_columns() {
    let client = prepare_database!();
    let table = "dynamic_nullable";

    client
        .query(
            "CREATE TABLE ?(id UInt64, label Nullable(String)) \
             ENGINE = MergeTree ORDER BY id",
        )
        .with_option("wait_end_of_query", "1")
        .bind(Identifier(table))
        .execute()
        .await
        .unwrap();

    let mut insert = client.dynamic_insert(&test_database_name!(), table);
    insert
        .write_map(json!({"id": 1, "label": "present"}).as_object().unwrap())
        .await
        .unwrap();
    insert
        .write_map(json!({"id": 2, "label": null}).as_object().unwrap())
        .await
        .unwrap();
    insert.end().await.unwrap();

    let result = client
        .query(&format!(
            "SELECT id, label FROM {table} ORDER BY id"
        ))
        .fetch_all::<(u64, Option<String>)>()
        .await
        .unwrap();

    assert_eq!(result.len(), 2);
    assert_eq!(result[0], (1, Some("present".to_string())));
    assert_eq!(result[1], (2, None));
}

#[tokio::test]
async fn skips_columns_with_defaults() {
    let client = prepare_database!();
    let table = "dynamic_defaults";

    client
        .query(
            "CREATE TABLE ?(\
                id UInt64, \
                name String, \
                created_at DateTime64(3) DEFAULT now64(3)\
             ) ENGINE = MergeTree ORDER BY id",
        )
        .with_option("wait_end_of_query", "1")
        .bind(Identifier(table))
        .execute()
        .await
        .unwrap();

    // Insert without created_at — should use server default
    let mut insert = client.dynamic_insert(&test_database_name!(), table);
    insert
        .write_map(json!({"id": 1, "name": "auto-ts"}).as_object().unwrap())
        .await
        .unwrap();
    insert.end().await.unwrap();

    // Verify row exists and created_at was populated by server
    let result = client
        .query(&format!(
            "SELECT id, name, created_at > 0 as has_ts FROM {table}"
        ))
        .fetch_all::<(u64, String, u8)>()
        .await
        .unwrap();

    assert_eq!(result.len(), 1);
    assert_eq!(result[0].0, 1);
    assert_eq!(result[0].1, "auto-ts");
    assert_eq!(result[0].2, 1); // created_at was filled
}

#[tokio::test]
async fn batcher_flushes_on_end() {
    let client = prepare_database!();
    let table = "dynamic_batcher";

    client
        .query(
            "CREATE TABLE ?(id UInt64, value String) ENGINE = MergeTree ORDER BY id",
        )
        .with_option("wait_end_of_query", "1")
        .bind(Identifier(table))
        .execute()
        .await
        .unwrap();

    let batcher = client.dynamic_batcher(
        &test_database_name!(),
        table,
        clickhouse::dynamic::DynamicBatchConfig {
            max_rows: 100,
            ..Default::default()
        },
    );

    for i in 0..10u64 {
        batcher
            .write_map(json!({"id": i, "value": format!("row-{i}")}).as_object().unwrap().clone())
            .await
            .unwrap();
    }

    let total = batcher.end().await.unwrap();
    assert_eq!(total, 10);

    let count = client
        .query(&format!("SELECT count() FROM {table}"))
        .fetch_one::<u64>()
        .await
        .unwrap();
    assert_eq!(count, 10);
}
