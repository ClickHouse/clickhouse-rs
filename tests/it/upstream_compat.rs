//! Backwards compatibility tests for the upstream clickhouse-rs API.
//!
//! These tests exercise the EXACT public API that existing users of
//! ClickHouse/clickhouse-rs rely on. If any of these break, we've
//! regressed upstream compatibility.
//!
//! Every test here uses only types and methods from the original upstream
//! crate -- no native transport, no unified client, no fork-specific APIs.

use crate::get_client;
use clickhouse::{Client, Row};
use clickhouse::sql::Identifier;
use serde::{Deserialize, Serialize};

// -----------------------------------------------------------------------
// Client construction -- upstream builder API
// -----------------------------------------------------------------------

#[tokio::test]
async fn compat_client_default_builder() {
    // Upstream pattern: default + with_url + with_database
    let _client = Client::default()
        .with_url("http://localhost:8123")
        .with_database("default")
        .with_user("default")
        .with_password("");

    // Just verifying it compiles and doesn't panic.
}

#[tokio::test]
async fn compat_client_with_compression() {
    use clickhouse::Compression;
    let _client = Client::default()
        .with_url("http://localhost:8123")
        .with_compression(Compression::Lz4);
}

#[tokio::test]
async fn compat_client_with_options() {
    let _client = Client::default()
        .with_url("http://localhost:8123")
        .with_option("max_threads", "4")
        .with_option("connect_timeout", "10");
}

// -----------------------------------------------------------------------
// Query -- upstream query API
// -----------------------------------------------------------------------

#[tokio::test]
async fn compat_query_execute() {
    let client = prepare_database!();
    client
        .query("CREATE TABLE test (x UInt32) ENGINE = Memory")
        .execute()
        .await
        .unwrap();
}

#[tokio::test]
async fn compat_query_fetch_all() {
    let client = prepare_database!();
    let rows: Vec<u64> = client
        .query("SELECT number FROM system.numbers LIMIT 5")
        .fetch_all()
        .await
        .unwrap();
    assert_eq!(rows, vec![0, 1, 2, 3, 4]);
}

#[tokio::test]
async fn compat_query_fetch_one() {
    let client = prepare_database!();
    let row: u64 = client
        .query("SELECT toUInt64(42)")
        .fetch_one()
        .await
        .unwrap();
    assert_eq!(row, 42);
}

#[tokio::test]
async fn compat_query_fetch_optional() {
    let client = prepare_database!();
    let row: Option<u64> = client
        .query("SELECT number FROM system.numbers WHERE number > 999 LIMIT 1")
        .fetch_optional()
        .await
        .unwrap();
    assert!(row.is_some());
}

#[tokio::test]
async fn compat_query_bind() {
    let client = prepare_database!();
    let row: u8 = client
        .query("SELECT ?")
        .bind(42u8)
        .fetch_one()
        .await
        .unwrap();
    assert_eq!(row, 42);
}

#[tokio::test]
async fn compat_query_with_option() {
    let client = prepare_database!();
    let rows: Vec<u64> = client
        .query("SELECT number FROM system.numbers LIMIT 3")
        .with_option("max_threads", "1")
        .fetch_all()
        .await
        .unwrap();
    assert_eq!(rows.len(), 3);
}

#[tokio::test]
async fn compat_query_param() {
    let client = prepare_database!();
    let row: u32 = client
        .query("SELECT {val:UInt32}")
        .param("val", 99u32)
        .fetch_one()
        .await
        .unwrap();
    assert_eq!(row, 99);
}

// -----------------------------------------------------------------------
// Cursor -- upstream cursor iteration
// -----------------------------------------------------------------------

#[tokio::test]
async fn compat_cursor_next() {
    let client = prepare_database!();
    let mut cursor = client
        .query("SELECT number FROM system.numbers LIMIT 3")
        .fetch::<u64>()
        .unwrap();

    let mut results = Vec::new();
    while let Some(row) = cursor.next().await.unwrap() {
        results.push(row);
    }
    assert_eq!(results, vec![0, 1, 2]);
}

#[tokio::test]
async fn compat_cursor_bytes() {
    let client = prepare_database!();
    let mut cursor = client
        .query("SELECT number FROM system.numbers LIMIT 3")
        .fetch::<u64>()
        .unwrap();

    // received_bytes and decoded_bytes are upstream API
    let _ = cursor.received_bytes();
    let _ = cursor.decoded_bytes();

    while let Some(_) = cursor.next().await.unwrap() {}
    assert!(cursor.received_bytes() > 0);
}

// -----------------------------------------------------------------------
// Insert -- upstream typed insert
// -----------------------------------------------------------------------

#[derive(Debug, Row, Serialize, Deserialize, PartialEq)]
struct CompatRow {
    id: u64,
    name: String,
}

#[tokio::test]
async fn compat_insert_and_select() {
    let client = prepare_database!();
    client
        .query("CREATE TABLE test (id UInt64, name String) ENGINE = Memory")
        .execute()
        .await
        .unwrap();

    let mut insert = client.insert::<CompatRow>("test").await.unwrap();
    insert
        .write(&CompatRow {
            id: 1,
            name: "alice".into(),
        })
        .await
        .unwrap();
    insert
        .write(&CompatRow {
            id: 2,
            name: "bob".into(),
        })
        .await
        .unwrap();
    insert.end().await.unwrap();

    let rows: Vec<CompatRow> = client
        .query("SELECT id, name FROM test ORDER BY id")
        .fetch_all()
        .await
        .unwrap();

    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].name, "alice");
    assert_eq!(rows[1].name, "bob");
}

// -----------------------------------------------------------------------
// Borrowed rows -- upstream zero-copy pattern
// -----------------------------------------------------------------------

#[tokio::test]
async fn compat_borrowed_rows() {
    #[derive(Debug, Row, Serialize)]
    struct NameRow {
        name: String,
    }

    #[derive(Debug, Row, Deserialize, PartialEq)]
    struct BorrowedRow<'a> {
        name: &'a str,
    }

    let client = prepare_database!();
    client
        .query("CREATE TABLE test (name String) ENGINE = Memory")
        .execute()
        .await
        .unwrap();

    let mut insert = client.insert::<NameRow>("test").await.unwrap();
    insert
        .write(&NameRow { name: "hello".into() })
        .await
        .unwrap();
    insert.end().await.unwrap();

    let mut cursor = client
        .query("SELECT name FROM test")
        .fetch::<BorrowedRow<'_>>()
        .unwrap();

    let row = cursor.next().await.unwrap().unwrap();
    assert_eq!(row.name, "hello");
}

// -----------------------------------------------------------------------
// DDL with Identifier binding
// -----------------------------------------------------------------------

#[tokio::test]
async fn compat_identifier_bind() {
    let client = prepare_database!();
    let table = "compat_ident_test";

    client
        .query("CREATE TABLE ? (x UInt32) ENGINE = Memory")
        .bind(Identifier(table))
        .execute()
        .await
        .unwrap();

    client
        .query("INSERT INTO ? (x) VALUES (1)")
        .bind(Identifier(table))
        .execute()
        .await
        .unwrap();

    let count: u64 = client
        .query("SELECT count() FROM ?")
        .bind(Identifier(table))
        .fetch_one()
        .await
        .unwrap();
    assert_eq!(count, 1);
}

// -----------------------------------------------------------------------
// Error handling -- upstream error types
// -----------------------------------------------------------------------

#[tokio::test]
async fn compat_error_row_not_found() {
    let client = prepare_database!();
    let result = client
        .query("SELECT number FROM system.numbers WHERE number > 99999999 LIMIT 0")
        .fetch_one::<u64>()
        .await;
    assert!(result.is_err());
}

// -----------------------------------------------------------------------
// New HTTP features -- verify they exist alongside upstream API
// -----------------------------------------------------------------------

#[tokio::test]
async fn compat_new_ping() {
    let client = get_client();
    client.ping().await.unwrap();
}

#[tokio::test]
async fn compat_new_server_version() {
    let client = get_client();
    let ver = client.server_version().await.unwrap();
    assert!(ver.major > 0);
}

#[tokio::test]
async fn compat_new_query_id() {
    let client = get_client();
    // with_query_id is new but shouldn't break existing query flow
    client
        .query("SELECT 1")
        .with_query_id("compat-test-id")
        .execute()
        .await
        .unwrap();
}

#[tokio::test]
async fn compat_new_settings() {
    let client = get_client();
    let rows: Vec<u64> = client
        .query("SELECT number FROM system.numbers LIMIT 3")
        .with_settings([("max_threads", "1")])
        .fetch_all()
        .await
        .unwrap();
    assert_eq!(rows.len(), 3);
}
