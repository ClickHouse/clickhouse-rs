//! RowBinary integration tests for every ClickHouse type via DynamicInsert.
//!
//! Each test creates a real table, inserts via `dynamic_insert` (RowBinary wire format),
//! then queries back to verify the data landed correctly. This catches encoder bugs that
//! unit tests on wire bytes alone cannot — the real ClickHouse server is the final arbiter.
//!
//! Covers: DateTime64, DateTime, Date, Date32, JSON, Nullable(JSON), Bool, UUID, IPv4, IPv6,
//! Array, Map, LowCardinality, Enum8, FixedString, Decimal, and composite DFE common header.

use clickhouse::sql::Identifier;
use serde_json::json;

// ============================================================================
// DateTime64 — the original issue #18
// ============================================================================

#[tokio::test]
async fn dynamic_datetime64_from_string() {
    let client = prepare_database!();
    let table = "dt64_str";

    client
        .query("CREATE TABLE ?(ts DateTime64(3)) ENGINE = MergeTree ORDER BY ts")
        .with_option("wait_end_of_query", "1")
        .bind(Identifier(table))
        .execute()
        .await
        .unwrap();

    let mut insert = client.dynamic_insert(&test_database_name!(), table);
    insert
        .write_map(
            json!({"ts": "2024-12-25 10:30:00.123"})
                .as_object()
                .unwrap(),
        )
        .await
        .unwrap();
    insert
        .write_map(
            json!({"ts": "2024-12-25T10:30:00.456Z"})
                .as_object()
                .unwrap(),
        )
        .await
        .unwrap();
    insert
        .write_map(json!({"ts": "2024-12-25 10:30:00"}).as_object().unwrap())
        .await
        .unwrap();
    let rows = insert.end().await.unwrap();
    assert_eq!(rows, 3);

    let result = client
        .query(&format!(
            "SELECT toUnixTimestamp64Milli(ts) FROM {table} ORDER BY ts"
        ))
        .fetch_all::<i64>()
        .await
        .unwrap();

    assert_eq!(result.len(), 3);
    // "2024-12-25 10:30:00.000" = 1735122600000 ms
    assert_eq!(result[0], 1735122600000);
    // "2024-12-25 10:30:00.123" = 1735122600123 ms
    assert_eq!(result[1], 1735122600123);
    // "2024-12-25 10:30:00.456" = 1735122600456 ms
    assert_eq!(result[2], 1735122600456);
}

#[tokio::test]
async fn dynamic_datetime64_from_epoch_number() {
    let client = prepare_database!();
    let table = "dt64_epoch";

    client
        .query("CREATE TABLE ?(ts DateTime64(3)) ENGINE = MergeTree ORDER BY ts")
        .with_option("wait_end_of_query", "1")
        .bind(Identifier(table))
        .execute()
        .await
        .unwrap();

    let mut insert = client.dynamic_insert(&test_database_name!(), table);
    insert
        .write_map(json!({"ts": 1735122600123_i64}).as_object().unwrap())
        .await
        .unwrap();
    let rows = insert.end().await.unwrap();
    assert_eq!(rows, 1);

    let result = client
        .query(&format!("SELECT toUnixTimestamp64Milli(ts) FROM {table}"))
        .fetch_one::<i64>()
        .await
        .unwrap();
    assert_eq!(result, 1735122600123);
}

#[tokio::test]
async fn dynamic_datetime64_precision_6() {
    let client = prepare_database!();
    let table = "dt64_us";

    client
        .query("CREATE TABLE ?(ts DateTime64(6)) ENGINE = MergeTree ORDER BY ts")
        .with_option("wait_end_of_query", "1")
        .bind(Identifier(table))
        .execute()
        .await
        .unwrap();

    let mut insert = client.dynamic_insert(&test_database_name!(), table);
    insert
        .write_map(
            json!({"ts": "2024-12-25 10:30:00.123456"})
                .as_object()
                .unwrap(),
        )
        .await
        .unwrap();
    let rows = insert.end().await.unwrap();
    assert_eq!(rows, 1);

    let result = client
        .query(&format!("SELECT toUnixTimestamp64Micro(ts) FROM {table}"))
        .fetch_one::<i64>()
        .await
        .unwrap();
    assert_eq!(result, 1735122600123456);
}

#[tokio::test]
async fn dynamic_nullable_datetime64() {
    let client = prepare_database!();
    let table = "dt64_nullable";

    client
        .query("CREATE TABLE ?(ts Nullable(DateTime64(3))) ENGINE = MergeTree ORDER BY tuple()")
        .with_option("wait_end_of_query", "1")
        .bind(Identifier(table))
        .execute()
        .await
        .unwrap();

    let mut insert = client.dynamic_insert(&test_database_name!(), table);
    insert
        .write_map(
            json!({"ts": "2024-12-25 10:30:00.123"})
                .as_object()
                .unwrap(),
        )
        .await
        .unwrap();
    insert
        .write_map(json!({"ts": null}).as_object().unwrap())
        .await
        .unwrap();
    let rows = insert.end().await.unwrap();
    assert_eq!(rows, 2);

    let result = client
        .query(&format!(
            "SELECT if(isNull(ts), NULL, toUnixTimestamp64Milli(assumeNotNull(ts))) \
             FROM {table} ORDER BY rowNumberInAllBlocks()"
        ))
        .fetch_all::<Option<i64>>()
        .await
        .unwrap();
    assert_eq!(result[0], Some(1735122600123));
    assert_eq!(result[1], None);
}

// ============================================================================
// DateTime (seconds since epoch, UInt32)
// ============================================================================

#[tokio::test]
async fn dynamic_datetime_from_string() {
    let client = prepare_database!();
    let table = "dt_str";

    client
        .query("CREATE TABLE ?(ts DateTime) ENGINE = MergeTree ORDER BY ts")
        .with_option("wait_end_of_query", "1")
        .bind(Identifier(table))
        .execute()
        .await
        .unwrap();

    let mut insert = client.dynamic_insert(&test_database_name!(), table);
    insert
        .write_map(json!({"ts": "2024-12-25 10:30:00"}).as_object().unwrap())
        .await
        .unwrap();
    insert
        .write_map(json!({"ts": 1735122600_u64}).as_object().unwrap())
        .await
        .unwrap();
    let rows = insert.end().await.unwrap();
    assert_eq!(rows, 2);

    let result = client
        .query(&format!(
            "SELECT toUnixTimestamp(ts) FROM {table} ORDER BY ts"
        ))
        .fetch_all::<u32>()
        .await
        .unwrap();
    assert_eq!(result.len(), 2);
    assert_eq!(result[0], 1735122600);
    assert_eq!(result[1], 1735122600);
}

// ============================================================================
// Date (days since epoch, UInt16) and Date32 (Int32)
// ============================================================================

#[tokio::test]
async fn dynamic_date_from_string() {
    let client = prepare_database!();
    let table = "date_str";

    client
        .query("CREATE TABLE ?(d Date) ENGINE = MergeTree ORDER BY d")
        .with_option("wait_end_of_query", "1")
        .bind(Identifier(table))
        .execute()
        .await
        .unwrap();

    let mut insert = client.dynamic_insert(&test_database_name!(), table);
    insert
        .write_map(json!({"d": "2024-12-25"}).as_object().unwrap())
        .await
        .unwrap();
    insert
        .write_map(json!({"d": "1970-01-01"}).as_object().unwrap())
        .await
        .unwrap();
    let rows = insert.end().await.unwrap();
    assert_eq!(rows, 2);

    let result = client
        .query(&format!("SELECT toString(d) FROM {table} ORDER BY d"))
        .fetch_all::<String>()
        .await
        .unwrap();
    assert_eq!(result[0], "1970-01-01");
    assert_eq!(result[1], "2024-12-25");
}

#[tokio::test]
async fn dynamic_date32_from_string() {
    let client = prepare_database!();
    let table = "date32_str";

    client
        .query("CREATE TABLE ?(d Date32) ENGINE = MergeTree ORDER BY d")
        .with_option("wait_end_of_query", "1")
        .bind(Identifier(table))
        .execute()
        .await
        .unwrap();

    let mut insert = client.dynamic_insert(&test_database_name!(), table);
    insert
        .write_map(json!({"d": "2024-12-25"}).as_object().unwrap())
        .await
        .unwrap();
    let rows = insert.end().await.unwrap();
    assert_eq!(rows, 1);

    let result = client
        .query(&format!("SELECT toString(d) FROM {table}"))
        .fetch_one::<String>()
        .await
        .unwrap();
    assert_eq!(result, "2024-12-25");
}

// ============================================================================
// JSON type — the _json column bug
// ============================================================================

#[tokio::test]
async fn dynamic_json_from_object() {
    let client = prepare_database!();
    let table = "json_obj";

    // JSON type requires CH 25.3+; skip gracefully
    if client
        .query(&format!(
            "CREATE TABLE {table}(data JSON) ENGINE = MergeTree ORDER BY tuple()"
        ))
        .with_option("wait_end_of_query", "1")
        .execute()
        .await
        .is_err()
    {
        eprintln!("Skipping: JSON type not supported");
        return;
    }

    let mut insert = client.dynamic_insert(&test_database_name!(), table);
    insert
        .write_map(
            json!({"data": {"key": "value", "num": 42}})
                .as_object()
                .unwrap(),
        )
        .await
        .unwrap();
    let rows = insert.end().await.unwrap();
    assert_eq!(rows, 1);

    let result = client
        .query(&format!("SELECT toString(data.key) FROM {table}"))
        .fetch_one::<String>()
        .await
        .unwrap();
    assert_eq!(result, "value");
}

#[tokio::test]
async fn dynamic_json_from_string() {
    let client = prepare_database!();
    let table = "json_str";

    if client
        .query(&format!(
            "CREATE TABLE {table}(data JSON) ENGINE = MergeTree ORDER BY tuple()"
        ))
        .with_option("wait_end_of_query", "1")
        .execute()
        .await
        .is_err()
    {
        eprintln!("Skipping: JSON type not supported");
        return;
    }

    // String containing JSON — the exact pattern from dfe-loader's _json column
    let raw_json = r#"{"event":"login","user":"alice"}"#;
    let mut insert = client.dynamic_insert(&test_database_name!(), table);
    insert
        .write_map(json!({"data": raw_json}).as_object().unwrap())
        .await
        .unwrap();
    let rows = insert.end().await.unwrap();
    assert_eq!(rows, 1);

    let result = client
        .query(&format!(
            "SELECT toString(data.event), toString(data.user) FROM {table}"
        ))
        .fetch_one::<(String, String)>()
        .await
        .unwrap();
    assert_eq!(result, ("login".to_string(), "alice".to_string()));
}

#[tokio::test]
async fn dynamic_nullable_json() {
    let client = prepare_database!();
    let table = "json_nullable";

    if client
        .query(&format!(
            "CREATE TABLE {table}(id UInt32, data Nullable(JSON)) ENGINE = MergeTree ORDER BY id"
        ))
        .with_option("wait_end_of_query", "1")
        .execute()
        .await
        .is_err()
    {
        eprintln!("Skipping: Nullable(JSON) not supported");
        return;
    }

    let mut insert = client.dynamic_insert(&test_database_name!(), table);
    insert
        .write_map(
            json!({"id": 1, "data": r#"{"key":"val"}"#})
                .as_object()
                .unwrap(),
        )
        .await
        .unwrap();
    insert
        .write_map(json!({"id": 2, "data": null}).as_object().unwrap())
        .await
        .unwrap();
    let rows = insert.end().await.unwrap();
    assert_eq!(rows, 2);

    let result = client
        .query(&format!("SELECT id, isNull(data) FROM {table} ORDER BY id"))
        .fetch_all::<(u32, u8)>()
        .await
        .unwrap();
    assert_eq!(result[0], (1, 0)); // not null
    assert_eq!(result[1], (2, 1)); // null
}

// ============================================================================
// Bool
// ============================================================================

#[tokio::test]
async fn dynamic_bool_variants() {
    let client = prepare_database!();
    let table = "bool_var";

    client
        .query("CREATE TABLE ?(b Bool) ENGINE = MergeTree ORDER BY tuple()")
        .with_option("wait_end_of_query", "1")
        .bind(Identifier(table))
        .execute()
        .await
        .unwrap();

    let mut insert = client.dynamic_insert(&test_database_name!(), table);
    // Bool values
    insert
        .write_map(json!({"b": true}).as_object().unwrap())
        .await
        .unwrap();
    insert
        .write_map(json!({"b": false}).as_object().unwrap())
        .await
        .unwrap();
    // Integer values
    insert
        .write_map(json!({"b": 1}).as_object().unwrap())
        .await
        .unwrap();
    insert
        .write_map(json!({"b": 0}).as_object().unwrap())
        .await
        .unwrap();
    // String values
    insert
        .write_map(json!({"b": "true"}).as_object().unwrap())
        .await
        .unwrap();
    insert
        .write_map(json!({"b": "false"}).as_object().unwrap())
        .await
        .unwrap();
    insert
        .write_map(json!({"b": "yes"}).as_object().unwrap())
        .await
        .unwrap();
    insert
        .write_map(json!({"b": "no"}).as_object().unwrap())
        .await
        .unwrap();
    let rows = insert.end().await.unwrap();
    assert_eq!(rows, 8);

    let result = client
        .query(&format!(
            "SELECT toUInt8(b) FROM {table} ORDER BY rowNumberInAllBlocks()"
        ))
        .fetch_all::<u8>()
        .await
        .unwrap();
    assert_eq!(result, vec![1, 0, 1, 0, 1, 0, 1, 0]);
}

// ============================================================================
// UUID
// ============================================================================

#[tokio::test]
async fn dynamic_uuid_variants() {
    let client = prepare_database!();
    let table = "uuid_var";

    client
        .query("CREATE TABLE ?(id UUID) ENGINE = MergeTree ORDER BY tuple()")
        .with_option("wait_end_of_query", "1")
        .bind(Identifier(table))
        .execute()
        .await
        .unwrap();

    let mut insert = client.dynamic_insert(&test_database_name!(), table);
    // Standard format
    insert
        .write_map(
            json!({"id": "550e8400-e29b-41d4-a716-446655440000"})
                .as_object()
                .unwrap(),
        )
        .await
        .unwrap();
    // No hyphens
    insert
        .write_map(
            json!({"id": "550e8400e29b41d4a716446655440001"})
                .as_object()
                .unwrap(),
        )
        .await
        .unwrap();
    let rows = insert.end().await.unwrap();
    assert_eq!(rows, 2);

    let result = client
        .query(&format!("SELECT toString(id) FROM {table} ORDER BY id"))
        .fetch_all::<String>()
        .await
        .unwrap();
    assert_eq!(result.len(), 2);
    assert!(result[0].contains("550e8400"));
    assert!(result[1].contains("550e8400"));
}

// ============================================================================
// IPv4 / IPv6
// ============================================================================

#[tokio::test]
async fn dynamic_ipv4() {
    let client = prepare_database!();
    let table = "ipv4_test";

    client
        .query("CREATE TABLE ?(ip IPv4) ENGINE = MergeTree ORDER BY ip")
        .with_option("wait_end_of_query", "1")
        .bind(Identifier(table))
        .execute()
        .await
        .unwrap();

    let mut insert = client.dynamic_insert(&test_database_name!(), table);
    insert
        .write_map(json!({"ip": "192.168.1.1"}).as_object().unwrap())
        .await
        .unwrap();
    insert
        .write_map(json!({"ip": "10.0.0.1"}).as_object().unwrap())
        .await
        .unwrap();
    let rows = insert.end().await.unwrap();
    assert_eq!(rows, 2);

    let result = client
        .query(&format!("SELECT toString(ip) FROM {table} ORDER BY ip"))
        .fetch_all::<String>()
        .await
        .unwrap();
    assert_eq!(result[0], "10.0.0.1");
    assert_eq!(result[1], "192.168.1.1");
}

#[tokio::test]
async fn dynamic_ipv6() {
    let client = prepare_database!();
    let table = "ipv6_test";

    client
        .query("CREATE TABLE ?(ip IPv6) ENGINE = MergeTree ORDER BY ip")
        .with_option("wait_end_of_query", "1")
        .bind(Identifier(table))
        .execute()
        .await
        .unwrap();

    let mut insert = client.dynamic_insert(&test_database_name!(), table);
    insert
        .write_map(json!({"ip": "::1"}).as_object().unwrap())
        .await
        .unwrap();
    insert
        .write_map(json!({"ip": "2001:db8::1"}).as_object().unwrap())
        .await
        .unwrap();
    let rows = insert.end().await.unwrap();
    assert_eq!(rows, 2);

    let result = client
        .query(&format!("SELECT toString(ip) FROM {table} ORDER BY ip"))
        .fetch_all::<String>()
        .await
        .unwrap();
    assert_eq!(result.len(), 2);
    assert!(result.iter().any(|s| s.contains("::1")));
}

// ============================================================================
// Array
// ============================================================================

#[tokio::test]
async fn dynamic_array_types() {
    let client = prepare_database!();
    let table = "arr_test";

    client
        .query(
            "CREATE TABLE ?(id UInt32, nums Array(UInt32), tags Array(String)) \
             ENGINE = MergeTree ORDER BY id",
        )
        .with_option("wait_end_of_query", "1")
        .bind(Identifier(table))
        .execute()
        .await
        .unwrap();

    let mut insert = client.dynamic_insert(&test_database_name!(), table);
    insert
        .write_map(
            json!({"id": 1, "nums": [10, 20, 30], "tags": ["a", "b"]})
                .as_object()
                .unwrap(),
        )
        .await
        .unwrap();
    insert
        .write_map(
            json!({"id": 2, "nums": [], "tags": ["c"]})
                .as_object()
                .unwrap(),
        )
        .await
        .unwrap();
    let rows = insert.end().await.unwrap();
    assert_eq!(rows, 2);

    let result = client
        .query(&format!(
            "SELECT id, length(nums), length(tags) FROM {table} ORDER BY id"
        ))
        .fetch_all::<(u32, u64, u64)>()
        .await
        .unwrap();
    assert_eq!(result[0], (1, 3, 2));
    assert_eq!(result[1], (2, 0, 1));
}

// ============================================================================
// Map
// ============================================================================

#[tokio::test]
async fn dynamic_map_type() {
    let client = prepare_database!();
    let table = "map_test";

    client
        .query("CREATE TABLE ?(m Map(String, UInt64)) ENGINE = MergeTree ORDER BY tuple()")
        .with_option("wait_end_of_query", "1")
        .bind(Identifier(table))
        .execute()
        .await
        .unwrap();

    let mut insert = client.dynamic_insert(&test_database_name!(), table);
    insert
        .write_map(
            json!({"m": {"clicks": 10, "views": 100}})
                .as_object()
                .unwrap(),
        )
        .await
        .unwrap();
    insert
        .write_map(json!({"m": {}}).as_object().unwrap())
        .await
        .unwrap();
    let rows = insert.end().await.unwrap();
    assert_eq!(rows, 2);

    let result = client
        .query(&format!("SELECT length(m) FROM {table} ORDER BY length(m)"))
        .fetch_all::<u64>()
        .await
        .unwrap();
    assert_eq!(result, vec![0, 2]);
}

// ============================================================================
// LowCardinality(String)
// ============================================================================

#[tokio::test]
async fn dynamic_lowcardinality_string() {
    let client = prepare_database!();
    let table = "lc_str";

    client
        .query(
            "CREATE TABLE ?(id UInt32, cat LowCardinality(String)) \
             ENGINE = MergeTree ORDER BY id",
        )
        .with_option("wait_end_of_query", "1")
        .bind(Identifier(table))
        .execute()
        .await
        .unwrap();

    let categories = ["auth", "api", "web", "auth", "api"];
    let mut insert = client.dynamic_insert(&test_database_name!(), table);
    for (i, cat) in categories.iter().enumerate() {
        insert
            .write_map(json!({"id": i as u32, "cat": cat}).as_object().unwrap())
            .await
            .unwrap();
    }
    let rows = insert.end().await.unwrap();
    assert_eq!(rows, 5);

    let result = client
        .query(&format!("SELECT cat FROM {table} ORDER BY id"))
        .fetch_all::<String>()
        .await
        .unwrap();
    assert_eq!(result, vec!["auth", "api", "web", "auth", "api"]);
}

// ============================================================================
// Enum8
// ============================================================================

#[tokio::test]
async fn dynamic_enum8() {
    let client = prepare_database!();
    let table = "enum8_test";

    client
        .query(
            "CREATE TABLE ?(e Enum8('low'=1, 'medium'=2, 'high'=3)) \
             ENGINE = MergeTree ORDER BY tuple()",
        )
        .with_option("wait_end_of_query", "1")
        .bind(Identifier(table))
        .execute()
        .await
        .unwrap();

    let mut insert = client.dynamic_insert(&test_database_name!(), table);
    insert
        .write_map(json!({"e": 1}).as_object().unwrap())
        .await
        .unwrap();
    insert
        .write_map(json!({"e": 3}).as_object().unwrap())
        .await
        .unwrap();
    let rows = insert.end().await.unwrap();
    assert_eq!(rows, 2);

    let result = client
        .query(&format!("SELECT toString(e) FROM {table} ORDER BY e"))
        .fetch_all::<String>()
        .await
        .unwrap();
    assert_eq!(result, vec!["low", "high"]);
}

// ============================================================================
// FixedString
// ============================================================================

#[tokio::test]
async fn dynamic_fixed_string() {
    let client = prepare_database!();
    let table = "fstr_test";

    client
        .query("CREATE TABLE ?(code FixedString(2)) ENGINE = MergeTree ORDER BY code")
        .with_option("wait_end_of_query", "1")
        .bind(Identifier(table))
        .execute()
        .await
        .unwrap();

    let mut insert = client.dynamic_insert(&test_database_name!(), table);
    insert
        .write_map(json!({"code": "AU"}).as_object().unwrap())
        .await
        .unwrap();
    insert
        .write_map(json!({"code": "US"}).as_object().unwrap())
        .await
        .unwrap();
    let rows = insert.end().await.unwrap();
    assert_eq!(rows, 2);

    let result = client
        .query(&format!("SELECT toString(code) FROM {table} ORDER BY code"))
        .fetch_all::<String>()
        .await
        .unwrap();
    assert_eq!(result.len(), 2);
    assert!(result.contains(&"AU".to_string()));
    assert!(result.contains(&"US".to_string()));
}

// ============================================================================
// Decimal types
// ============================================================================

#[tokio::test]
async fn dynamic_decimal64() {
    let client = prepare_database!();
    let table = "dec64_test";

    client
        .query("CREATE TABLE ?(val Decimal64(2)) ENGINE = MergeTree ORDER BY tuple()")
        .with_option("wait_end_of_query", "1")
        .bind(Identifier(table))
        .execute()
        .await
        .unwrap();

    let mut insert = client.dynamic_insert(&test_database_name!(), table);
    // Decimal64(2): stored as i64 scaled by 10^2 → 1234 means 12.34
    insert
        .write_map(json!({"val": 1234}).as_object().unwrap())
        .await
        .unwrap();
    let rows = insert.end().await.unwrap();
    assert_eq!(rows, 1);

    let result = client
        .query(&format!("SELECT toFloat64(val) FROM {table}"))
        .fetch_one::<f64>()
        .await
        .unwrap();
    assert!((result - 12.34).abs() < 0.001);
}

// ============================================================================
// Composite: DFE common header (the real-world scenario)
// ============================================================================

#[tokio::test]
async fn dynamic_dfe_common_header() {
    let client = prepare_database!();
    let table = "dfe_common";

    // Create table matching the DFE common header schema
    let ddl = format!(
        "CREATE TABLE {table}(\
            _timestamp DateTime64(3),\
            _timestamp_load DateTime64(3) DEFAULT now64(3),\
            _uuid UUID DEFAULT generateUUIDv7(),\
            _org_id LowCardinality(String),\
            _source LowCardinality(String),\
            _raw Nullable(String),\
            _json Nullable(JSON),\
            _tags Nullable(JSON)\
        ) ENGINE = MergeTree ORDER BY (_org_id, _timestamp_load, _uuid)"
    );
    if client
        .query(&ddl)
        .with_option("wait_end_of_query", "1")
        .execute()
        .await
        .is_err()
    {
        eprintln!("Skipping: common header DDL failed (JSON type requires CH 25.3+)");
        return;
    }

    let db_name = test_database_name!();

    // Insert a row mimicking what dfe-loader sends: _timestamp as string,
    // _json as raw JSON string, defaults omitted (_uuid, _timestamp_load)
    let mut insert = client.dynamic_insert(&db_name, table);
    insert
        .write_map(
            json!({
                "_timestamp": "2024-12-25 10:30:00.123",
                "_org_id": "acme",
                "_source": "auth",
                "_raw": "Dec 25 10:30:00 server sshd[1234]: Accepted publickey",
                "_json": r#"{"event":"login","user":"alice","src":"10.0.0.1"}"#,
                "_tags": r#"{"collector":"vector","version":"0.42"}"#
            })
            .as_object()
            .unwrap(),
        )
        .await
        .unwrap();
    // Second row with nulls for optional fields
    insert
        .write_map(
            json!({
                "_timestamp": "2024-12-25 10:31:00.000",
                "_org_id": "acme",
                "_source": "auth",
                "_raw": null,
                "_json": null,
                "_tags": null
            })
            .as_object()
            .unwrap(),
        )
        .await
        .unwrap();
    let rows = insert.end().await.unwrap();
    assert_eq!(rows, 2);

    // Verify timestamps
    let ts_result = client
        .query(&format!(
            "SELECT toUnixTimestamp64Milli(_timestamp) FROM {table} ORDER BY _timestamp"
        ))
        .fetch_all::<i64>()
        .await
        .unwrap();
    assert_eq!(ts_result[0], 1735122600123);
    assert_eq!(ts_result[1], 1735122660000);

    // Verify _org_id
    let org = client
        .query(&format!("SELECT DISTINCT _org_id FROM {table}"))
        .fetch_one::<String>()
        .await
        .unwrap();
    assert_eq!(org, "acme");

    // Verify _json sub-column access (CH 25.3+ JSON type)
    let event = client
        .query(&format!(
            "SELECT toString(_json.event) FROM {table} WHERE _json IS NOT NULL"
        ))
        .fetch_one::<String>()
        .await
        .unwrap();
    assert_eq!(event, "login");

    // Verify _uuid was auto-generated (DEFAULT generateUUIDv7)
    let uuid_count = client
        .query(&format!(
            "SELECT count() FROM {table} WHERE _uuid != '00000000-0000-0000-0000-000000000000'"
        ))
        .fetch_one::<u64>()
        .await
        .unwrap();
    assert_eq!(uuid_count, 2);

    // Verify _timestamp_load was auto-generated (DEFAULT now64(3))
    let load_count = client
        .query(&format!(
            "SELECT count() FROM {table} WHERE _timestamp_load > 0"
        ))
        .fetch_one::<u64>()
        .await
        .unwrap();
    assert_eq!(load_count, 2);
}

// ============================================================================
// Nullable/non-nullable tests for ALL types
// ============================================================================

/// Wide table with every supported type in both nullable and non-nullable forms.
/// Inserts rows with values and NULLs, reads back to verify correctness.
#[tokio::test]
async fn dynamic_nullable_all_types() {
    let client = prepare_database!();
    let table = "nullable_all";

    // Create table with every type in both nullable and non-nullable forms.
    // ORDER BY tuple() since we have nullable columns that can't be in ORDER BY.
    client
        .query(&format!(
            "CREATE TABLE {table}(\
                id UInt64,\
                \
                val_u8 UInt8, n_u8 Nullable(UInt8),\
                val_u16 UInt16, n_u16 Nullable(UInt16),\
                val_u32 UInt32, n_u32 Nullable(UInt32),\
                val_u64 UInt64, n_u64 Nullable(UInt64),\
                val_i8 Int8, n_i8 Nullable(Int8),\
                val_i16 Int16, n_i16 Nullable(Int16),\
                val_i32 Int32, n_i32 Nullable(Int32),\
                val_i64 Int64, n_i64 Nullable(Int64),\
                \
                val_f32 Float32, n_f32 Nullable(Float32),\
                val_f64 Float64, n_f64 Nullable(Float64),\
                \
                val_bool Bool, n_bool Nullable(UInt8),\
                \
                val_str String, n_str Nullable(String),\
                val_fstr FixedString(4), n_fstr Nullable(FixedString(4)),\
                \
                val_date Date, n_date Nullable(Date),\
                val_date32 Date32, n_date32 Nullable(Date32),\
                val_dt DateTime, n_dt Nullable(DateTime),\
                val_dt64_3 DateTime64(3), n_dt64_3 Nullable(DateTime64(3)),\
                val_dt64_6 DateTime64(6), n_dt64_6 Nullable(DateTime64(6)),\
                \
                val_uuid UUID, n_uuid Nullable(UUID),\
                val_ip4 IPv4, n_ip4 Nullable(IPv4),\
                val_ip6 IPv6, n_ip6 Nullable(IPv6),\
                \
                val_dec64 Decimal64(2), n_dec64 Nullable(Decimal64(2)),\
                \
                val_enum8 Enum8('a'=1,'b'=2), n_enum8 Nullable(Int8),\
                val_enum16 Enum16('x'=100,'y'=200),\
                \
                val_arr_u32 Array(UInt32),\
                val_arr_str Array(String),\
                val_arr_nstr Array(Nullable(String)),\
                val_map Map(String, UInt64),\
                val_lc LowCardinality(String),\
                val_lc_n LowCardinality(Nullable(String))\
            ) ENGINE = MergeTree ORDER BY id"
        ))
        .with_option("wait_end_of_query", "1")
        .execute()
        .await
        .unwrap();

    let db_name = test_database_name!();

    // ---- Row 1: all columns with non-NULL values ----
    let mut insert = client.dynamic_insert(&db_name, table);
    insert
        .write_map(
            json!({
                "id": 1,
                "val_u8": 255, "n_u8": 42,
                "val_u16": 65535, "n_u16": 1000,
                "val_u32": 4294967295_u64, "n_u32": 123456,
                "val_u64": 18446744073709551615_u64, "n_u64": 999999,
                "val_i8": -128, "n_i8": -1,
                "val_i16": -32768, "n_i16": 100,
                "val_i32": -2147483648_i64, "n_i32": -42,
                "val_i64": -1, "n_i64": 9876543210_i64,
                "val_f32": 3.14, "n_f32": -1.5,
                "val_f64": 2.718281828, "n_f64": 0.0,
                "val_bool": true, "n_bool": 1,
                "val_str": "hello", "n_str": "world",
                "val_fstr": "ABCD", "n_fstr": "EF",
                "val_date": "2024-12-25", "n_date": "1970-01-01",
                "val_date32": "2024-12-25", "n_date32": "2000-01-01",
                "val_dt": "2024-12-25 10:30:00", "n_dt": "2000-01-01 00:00:00",
                "val_dt64_3": "2024-12-25 10:30:00.123", "n_dt64_3": "2024-12-25 10:30:00.456",
                "val_dt64_6": "2024-12-25 10:30:00.123456", "n_dt64_6": "2024-12-25 10:30:00.789012",
                "val_uuid": "550e8400-e29b-41d4-a716-446655440000",
                "n_uuid": "12345678-1234-1234-1234-123456789abc",
                "val_ip4": "192.168.1.1", "n_ip4": "10.0.0.1",
                "val_ip6": "2001:db8::1", "n_ip6": "::1",
                "val_dec64": 1234, "n_dec64": -5678,
                "val_enum8": 1, "n_enum8": 2,
                "val_enum16": 100,
                "val_arr_u32": [10, 20, 30],
                "val_arr_str": ["foo", "bar"],
                "val_arr_nstr": ["hello", null, "world"],
                "val_map": {"clicks": 10, "views": 100},
                "val_lc": "category_a",
                "val_lc_n": "category_b"
            })
            .as_object()
            .unwrap(),
        )
        .await
        .unwrap();

    // ---- Row 2: all nullable columns set to NULL ----
    insert
        .write_map(
            json!({
                "id": 2,
                "val_u8": 0, "n_u8": null,
                "val_u16": 0, "n_u16": null,
                "val_u32": 0, "n_u32": null,
                "val_u64": 0, "n_u64": null,
                "val_i8": 0, "n_i8": null,
                "val_i16": 0, "n_i16": null,
                "val_i32": 0, "n_i32": null,
                "val_i64": 0, "n_i64": null,
                "val_f32": 0.0, "n_f32": null,
                "val_f64": 0.0, "n_f64": null,
                "val_bool": false, "n_bool": null,
                "val_str": "", "n_str": null,
                "val_fstr": "", "n_fstr": null,
                "val_date": "1970-01-01", "n_date": null,
                "val_date32": "1970-01-01", "n_date32": null,
                "val_dt": "1970-01-01 00:00:00", "n_dt": null,
                "val_dt64_3": "1970-01-01 00:00:00.000", "n_dt64_3": null,
                "val_dt64_6": "1970-01-01 00:00:00.000000", "n_dt64_6": null,
                "val_uuid": "00000000-0000-0000-0000-000000000000",
                "n_uuid": null,
                "val_ip4": "0.0.0.0", "n_ip4": null,
                "val_ip6": "::", "n_ip6": null,
                "val_dec64": 0, "n_dec64": null,
                "val_enum8": 1, "n_enum8": null,
                "val_enum16": 100,
                "val_arr_u32": [],
                "val_arr_str": [],
                "val_arr_nstr": [],
                "val_map": {},
                "val_lc": "",
                "val_lc_n": null
            })
            .as_object()
            .unwrap(),
        )
        .await
        .unwrap();

    let rows = insert.end().await.unwrap();
    assert_eq!(rows, 2);

    // ---- Verify row 1: all non-NULL values ----

    // Unsigned integer types (max 9 cols per query — RowRead tuple limit)
    let r = client
        .query(&format!(
            "SELECT val_u8, n_u8, val_u16, n_u16, val_u32, n_u32 \
             FROM {table} WHERE id = 1"
        ))
        .fetch_one::<(u8, Option<u8>, u16, Option<u16>, u32, Option<u32>)>()
        .await
        .unwrap();
    assert_eq!(r.0, 255);
    assert_eq!(r.1, Some(42));
    assert_eq!(r.2, 65535);
    assert_eq!(r.3, Some(1000));
    assert_eq!(r.4, 4294967295);
    assert_eq!(r.5, Some(123456));

    // Signed integer types
    let r = client
        .query(&format!(
            "SELECT val_i8, n_i8, val_i16, n_i16, val_i32, n_i32, val_i64, n_i64 \
             FROM {table} WHERE id = 1"
        ))
        .fetch_one::<(
            i8,
            Option<i8>,
            i16,
            Option<i16>,
            i32,
            Option<i32>,
            i64,
            Option<i64>,
        )>()
        .await
        .unwrap();
    assert_eq!(r.0, -128);
    assert_eq!(r.1, Some(-1));
    assert_eq!(r.2, -32768);
    assert_eq!(r.3, Some(100));
    assert_eq!(r.4, -2147483648);
    assert_eq!(r.5, Some(-42));
    assert_eq!(r.6, -1);
    assert_eq!(r.7, Some(9876543210));

    // Float types
    let r = client
        .query(&format!(
            "SELECT val_f32, n_f32, val_f64, n_f64 \
             FROM {table} WHERE id = 1"
        ))
        .fetch_one::<(f32, Option<f32>, f64, Option<f64>)>()
        .await
        .unwrap();
    assert!((r.0 - 3.14).abs() < 0.01);
    assert!((r.1.unwrap() - (-1.5)).abs() < 0.01);
    assert!((r.2 - 2.718281828).abs() < 0.0001);
    assert_eq!(r.3, Some(0.0));

    // Bool, String, FixedString
    let r = client
        .query(&format!(
            "SELECT toUInt8(val_bool), n_bool, val_str, n_str, \
                    toString(val_fstr), toString(n_fstr) \
             FROM {table} WHERE id = 1"
        ))
        .fetch_one::<(
            u8,
            Option<u8>,
            String,
            Option<String>,
            String,
            Option<String>,
        )>()
        .await
        .unwrap();
    assert_eq!(r.0, 1);
    assert_eq!(r.1, Some(1));
    assert_eq!(r.2, "hello");
    assert_eq!(r.3, Some("world".to_string()));
    assert_eq!(r.4, "ABCD");
    // FixedString(4) with "EF" → "EF\0\0" — ClickHouse stores zero-padded
    assert!(r.5.as_ref().unwrap().starts_with("EF"));

    // Date types
    let r = client
        .query(&format!(
            "SELECT toString(val_date), toString(n_date), \
                    toString(val_date32), toString(n_date32), \
                    toUnixTimestamp(val_dt), toUnixTimestamp(assumeNotNull(n_dt)) \
             FROM {table} WHERE id = 1"
        ))
        .fetch_one::<(String, Option<String>, String, Option<String>, u32, u32)>()
        .await
        .unwrap();
    assert_eq!(r.0, "2024-12-25");
    assert_eq!(r.1, Some("1970-01-01".to_string()));
    assert_eq!(r.2, "2024-12-25");
    assert_eq!(r.3, Some("2000-01-01".to_string()));
    assert_eq!(r.4, 1735122600);
    assert_eq!(r.5, 946684800); // 2000-01-01

    // DateTime64 precision 3 and 6
    let r = client
        .query(&format!(
            "SELECT toUnixTimestamp64Milli(val_dt64_3), \
                    toUnixTimestamp64Milli(assumeNotNull(n_dt64_3)), \
                    toUnixTimestamp64Micro(val_dt64_6), \
                    toUnixTimestamp64Micro(assumeNotNull(n_dt64_6)) \
             FROM {table} WHERE id = 1"
        ))
        .fetch_one::<(i64, i64, i64, i64)>()
        .await
        .unwrap();
    assert_eq!(r.0, 1735122600123);
    assert_eq!(r.1, 1735122600456);
    assert_eq!(r.2, 1735122600123456);
    assert_eq!(r.3, 1735122600789012);

    // UUID
    let r = client
        .query(&format!(
            "SELECT toString(val_uuid), toString(n_uuid) FROM {table} WHERE id = 1"
        ))
        .fetch_one::<(String, Option<String>)>()
        .await
        .unwrap();
    assert!(r.0.contains("550e8400"));
    assert!(r.1.as_ref().unwrap().contains("12345678"));

    // IP addresses
    let r = client
        .query(&format!(
            "SELECT toString(val_ip4), toString(n_ip4), \
                    toString(val_ip6), toString(n_ip6) \
             FROM {table} WHERE id = 1"
        ))
        .fetch_one::<(String, Option<String>, String, Option<String>)>()
        .await
        .unwrap();
    assert_eq!(r.0, "192.168.1.1");
    assert_eq!(r.1, Some("10.0.0.1".to_string()));
    assert!(r.2.contains("2001:db8"));
    assert!(r.3.as_ref().unwrap().contains("::1"));

    // Decimal
    let r = client
        .query(&format!(
            "SELECT toFloat64(val_dec64), toFloat64(assumeNotNull(n_dec64)) \
             FROM {table} WHERE id = 1"
        ))
        .fetch_one::<(f64, f64)>()
        .await
        .unwrap();
    assert!((r.0 - 12.34).abs() < 0.01);
    assert!((r.1 - (-56.78)).abs() < 0.01);

    // Arrays, Map, LowCardinality
    let r = client
        .query(&format!(
            "SELECT length(val_arr_u32), length(val_arr_str), length(val_arr_nstr), \
                    length(val_map), val_lc, val_lc_n \
             FROM {table} WHERE id = 1",
        ))
        .fetch_one::<(u64, u64, u64, u64, String, Option<String>)>()
        .await
        .unwrap();
    assert_eq!(r.0, 3);
    assert_eq!(r.1, 2);
    assert_eq!(r.2, 3); // ["hello", null, "world"]
    assert_eq!(r.3, 2);
    assert_eq!(r.4, "category_a");
    assert_eq!(r.5, Some("category_b".to_string()));

    // Verify Array(Nullable(String)) contents — null element preserved
    let arr_nstr = client
        .query(&format!("SELECT val_arr_nstr FROM {table} WHERE id = 1"))
        .fetch_one::<Vec<Option<String>>>()
        .await
        .unwrap();
    assert_eq!(arr_nstr.len(), 3);
    assert_eq!(arr_nstr[0], Some("hello".to_string()));
    assert_eq!(arr_nstr[1], None);
    assert_eq!(arr_nstr[2], Some("world".to_string()));

    // ---- Verify row 2: all nullable columns are NULL ----
    let r = client
        .query(&format!(
            "SELECT n_u8, n_u16, n_u32, n_u64, n_i8, n_i16, n_i32, n_i64 \
             FROM {table} WHERE id = 2"
        ))
        .fetch_one::<(
            Option<u8>,
            Option<u16>,
            Option<u32>,
            Option<u64>,
            Option<i8>,
            Option<i16>,
            Option<i32>,
            Option<i64>,
        )>()
        .await
        .unwrap();
    assert_eq!(r.0, None);
    assert_eq!(r.1, None);
    assert_eq!(r.2, None);
    assert_eq!(r.3, None);
    assert_eq!(r.4, None);
    assert_eq!(r.5, None);
    assert_eq!(r.6, None);
    assert_eq!(r.7, None);

    let r = client
        .query(&format!(
            "SELECT n_f32, n_f64, n_bool, n_str, n_fstr \
             FROM {table} WHERE id = 2"
        ))
        .fetch_one::<(
            Option<f32>,
            Option<f64>,
            Option<u8>,
            Option<String>,
            Option<String>,
        )>()
        .await
        .unwrap();
    assert_eq!(r.0, None);
    assert_eq!(r.1, None);
    assert_eq!(r.2, None);
    assert_eq!(r.3, None);
    assert_eq!(r.4, None);

    let r = client
        .query(&format!(
            "SELECT n_date, n_date32, isNull(n_dt), isNull(n_dt64_3), isNull(n_dt64_6) \
             FROM {table} WHERE id = 2"
        ))
        .fetch_one::<(Option<u16>, Option<i32>, u8, u8, u8)>()
        .await
        .unwrap();
    assert_eq!(r.0, None);
    assert_eq!(r.1, None);
    assert_eq!(r.2, 1); // is null
    assert_eq!(r.3, 1);
    assert_eq!(r.4, 1);

    let r = client
        .query(&format!(
            "SELECT n_uuid, n_ip4, n_ip6, n_dec64, n_enum8, val_lc_n \
             FROM {table} WHERE id = 2"
        ))
        .fetch_one::<(
            Option<String>,
            Option<String>,
            Option<String>,
            Option<i64>,
            Option<i8>,
            Option<String>,
        )>()
        .await
        .unwrap();
    assert_eq!(r.0, None);
    assert_eq!(r.1, None);
    assert_eq!(r.2, None);
    assert_eq!(r.3, None);
    assert_eq!(r.4, None);
    assert_eq!(r.5, None);

    // Verify row 2 zero/default values for non-nullable columns
    let r = client
        .query(&format!(
            "SELECT val_u8, val_u16, val_u32, val_u64, val_i8, val_i16, val_i32, val_i64 \
             FROM {table} WHERE id = 2"
        ))
        .fetch_one::<(u8, u16, u32, u64, i8, i16, i32, i64)>()
        .await
        .unwrap();
    assert_eq!(r, (0, 0, 0, 0, 0, 0, 0, 0));

    let r = client
        .query(&format!(
            "SELECT val_str, val_lc, length(val_arr_u32), length(val_arr_str), length(val_map) \
             FROM {table} WHERE id = 2"
        ))
        .fetch_one::<(String, String, u64, u64, u64)>()
        .await
        .unwrap();
    assert_eq!(r.0, ""); // empty string
    assert_eq!(r.1, ""); // empty LC string
    assert_eq!(r.2, 0); // empty array
    assert_eq!(r.3, 0);
    assert_eq!(r.4, 0); // empty map
}

/// Test that inserting NULL into non-nullable columns produces the type's
/// zero/default value (String="", UInt32=0, Date=1970-01-01, etc.).
#[tokio::test]
async fn dynamic_non_nullable_null_handling() {
    let client = prepare_database!();
    let table = "nn_null";

    client
        .query(&format!(
            "CREATE TABLE {table}(\
                id UInt64,\
                s String,\
                u32 UInt32,\
                i64 Int64,\
                f64 Float64,\
                b Bool,\
                d Date,\
                dt DateTime,\
                dt64 DateTime64(3),\
                uid UUID,\
                ip4 IPv4,\
                ip6 IPv6,\
                arr Array(UInt32),\
                m Map(String, UInt64),\
                lc LowCardinality(String),\
                fstr FixedString(4)\
            ) ENGINE = MergeTree ORDER BY id"
        ))
        .with_option("wait_end_of_query", "1")
        .execute()
        .await
        .unwrap();

    // Insert with all columns explicitly set to null
    let mut insert = client.dynamic_insert(&test_database_name!(), table);
    insert
        .write_map(
            json!({
                "id": 1,
                "s": null, "u32": null, "i64": null, "f64": null,
                "b": null, "d": null, "dt": null, "dt64": null,
                "uid": null, "ip4": null, "ip6": null,
                "arr": null, "m": null, "lc": null, "fstr": null
            })
            .as_object()
            .unwrap(),
        )
        .await
        .unwrap();
    let rows = insert.end().await.unwrap();
    assert_eq!(rows, 1);

    // Verify zero/default values
    let r = client
        .query(&format!(
            "SELECT s, u32, i64, f64, toUInt8(b), toString(d), \
                    toUnixTimestamp(dt), toUnixTimestamp64Milli(dt64) \
             FROM {table} WHERE id = 1"
        ))
        .fetch_one::<(String, u32, i64, f64, u8, String, u32, i64)>()
        .await
        .unwrap();
    assert_eq!(r.0, ""); // String default = empty
    assert_eq!(r.1, 0); // UInt32 default = 0
    assert_eq!(r.2, 0); // Int64 default = 0
    assert_eq!(r.3, 0.0); // Float64 default = 0.0
    assert_eq!(r.4, 0); // Bool default = false
    assert_eq!(r.5, "1970-01-01"); // Date default = epoch
    assert_eq!(r.6, 0); // DateTime default = epoch
    assert_eq!(r.7, 0); // DateTime64 default = epoch

    let r = client
        .query(&format!(
            "SELECT toString(uid), toString(ip4), toString(ip6), \
                    length(arr), length(m), lc \
             FROM {table} WHERE id = 1"
        ))
        .fetch_one::<(String, String, String, u64, u64, String)>()
        .await
        .unwrap();
    assert_eq!(r.0, "00000000-0000-0000-0000-000000000000"); // UUID default
    assert_eq!(r.1, "0.0.0.0"); // IPv4 default
    assert_eq!(r.2, "::"); // IPv6 default
    assert_eq!(r.3, 0); // empty array
    assert_eq!(r.4, 0); // empty map
    assert_eq!(r.5, ""); // LC string default
}

/// Edge cases: empty strings, zero numerics, epoch dates, empty collections,
/// mixed null/non-null in Array(Nullable(T)).
#[tokio::test]
async fn dynamic_edge_cases() {
    let client = prepare_database!();
    let table = "edge_cases";

    client
        .query(&format!(
            "CREATE TABLE {table}(\
                id UInt64,\
                empty_str String,\
                zero_u32 UInt32,\
                epoch_date Date,\
                epoch_dt DateTime,\
                epoch_dt64 DateTime64(3),\
                empty_arr Array(UInt32),\
                empty_map Map(String, String),\
                mixed_arr Array(Nullable(String)),\
                single_arr Array(UInt32),\
                fstr_short FixedString(8)\
            ) ENGINE = MergeTree ORDER BY id"
        ))
        .with_option("wait_end_of_query", "1")
        .execute()
        .await
        .unwrap();

    let mut insert = client.dynamic_insert(&test_database_name!(), table);
    insert
        .write_map(
            json!({
                "id": 1,
                "empty_str": "",
                "zero_u32": 0,
                "epoch_date": "1970-01-01",
                "epoch_dt": "1970-01-01 00:00:00",
                "epoch_dt64": "1970-01-01 00:00:00.000",
                "empty_arr": [],
                "empty_map": {},
                "mixed_arr": [null, "hello", null, "world", null],
                "single_arr": [42],
                "fstr_short": "AB"
            })
            .as_object()
            .unwrap(),
        )
        .await
        .unwrap();
    let rows = insert.end().await.unwrap();
    assert_eq!(rows, 1);

    // Verify empty string
    let r = client
        .query(&format!("SELECT empty_str FROM {table}"))
        .fetch_one::<String>()
        .await
        .unwrap();
    assert_eq!(r, "");

    // Verify zero numeric
    let r = client
        .query(&format!("SELECT zero_u32 FROM {table}"))
        .fetch_one::<u32>()
        .await
        .unwrap();
    assert_eq!(r, 0);

    // Verify epoch dates
    let r = client
        .query(&format!(
            "SELECT toString(epoch_date), toUnixTimestamp(epoch_dt), \
                    toUnixTimestamp64Milli(epoch_dt64) FROM {table}"
        ))
        .fetch_one::<(String, u32, i64)>()
        .await
        .unwrap();
    assert_eq!(r.0, "1970-01-01");
    assert_eq!(r.1, 0);
    assert_eq!(r.2, 0);

    // Verify empty collections
    let r = client
        .query(&format!(
            "SELECT length(empty_arr), length(empty_map) FROM {table}"
        ))
        .fetch_one::<(u64, u64)>()
        .await
        .unwrap();
    assert_eq!(r.0, 0);
    assert_eq!(r.1, 0);

    // Verify mixed null/non-null in Array(Nullable(String))
    let arr = client
        .query(&format!("SELECT mixed_arr FROM {table}"))
        .fetch_one::<Vec<Option<String>>>()
        .await
        .unwrap();
    assert_eq!(arr.len(), 5);
    assert_eq!(arr[0], None);
    assert_eq!(arr[1], Some("hello".to_string()));
    assert_eq!(arr[2], None);
    assert_eq!(arr[3], Some("world".to_string()));
    assert_eq!(arr[4], None);

    // Verify single-element array
    let r = client
        .query(&format!("SELECT single_arr FROM {table}"))
        .fetch_one::<Vec<u32>>()
        .await
        .unwrap();
    assert_eq!(r, vec![42]);

    // Verify FixedString shorter than N is zero-padded
    let r = client
        .query(&format!("SELECT length(fstr_short) FROM {table}"))
        .fetch_one::<u64>()
        .await
        .unwrap();
    assert_eq!(r, 8); // FixedString(8) always stores 8 bytes
}

// ============================================================================
// Mixed: wide table with every type at once
// ============================================================================

#[tokio::test]
async fn dynamic_all_scalar_types() {
    let client = prepare_database!();
    let table = "all_types";

    client
        .query(&format!(
            "CREATE TABLE {table}(\
                u8 UInt8, u16 UInt16, u32 UInt32, u64 UInt64,\
                i8 Int8, i16 Int16, i32 Int32, i64 Int64,\
                f32 Float32, f64 Float64,\
                b Bool,\
                s String,\
                lc LowCardinality(String),\
                dt DateTime,\
                dt64 DateTime64(3),\
                d Date,\
                uid UUID,\
                ip4 IPv4,\
                ip6 IPv6\
            ) ENGINE = MergeTree ORDER BY u64"
        ))
        .with_option("wait_end_of_query", "1")
        .execute()
        .await
        .unwrap();

    let mut insert = client.dynamic_insert(&test_database_name!(), table);
    insert
        .write_map(
            json!({
                "u8": 255, "u16": 65535, "u32": 4294967295_u64, "u64": 1,
                "i8": -128, "i16": -32768, "i32": -2147483648_i64, "i64": -1,
                "f32": 3.14, "f64": 2.71828,
                "b": true,
                "s": "hello world",
                "lc": "category_a",
                "dt": "2024-12-25 10:30:00",
                "dt64": "2024-12-25 10:30:00.123",
                "d": "2024-12-25",
                "uid": "550e8400-e29b-41d4-a716-446655440000",
                "ip4": "192.168.1.1",
                "ip6": "::1"
            })
            .as_object()
            .unwrap(),
        )
        .await
        .unwrap();
    let rows = insert.end().await.unwrap();
    assert_eq!(rows, 1);

    // Spot-check a few columns
    let result = client
        .query(&format!(
            "SELECT u8, i8, toUInt8(b), s, toUnixTimestamp64Milli(dt64) FROM {table}"
        ))
        .fetch_one::<(u8, i8, u8, String, i64)>()
        .await
        .unwrap();
    assert_eq!(result.0, 255);
    assert_eq!(result.1, -128);
    assert_eq!(result.2, 1); // bool
    assert_eq!(result.3, "hello world");
    assert_eq!(result.4, 1735122600123);
}
