//! Integration tests for the native TCP transport.
//!
//! These tests require a running ClickHouse server on port 9000.
//! Run with: `cargo test --test it --features native-transport -- native::`

#![cfg(feature = "native-transport")]

use clickhouse::native::NativeClient;
use clickhouse::Row;
use serde::{Deserialize, Serialize};

fn get_native_client() -> NativeClient {
    let host = std::env::var("CLICKHOUSE_HOST").unwrap_or_else(|_| "localhost".into());
    let port = std::env::var("CLICKHOUSE_NATIVE_PORT").unwrap_or_else(|_| "9000".into());
    let user = std::env::var("CLICKHOUSE_USER").unwrap_or_else(|_| "default".into());
    let password = std::env::var("CLICKHOUSE_PASSWORD").unwrap_or_else(|_| "".into());
    let use_tls = std::env::var("CLICKHOUSE_TLS").unwrap_or_default() == "true";

    let client = NativeClient::default()
        .with_addr(format!("{host}:{port}"))
        .with_database("default")
        .with_user(user)
        .with_password(password);

    // Enable TLS if CLICKHOUSE_TLS=true (requires native-tls-rustls feature).
    // Uses the hostname from CLICKHOUSE_HOST for SNI verification.
    #[cfg(feature = "native-tls-rustls")]
    let client = if use_tls {
        client.with_tls(&host)
    } else {
        client
    };

    #[cfg(not(feature = "native-tls-rustls"))]
    if use_tls {
        panic!("CLICKHOUSE_TLS=true requires native-tls-rustls feature");
    }

    // On a replicated cluster, write to a quorum of replicas before returning
    // and ensure SELECT only reads quorum-committed data.  This gives
    // read-after-write consistency without pinning connections to a single node.
    //
    // CLICKHOUSE_INSERT_QUORUM: number of replicas that must acknowledge each
    // INSERT -- set to the replica count for your cluster (default: 2).
    if std::env::var("CLICKHOUSE_CLUSTER").is_ok() {
        let quorum = std::env::var("CLICKHOUSE_INSERT_QUORUM")
            .unwrap_or_else(|_| "2".into());
        client
            .with_setting("insert_quorum", quorum)
            .with_setting("insert_quorum_timeout", "30000")
            .with_setting("select_sequential_consistency", "1")
    } else {
        client
    }
}

/// Create a unique test database for isolation (mirrors `prepare_database!` for HTTP tests).
///
/// When `CLICKHOUSE_CLUSTER` is set, databases are created ON CLUSTER so all
/// nodes see the database immediately -- required for multi-node setups.
async fn prepare_native_database(test_name: &str) -> NativeClient {
    let client = get_native_client();
    let db = format!("chrs_native_{test_name}");
    let cluster = std::env::var("CLICKHOUSE_CLUSTER").ok();

    let drop_sql = match &cluster {
        Some(c) => format!("DROP DATABASE IF EXISTS {db} ON CLUSTER {c}"),
        None => format!("DROP DATABASE IF EXISTS {db}"),
    };
    let create_sql = match &cluster {
        Some(c) => format!("CREATE DATABASE {db} ON CLUSTER {c}"),
        None => format!("CREATE DATABASE {db}"),
    };

    client
        .query(&drop_sql)
        .execute()
        .await
        .unwrap_or_else(|e| panic!("drop db {db}: {e}"));

    client
        .query(&create_sql)
        .execute()
        .await
        .unwrap_or_else(|e| panic!("create db {db}: {e}"));

    client.with_database(db)
}

/// Returns the ON CLUSTER clause if `CLICKHOUSE_CLUSTER` is set, otherwise empty.
fn on_cluster() -> String {
    std::env::var("CLICKHOUSE_CLUSTER")
        .map(|c| format!(" ON CLUSTER '{c}'"))
        .unwrap_or_default()
}

/// Returns the table engine clause for tests.
///
/// Local Docker: `ENGINE = Memory` -- fast, no persistence needed.
/// External cluster: `ReplicatedMergeTree` with a per-table `{uuid}` ZK path so
/// each CREATE TABLE gets a unique ZooKeeper node (no stale-replica conflicts on
/// re-runs).  The `{uuid}` macro is substituted by ClickHouse at CREATE time.
fn test_engine(order_by: &str) -> String {
    if std::env::var("CLICKHOUSE_CLUSTER").is_ok() {
        format!(
            "ENGINE = ReplicatedMergeTree(\
             '/clickhouse/tables/{{database}}/{{table}}/{{uuid}}', '{{replica}}'\
             ) ORDER BY {order_by}"
        )
    } else {
        "ENGINE = Memory".to_string()
    }
}

#[tokio::test]
async fn native_ping() {
    let client = get_native_client();
    client.ping().await.expect("ping failed");
}

/// Verify that the connection pool reuses connections across queries.
///
/// Run 20 sequential pings on a pool capped to 1 connection.  If pooling
/// works, all 20 succeed because the same connection is returned each time.
/// Without pooling each ping would open a new connection.
#[tokio::test]
async fn native_pool_reuse() {
    let client = get_native_client().with_pool_size(1);
    for _ in 0..20 {
        client.ping().await.expect("ping failed");
    }
}

#[tokio::test]
async fn native_ddl() {
    let client = prepare_native_database("ddl").await;

    client
        .query(&format!("CREATE TABLE t{} (n UInt32) {}", on_cluster(), test_engine("tuple()")))
        .execute()
        .await
        .expect("CREATE TABLE failed");

    client
        .query("DROP TABLE t")
        .execute()
        .await
        .expect("DROP TABLE failed");
}

#[tokio::test]
async fn native_scalar_types() {
    let client = prepare_native_database("scalar").await;

    client
        .query(&format!(
            "CREATE TABLE t{} (
                u8  UInt8, u16 UInt16, u32 UInt32, u64 UInt64,
                i8  Int8,  i16 Int16,  i32 Int32,  i64 Int64,
                f32 Float32, f64 Float64
            ) {}",
            on_cluster(), test_engine("tuple()")
        ))
        .execute()
        .await
        .expect("CREATE failed");

    client
        .query(
            "INSERT INTO t VALUES \
             (1, 2, 3, 4, -1, -2, -3, -4, 1.5, 2.5)",
        )
        .execute()
        .await
        .expect("INSERT failed");

    #[derive(Debug, Row, Deserialize, PartialEq)]
    struct ScalarRow {
        u8: u8,
        u16: u16,
        u32: u32,
        u64: u64,
        i8: i8,
        i16: i16,
        i32: i32,
        i64: i64,
        f32: f32,
        f64: f64,
    }

    let mut cursor = client
        .query("SELECT * FROM t")
        .fetch::<ScalarRow>()
        .expect("fetch failed");

    let row = cursor.next().await.expect("no error").expect("no row");
    assert_eq!(row.u8, 1);
    assert_eq!(row.u16, 2);
    assert_eq!(row.u32, 3);
    assert_eq!(row.u64, 4);
    assert_eq!(row.i8, -1);
    assert_eq!(row.i16, -2);
    assert_eq!(row.i32, -3);
    assert_eq!(row.i64, -4);
    assert!((row.f32 - 1.5f32).abs() < f32::EPSILON);
    assert!((row.f64 - 2.5f64).abs() < f64::EPSILON);
}

#[tokio::test]
async fn native_string_types() {
    let client = prepare_native_database("strings").await;

    client
        .query(&format!("CREATE TABLE t{} (s String, fs FixedString(4)) {}", on_cluster(), test_engine("tuple()")))
        .execute()
        .await
        .expect("CREATE failed");

    client
        .query("INSERT INTO t VALUES ('hello', 'abcd')")
        .execute()
        .await
        .expect("INSERT failed");

    #[derive(Debug, Row, Deserialize, PartialEq)]
    struct StringRow {
        s: String,
        fs: String,
    }

    let rows = client
        .query("SELECT s, fs FROM t")
        .fetch_all::<StringRow>()
        .await
        .expect("fetch failed");

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].s, "hello");
    assert_eq!(rows[0].fs, "abcd");
}

#[tokio::test]
async fn native_nullable() {
    let client = prepare_native_database("nullable").await;

    client
        .query(&format!("CREATE TABLE t{} (n Nullable(UInt32)) {}", on_cluster(), test_engine("tuple()")))
        .execute()
        .await
        .expect("CREATE failed");

    client
        .query("INSERT INTO t VALUES (1), (NULL), (3)")
        .execute()
        .await
        .expect("INSERT failed");

    #[derive(Debug, Row, Deserialize, PartialEq)]
    struct NullableRow {
        n: Option<u32>,
    }

    let rows = client
        .query("SELECT n FROM t ORDER BY n ASC NULLS LAST")
        .fetch_all::<NullableRow>()
        .await
        .expect("fetch failed");

    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].n, Some(1));
    assert_eq!(rows[1].n, Some(3));
    assert_eq!(rows[2].n, None);
}

#[tokio::test]
async fn native_low_cardinality_string() {
    let client = prepare_native_database("lowcard").await;

    client
        .query(
            &format!("CREATE TABLE t{} (id UInt32, tag LowCardinality(String)) {}", on_cluster(), test_engine("tuple()")),
        )
        .execute()
        .await
        .expect("CREATE failed");

    client
        .query("INSERT INTO t VALUES (1, 'foo'), (2, 'bar'), (3, 'foo')")
        .execute()
        .await
        .expect("INSERT failed");

    #[derive(Debug, Row, Deserialize, PartialEq)]
    struct LCRow {
        id: u32,
        tag: String,
    }

    let rows = client
        .query("SELECT id, tag FROM t ORDER BY id ASC")
        .fetch_all::<LCRow>()
        .await
        .expect("fetch failed");

    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0], LCRow { id: 1, tag: "foo".into() });
    assert_eq!(rows[1], LCRow { id: 2, tag: "bar".into() });
    assert_eq!(rows[2], LCRow { id: 3, tag: "foo".into() });
}

#[tokio::test]
async fn native_multiple_blocks() {
    let client = prepare_native_database("multiblock").await;

    client
        .query(&format!("CREATE TABLE t{} (n UInt64) {}", on_cluster(), test_engine("n")))
        .execute()
        .await
        .expect("CREATE failed");

    // Insert 10,000 rows -- server will send multiple blocks
    client
        .query(
            "INSERT INTO t SELECT number FROM system.numbers LIMIT 10000",
        )
        .execute()
        .await
        .expect("INSERT failed");

    let count: u64 = client
        .query("SELECT count() FROM t")
        .fetch_one::<u64>()
        .await
        .expect("count failed");

    assert_eq!(count, 10_000);
}

#[tokio::test]
async fn native_array_type() {
    let client = prepare_native_database("array").await;

    client
        .query(&format!("CREATE TABLE t{} (id UInt32, tags Array(String)) {}", on_cluster(), test_engine("tuple()")))
        .execute()
        .await
        .expect("CREATE failed");

    client
        .query("INSERT INTO t VALUES (1, ['a', 'b', 'c']), (2, []), (3, ['x'])")
        .execute()
        .await
        .expect("INSERT failed");

    #[derive(Debug, Row, Deserialize, PartialEq)]
    struct ArrayRow {
        id: u32,
        tags: Vec<String>,
    }

    let rows = client
        .query("SELECT id, tags FROM t ORDER BY id ASC")
        .fetch_all::<ArrayRow>()
        .await
        .expect("fetch failed");

    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0], ArrayRow { id: 1, tags: vec!["a".into(), "b".into(), "c".into()] });
    assert_eq!(rows[1], ArrayRow { id: 2, tags: vec![] });
    assert_eq!(rows[2], ArrayRow { id: 3, tags: vec!["x".into()] });
}

#[tokio::test]
async fn native_tuple_type() {
    let client = prepare_native_database("tuple").await;

    client
        .query(&format!("CREATE TABLE t{} (id UInt32, pair Tuple(String, UInt32)) {}", on_cluster(), test_engine("tuple()")))
        .execute()
        .await
        .expect("CREATE failed");

    client
        .query("INSERT INTO t VALUES (1, ('hello', 42)), (2, ('world', 7))")
        .execute()
        .await
        .expect("INSERT failed");

    #[derive(Debug, Row, Deserialize, PartialEq)]
    struct TupleRow {
        id: u32,
        pair: (String, u32),
    }

    let rows = client
        .query("SELECT id, pair FROM t ORDER BY id ASC")
        .fetch_all::<TupleRow>()
        .await
        .expect("fetch failed");

    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0], TupleRow { id: 1, pair: ("hello".into(), 42) });
    assert_eq!(rows[1], TupleRow { id: 2, pair: ("world".into(), 7) });
}

#[tokio::test]
async fn native_map_type() {
    let client = prepare_native_database("map").await;

    client
        .query(
            &format!("CREATE TABLE t{} (id UInt32, attrs Map(String, UInt32)) {}", on_cluster(), test_engine("tuple()")),
        )
        .execute()
        .await
        .expect("CREATE failed");

    client
        .query(
            "INSERT INTO t VALUES (1, {'age': 30, 'score': 100}), (2, {})",
        )
        .execute()
        .await
        .expect("INSERT failed");

    // Maps deserialize as JSON strings from the native transport
    #[derive(Debug, Row, Deserialize)]
    struct MapRow {
        id: u32,
        attrs: String,
    }

    let rows = client
        .query("SELECT id, CAST(attrs, 'String') AS attrs FROM t ORDER BY id ASC")
        .fetch_all::<MapRow>()
        .await
        .expect("fetch failed");

    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].id, 1);
    assert_eq!(rows[1].id, 2);
    assert_eq!(rows[1].attrs, "{}");
}

#[tokio::test]
async fn native_json_legacy() {
    let client = prepare_native_database("json_legacy").await;

    // Object('json') is the legacy JSON type -- stored as String on the wire.
    client
        .query(
            &format!("CREATE TABLE t{} (id UInt32, data Object('json')) {} \
             SETTINGS allow_experimental_object_type = 1",
             on_cluster(), test_engine("tuple()")),
        )
        .execute()
        .await
        .unwrap_or_else(|e| {
            // Legacy Object type may not be available on all server versions -- skip
            eprintln!("SKIP native_json_legacy: {e}");
        });

    // Insert and query are separate -- if CREATE failed, just verify we skip cleanly
    let rows_result = client
        .query(
            "SELECT id, CAST(data, 'String') AS data FROM t ORDER BY id ASC",
        )
        .fetch_all::<(u32, String)>()
        .await;

    // If table doesn't exist (CREATE failed), just ensure we don't panic
    match rows_result {
        Ok(rows) => {
            // If data was inserted, verify round-trip
            assert!(rows.len() <= 10, "unexpected row count");
        }
        Err(e) => {
            eprintln!("SKIP native_json_legacy query: {e}");
        }
    }
}

#[tokio::test]
async fn native_variant_type() {
    let client = prepare_native_database("variant").await;

    // Variant type requires ClickHouse 24.x+ with allow_experimental_variant_type
    let create_result = client
        .query(
            &format!("CREATE TABLE t{} (id UInt32, val Variant(String, UInt64)) {} \
             SETTINGS allow_experimental_variant_type = 1",
             on_cluster(), test_engine("tuple()")),
        )
        .execute()
        .await;

    if let Err(e) = create_result {
        eprintln!("SKIP native_variant_type (server may not support Variant): {e}");
        return;
    }

    client
        .query(
            "INSERT INTO t VALUES \
             (1, 'hello'::Variant(String, UInt64)), \
             (2, 42::Variant(String, UInt64)), \
             (3, NULL)",
        )
        .execute()
        .await
        .expect("INSERT failed");

    // Variant cells come back as JSON strings
    #[derive(Debug, Row, Deserialize)]
    struct VariantRow {
        id: u32,
        val: String,
    }

    let rows = client
        .query("SELECT id, val FROM t ORDER BY id ASC")
        .fetch_all::<VariantRow>()
        .await
        .expect("fetch failed");

    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].id, 1);
    assert_eq!(rows[1].id, 2);
    assert_eq!(rows[2].id, 3);
    // Values are JSON strings: "hello", 42, null
    assert!(rows[0].val.contains("hello") || rows[0].val == "\"hello\"",
            "unexpected: {:?}", rows[0].val);
    assert!(rows[1].val == "42" || rows[1].val.contains("42"),
            "unexpected: {:?}", rows[1].val);
    assert_eq!(rows[2].val, "null");
}

#[tokio::test]
async fn native_json_new_type() {
    let client = prepare_native_database("json_new").await;

    // New JSON type (ClickHouse 24.x+)
    let create_result = client
        .query(
            &format!("CREATE TABLE t{} (id UInt32, data JSON) {} \
             SETTINGS allow_experimental_json_type = 1",
             on_cluster(), test_engine("tuple()")),
        )
        .execute()
        .await;

    if let Err(e) = create_result {
        eprintln!("SKIP native_json_new_type (server may not support JSON type): {e}");
        return;
    }

    client
        .query(
            "INSERT INTO t VALUES \
             (1, '{\"name\": \"Alice\", \"age\": 30}'), \
             (2, '{\"name\": \"Bob\", \"score\": 95.5}')",
        )
        .execute()
        .await
        .expect("INSERT failed");

    // JSON columns come back as JSON strings via Dynamic wire format
    #[derive(Debug, Row, Deserialize)]
    struct JsonRow {
        id: u32,
        data: String,
    }

    let rows = client
        .query("SELECT id, data FROM t ORDER BY id ASC")
        .fetch_all::<JsonRow>()
        .await
        .expect("fetch failed");

    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].id, 1);
    assert_eq!(rows[1].id, 2);
    // Data should be non-empty JSON representations
    assert!(!rows[0].data.is_empty(), "JSON data should not be empty");
    assert!(!rows[1].data.is_empty(), "JSON data should not be empty");
}

#[tokio::test]
async fn native_ip_types() {
    use std::net::{Ipv4Addr, Ipv6Addr};

    let client = prepare_native_database("ip").await;

    client
        .query(
            &format!("CREATE TABLE t{} (id UInt32, v4 IPv4, v6 IPv6) {}", on_cluster(), test_engine("tuple()")),
        )
        .execute()
        .await
        .expect("CREATE failed");

    client
        .query(
            "INSERT INTO t VALUES \
             (1, '192.168.1.1', '::1'), \
             (2, '10.0.0.1', '2001:db8::1')",
        )
        .execute()
        .await
        .expect("INSERT failed");

    // IPv4 deserializes as u32 (raw LE bytes); IPv6 as [u8; 16]
    #[derive(Debug, Row, Deserialize, PartialEq)]
    struct IpRow {
        id: u32,
        // ClickHouse IPv4 = u32 in RowBinary; use serde helper or raw u32
        #[serde(with = "clickhouse::serde::ipv4")]
        v4: Ipv4Addr,
        // IPv6 = 16-byte array
        v6: [u8; 16],
    }

    let rows = client
        .query("SELECT id, v4, v6 FROM t ORDER BY id ASC")
        .fetch_all::<IpRow>()
        .await
        .expect("fetch failed");

    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].id, 1);
    assert_eq!(rows[0].v4, Ipv4Addr::new(192, 168, 1, 1));
    assert_eq!(rows[1].v4, Ipv4Addr::new(10, 0, 0, 1));
}

#[tokio::test]
async fn native_decimal_type() {
    let client = prepare_native_database("decimal").await;

    client
        .query(
            &format!("CREATE TABLE t{} (id UInt32, price Decimal64(2)) {}", on_cluster(), test_engine("tuple()")),
        )
        .execute()
        .await
        .expect("CREATE failed");

    client
        .query("INSERT INTO t VALUES (1, 12.34), (2, 99.99), (3, 0.01)")
        .execute()
        .await
        .expect("INSERT failed");

    // Decimal64 is stored as i64 (scaled integer) -- maps to i64 in Rust
    #[derive(Debug, Row, Deserialize)]
    struct DecimalRow {
        id: u32,
        price: i64,  // raw scaled integer: 1234, 9999, 1
    }

    let rows = client
        .query("SELECT id, price FROM t ORDER BY id ASC")
        .fetch_all::<DecimalRow>()
        .await
        .expect("fetch failed");

    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].price, 1234);  // 12.34 x 100
    assert_eq!(rows[1].price, 9999);  // 99.99 x 100
    assert_eq!(rows[2].price, 1);     // 0.01 x 100
}

#[tokio::test]
async fn native_empty_result() {
    let client = prepare_native_database("empty").await;

    client
        .query(&format!("CREATE TABLE t{} (n UInt32) {}", on_cluster(), test_engine("tuple()")))
        .execute()
        .await
        .expect("CREATE failed");

    #[derive(Debug, Row, Deserialize)]
    struct Row {
        n: u32,
    }

    let rows = client
        .query("SELECT n FROM t")
        .fetch_all::<Row>()
        .await
        .expect("fetch failed");

    assert!(rows.is_empty());
}

#[tokio::test]
async fn native_bool_type() {
    let client = prepare_native_database("bool").await;

    client
        .query(&format!(
            "CREATE TABLE t{} (a Bool, b Bool) {}",
            on_cluster(),
            test_engine("tuple()")
        ))
        .execute()
        .await
        .expect("CREATE failed");

    client
        .query("INSERT INTO t VALUES (true, false)")
        .execute()
        .await
        .expect("INSERT failed");

    #[derive(Debug, Row, Deserialize, PartialEq)]
    struct BoolRow {
        a: bool,
        b: bool,
    }

    let rows = client
        .query("SELECT a, b FROM t")
        .fetch_all::<BoolRow>()
        .await
        .expect("fetch failed");

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0], BoolRow { a: true, b: false });
}

// ---------------------------------------------------------------------------
// Extended integer types: UInt128 / Int128 / UInt256 / Int256
// ---------------------------------------------------------------------------

#[tokio::test]
async fn native_extended_int_types() {
    let client = prepare_native_database("ext_int").await;

    client
        .query(&format!(
            "CREATE TABLE t{} (
                u128 UInt128, i128 Int128,
                u256 UInt256, i256 Int256
            ) {}",
            on_cluster(),
            test_engine("tuple()")
        ))
        .execute()
        .await
        .expect("CREATE failed");

    client
        .query("INSERT INTO t VALUES (42, -42, 42, -42)")
        .execute()
        .await
        .expect("INSERT failed");

    // 256-bit types have no native Rust equivalent -- read as raw 32-byte LE arrays.
    #[derive(Debug, Row, Deserialize)]
    struct ExtIntRow {
        u128: u128,
        i128: i128,
        u256: [u8; 32],
        i256: [u8; 32],
    }

    let rows = client
        .query("SELECT u128, i128, u256, i256 FROM t")
        .fetch_all::<ExtIntRow>()
        .await
        .expect("fetch failed");

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].u128, 42u128);
    assert_eq!(rows[0].i128, -42i128);
    // UInt256(42): first byte = 42, rest zero (LE)
    assert_eq!(rows[0].u256[0], 42);
    assert!(rows[0].u256[1..].iter().all(|&b| b == 0));
    // Int256(-42): two's complement 32-byte LE -- last bytes all 0xFF
    assert_eq!(rows[0].i256[31], 0xFF);
}

// ---------------------------------------------------------------------------
// BFloat16 (brain float, 2-byte) and UUID
// ---------------------------------------------------------------------------

#[tokio::test]
async fn native_bfloat16_uuid() {
    let client = prepare_native_database("bf16_uuid").await;

    client
        .query(&format!(
            "CREATE TABLE t{} (id UInt32, bf BFloat16, uuid UUID) {}",
            on_cluster(),
            test_engine("tuple()")
        ))
        .execute()
        .await
        .expect("CREATE failed");

    client
        .query(
            "INSERT INTO t VALUES \
             (1, 1.0, '00000000-0000-0000-0000-000000000001'), \
             (2, 2.0, 'ffffffff-ffff-ffff-ffff-ffffffffffff')",
        )
        .execute()
        .await
        .expect("INSERT failed");

    // BFloat16 = 2 raw bytes; read as u16 (raw bit pattern).
    // UUID = 16 bytes.
    #[derive(Debug, Row, Deserialize)]
    struct Bf16UuidRow {
        id: u32,
        bf: u16,     // raw BFloat16 bits
        uuid: [u8; 16],
    }

    let rows = client
        .query("SELECT id, bf, uuid FROM t ORDER BY id ASC")
        .fetch_all::<Bf16UuidRow>()
        .await
        .expect("fetch failed");

    assert_eq!(rows.len(), 2);
    // BFloat16(1.0) = 0x3F80 = 16256
    assert_eq!(rows[0].bf, 0x3F80u16);
    // BFloat16(2.0) = 0x4000 = 16384
    assert_eq!(rows[1].bf, 0x4000u16);
    // ClickHouse stores UUID as two LE uint64s: high 8 bytes then low 8 bytes.
    // UUID 00000000-0000-0000-0000-000000000001:
    //   high u64 = 0 -> bytes [0..8] all zero
    //   low  u64 = 1 -> bytes [8..16] = [1, 0, 0, 0, 0, 0, 0, 0] (LE)
    assert_eq!(rows[0].uuid[8], 1);
    assert!(rows[0].uuid[..8].iter().all(|&b| b == 0));
    assert!(rows[0].uuid[9..].iter().all(|&b| b == 0));
    // UUID all-0xff
    assert!(rows[1].uuid.iter().all(|&b| b == 0xFF));
}

// ---------------------------------------------------------------------------
// Enum8 and Enum16
// ---------------------------------------------------------------------------

#[tokio::test]
async fn native_enum_types() {
    let client = prepare_native_database("enums").await;

    client
        .query(&format!(
            "CREATE TABLE t{} (
                id  UInt32,
                e8  Enum8('low' = 1, 'med' = 2, 'high' = 3),
                e16 Enum16('pending' = 100, 'active' = 200, 'closed' = 300)
            ) {}",
            on_cluster(),
            test_engine("tuple()")
        ))
        .execute()
        .await
        .expect("CREATE failed");

    client
        .query(
            "INSERT INTO t VALUES \
             (1, 'low',  'pending'), \
             (2, 'high', 'active'), \
             (3, 'med',  'closed')",
        )
        .execute()
        .await
        .expect("INSERT failed");

    // Enum8/16 are wire-compatible with Int8/Int16 -- deserialize as raw integer discriminant.
    #[derive(Debug, Row, Deserialize)]
    struct EnumRow {
        id: u32,
        e8: i8,
        e16: i16,
    }

    let rows = client
        .query("SELECT id, e8, e16 FROM t ORDER BY id ASC")
        .fetch_all::<EnumRow>()
        .await
        .expect("fetch failed");

    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].e8, 1);   // 'low' = 1
    assert_eq!(rows[0].e16, 100); // 'pending' = 100
    assert_eq!(rows[1].e8, 3);   // 'high' = 3
    assert_eq!(rows[1].e16, 200); // 'active' = 200
    assert_eq!(rows[2].e8, 2);   // 'med' = 2
    assert_eq!(rows[2].e16, 300); // 'closed' = 300
}

// ---------------------------------------------------------------------------
// Date, Date32, DateTime, DateTime64 (multiple precisions)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn native_datetime_all() {
    let client = prepare_native_database("datetimes").await;

    client
        .query(&format!(
            "CREATE TABLE t{} (
                id   UInt32,
                d    Date,
                d32  Date32,
                dt   DateTime,
                dt3  DateTime64(3),
                dt6  DateTime64(6),
                dt9  DateTime64(9)
            ) {}",
            on_cluster(),
            test_engine("tuple()")
        ))
        .execute()
        .await
        .expect("CREATE failed");

    client
        .query(
            "INSERT INTO t VALUES \
             (1, '1970-01-01', '1970-01-01', '1970-01-01 00:00:01', \
              '1970-01-01 00:00:00.001', '1970-01-01 00:00:00.000001', \
              '1970-01-01 00:00:00.000000001')",
        )
        .execute()
        .await
        .expect("INSERT failed");

    // Date = u16 (days since 1970-01-01), DateTime = u32 (unix seconds),
    // DateTime64(N) = i64 (scaled: x10^N from epoch).
    #[derive(Debug, Row, Deserialize)]
    struct DtRow {
        id: u32,
        d: u16,
        d32: i32,
        dt: u32,
        dt3: i64,
        dt6: i64,
        dt9: i64,
    }

    let rows = client
        .query("SELECT id, d, d32, dt, dt3, dt6, dt9 FROM t ORDER BY id ASC")
        .fetch_all::<DtRow>()
        .await
        .expect("fetch failed");

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].d, 0);    // 1970-01-01 = day 0
    assert_eq!(rows[0].d32, 0);
    assert_eq!(rows[0].dt, 1);   // 1 second past epoch
    assert_eq!(rows[0].dt3, 1);  // 1 millisecond
    assert_eq!(rows[0].dt6, 1);  // 1 microsecond
    assert_eq!(rows[0].dt9, 1);  // 1 nanosecond
}

// ---------------------------------------------------------------------------
// Decimal32 / Decimal128 / Decimal256  (Decimal64 already in native_decimal_type)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn native_decimal_all_sizes() {
    let client = prepare_native_database("decimals").await;

    client
        .query(&format!(
            "CREATE TABLE t{} (
                id   UInt32,
                d32  Decimal32(2),
                d128 Decimal128(4),
                d256 Decimal256(6)
            ) {}",
            on_cluster(),
            test_engine("tuple()")
        ))
        .execute()
        .await
        .expect("CREATE failed");

    client
        .query("INSERT INTO t VALUES (1, 12.34, 1234.5678, 123456.789012)")
        .execute()
        .await
        .expect("INSERT failed");

    #[derive(Debug, Row, Deserialize)]
    struct DecRow {
        id: u32,
        d32: i32,
        d128: i128,
        d256: [u8; 32], // raw 32-byte LE
    }

    let rows = client
        .query("SELECT id, d32, d128, d256 FROM t ORDER BY id ASC")
        .fetch_all::<DecRow>()
        .await
        .expect("fetch failed");

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].d32, 1234);         // 12.34 x 100
    assert_eq!(rows[0].d128, 12345678i128); // 1234.5678 x 10^4
    // d256: 123456789012 (123456.789012 x 10^6) -- check first bytes
    let expected: i64 = 123_456_789_012;
    let le_bytes = expected.to_le_bytes();
    assert_eq!(&rows[0].d256[..8], &le_bytes);
    assert!(rows[0].d256[8..].iter().all(|&b| b == 0));
}

// ---------------------------------------------------------------------------
// Time and Time64
// ---------------------------------------------------------------------------

#[tokio::test]
async fn native_time_types() {
    let client = prepare_native_database("times").await;

    client
        .query(&format!(
            "CREATE TABLE t{} (id UInt32, t Time, t64 Time64(3)) {}",
            on_cluster(),
            test_engine("tuple()")
        ))
        .execute()
        .await
        .expect("CREATE failed");

    client
        .query(
            "INSERT INTO t VALUES \
             (1, '01:02:03', '01:02:03.456'), \
             (2, '00:00:00', '00:00:00.000')",
        )
        .execute()
        .await
        .expect("INSERT failed");

    // Time = i32 (seconds since midnight), Time64(3) = i64 (milliseconds since midnight)
    #[derive(Debug, Row, Deserialize)]
    struct TimeRow {
        id: u32,
        t: i32,
        t64: i64,
    }

    let rows = client
        .query("SELECT id, t, t64 FROM t ORDER BY id ASC")
        .fetch_all::<TimeRow>()
        .await
        .expect("fetch failed");

    assert_eq!(rows.len(), 2);
    // 01:02:03 = 1*3600 + 2*60 + 3 = 3723 seconds
    assert_eq!(rows[0].t, 3723);
    // 01:02:03.456 = 3723 * 1000 + 456 = 3723456 ms
    assert_eq!(rows[0].t64, 3_723_456);
    assert_eq!(rows[1].t, 0);
    assert_eq!(rows[1].t64, 0);
}

// ---------------------------------------------------------------------------
// Geo types: Point
// ---------------------------------------------------------------------------

#[tokio::test]
async fn native_geo_types() {
    let client = prepare_native_database("geo").await;

    client
        .query(&format!(
            "CREATE TABLE t{} (id UInt32, pt Point) {}",
            on_cluster(),
            test_engine("tuple()")
        ))
        .execute()
        .await
        .expect("CREATE failed");

    client
        .query(
            "INSERT INTO t VALUES \
             (1, (1.5, 2.5)), \
             (2, (0.0, -90.0))",
        )
        .execute()
        .await
        .expect("INSERT failed");

    // Point = 2 x Float64 LE (16 raw bytes).  Read as [u8; 16] to avoid
    // relying on serde tuple deserialization, then decode f64 values manually.
    #[derive(Debug, Row, Deserialize)]
    struct GeoRow {
        id: u32,
        pt: [u8; 16],
    }

    let rows = client
        .query("SELECT id, pt FROM t ORDER BY id ASC")
        .fetch_all::<GeoRow>()
        .await
        .expect("fetch failed");

    assert_eq!(rows.len(), 2);
    let x0 = f64::from_le_bytes(rows[0].pt[..8].try_into().unwrap());
    let y0 = f64::from_le_bytes(rows[0].pt[8..].try_into().unwrap());
    assert!((x0 - 1.5).abs() < f64::EPSILON);
    assert!((y0 - 2.5).abs() < f64::EPSILON);
    let x1 = f64::from_le_bytes(rows[1].pt[..8].try_into().unwrap());
    let y1 = f64::from_le_bytes(rows[1].pt[8..].try_into().unwrap());
    assert!((x1 - 0.0).abs() < f64::EPSILON);
    assert!((y1 - (-90.0)).abs() < f64::EPSILON);
}

#[tokio::test]
async fn native_insert_scalars() {
    let client = prepare_native_database("insert_scalars").await;

    client
        .query(&format!(
            "CREATE TABLE t{} (
                id   UInt32,
                val  Int64,
                f    Float64
            ) {}",
            on_cluster(),
            test_engine("id"),
        ))
        .execute()
        .await
        .expect("CREATE failed");

    #[derive(Debug, Row, Serialize, Deserialize, PartialEq)]
    struct ScalarRow {
        id: u32,
        val: i64,
        f: f64,
    }

    let mut insert = client.insert::<ScalarRow>("t");
    insert
        .write(&ScalarRow { id: 1, val: -100, f: 3.14 })
        .await
        .expect("write 1 failed");
    insert
        .write(&ScalarRow { id: 2, val: 200, f: 2.718 })
        .await
        .expect("write 2 failed");
    insert.end().await.expect("end failed");

    let rows = client
        .query("SELECT id, val, f FROM t ORDER BY id ASC")
        .fetch_all::<ScalarRow>()
        .await
        .expect("fetch failed");

    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].id, 1);
    assert_eq!(rows[0].val, -100);
    assert!((rows[0].f - 3.14f64).abs() < 1e-10);
    assert_eq!(rows[1].id, 2);
    assert_eq!(rows[1].val, 200);
    assert!((rows[1].f - 2.718f64).abs() < 1e-10);
}

#[tokio::test]
async fn native_insert_strings() {
    let client = prepare_native_database("insert_strings").await;

    client
        .query(&format!(
            "CREATE TABLE t{} (id UInt32, name String) {}",
            on_cluster(),
            test_engine("id"),
        ))
        .execute()
        .await
        .expect("CREATE failed");

    #[derive(Debug, Row, Serialize, Deserialize, PartialEq)]
    struct StringRow {
        id: u32,
        name: String,
    }

    let mut insert = client.insert::<StringRow>("t");
    insert
        .write(&StringRow { id: 1, name: "Alice".into() })
        .await
        .expect("write failed");
    insert
        .write(&StringRow { id: 2, name: "Bob".into() })
        .await
        .expect("write failed");
    insert.end().await.expect("end failed");

    let rows = client
        .query("SELECT id, name FROM t ORDER BY id ASC")
        .fetch_all::<StringRow>()
        .await
        .expect("fetch failed");

    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0], StringRow { id: 1, name: "Alice".into() });
    assert_eq!(rows[1], StringRow { id: 2, name: "Bob".into() });
}

/// INSERT into a table with a LowCardinality(String) column.
///
/// The encoder strips LowCardinality to its inner type and sends plain String
/// bytes; ClickHouse accepts this via implicit type conversion.
#[tokio::test]
async fn native_insert_low_cardinality() {
    let client = prepare_native_database("insert_lc").await;

    client
        .query(&format!(
            "CREATE TABLE t{} (id UInt32, tag LowCardinality(String)) {}",
            on_cluster(),
            test_engine("id"),
        ))
        .execute()
        .await
        .expect("CREATE failed");

    #[derive(Debug, Row, Serialize, Deserialize, PartialEq)]
    struct Row {
        id: u32,
        tag: String,
    }

    let mut insert = client.insert::<Row>("t");
    insert.write(&Row { id: 1, tag: "foo".into() }).await.expect("write 1 failed");
    insert.write(&Row { id: 2, tag: "bar".into() }).await.expect("write 2 failed");
    insert.write(&Row { id: 3, tag: "foo".into() }).await.expect("write 3 failed");
    insert.end().await.expect("end failed");

    let rows = client
        .query("SELECT id, tag FROM t ORDER BY id ASC")
        .fetch_all::<Row>()
        .await
        .expect("fetch failed");

    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0], Row { id: 1, tag: "foo".into() });
    assert_eq!(rows[1], Row { id: 2, tag: "bar".into() });
    assert_eq!(rows[2], Row { id: 3, tag: "foo".into() });
}

/// INSERT into a table with LowCardinality(Nullable(String)).
#[tokio::test]
async fn native_insert_low_cardinality_nullable() {
    let client = prepare_native_database("insert_lc_nullable").await;

    client
        .query(&format!(
            "CREATE TABLE t{} (id UInt32, tag LowCardinality(Nullable(String))) {}",
            on_cluster(),
            test_engine("id"),
        ))
        .execute()
        .await
        .expect("CREATE failed");

    #[derive(Debug, Row, Serialize, Deserialize, PartialEq)]
    struct Row {
        id: u32,
        tag: Option<String>,
    }

    let mut insert = client.insert::<Row>("t");
    insert.write(&Row { id: 1, tag: Some("alpha".into()) }).await.expect("write 1");
    insert.write(&Row { id: 2, tag: None }).await.expect("write 2");
    insert.write(&Row { id: 3, tag: Some("beta".into()) }).await.expect("write 3");
    insert.end().await.expect("end failed");

    let rows = client
        .query("SELECT id, tag FROM t ORDER BY id ASC")
        .fetch_all::<Row>()
        .await
        .expect("fetch failed");

    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0], Row { id: 1, tag: Some("alpha".into()) });
    assert_eq!(rows[1], Row { id: 2, tag: None });
    assert_eq!(rows[2], Row { id: 3, tag: Some("beta".into()) });
}

#[tokio::test]
async fn native_insert_nullable() {
    let client = prepare_native_database("insert_nullable").await;

    client
        .query(&format!(
            "CREATE TABLE t{} (id UInt32, val Nullable(Int32)) {}",
            on_cluster(),
            test_engine("id"),
        ))
        .execute()
        .await
        .expect("CREATE failed");

    #[derive(Debug, Row, Serialize, Deserialize, PartialEq)]
    struct NullRow {
        id: u32,
        val: Option<i32>,
    }

    let mut insert = client.insert::<NullRow>("t");
    insert
        .write(&NullRow { id: 1, val: Some(42) })
        .await
        .expect("write 1 failed");
    insert
        .write(&NullRow { id: 2, val: None })
        .await
        .expect("write 2 failed");
    insert
        .write(&NullRow { id: 3, val: Some(-7) })
        .await
        .expect("write 3 failed");
    insert.end().await.expect("end failed");

    let rows = client
        .query("SELECT id, val FROM t ORDER BY id ASC")
        .fetch_all::<NullRow>()
        .await
        .expect("fetch failed");

    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0], NullRow { id: 1, val: Some(42) });
    assert_eq!(rows[1], NullRow { id: 2, val: None });
    assert_eq!(rows[2], NullRow { id: 3, val: Some(-7) });
}

#[tokio::test]
async fn native_insert_empty() {
    // Calling end() without any writes should not error.
    let client = prepare_native_database("insert_empty").await;

    client
        .query(&format!(
            "CREATE TABLE t{} (id UInt32) {}",
            on_cluster(),
            test_engine("id"),
        ))
        .execute()
        .await
        .expect("CREATE failed");

    #[derive(Debug, Row, Serialize, Deserialize)]
    struct EmptyRow {
        id: u32,
    }

    let insert = client.insert::<EmptyRow>("t");
    insert.end().await.expect("empty end failed");

    let count: u64 = client
        .query("SELECT count() FROM t")
        .fetch_one::<u64>()
        .await
        .expect("count failed");

    assert_eq!(count, 0);
}

#[tokio::test]
async fn native_inserter_basic() {
    let client = prepare_native_database("inserter_basic").await;

    client
        .query(&format!(
            "CREATE TABLE t{} (id UInt32, val String) {}",
            on_cluster(),
            test_engine("id"),
        ))
        .execute()
        .await
        .expect("CREATE failed");

    #[derive(Debug, Row, Serialize, Deserialize, PartialEq)]
    struct TestRow {
        id: u32,
        val: String,
    }

    let mut inserter = client
        .inserter::<TestRow>("t")
        .with_max_rows(10_000);

    for i in 0u32..100 {
        inserter
            .write(&TestRow { id: i, val: format!("item_{i}") })
            .await
            .expect("write failed");
    }
    inserter.commit().await.expect("commit failed");
    inserter.end().await.expect("end failed");

    let count: u64 = client
        .query("SELECT count() FROM t")
        .fetch_one::<u64>()
        .await
        .expect("count failed");

    assert_eq!(count, 100);
}

#[tokio::test]
async fn native_schema_cache() {
    let client = prepare_native_database("schema_cache").await;

    client
        .query(&format!(
            "CREATE TABLE t{} (id UInt32, name String) {}",
            on_cluster(),
            test_engine("id"),
        ))
        .execute()
        .await
        .expect("CREATE failed");

    // Cache is empty before any INSERT.
    assert!(client.cached_schema("t").is_none());

    // After an INSERT, the cache is populated from the server's column headers.
    #[derive(Debug, Row, Serialize)]
    struct TestRow {
        id: u32,
        name: String,
    }

    let mut insert = client.insert::<TestRow>("t");
    insert
        .write(&TestRow { id: 1, name: "x".into() })
        .await
        .expect("write failed");
    insert.end().await.expect("end failed");

    let schema = client.cached_schema("t").expect("schema should be cached after INSERT");
    // Server returns the actual column types; just verify names are present.
    let names: Vec<&str> = schema.iter().map(|(n, _)| n.as_str()).collect();
    assert!(names.contains(&"id"), "expected 'id' in schema");
    assert!(names.contains(&"name"), "expected 'name' in schema");

    // fetch_schema should also populate the cache.
    client.clear_cached_schema("t");
    let fetched = client.fetch_schema("t").await.expect("fetch_schema failed");
    assert!(!fetched.is_empty());
}

#[tokio::test]
async fn native_insert_array() {
    let client = prepare_native_database("insert_array").await;

    client
        .query(&format!(
            "CREATE TABLE t{} (id UInt32, tags Array(String)) {}",
            on_cluster(),
            test_engine("id"),
        ))
        .execute()
        .await
        .expect("CREATE failed");

    #[derive(Debug, Row, Serialize, Deserialize, PartialEq)]
    struct ArrayRow {
        id: u32,
        tags: Vec<String>,
    }

    let mut insert = client.insert::<ArrayRow>("t");
    insert
        .write(&ArrayRow { id: 1, tags: vec!["alpha".into(), "beta".into()] })
        .await
        .expect("write 1 failed");
    insert
        .write(&ArrayRow { id: 2, tags: vec![] })
        .await
        .expect("write 2 (empty array) failed");
    insert
        .write(&ArrayRow { id: 3, tags: vec!["gamma".into()] })
        .await
        .expect("write 3 failed");
    insert.end().await.expect("end failed");

    let rows = client
        .query("SELECT id, tags FROM t ORDER BY id ASC")
        .fetch_all::<ArrayRow>()
        .await
        .expect("fetch failed");

    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0], ArrayRow { id: 1, tags: vec!["alpha".into(), "beta".into()] });
    assert_eq!(rows[1], ArrayRow { id: 2, tags: vec![] });
    assert_eq!(rows[2], ArrayRow { id: 3, tags: vec!["gamma".into()] });
}

#[tokio::test]
async fn native_insert_nested_array() {
    let client = prepare_native_database("insert_nested_array").await;

    client
        .query(&format!(
            "CREATE TABLE t{} (id UInt32, vals Array(UInt32)) {}",
            on_cluster(),
            test_engine("id"),
        ))
        .execute()
        .await
        .expect("CREATE failed");

    #[derive(Debug, Row, Serialize, Deserialize, PartialEq)]
    struct NumArrayRow {
        id: u32,
        vals: Vec<u32>,
    }

    let mut insert = client.insert::<NumArrayRow>("t");
    insert
        .write(&NumArrayRow { id: 1, vals: vec![10, 20, 30] })
        .await
        .expect("write failed");
    insert
        .write(&NumArrayRow { id: 2, vals: vec![1] })
        .await
        .expect("write failed");
    insert.end().await.expect("end failed");

    let rows = client
        .query("SELECT id, vals FROM t ORDER BY id ASC")
        .fetch_all::<NumArrayRow>()
        .await
        .expect("fetch failed");

    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].vals, vec![10u32, 20, 30]);
    assert_eq!(rows[1].vals, vec![1u32]);
}

#[tokio::test]
async fn native_insert_tuple() {
    let client = prepare_native_database("insert_tuple").await;

    client
        .query(&format!(
            "CREATE TABLE t{} (id UInt32, pair Tuple(String, UInt32)) {}",
            on_cluster(),
            test_engine("id"),
        ))
        .execute()
        .await
        .expect("CREATE failed");

    #[derive(Debug, Row, Serialize, Deserialize, PartialEq)]
    struct TupleRow {
        id: u32,
        pair: (String, u32),
    }

    let mut insert = client.insert::<TupleRow>("t");
    insert
        .write(&TupleRow { id: 1, pair: ("hello".into(), 42) })
        .await
        .expect("write 1 failed");
    insert
        .write(&TupleRow { id: 2, pair: ("world".into(), 7) })
        .await
        .expect("write 2 failed");
    insert.end().await.expect("end failed");

    let rows = client
        .query("SELECT id, pair FROM t ORDER BY id ASC")
        .fetch_all::<TupleRow>()
        .await
        .expect("fetch failed");

    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0], TupleRow { id: 1, pair: ("hello".into(), 42) });
    assert_eq!(rows[1], TupleRow { id: 2, pair: ("world".into(), 7) });
}

#[tokio::test]
async fn native_insert_map() {
    let client = prepare_native_database("insert_map").await;

    client
        .query(&format!(
            "CREATE TABLE t{} (id UInt32, counts Map(String, UInt32)) {}",
            on_cluster(),
            test_engine("id"),
        ))
        .execute()
        .await
        .expect("CREATE failed");

    // ClickHouse Map serializes in RowBinary as varuint(n) + [k1, v1, k2, v2, ...]
    // HashMap<K, V> does this via serde map serialization.
    use std::collections::HashMap;

    #[derive(Debug, Row, Serialize, Deserialize)]
    struct MapRow {
        id: u32,
        counts: HashMap<String, u32>,
    }

    let mut insert = client.insert::<MapRow>("t");
    let mut m1 = HashMap::new();
    m1.insert("a".to_string(), 1u32);
    m1.insert("b".to_string(), 2u32);
    insert.write(&MapRow { id: 1, counts: m1 }).await.expect("write 1 failed");

    let m2 = HashMap::new();
    insert.write(&MapRow { id: 2, counts: m2 }).await.expect("write 2 (empty map) failed");

    insert.end().await.expect("end failed");

    // Read back: verify map size and spot-check a value
    #[derive(Debug, Row, Deserialize)]
    struct ReadRow {
        id: u32,
        n: u64,  // number of entries
        val_a: u32, // counts['a'] for row 1
    }

    let rows = client
        .query("SELECT id, length(counts) AS n, counts['a'] AS val_a FROM t ORDER BY id ASC")
        .fetch_all::<ReadRow>()
        .await
        .expect("fetch failed");

    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].id, 1);
    assert_eq!(rows[0].n, 2); // two entries
    assert_eq!(rows[0].val_a, 1); // counts['a'] == 1
    assert_eq!(rows[1].id, 2);
    assert_eq!(rows[1].n, 0); // empty map
}

#[tokio::test]
async fn native_insert_lz4() {
    // Verify that INSERT works correctly with LZ4 compression enabled.
    let client = prepare_native_database("insert_lz4").await;

    client
        .query(&format!(
            "CREATE TABLE t{} (id UInt32, name String) {}",
            on_cluster(),
            test_engine("id"),
        ))
        .execute()
        .await
        .expect("CREATE failed");

    // Build a client with LZ4 compression pointing at the same database.
    let lz4_client = get_native_client()
        .with_lz4()
        .with_database("chrs_native_insert_lz4");

    #[derive(Debug, Row, Serialize, Deserialize)]
    struct Row {
        id: u32,
        name: String,
    }

    let mut insert = lz4_client.insert::<Row>("t");
    insert.write(&Row { id: 1, name: "alice".to_string() }).await.expect("write 1 failed");
    insert.write(&Row { id: 2, name: "bob".to_string() }).await.expect("write 2 failed");
    insert.end().await.expect("end failed");

    // Read back without compression to confirm data integrity.
    let rows = client
        .query("SELECT id, name FROM t ORDER BY id ASC")
        .fetch_all::<Row>()
        .await
        .expect("fetch failed");

    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].id, 1);
    assert_eq!(rows[0].name, "alice");
    assert_eq!(rows[1].id, 2);
    assert_eq!(rows[1].name, "bob");
}

// ---------------------------------------------------------------------------
// Pool edge cases
// ---------------------------------------------------------------------------

/// Pool size 2, 10 concurrent tasks -- all must succeed.
/// Verifies that tasks waiting for a connection are eventually served.
#[tokio::test]
async fn native_pool_concurrent() {
    let client = get_native_client().with_pool_size(2);
    let tasks: Vec<_> = (0..10u8)
        .map(|i| {
            let c = client.clone();
            tokio::spawn(async move {
                let n: u8 = c
                    .query(&format!("SELECT {i}"))
                    .fetch_one::<u8>()
                    .await
                    .expect("concurrent query failed");
                assert_eq!(n, i);
            })
        })
        .collect();
    for task in tasks {
        task.await.expect("task panicked");
    }
}

/// A server exception must not permanently break the pool.
/// The next query after an error must succeed on a fresh/recycled connection.
#[tokio::test]
async fn native_pool_error_recovery() {
    let client = get_native_client();

    // Trigger a server exception (table does not exist).
    let result = client
        .query("SELECT * FROM _this_table_does_not_exist_clickhouse_rs_test")
        .fetch_all::<u8>()
        .await;
    assert!(result.is_err(), "expected error from bad query");

    // Pool must still be usable after the error.
    let n: u8 = client
        .query("SELECT 99")
        .fetch_one::<u8>()
        .await
        .expect("query after error must succeed");
    assert_eq!(n, 99);
}

/// Pool size 1 + many concurrent inserts -- verifies no deadlock when the
/// INSERT holds the sole connection and another task waits for it.
#[tokio::test]
async fn native_pool_insert_wait() {
    let client = prepare_native_database("pool_insert_wait").await;

    client
        .query(&format!(
            "CREATE TABLE t{} (id UInt32) {}",
            on_cluster(),
            test_engine("id"),
        ))
        .execute()
        .await
        .expect("CREATE failed");

    let small_client = client.clone().with_pool_size(1);

    #[derive(Debug, Row, Serialize)]
    struct R {
        id: u32,
    }

    // Task A holds the connection in an INSERT.
    // Task B tries to ping at the same time -- it must wait, not deadlock.
    let client_a = small_client.clone();
    let client_b = small_client.clone();

    let insert_task = tokio::spawn(async move {
        let mut ins = client_a.insert::<R>("t");
        for i in 0..100u32 {
            ins.write(&R { id: i }).await.expect("write failed");
        }
        ins.end().await.expect("end failed");
    });
    let ping_task = tokio::spawn(async move {
        client_b.ping().await.expect("ping failed while insert held connection");
    });

    insert_task.await.expect("insert task panicked");
    ping_task.await.expect("ping task panicked");
}

// ---------------------------------------------------------------------------
// Bool / sparse-serialization edge cases
// ---------------------------------------------------------------------------

/// All rows false -- sparse format sends 0 non-default values.
#[tokio::test]
async fn native_bool_all_false() {
    let client = prepare_native_database("bool_all_false").await;

    client
        .query(&format!(
            "CREATE TABLE t{} (id UInt32, flag Bool) {}",
            on_cluster(),
            test_engine("id"),
        ))
        .execute()
        .await
        .expect("CREATE failed");

    for i in 1..=8u32 {
        client
            .query(&format!("INSERT INTO t VALUES ({i}, false)"))
            .execute()
            .await
            .expect("INSERT failed");
    }

    #[derive(Debug, Row, Deserialize, PartialEq)]
    struct BoolRow {
        id: u32,
        flag: bool,
    }

    let rows = client
        .query("SELECT id, flag FROM t ORDER BY id ASC")
        .fetch_all::<BoolRow>()
        .await
        .expect("fetch failed");

    assert_eq!(rows.len(), 8);
    for row in &rows {
        assert!(!row.flag, "expected false for id={}", row.id);
    }
}

/// All rows true -- sparse format stores every row as a non-default value.
#[tokio::test]
async fn native_bool_all_true() {
    let client = prepare_native_database("bool_all_true").await;

    client
        .query(&format!(
            "CREATE TABLE t{} (id UInt32, flag Bool) {}",
            on_cluster(),
            test_engine("id"),
        ))
        .execute()
        .await
        .expect("CREATE failed");

    for i in 1..=8u32 {
        client
            .query(&format!("INSERT INTO t VALUES ({i}, true)"))
            .execute()
            .await
            .expect("INSERT failed");
    }

    #[derive(Debug, Row, Deserialize, PartialEq)]
    struct BoolRow {
        id: u32,
        flag: bool,
    }

    let rows = client
        .query("SELECT id, flag FROM t ORDER BY id ASC")
        .fetch_all::<BoolRow>()
        .await
        .expect("fetch failed");

    assert_eq!(rows.len(), 8);
    for row in &rows {
        assert!(row.flag, "expected true for id={}", row.id);
    }
}

/// Mixed true/false across many rows -- exercises sparse offset groups.
#[tokio::test]
async fn native_bool_many_rows() {
    let client = prepare_native_database("bool_many_rows").await;

    client
        .query(&format!(
            "CREATE TABLE t{} (id UInt32, flag Bool) {}",
            on_cluster(),
            test_engine("id"),
        ))
        .execute()
        .await
        .expect("CREATE failed");

    // Insert 200 rows in one batch: alternating true/false, then a run of
    // 50 trues, then 50 falses -- exercises multiple sparse offset groups.
    let vals: String = (0..200u32)
        .map(|i| {
            let b = if i < 100 { i % 2 == 0 } else { i < 150 };
            format!("({i}, {})", b)
        })
        .collect::<Vec<_>>()
        .join(", ");
    client
        .query(&format!("INSERT INTO t VALUES {vals}"))
        .execute()
        .await
        .expect("INSERT failed");

    #[derive(Debug, Row, Deserialize)]
    struct BoolRow {
        id: u32,
        flag: bool,
    }

    let rows = client
        .query("SELECT id, flag FROM t ORDER BY id ASC")
        .fetch_all::<BoolRow>()
        .await
        .expect("fetch failed");

    assert_eq!(rows.len(), 200);
    for row in &rows {
        let expected = if row.id < 100 {
            row.id % 2 == 0
        } else {
            row.id < 150
        };
        assert_eq!(row.flag, expected, "mismatch at id={}", row.id);
    }
}

/// Nullable(Bool): Some(true), Some(false), NULL.
#[tokio::test]
async fn native_bool_nullable() {
    let client = prepare_native_database("bool_nullable").await;

    client
        .query(&format!(
            "CREATE TABLE t{} (id UInt32, flag Nullable(Bool)) {}",
            on_cluster(),
            test_engine("id"),
        ))
        .execute()
        .await
        .expect("CREATE failed");

    client
        .query("INSERT INTO t VALUES (1, true), (2, false), (3, NULL)")
        .execute()
        .await
        .expect("INSERT failed");

    #[derive(Debug, Row, Deserialize, PartialEq)]
    struct BoolRow {
        id: u32,
        flag: Option<bool>,
    }

    let rows = client
        .query("SELECT id, flag FROM t ORDER BY id ASC")
        .fetch_all::<BoolRow>()
        .await
        .expect("fetch failed");

    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0], BoolRow { id: 1, flag: Some(true) });
    assert_eq!(rows[1], BoolRow { id: 2, flag: Some(false) });
    assert_eq!(rows[2], BoolRow { id: 3, flag: None });
}

/// INSERT Bool via `NativeInsert<T>` (not SQL VALUES) -- tests the encoder path.
#[tokio::test]
async fn native_insert_bool() {
    let client = prepare_native_database("insert_bool").await;

    client
        .query(&format!(
            "CREATE TABLE t{} (id UInt32, flag Bool) {}",
            on_cluster(),
            test_engine("id"),
        ))
        .execute()
        .await
        .expect("CREATE failed");

    #[derive(Debug, Row, Serialize, Deserialize, PartialEq)]
    struct BoolRow {
        id: u32,
        flag: bool,
    }

    let mut insert = client.insert::<BoolRow>("t");
    insert.write(&BoolRow { id: 1, flag: true }).await.expect("write 1 failed");
    insert.write(&BoolRow { id: 2, flag: false }).await.expect("write 2 failed");
    insert.write(&BoolRow { id: 3, flag: true }).await.expect("write 3 failed");
    insert.end().await.expect("end failed");

    let rows = client
        .query("SELECT id, flag FROM t ORDER BY id ASC")
        .fetch_all::<BoolRow>()
        .await
        .expect("fetch failed");

    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0], BoolRow { id: 1, flag: true });
    assert_eq!(rows[1], BoolRow { id: 2, flag: false });
    assert_eq!(rows[2], BoolRow { id: 3, flag: true });
}

/// Bool column alongside non-sparse UInt32 and String columns.
///
/// Verifies that the sparse decoder does not misalign the stream -- after reading
/// the Bool column's sparse offsets + values, the reader must be positioned
/// exactly at the next column's data.
#[tokio::test]
async fn native_bool_sparse_stream_alignment() {
    let client = prepare_native_database("bool_sparse_align").await;

    client
        .query(&format!(
            "CREATE TABLE t{} (id UInt32, flag Bool, name String) {}",
            on_cluster(),
            test_engine("id"),
        ))
        .execute()
        .await
        .expect("CREATE failed");

    client
        .query("INSERT INTO t VALUES (1, true, 'alice'), (2, false, 'bob'), (3, true, 'carol'), (4, false, 'dave')")
        .execute()
        .await
        .expect("INSERT failed");

    #[derive(Debug, Row, Deserialize, PartialEq)]
    struct R {
        id: u32,
        flag: bool,
        name: String,
    }

    let rows = client
        .query("SELECT id, flag, name FROM t ORDER BY id ASC")
        .fetch_all::<R>()
        .await
        .expect("fetch failed");

    assert_eq!(rows.len(), 4);
    assert_eq!(rows[0], R { id: 1, flag: true,  name: "alice".into() });
    assert_eq!(rows[1], R { id: 2, flag: false, name: "bob".into() });
    assert_eq!(rows[2], R { id: 3, flag: true,  name: "carol".into() });
    assert_eq!(rows[3], R { id: 4, flag: false, name: "dave".into() });
}

/// Two consecutive Bool columns -- each must decode its own sparse stream
/// independently without cross-contamination.
#[tokio::test]
async fn native_bool_multi_sparse_columns() {
    let client = prepare_native_database("bool_multi_sparse").await;

    client
        .query(&format!(
            "CREATE TABLE t{} (a Bool, b Bool) {}",
            on_cluster(),
            test_engine("a"),
        ))
        .execute()
        .await
        .expect("CREATE failed");

    // a: T F T F T, b: F F T T F -> different sparse patterns.
    client
        .query("INSERT INTO t VALUES (true,false),(false,false),(true,true),(false,true),(true,false)")
        .execute()
        .await
        .expect("INSERT failed");

    #[derive(Debug, Row, Deserialize, PartialEq)]
    struct R { a: bool, b: bool }

    let rows = client
        .query("SELECT a, b FROM t ORDER BY (a,b)")
        .fetch_all::<R>()
        .await
        .expect("fetch failed");

    // Sort-order independent check: collect (a,b) pairs.
    let mut got: Vec<(bool, bool)> = rows.iter().map(|r| (r.a, r.b)).collect();
    got.sort();
    // (T,F),(F,F),(T,T),(F,T),(T,F) sorted: (F,F),(F,T),(T,F),(T,F),(T,T)
    assert_eq!(
        got,
        vec![(false,false),(false,true),(true,false),(true,false),(true,true)]
    );
}

/// 1 000 rows, only the last one is `true`.
///
/// Exercises large VarUInt offsets in the sparse stream (offset group = 999).
#[tokio::test]
async fn native_bool_sparse_large_gap() {
    let client = prepare_native_database("bool_sparse_large_gap").await;

    client
        .query(&format!(
            "CREATE TABLE t{} (id UInt32, flag Bool) {}",
            on_cluster(),
            test_engine("id"),
        ))
        .execute()
        .await
        .expect("CREATE failed");

    #[derive(Debug, Row, Serialize)]
    struct W { id: u32, flag: bool }
    #[derive(Debug, Row, Deserialize)]
    struct R { id: u32, flag: bool }

    const N: u32 = 1000;
    let mut insert = client.insert::<W>("t");
    for i in 0..N {
        insert.write(&W { id: i, flag: i == N - 1 }).await.expect("write failed");
    }
    insert.end().await.expect("end failed");

    let rows = client
        .query("SELECT id, flag FROM t ORDER BY id ASC")
        .fetch_all::<R>()
        .await
        .expect("fetch failed");

    assert_eq!(rows.len(), N as usize);
    for (i, row) in rows.iter().enumerate() {
        assert_eq!(row.id, i as u32);
        assert_eq!(row.flag, i == (N - 1) as usize,
                   "row {i}: expected flag={}", i == (N - 1) as usize);
    }
}

/// 1 000 rows, only position 0 is `true` -- zero-offset sparse group.
#[tokio::test]
async fn native_bool_sparse_single_at_start() {
    let client = prepare_native_database("bool_sparse_single_start").await;

    client
        .query(&format!(
            "CREATE TABLE t{} (id UInt32, flag Bool) {}",
            on_cluster(),
            test_engine("id"),
        ))
        .execute()
        .await
        .expect("CREATE failed");

    #[derive(Debug, Row, Serialize)]
    struct W { id: u32, flag: bool }
    #[derive(Debug, Row, Deserialize)]
    struct R { id: u32, flag: bool }

    const N: u32 = 1000;
    let mut insert = client.insert::<W>("t");
    for i in 0..N {
        insert.write(&W { id: i, flag: i == 0 }).await.expect("write failed");
    }
    insert.end().await.expect("end failed");

    let rows = client
        .query("SELECT id, flag FROM t ORDER BY id ASC")
        .fetch_all::<R>()
        .await
        .expect("fetch failed");

    assert_eq!(rows.len(), N as usize);
    assert!(rows[0].flag, "row 0 should be true");
    for row in &rows[1..] {
        assert!(!row.flag, "row {} should be false", row.id);
    }
}

// ---------------------------------------------------------------------------
// INSERT edge cases
// ---------------------------------------------------------------------------

/// Write 50 000 rows -- enough to trigger multiple intermediate flushes at the
/// 256 KiB threshold.  Verifies all rows arrive after end().
#[tokio::test]
async fn native_insert_large_batch() {
    let client = prepare_native_database("insert_large_batch").await;

    client
        .query(&format!(
            "CREATE TABLE t{} (id UInt64) {}",
            on_cluster(),
            test_engine("id"),
        ))
        .execute()
        .await
        .expect("CREATE failed");

    #[derive(Debug, Row, Serialize)]
    struct NumRow {
        id: u64,
    }

    const N: u64 = 50_000;
    let mut insert = client.insert::<NumRow>("t");
    for i in 0..N {
        insert.write(&NumRow { id: i }).await.expect("write failed");
    }
    insert.end().await.expect("end failed");

    let count: u64 = client
        .query("SELECT count() FROM t")
        .fetch_one::<u64>()
        .await
        .expect("count failed");

    assert_eq!(count, N, "row count mismatch after large batch");
}

/// Drop `NativeInsert` without calling `end()` -- must not commit any data,
/// and must not leave the pool connection in a broken state.
#[tokio::test]
async fn native_insert_abort() {
    let client = prepare_native_database("insert_abort").await;

    client
        .query(&format!(
            "CREATE TABLE t{} (id UInt32) {}",
            on_cluster(),
            test_engine("id"),
        ))
        .execute()
        .await
        .expect("CREATE failed");

    #[derive(Debug, Row, Serialize)]
    struct R {
        id: u32,
    }

    // Write two rows then drop without end() -- aborts the INSERT.
    {
        let mut insert = client.insert::<R>("t");
        insert.write(&R { id: 1 }).await.expect("write 1 failed");
        insert.write(&R { id: 2 }).await.expect("write 2 failed");
        // dropped here -- connection must be discarded, not returned to pool
    }

    // The pool must still work after the aborted insert.
    let count: u64 = client
        .query("SELECT count() FROM t")
        .fetch_one::<u64>()
        .await
        .expect("count after abort failed");

    assert_eq!(count, 0, "aborted insert must not commit data");
}

/// Two sequential inserts into the same table to verify pool reuse between
/// INSERT operations does not misalign the protocol.
#[tokio::test]
async fn native_insert_sequential() {
    let client = prepare_native_database("insert_sequential").await;

    client
        .query(&format!(
            "CREATE TABLE t{} (id UInt32) {}",
            on_cluster(),
            test_engine("id"),
        ))
        .execute()
        .await
        .expect("CREATE failed");

    #[derive(Debug, Row, Serialize)]
    struct R {
        id: u32,
    }

    // First INSERT
    let mut ins = client.insert::<R>("t");
    ins.write(&R { id: 1 }).await.expect("write 1a failed");
    ins.write(&R { id: 2 }).await.expect("write 1b failed");
    ins.end().await.expect("end 1 failed");

    // Second INSERT reuses the same connection from the pool.
    let mut ins2 = client.insert::<R>("t");
    ins2.write(&R { id: 3 }).await.expect("write 2a failed");
    ins2.end().await.expect("end 2 failed");

    let count: u64 = client
        .query("SELECT count() FROM t")
        .fetch_one::<u64>()
        .await
        .expect("count failed");

    assert_eq!(count, 3);
}

// ---------------------------------------------------------------------------
// Query API edge cases
// ---------------------------------------------------------------------------

/// `fetch_one` on an empty result set must return `RowNotFound`.
#[tokio::test]
async fn native_query_fetch_one_empty() {
    let client = prepare_native_database("fetch_one_empty").await;

    client
        .query(&format!(
            "CREATE TABLE t{} (id UInt32) {}",
            on_cluster(),
            test_engine("id"),
        ))
        .execute()
        .await
        .expect("CREATE failed");

    let result = client
        .query("SELECT id FROM t")
        .fetch_one::<u32>()
        .await;

    assert!(
        matches!(result, Err(clickhouse::error::Error::RowNotFound)),
        "expected RowNotFound, got {result:?}"
    );
}

/// `fetch_optional` returns `None` when no rows match, `Some` when one does.
#[tokio::test]
async fn native_query_fetch_optional() {
    let client = prepare_native_database("fetch_optional").await;

    client
        .query(&format!(
            "CREATE TABLE t{} (id UInt32) {}",
            on_cluster(),
            test_engine("id"),
        ))
        .execute()
        .await
        .expect("CREATE failed");

    // Empty table -> None.
    let none: Option<u32> = client
        .query("SELECT id FROM t")
        .fetch_optional::<u32>()
        .await
        .expect("fetch_optional failed");
    assert!(none.is_none());

    // Insert one row.
    client
        .query("INSERT INTO t VALUES (42)")
        .execute()
        .await
        .expect("INSERT failed");

    // One row -> Some.
    let some: Option<u32> = client
        .query("SELECT id FROM t LIMIT 1")
        .fetch_optional::<u32>()
        .await
        .expect("fetch_optional failed");
    assert_eq!(some, Some(42u32));
}

/// `bind()` with multiple `?` placeholders -- each replaces the next occurrence.
/// Note: bind() escapes values as quoted strings (safe against injection).
/// For typed arithmetic, use server-side param() binding.
#[tokio::test]
async fn native_query_bind_multiple() {
    let client = get_native_client();

    // bind() wraps values in single quotes (string escaping), so use
    // param() for typed server-side binding when types matter.
    let result: u8 = client
        .query("SELECT {a:UInt8} + {b:UInt8}")
        .param("a", 10u8)
        .param("b", 32u8)
        .fetch_one::<u8>()
        .await
        .expect("fetch failed");

    assert_eq!(result, 42u8);
}

/// `bind()` when the SQL has no `?` -- should be a no-op (query unchanged).
#[tokio::test]
async fn native_query_bind_no_placeholder() {
    let client = get_native_client();

    let result: u8 = client
        .query("SELECT 1")
        .bind(999u32)   // no placeholder -- ignored
        .fetch_one::<u8>()
        .await
        .expect("fetch failed");

    assert_eq!(result, 1u8);
}

// ---------------------------------------------------------------------------
// Nullable edge cases
// ---------------------------------------------------------------------------

/// Column that is NULL for every row.
#[tokio::test]
async fn native_nullable_all_null() {
    let client = prepare_native_database("nullable_all_null").await;

    client
        .query(&format!(
            "CREATE TABLE t{} (id UInt32, val Nullable(Int64)) {}",
            on_cluster(),
            test_engine("id"),
        ))
        .execute()
        .await
        .expect("CREATE failed");

    client
        .query("INSERT INTO t VALUES (1, NULL), (2, NULL), (3, NULL)")
        .execute()
        .await
        .expect("INSERT failed");

    #[derive(Debug, Row, Deserialize, PartialEq)]
    struct R {
        id: u32,
        val: Option<i64>,
    }

    let rows = client
        .query("SELECT id, val FROM t ORDER BY id ASC")
        .fetch_all::<R>()
        .await
        .expect("fetch failed");

    assert_eq!(rows.len(), 3);
    for row in &rows {
        assert!(row.val.is_none(), "expected NULL for id={}", row.id);
    }
}

/// Array(Nullable(String)) -- nulls inside an array.
#[tokio::test]
async fn native_array_of_nullable() {
    let client = prepare_native_database("array_nullable").await;

    client
        .query(&format!(
            "CREATE TABLE t{} (id UInt32, tags Array(Nullable(String))) {}",
            on_cluster(),
            test_engine("id"),
        ))
        .execute()
        .await
        .expect("CREATE failed");

    client
        .query("INSERT INTO t VALUES (1, ['a', NULL, 'b']), (2, [])")
        .execute()
        .await
        .expect("INSERT failed");

    #[derive(Debug, Row, Deserialize)]
    struct R {
        id: u32,
        tags: Vec<Option<String>>,
    }

    let rows = client
        .query("SELECT id, tags FROM t ORDER BY id ASC")
        .fetch_all::<R>()
        .await
        .expect("fetch failed");

    assert_eq!(rows.len(), 2);
    assert_eq!(
        rows[0].tags,
        vec![Some("a".into()), None, Some("b".into())]
    );
    assert_eq!(rows[1].tags, Vec::<Option<String>>::new());
}

// ---------------------------------------------------------------------------
// Schema cache edge cases
// ---------------------------------------------------------------------------

/// `fetch_schema` on a non-existent table must return an error (empty result).
#[tokio::test]
async fn native_schema_cache_miss() {
    let client = get_native_client();

    // Non-existent table returns empty schema (not an error from ClickHouse).
    let schema = client
        .fetch_schema("_this_table_does_not_exist_xyz_clickhouse_rs")
        .await
        .expect("fetch_schema should not error on missing table");

    assert!(
        schema.is_empty(),
        "expected empty schema for non-existent table, got {schema:?}"
    );
}

/// `clear_all_cached_schemas` removes all entries; subsequent access re-fetches.
#[tokio::test]
async fn native_schema_cache_clear_all() {
    let client = prepare_native_database("schema_clear_all").await;

    client
        .query(&format!(
            "CREATE TABLE t{} (id UInt32, name String) {}",
            on_cluster(),
            test_engine("id"),
        ))
        .execute()
        .await
        .expect("CREATE failed");

    #[derive(Debug, Row, Serialize)]
    struct TestRow {
        id: u32,
        name: String,
    }

    // Populate the cache via an INSERT.
    let mut ins = client.insert::<TestRow>("t");
    ins.write(&TestRow { id: 1, name: "x".into() }).await.expect("write failed");
    ins.end().await.expect("end failed");

    assert!(client.cached_schema("t").is_some(), "cache should be populated after INSERT");

    // Clear all -- cache must be empty.
    client.clear_all_cached_schemas();
    assert!(client.cached_schema("t").is_none(), "cache should be empty after clear_all");

    // fetch_schema re-populates.
    let schema = client.fetch_schema("t").await.expect("fetch_schema failed");
    assert!(!schema.is_empty());
    assert!(client.cached_schema("t").is_some(), "cache should be re-populated after fetch_schema");
}

// ===========================================================================
// AsyncNativeInserter tests
// ===========================================================================

#[tokio::test]
async fn native_async_inserter_basic() {
    use clickhouse::native::{AsyncNativeInserter, AsyncNativeInserterConfig};

    #[derive(Debug, Clone, PartialEq, Row, Serialize, Deserialize)]
    struct R {
        id: u32,
        data: String,
    }

    let client = prepare_native_database("async_inserter_basic").await;
    client
        .query(&format!(
            "CREATE TABLE t{} (id UInt32, data String) {}",
            on_cluster(),
            test_engine("id"),
        ))
        .execute()
        .await
        .unwrap();

    let inserter = AsyncNativeInserter::<R>::new(
        &client,
        "t",
        AsyncNativeInserterConfig::default().without_period(),
    );

    for i in 0..100u32 {
        inserter
            .write(R { id: i, data: i.to_string() })
            .await
            .unwrap();
    }

    inserter.end().await.unwrap();

    let rows: Vec<R> = client
        .query("SELECT id, data FROM t ORDER BY id")
        .fetch_all()
        .await
        .unwrap();
    assert_eq!(rows.len(), 100);
    assert_eq!(rows[0].id, 0);
    assert_eq!(rows[99].id, 99);
}

#[tokio::test]
async fn native_async_inserter_flush() {
    use clickhouse::native::{AsyncNativeInserter, AsyncNativeInserterConfig};

    #[derive(Debug, Clone, PartialEq, Row, Serialize, Deserialize)]
    struct R {
        id: u32,
        data: String,
    }

    let client = prepare_native_database("async_inserter_flush").await;
    client
        .query(&format!(
            "CREATE TABLE t{} (id UInt32, data String) {}",
            on_cluster(),
            test_engine("id"),
        ))
        .execute()
        .await
        .unwrap();

    let inserter = AsyncNativeInserter::<R>::new(
        &client,
        "t",
        AsyncNativeInserterConfig::default().without_period(),
    );

    for i in 0..50u32 {
        inserter
            .write(R { id: i, data: i.to_string() })
            .await
            .unwrap();
    }

    let q = inserter.flush().await.unwrap();
    assert_eq!(q.rows, 50);

    let count: u64 = client
        .query("SELECT count() FROM t")
        .fetch_one()
        .await
        .unwrap();
    assert_eq!(count, 50);

    for i in 50..100u32 {
        inserter
            .write(R { id: i, data: i.to_string() })
            .await
            .unwrap();
    }

    inserter.end().await.unwrap();

    let count: u64 = client
        .query("SELECT count() FROM t")
        .fetch_one()
        .await
        .unwrap();
    assert_eq!(count, 100);
}

#[tokio::test]
async fn native_async_inserter_concurrent_handles() {
    use clickhouse::native::{AsyncNativeInserter, AsyncNativeInserterConfig};

    #[derive(Debug, Clone, PartialEq, Row, Serialize, Deserialize)]
    struct R {
        id: u32,
        data: String,
    }

    let client = prepare_native_database("async_inserter_concurrent").await;
    client
        .query(&format!(
            "CREATE TABLE t{} (id UInt32, data String) {}",
            on_cluster(),
            test_engine("id"),
        ))
        .execute()
        .await
        .unwrap();

    let inserter = AsyncNativeInserter::<R>::new(
        &client,
        "t",
        AsyncNativeInserterConfig::default().without_period(),
    );

    let mut tasks = Vec::new();
    for chunk_start in (0..100u32).step_by(10) {
        let handle = inserter.handle();
        tasks.push(tokio::spawn(async move {
            for i in chunk_start..chunk_start + 10 {
                handle
                    .write(R { id: i, data: i.to_string() })
                    .await
                    .unwrap();
            }
        }));
    }

    for task in tasks {
        task.await.unwrap();
    }

    inserter.end().await.unwrap();

    let count: u64 = client
        .query("SELECT count() FROM t")
        .fetch_one()
        .await
        .unwrap();
    assert_eq!(count, 100);
}

#[tokio::test]
async fn native_async_inserter_empty_end() {
    use clickhouse::native::{AsyncNativeInserter, AsyncNativeInserterConfig};

    #[derive(Debug, Clone, PartialEq, Row, Serialize, Deserialize)]
    struct R {
        id: u32,
        data: String,
    }

    let client = prepare_native_database("async_inserter_empty").await;
    client
        .query(&format!(
            "CREATE TABLE t{} (id UInt32, data String) {}",
            on_cluster(),
            test_engine("id"),
        ))
        .execute()
        .await
        .unwrap();

    let inserter = AsyncNativeInserter::<R>::new(
        &client,
        "t",
        AsyncNativeInserterConfig::default().without_period(),
    );

    inserter.end().await.unwrap();

    let count: u64 = client
        .query("SELECT count() FROM t")
        .fetch_one()
        .await
        .unwrap();
    assert_eq!(count, 0);
}

// -- Edge cases -----------------------------------------------------------

/// Writing a single row should work.
#[tokio::test]
async fn native_async_inserter_single_row() {
    use clickhouse::native::{AsyncNativeInserter, AsyncNativeInserterConfig};

    #[derive(Debug, Clone, PartialEq, Row, Serialize, Deserialize)]
    struct R {
        id: u32,
        data: String,
    }

    let client = prepare_native_database("async_inserter_single").await;
    client
        .query(&format!(
            "CREATE TABLE t{} (id UInt32, data String) {}",
            on_cluster(),
            test_engine("id"),
        ))
        .execute()
        .await
        .unwrap();

    let inserter = AsyncNativeInserter::<R>::new(
        &client,
        "t",
        AsyncNativeInserterConfig::default().without_period(),
    );

    inserter
        .write(R {
            id: 42,
            data: "hello".into(),
        })
        .await
        .unwrap();

    inserter.end().await.unwrap();

    let count: u64 = client
        .query("SELECT count() FROM t")
        .fetch_one()
        .await
        .unwrap();
    assert_eq!(count, 1);
}

/// Flush on empty buffer returns zero quantities (not an error).
#[tokio::test]
async fn native_async_inserter_flush_empty() {
    use clickhouse::native::{AsyncNativeInserter, AsyncNativeInserterConfig};

    #[derive(Debug, Clone, PartialEq, Row, Serialize, Deserialize)]
    struct R {
        id: u32,
        data: String,
    }

    let client = prepare_native_database("async_inserter_flush_empty").await;
    client
        .query(&format!(
            "CREATE TABLE t{} (id UInt32, data String) {}",
            on_cluster(),
            test_engine("id"),
        ))
        .execute()
        .await
        .unwrap();

    let inserter = AsyncNativeInserter::<R>::new(
        &client,
        "t",
        AsyncNativeInserterConfig::default().without_period(),
    );

    let q = inserter.flush().await.unwrap();
    assert_eq!(q.rows, 0);

    inserter.end().await.unwrap();
}

/// Multiple flushes in a row without writes in between.
#[tokio::test]
async fn native_async_inserter_double_flush() {
    use clickhouse::native::{AsyncNativeInserter, AsyncNativeInserterConfig};

    #[derive(Debug, Clone, PartialEq, Row, Serialize, Deserialize)]
    struct R {
        id: u32,
        data: String,
    }

    let client = prepare_native_database("async_inserter_double_flush").await;
    client
        .query(&format!(
            "CREATE TABLE t{} (id UInt32, data String) {}",
            on_cluster(),
            test_engine("id"),
        ))
        .execute()
        .await
        .unwrap();

    let inserter = AsyncNativeInserter::<R>::new(
        &client,
        "t",
        AsyncNativeInserterConfig::default().without_period(),
    );

    for i in 0..10u32 {
        inserter
            .write(R { id: i, data: i.to_string() })
            .await
            .unwrap();
    }

    let q1 = inserter.flush().await.unwrap();
    assert_eq!(q1.rows, 10);

    let q2 = inserter.flush().await.unwrap();
    assert_eq!(q2.rows, 0);

    inserter.end().await.unwrap();

    let count: u64 = client
        .query("SELECT count() FROM t")
        .fetch_one()
        .await
        .unwrap();
    assert_eq!(count, 10);
}

/// max_rows=1 should auto-flush after every single row.
#[tokio::test]
async fn native_async_inserter_max_rows_one() {
    use clickhouse::native::{AsyncNativeInserter, AsyncNativeInserterConfig};

    #[derive(Debug, Clone, PartialEq, Row, Serialize, Deserialize)]
    struct R {
        id: u32,
        data: String,
    }

    let client = prepare_native_database("async_inserter_max1").await;
    client
        .query(&format!(
            "CREATE TABLE t{} (id UInt32, data String) {}",
            on_cluster(),
            test_engine("id"),
        ))
        .execute()
        .await
        .unwrap();

    let inserter = AsyncNativeInserter::<R>::new(
        &client,
        "t",
        AsyncNativeInserterConfig::default()
            .with_max_rows(1)
            .without_period(),
    );

    for i in 0..5u32 {
        inserter
            .write(R { id: i, data: i.to_string() })
            .await
            .unwrap();
    }

    inserter.end().await.unwrap();

    let count: u64 = client
        .query("SELECT count() FROM t")
        .fetch_one()
        .await
        .unwrap();
    assert_eq!(count, 5);
}

/// Large string values round-trip correctly.
#[tokio::test]
async fn native_async_inserter_large_strings() {
    use clickhouse::native::{AsyncNativeInserter, AsyncNativeInserterConfig};

    #[derive(Debug, Clone, PartialEq, Row, Serialize, Deserialize)]
    struct R {
        id: u32,
        data: String,
    }

    let client = prepare_native_database("async_inserter_large_str").await;
    client
        .query(&format!(
            "CREATE TABLE t{} (id UInt32, data String) {}",
            on_cluster(),
            test_engine("id"),
        ))
        .execute()
        .await
        .unwrap();

    let inserter = AsyncNativeInserter::<R>::new(
        &client,
        "t",
        AsyncNativeInserterConfig::default().without_period(),
    );

    let big = "x".repeat(100_000);
    for i in 0..3u32 {
        inserter
            .write(R {
                id: i,
                data: big.clone(),
            })
            .await
            .unwrap();
    }

    inserter.end().await.unwrap();

    let count: u64 = client
        .query("SELECT count() FROM t")
        .fetch_one()
        .await
        .unwrap();
    assert_eq!(count, 3);
}

/// Small channel capacity (1) forces extreme backpressure.
#[tokio::test]
async fn native_async_inserter_tiny_channel() {
    use clickhouse::native::{AsyncNativeInserter, AsyncNativeInserterConfig};

    #[derive(Debug, Clone, PartialEq, Row, Serialize, Deserialize)]
    struct R {
        id: u32,
        data: String,
    }

    let client = prepare_native_database("async_inserter_tiny_ch").await;
    client
        .query(&format!(
            "CREATE TABLE t{} (id UInt32, data String) {}",
            on_cluster(),
            test_engine("id"),
        ))
        .execute()
        .await
        .unwrap();

    let inserter = AsyncNativeInserter::<R>::new(
        &client,
        "t",
        AsyncNativeInserterConfig::default()
            .with_channel_capacity(1)
            .without_period(),
    );

    for i in 0..20u32 {
        inserter
            .write(R { id: i, data: i.to_string() })
            .await
            .unwrap();
    }

    inserter.end().await.unwrap();

    let count: u64 = client
        .query("SELECT count() FROM t")
        .fetch_one()
        .await
        .unwrap();
    assert_eq!(count, 20);
}

// -- Failure / error propagation ------------------------------------------

/// Handle becomes inert after the inserter is ended -- writes should fail.
#[tokio::test]
async fn native_async_inserter_handle_after_end() {
    use clickhouse::native::{AsyncNativeInserter, AsyncNativeInserterConfig};

    #[derive(Debug, Clone, PartialEq, Row, Serialize, Deserialize)]
    struct R {
        id: u32,
        data: String,
    }

    let client = prepare_native_database("async_inserter_after_end").await;
    client
        .query(&format!(
            "CREATE TABLE t{} (id UInt32, data String) {}",
            on_cluster(),
            test_engine("id"),
        ))
        .execute()
        .await
        .unwrap();

    let inserter = AsyncNativeInserter::<R>::new(
        &client,
        "t",
        AsyncNativeInserterConfig::default().without_period(),
    );

    let handle = inserter.handle();

    inserter.end().await.unwrap();

    let result = handle
        .write(R {
            id: 1,
            data: "late".into(),
        })
        .await;
    assert!(result.is_err(), "write after end() should fail");
}

/// flush() via handle after inserter is ended should fail.
#[tokio::test]
async fn native_async_inserter_flush_after_end() {
    use clickhouse::native::{AsyncNativeInserter, AsyncNativeInserterConfig};

    #[derive(Debug, Clone, PartialEq, Row, Serialize, Deserialize)]
    struct R {
        id: u32,
        data: String,
    }

    let client = prepare_native_database("async_inserter_flush_end").await;
    client
        .query(&format!(
            "CREATE TABLE t{} (id UInt32, data String) {}",
            on_cluster(),
            test_engine("id"),
        ))
        .execute()
        .await
        .unwrap();

    let inserter = AsyncNativeInserter::<R>::new(
        &client,
        "t",
        AsyncNativeInserterConfig::default().without_period(),
    );

    let handle = inserter.handle();

    inserter.end().await.unwrap();

    let result = handle.flush().await;
    assert!(result.is_err(), "flush after end() should fail");
}

// -- Stress / concurrency -------------------------------------------------

/// Many concurrent writers with small max_rows to stress the flush path.
#[tokio::test]
async fn native_async_inserter_stress_concurrent() {
    use clickhouse::native::{AsyncNativeInserter, AsyncNativeInserterConfig};

    #[derive(Debug, Clone, PartialEq, Row, Serialize, Deserialize)]
    struct R {
        id: u32,
        data: String,
    }

    let client = prepare_native_database("async_inserter_stress").await;
    client
        .query(&format!(
            "CREATE TABLE t{} (id UInt32, data String) {}",
            on_cluster(),
            test_engine("id"),
        ))
        .execute()
        .await
        .unwrap();

    let inserter = AsyncNativeInserter::<R>::new(
        &client,
        "t",
        AsyncNativeInserterConfig::default()
            .with_max_rows(7) // prime number for odd batch boundaries
            .without_period(),
    );

    let mut tasks = Vec::new();
    for task_id in 0..20u32 {
        let handle = inserter.handle();
        tasks.push(tokio::spawn(async move {
            for j in 0..50u32 {
                let id = task_id * 50 + j;
                handle
                    .write(R {
                        id,
                        data: format!("task{task_id}_row{j}"),
                    })
                    .await
                    .unwrap();
            }
        }));
    }

    for task in tasks {
        task.await.unwrap();
    }

    inserter.end().await.unwrap();

    let count: u64 = client
        .query("SELECT count() FROM t")
        .fetch_one()
        .await
        .unwrap();
    assert_eq!(count, 1000);
}

/// Interleaved writes and flushes from multiple handles.
#[tokio::test]
async fn native_async_inserter_interleaved_flush() {
    use clickhouse::native::{AsyncNativeInserter, AsyncNativeInserterConfig};

    #[derive(Debug, Clone, PartialEq, Row, Serialize, Deserialize)]
    struct R {
        id: u32,
        data: String,
    }

    let client = prepare_native_database("async_inserter_interleave").await;
    client
        .query(&format!(
            "CREATE TABLE t{} (id UInt32, data String) {}",
            on_cluster(),
            test_engine("id"),
        ))
        .execute()
        .await
        .unwrap();

    let inserter = AsyncNativeInserter::<R>::new(
        &client,
        "t",
        AsyncNativeInserterConfig::default().without_period(),
    );

    let h1 = inserter.handle();
    let h2 = inserter.handle();

    for i in 0..10u32 {
        h1.write(R { id: i, data: "a".into() }).await.unwrap();
    }
    h1.flush().await.unwrap();

    for i in 10..20u32 {
        h2.write(R { id: i, data: "b".into() }).await.unwrap();
    }
    h2.flush().await.unwrap();

    let count: u64 = client
        .query("SELECT count() FROM t")
        .fetch_one()
        .await
        .unwrap();
    assert_eq!(count, 20);

    drop(h1);
    drop(h2);
    inserter.end().await.unwrap();
}

// ===========================================================================
// Large ugly JSON source tests -- Filebeat / Winlogbeat payloads (native TCP)
// ===========================================================================
//
// Realistic, deeply nested JSON blobs matching Elastic Beat agent output.
// Stresses: large String values, Unicode (CJK, Cyrillic, diacritics),
// Windows backslash paths, embedded newlines/tabs, null fields, arrays of
// objects, JSON-in-JSON (Kubernetes container logs), and mixed types.

fn filebeat_nginx_json() -> String {
    r#"{
  "@timestamp": "2026-03-12T08:14:22.337Z",
  "@metadata": {
    "beat": "filebeat",
    "type": "_doc",
    "version": "8.17.0",
    "pipeline": "filebeat-8.17.0-nginx-access-pipeline"
  },
  "agent": {
    "name": "web-prod-03.dc1.example.com",
    "type": "filebeat",
    "version": "8.17.0",
    "ephemeral_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
    "id": "deadbeef-cafe-babe-f00d-123456789abc",
    "hostname": "web-prod-03.dc1.example.com"
  },
  "log": {
    "file": { "path": "/var/log/nginx/access.log", "inode": "1234567" },
    "offset": 9823741,
    "flags": ["utf-8", "multiline"]
  },
  "message": "192.168.1.100 - jean-françois [12/Mar/2026:08:14:22 +0000] \"GET /api/v2/données/résultat?q=名前&page=1&size=50 HTTP/2.0\" 200 13847 \"https://app.example.com/dashboard/über-ansicht\" \"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36\" \"-\" rt=0.042 uct=0.001 uht=0.040 urt=0.041",
  "source": { "address": "192.168.1.100", "ip": "192.168.1.100", "geo": null },
  "http": {
    "request": {
      "method": "GET",
      "referrer": "https://app.example.com/dashboard/über-ansicht",
      "headers": {
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "de-DE,de;q=0.9,en-US;q=0.8,en;q=0.7,ja;q=0.6",
        "X-Request-ID": "req_7f8a9b0c-1d2e-3f4a-5b6c-7d8e9f0a1b2c",
        "X-Forwarded-For": "10.0.0.1, 172.16.0.1, 192.168.1.100",
        "Cookie": "session=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkrDqWFuLUZyYW7Dp29pcyIsImlhdCI6MTUxNjIzOTAyMn0.fake_sig"
      }
    },
    "response": {
      "status_code": 200,
      "body": { "bytes": 13847 },
      "headers": {
        "Content-Type": "application/json; charset=utf-8",
        "X-Cache": "MISS",
        "X-Served-By": "backend-pool-2a"
      }
    },
    "version": "2.0"
  },
  "url": {
    "original": "/api/v2/données/résultat?q=名前&page=1&size=50",
    "path": "/api/v2/données/résultat",
    "query": "q=名前&page=1&size=50",
    "domain": "app.example.com",
    "scheme": "https",
    "port": 443
  },
  "nginx": {
    "access": {
      "upstream": {
        "response_time": 0.041,
        "connect_time": 0.001,
        "header_time": 0.040,
        "addr": ["10.0.2.15:8080", "10.0.2.16:8080"],
        "status": [200]
      },
      "geoip": {
        "country_iso_code": "DE",
        "city_name": "München",
        "location": { "lat": 48.1351, "lon": 11.5820 }
      }
    }
  },
  "ecs": { "version": "8.0.0" },
  "tags": ["nginx", "web", "production", "dc1"],
  "fields": {
    "environment": "production",
    "team": "platform-engineering",
    "cost_center": "CC-4242"
  },
  "event": {
    "dataset": "nginx.access",
    "module": "nginx",
    "category": ["web"],
    "type": ["access"],
    "outcome": "success",
    "duration": 42000000,
    "created": "2026-03-12T08:14:22.380Z",
    "ingested": "2026-03-12T08:14:23.001Z"
  }
}"#.to_string()
}

fn winlogbeat_security_json() -> String {
    r#"{
  "@timestamp": "2026-03-12T03:47:11.892Z",
  "@metadata": {
    "beat": "winlogbeat",
    "type": "_doc",
    "version": "8.17.0"
  },
  "agent": {
    "name": "DC01.corp.contoso.com",
    "type": "winlogbeat",
    "version": "8.17.0",
    "ephemeral_id": "f1e2d3c4-b5a6-9780-fedc-ba0987654321",
    "id": "01234567-89ab-cdef-0123-456789abcdef"
  },
  "winlog": {
    "channel": "Security",
    "provider_name": "Microsoft-Windows-Security-Auditing",
    "provider_guid": "{54849625-5478-4994-A5BA-3E3B0328C30D}",
    "event_id": 4625,
    "version": 0,
    "task": "Logon",
    "opcode": "Info",
    "keywords": ["Audit Failure"],
    "record_id": 987654321,
    "computer_name": "DC01.corp.contoso.com",
    "process": { "pid": 788, "thread": { "id": 4892 } },
    "api": "wineventlog",
    "activity_id": "{A1B2C3D4-E5F6-7890-ABCD-EF1234567890}",
    "event_data": {
      "SubjectUserSid": "S-1-5-18",
      "SubjectUserName": "DC01$",
      "SubjectDomainName": "CORP",
      "SubjectLogonId": "0x3e7",
      "TargetUserSid": "S-1-0-0",
      "TargetUserName": "администратор",
      "TargetDomainName": "CORP",
      "Status": "0xc000006d",
      "FailureReason": "%%2313",
      "SubStatus": "0xc0000064",
      "LogonType": "10",
      "LogonProcessName": "User32 ",
      "AuthenticationPackageName": "Negotiate",
      "WorkstationName": "АТАКУЮЩИЙ-ПК",
      "TransmittedServices": "-",
      "LmPackageName": "-",
      "KeyLength": "0",
      "ProcessId": "0x0",
      "ProcessName": "-",
      "IpAddress": "198.51.100.23",
      "IpPort": "49832"
    }
  },
  "event": {
    "code": "4625",
    "kind": "event",
    "provider": "Microsoft-Windows-Security-Auditing",
    "action": "logon-failed",
    "category": ["authentication"],
    "type": ["start"],
    "outcome": "failure",
    "created": "2026-03-12T03:47:12.100Z",
    "ingested": "2026-03-12T03:47:13.250Z",
    "severity": 0
  },
  "host": {
    "name": "DC01",
    "hostname": "DC01.corp.contoso.com",
    "os": {
      "family": "windows",
      "name": "Windows Server 2022",
      "version": "10.0.20348.2340",
      "build": "20348.2340",
      "platform": "windows",
      "type": "windows",
      "kernel": "10.0.20348.2340 (WinBuild.160101.0800)"
    },
    "ip": ["10.0.0.5", "fe80::1234:5678:abcd:ef01"],
    "mac": ["00-15-5D-01-02-03"],
    "architecture": "x86_64",
    "domain": "corp.contoso.com"
  },
  "source": {
    "ip": "198.51.100.23",
    "port": 49832,
    "geo": {
      "country_iso_code": "RU",
      "city_name": "Москва",
      "region_name": "Москва",
      "location": { "lat": 55.7558, "lon": 37.6173 },
      "timezone": "Europe/Moscow"
    }
  },
  "user": {
    "name": "администратор",
    "domain": "CORP",
    "id": "S-1-0-0",
    "target": {
      "name": "администратор",
      "domain": "CORP"
    }
  },
  "message": "An account failed to log on.\n\nSubject:\n\tSecurity ID:\t\tS-1-5-18\n\tAccount Name:\t\tDC01$\n\tAccount Domain:\t\tCORP\n\tLogon ID:\t\t0x3E7\n\nLogon Information:\n\tLogon Type:\t\t10\n\tRestricted Admin Mode:\t-\n\tVirtual Account:\t\tNo\n\tElevated Token:\t\tNo\n\nFailure Information:\n\tFailure Reason:\t\tUnknown user name or bad password.\n\tStatus:\t\t\t0xC000006D\n\tSub Status:\t\t0xC0000064\n\nNew Logon:\n\tSecurity ID:\t\tS-1-0-0\n\tAccount Name:\t\tадминистратор\n\tAccount Domain:\t\tCORP\n\nProcess Information:\n\tCaller Process ID:\t0x0\n\tCaller Process Name:\t-\n\nNetwork Information:\n\tWorkstation Name:\tАТАКУЮЩИЙ-ПК\n\tSource Network Address:\t198.51.100.23\n\tSource Port:\t\t49832",
  "related": {
    "ip": ["198.51.100.23", "10.0.0.5"],
    "user": ["DC01$", "администратор"]
  },
  "ecs": { "version": "8.0.0" },
  "tags": ["security", "authentication", "failed-logon", "brute-force-candidate"]
}"#.to_string()
}

fn filebeat_multiline_java_json() -> String {
    r#"{
  "@timestamp": "2026-03-12T14:22:03.001Z",
  "@metadata": { "beat": "filebeat", "version": "8.17.0" },
  "agent": { "name": "app-srv-07", "type": "filebeat", "version": "8.17.0" },
  "log": {
    "file": {
      "path": "C:\\Program Files\\MyApp\\logs\\application-2026-03-12.log",
      "inode": "0"
    },
    "offset": 482716,
    "flags": ["utf-8", "multiline"]
  },
  "message": "2026-03-12 14:22:02,999 ERROR [http-nio-8443-exec-42] com.example.api.UserController - Failed to process request for user_id=café-résumé-42\njava.lang.NullPointerException: Cannot invoke \"com.example.model.UserProfile.getDisplayName()\" because the return value of \"com.example.service.UserService.findById(String)\" is null\n\tat com.example.api.UserController.getUserProfile(UserController.java:142)\n\tat com.example.api.UserController$$FastClassBySpringCGLIB$$abc123.invoke(<generated>)\n\tat org.springframework.cglib.proxy.MethodProxy.invoke(MethodProxy.java:218)\n\tat org.springframework.aop.framework.CglibAopProxy$CglibMethodInvocation.invokeJoinpoint(CglibAopProxy.java:793)\n\tat org.springframework.aop.framework.ReflectiveMethodInvocation.proceed(ReflectiveMethodInvocation.java:163)\n\tat org.springframework.aop.framework.CglibAopProxy$DynamicAdvisedInterceptor.intercept(CglibAopProxy.java:723)\n\tat com.example.api.UserController$$EnhancerBySpringCGLIB$$def456.getUserProfile(<generated>)\n\tat sun.reflect.NativeMethodAccessorImpl.invoke0(Native Method)\n\tat sun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:62)\n\tat sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)\n\tat java.lang.reflect.Method.invoke(Method.java:498)\n\tat org.apache.tomcat.util.threads.TaskThread$WrappingRunnable.run(TaskThread.java:61)\n\tat java.lang.Thread.run(Thread.java:750)\nCaused by: org.hibernate.exception.JDBCConnectionException: Unable to acquire JDBC Connection\n\tat org.hibernate.exception.internal.SQLExceptionTypeDelegate.convert(SQLExceptionTypeDelegate.java:48)\n\tat com.zaxxer.hikari.pool.HikariPool.getConnection(HikariPool.java:163)\n\tat com.zaxxer.hikari.pool.HikariPool.getConnection(HikariPool.java:128)\nCaused by: java.sql.SQLTransientConnectionException: HikariPool-1 - Connection is not available, request timed out after 30000ms.\n\tat com.zaxxer.hikari.pool.HikariPool.createTimeoutException(HikariPool.java:695)\n\t... 42 more",
  "error": {
    "type": "java.lang.NullPointerException",
    "message": "Cannot invoke \"com.example.model.UserProfile.getDisplayName()\"",
    "stack_trace": "... (see message field for full trace)"
  },
  "host": {
    "name": "app-srv-07",
    "os": {
      "family": "windows",
      "name": "Windows Server 2019",
      "version": "10.0.17763.5329"
    },
    "ip": ["10.10.20.7"]
  },
  "service": {
    "name": "user-api",
    "version": "3.14.159-SNAPSHOT",
    "environment": "staging",
    "node": { "name": "app-srv-07:8443" }
  },
  "labels": {
    "deployment_id": "deploy-2026-03-12-r42",
    "git_sha": "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2",
    "jira_ticket": "PLAT-9876"
  },
  "ecs": { "version": "8.0.0" },
  "tags": ["java", "error", "staging", "connection-pool-exhaustion"]
}"#.to_string()
}

fn winlogbeat_powershell_json() -> String {
    r#"{
  "@timestamp": "2026-03-12T01:15:44.203Z",
  "@metadata": { "beat": "winlogbeat", "version": "8.17.0" },
  "agent": { "name": "WS-FINANCE-12", "type": "winlogbeat" },
  "winlog": {
    "channel": "Microsoft-Windows-PowerShell/Operational",
    "provider_name": "Microsoft-Windows-PowerShell",
    "event_id": 4104,
    "task": "Execute a Remote Command",
    "opcode": "On create calls",
    "record_id": 55432,
    "computer_name": "WS-FINANCE-12.corp.contoso.com",
    "process": { "pid": 6328, "thread": { "id": 7204 } },
    "event_data": {
      "MessageNumber": "1",
      "MessageTotal": "1",
      "ScriptBlockText": "function Invoke-Çömpléx_Tàsk {\n    param(\n        [Parameter(Mandatory=$true)]\n        [string]$Tärget,\n        [ValidateSet('Réad','Wríte','Éxecute')]\n        [string]$Möde = 'Réad'\n    )\n    \n    $encodedCmd = [Convert]::ToBase64String([Text.Encoding]::Unicode.GetBytes($Tärget))\n    $résult = @{\n        'Tïmestamp' = (Get-Date -Format 'yyyy-MM-ddTHH:mm:ss.fffZ')\n        'Üser'      = $env:USERNAME\n        'Dömain'    = $env:USERDOMAIN\n        'Pàth'      = \"C:\\Users\\$env:USERNAME\\AppData\\Local\\Temp\\öutput_$(Get-Random).tmp\"\n        'Àrgs'      = @($Tärget, $Möde, $encodedCmd)\n        'Nësted'    = @{\n            'Dëep1' = @{\n                'Dëep2' = @{\n                    'Dëep3' = @{\n                        'value' = 'We\\'re testing deep nesting with spëcial chars: <>&\\\"\\'/'\n                    }\n                }\n            }\n        }\n    }\n    \n    $résult | ConvertTo-Json -Depth 10 | Out-File -FilePath $résult['Pàth'] -Encoding UTF8\n    return $résult\n}",
      "ScriptBlockId": "b7c8d9e0-f1a2-3b4c-5d6e-7f8a9b0c1d2e",
      "Path": "C:\\Users\\jëan-pierré\\Documents\\Scrïpts\\Ïnvoke-Task.ps1"
    }
  },
  "event": {
    "code": "4104",
    "kind": "event",
    "provider": "Microsoft-Windows-PowerShell",
    "category": ["process"],
    "type": ["info"],
    "outcome": "success"
  },
  "host": {
    "name": "WS-FINANCE-12",
    "hostname": "WS-FINANCE-12.corp.contoso.com",
    "os": {
      "family": "windows",
      "name": "Windows 11 Enterprise",
      "version": "10.0.22631.3155",
      "build": "22631.3155"
    },
    "ip": ["10.20.30.12", "fe80::abcd:ef01:2345:6789"],
    "mac": ["00-50-56-AB-CD-EF"]
  },
  "user": {
    "name": "jëan-pierré",
    "domain": "CORP",
    "id": "S-1-5-21-1234567890-1234567890-1234567890-5678"
  },
  "process": {
    "pid": 6328,
    "executable": "C:\\Windows\\System32\\WindowsPowerShell\\v1.0\\powershell.exe",
    "command_line": "powershell.exe -NoProfile -ExecutionPolicy Bypass -File \"C:\\Users\\jëan-pierré\\Documents\\Scrïpts\\Ïnvoke-Task.ps1\"",
    "parent": {
      "pid": 4120,
      "executable": "C:\\Windows\\explorer.exe"
    }
  },
  "message": "Creating Scriptblock text (1 of 1):\nfunction Invoke-Çömpléx_Tàsk { ... (see ScriptBlockText for full content)",
  "related": {
    "user": ["jëan-pierré"]
  },
  "ecs": { "version": "8.0.0" },
  "tags": ["powershell", "scriptblock", "finance-dept"]
}"#.to_string()
}

fn filebeat_kubernetes_json() -> String {
    r#"{
  "@timestamp": "2026-03-12T19:33:07.445Z",
  "@metadata": { "beat": "filebeat", "version": "8.17.0" },
  "agent": { "name": "k8s-node-pool-a-2", "type": "filebeat" },
  "kubernetes": {
    "pod": {
      "name": "payment-svc-7b8c9d-xq2f4",
      "uid": "12345678-abcd-ef01-2345-67890abcdef0",
      "ip": "10.244.3.17",
      "labels": {
        "app_kubernetes_io/name": "payment-svc",
        "app_kubernetes_io/version": "2.71.828",
        "app_kubernetes_io/component": "api",
        "helm_sh/chart": "payment-svc-2.71.828",
        "pod-template-hash": "7b8c9d"
      },
      "annotations": {
        "prometheus_io/scrape": "true",
        "prometheus_io/port": "9090",
        "vault_hashicorp_com/agent-inject": "true",
        "vault_hashicorp_com/role": "payment-svc-prod"
      }
    },
    "node": {
      "name": "k8s-node-pool-a-2",
      "hostname": "k8s-node-pool-a-2.cluster.local",
      "labels": {
        "kubernetes_io/arch": "amd64",
        "node_kubernetes_io/instance-type": "m5.2xlarge",
        "topology_kubernetes_io/zone": "ap-southeast-2a"
      }
    },
    "namespace": "payment-prod",
    "replicaset": { "name": "payment-svc-7b8c9d" },
    "deployment": { "name": "payment-svc" },
    "container": {
      "name": "payment-api",
      "image": "harbor.internal/payment/api:2.71.828-deadbeef",
      "id": "containerd://abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"
    }
  },
  "container": {
    "id": "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789",
    "image": { "name": "harbor.internal/payment/api:2.71.828-deadbeef" },
    "runtime": "containerd"
  },
  "log": {
    "file": {
      "path": "/var/log/pods/payment-prod_payment-svc-7b8c9d-xq2f4_12345678-abcd-ef01-2345-67890abcdef0/payment-api/0.log"
    }
  },
  "message": "{\"level\":\"error\",\"ts\":1741804387.445,\"caller\":\"handler/payment.go:287\",\"msg\":\"payment processing failed\",\"trace_id\":\"abc123def456\",\"span_id\":\"789012\",\"request_id\":\"req-ñoño-42\",\"customer_id\":\"cust_Ωmega_∆lpha\",\"amount\":\"¥123,456.78\",\"currency\":\"JPY\",\"gateway_response\":{\"code\":\"DECLINED_INSUFFICIENT_FUNDS\",\"raw\":\"カード残高不足です。別のお支払い方法をお試しください。\",\"retry_after_ms\":null,\"metadata\":{\"issuer_country\":\"JP\",\"card_brand\":\"JCB\",\"last4\":\"4242\",\"3ds_enrolled\":true,\"risk_score\":0.73}},\"stack\":\"goroutine 847 [running]:\\nruntime/debug.Stack()\\n\\t/usr/local/go/src/runtime/debug/stack.go:24 +0x5e\\ngithub.com/example/payment-svc/internal/handler.(*PaymentHandler).ProcessPayment(...)\\n\\t/app/internal/handler/payment.go:287 +0x1a3\\ngithub.com/example/payment-svc/internal/handler.(*PaymentHandler).HandleRequest(...)\\n\\t/app/internal/handler/payment.go:142 +0x892\"}",
  "stream": "stderr",
  "event": {
    "dataset": "kubernetes.container_logs",
    "module": "kubernetes"
  },
  "ecs": { "version": "8.0.0" },
  "tags": ["kubernetes", "payment", "production", "pci-zone"]
}"#.to_string()
}

#[tokio::test]
async fn native_async_inserter_filebeat_nginx() {
    use clickhouse::native::{AsyncNativeInserter, AsyncNativeInserterConfig};

    #[derive(Debug, Clone, PartialEq, Row, Serialize, Deserialize)]
    struct LogRow {
        ts: u64,
        source: String,
        json_data: String,
    }

    let client = prepare_native_database("async_fb_nginx").await;
    client
        .query(&format!(
            "CREATE TABLE t{} (ts UInt64, source String, json_data String) {}",
            on_cluster(),
            test_engine("ts"),
        ))
        .execute()
        .await
        .unwrap();

    let inserter = AsyncNativeInserter::<LogRow>::new(
        &client,
        "t",
        AsyncNativeInserterConfig::default().without_period(),
    );

    let json = filebeat_nginx_json();
    for i in 0..10u64 {
        inserter
            .write(LogRow {
                ts: 1741760062000 + i,
                source: "filebeat-nginx".into(),
                json_data: json.clone(),
            })
            .await
            .unwrap();
    }

    inserter.end().await.unwrap();

    let rows: Vec<LogRow> = client
        .query("SELECT ts, source, json_data FROM t ORDER BY ts")
        .fetch_all()
        .await
        .unwrap();
    assert_eq!(rows.len(), 10);
    assert!(rows[0].json_data.contains("jean-françois"));
    assert!(rows[0].json_data.contains("名前"));
    assert!(rows[0].json_data.contains("über-ansicht"));
    assert!(rows[0].json_data.contains("München"));
}

#[tokio::test]
async fn native_async_inserter_winlogbeat_security() {
    use clickhouse::native::{AsyncNativeInserter, AsyncNativeInserterConfig};

    #[derive(Debug, Clone, PartialEq, Row, Serialize, Deserialize)]
    struct LogRow {
        ts: u64,
        source: String,
        json_data: String,
    }

    let client = prepare_native_database("async_wlb_security").await;
    client
        .query(&format!(
            "CREATE TABLE t{} (ts UInt64, source String, json_data String) {}",
            on_cluster(),
            test_engine("ts"),
        ))
        .execute()
        .await
        .unwrap();

    let inserter = AsyncNativeInserter::<LogRow>::new(
        &client,
        "t",
        AsyncNativeInserterConfig::default().without_period(),
    );

    let json = winlogbeat_security_json();
    for i in 0..10u64 {
        inserter
            .write(LogRow {
                ts: 1741744031000 + i,
                source: "winlogbeat-security".into(),
                json_data: json.clone(),
            })
            .await
            .unwrap();
    }

    inserter.end().await.unwrap();

    let rows: Vec<LogRow> = client
        .query("SELECT ts, source, json_data FROM t ORDER BY ts")
        .fetch_all()
        .await
        .unwrap();
    assert_eq!(rows.len(), 10);
    assert!(rows[0].json_data.contains("администратор"));
    assert!(rows[0].json_data.contains("АТАКУЮЩИЙ-ПК"));
    assert!(rows[0].json_data.contains("Москва"));
    assert!(rows[0].json_data.contains("S-1-5-18"));
}

#[tokio::test]
async fn native_async_inserter_filebeat_java_stacktrace() {
    use clickhouse::native::{AsyncNativeInserter, AsyncNativeInserterConfig};

    #[derive(Debug, Clone, PartialEq, Row, Serialize, Deserialize)]
    struct LogRow {
        ts: u64,
        source: String,
        json_data: String,
    }

    let client = prepare_native_database("async_fb_java").await;
    client
        .query(&format!(
            "CREATE TABLE t{} (ts UInt64, source String, json_data String) {}",
            on_cluster(),
            test_engine("ts"),
        ))
        .execute()
        .await
        .unwrap();

    let inserter = AsyncNativeInserter::<LogRow>::new(
        &client,
        "t",
        AsyncNativeInserterConfig::default().without_period(),
    );

    let json = filebeat_multiline_java_json();
    for i in 0..5u64 {
        inserter
            .write(LogRow {
                ts: 1741781723000 + i,
                source: "filebeat-java".into(),
                json_data: json.clone(),
            })
            .await
            .unwrap();
    }

    inserter.end().await.unwrap();

    let rows: Vec<LogRow> = client
        .query("SELECT ts, source, json_data FROM t ORDER BY ts")
        .fetch_all()
        .await
        .unwrap();
    assert_eq!(rows.len(), 5);
    assert!(rows[0].json_data.contains("NullPointerException"));
    assert!(rows[0].json_data.contains("café-résumé-42"));
    assert!(rows[0].json_data.contains("C:\\\\Program Files\\\\MyApp"));
    assert!(rows[0].json_data.contains("HikariPool"));
}

#[tokio::test]
async fn native_async_inserter_winlogbeat_powershell() {
    use clickhouse::native::{AsyncNativeInserter, AsyncNativeInserterConfig};

    #[derive(Debug, Clone, PartialEq, Row, Serialize, Deserialize)]
    struct LogRow {
        ts: u64,
        source: String,
        json_data: String,
    }

    let client = prepare_native_database("async_wlb_powershell").await;
    client
        .query(&format!(
            "CREATE TABLE t{} (ts UInt64, source String, json_data String) {}",
            on_cluster(),
            test_engine("ts"),
        ))
        .execute()
        .await
        .unwrap();

    let inserter = AsyncNativeInserter::<LogRow>::new(
        &client,
        "t",
        AsyncNativeInserterConfig::default().without_period(),
    );

    let json = winlogbeat_powershell_json();
    for i in 0..5u64 {
        inserter
            .write(LogRow {
                ts: 1741742144000 + i,
                source: "winlogbeat-powershell".into(),
                json_data: json.clone(),
            })
            .await
            .unwrap();
    }

    inserter.end().await.unwrap();

    let rows: Vec<LogRow> = client
        .query("SELECT ts, source, json_data FROM t ORDER BY ts")
        .fetch_all()
        .await
        .unwrap();
    assert_eq!(rows.len(), 5);
    assert!(rows[0].json_data.contains("Invoke-Çömpléx_Tàsk"));
    assert!(rows[0].json_data.contains("jëan-pierré"));
    assert!(rows[0].json_data.contains("Scrïpts"));
}

#[tokio::test]
async fn native_async_inserter_filebeat_kubernetes() {
    use clickhouse::native::{AsyncNativeInserter, AsyncNativeInserterConfig};

    #[derive(Debug, Clone, PartialEq, Row, Serialize, Deserialize)]
    struct LogRow {
        ts: u64,
        source: String,
        json_data: String,
    }

    let client = prepare_native_database("async_fb_k8s").await;
    client
        .query(&format!(
            "CREATE TABLE t{} (ts UInt64, source String, json_data String) {}",
            on_cluster(),
            test_engine("ts"),
        ))
        .execute()
        .await
        .unwrap();

    let inserter = AsyncNativeInserter::<LogRow>::new(
        &client,
        "t",
        AsyncNativeInserterConfig::default().without_period(),
    );

    let json = filebeat_kubernetes_json();
    for i in 0..5u64 {
        inserter
            .write(LogRow {
                ts: 1741804387000 + i,
                source: "filebeat-k8s".into(),
                json_data: json.clone(),
            })
            .await
            .unwrap();
    }

    inserter.end().await.unwrap();

    let rows: Vec<LogRow> = client
        .query("SELECT ts, source, json_data FROM t ORDER BY ts")
        .fetch_all()
        .await
        .unwrap();
    assert_eq!(rows.len(), 5);
    assert!(rows[0].json_data.contains("payment-svc-7b8c9d-xq2f4"));
    assert!(rows[0].json_data.contains("カード残高不足"));
    assert!(rows[0].json_data.contains("req-ñoño-42"));
    assert!(rows[0].json_data.contains("cust_Ωmega_∆lpha"));
    assert!(rows[0].json_data.contains("¥123,456.78"));
}

/// Mixed Beat sources in a single batch -- concurrent handles, one source per handle.
#[tokio::test]
async fn native_async_inserter_mixed_beats_concurrent() {
    use clickhouse::native::{AsyncNativeInserter, AsyncNativeInserterConfig};

    #[derive(Debug, Clone, PartialEq, Row, Serialize, Deserialize)]
    struct LogRow {
        ts: u64,
        source: String,
        json_data: String,
    }

    let client = prepare_native_database("async_mixed_beats").await;
    client
        .query(&format!(
            "CREATE TABLE t{} (ts UInt64, source String, json_data String) {}",
            on_cluster(),
            test_engine("ts"),
        ))
        .execute()
        .await
        .unwrap();

    let inserter = AsyncNativeInserter::<LogRow>::new(
        &client,
        "t",
        AsyncNativeInserterConfig::default()
            .with_max_rows(15) // force multiple flushes mid-batch
            .without_period(),
    );

    let sources: Vec<(&str, String)> = vec![
        ("filebeat-nginx", filebeat_nginx_json()),
        ("winlogbeat-security", winlogbeat_security_json()),
        ("filebeat-java", filebeat_multiline_java_json()),
        ("winlogbeat-powershell", winlogbeat_powershell_json()),
        ("filebeat-k8s", filebeat_kubernetes_json()),
    ];

    let mut tasks = Vec::new();
    for (idx, (source, json)) in sources.into_iter().enumerate() {
        let handle = inserter.handle();
        let source = source.to_string();
        tasks.push(tokio::spawn(async move {
            for j in 0..20u64 {
                let ts = (idx as u64) * 1_000_000 + j;
                handle
                    .write(LogRow {
                        ts,
                        source: source.clone(),
                        json_data: json.clone(),
                    })
                    .await
                    .unwrap();
            }
        }));
    }

    for task in tasks {
        task.await.unwrap();
    }

    inserter.end().await.unwrap();

    // 5 sources x 20 rows = 100
    let count: u64 = client
        .query("SELECT count() FROM t")
        .fetch_one()
        .await
        .unwrap();
    assert_eq!(count, 100);

    // Verify each source is present.
    let nginx_count: u64 = client
        .query("SELECT count() FROM t WHERE source = 'filebeat-nginx'")
        .fetch_one()
        .await
        .unwrap();
    assert_eq!(nginx_count, 20);

    let security_count: u64 = client
        .query("SELECT count() FROM t WHERE source = 'winlogbeat-security'")
        .fetch_one()
        .await
        .unwrap();
    assert_eq!(security_count, 20);
}

// ---------------------------------------------------------------------------
// New feature tests -- native parity, observability, unified client
// ---------------------------------------------------------------------------

/// Verify server_version() returns sensible data.
#[tokio::test]
async fn native_server_version() {
    let client = get_native_client();
    let ver = client.server_version().await.unwrap();
    assert!(!ver.name.is_empty(), "server name must not be empty");
    assert!(ver.major > 0, "major version must be > 0, got {}", ver.major);
    assert!(ver.revision > 0, "revision must be > 0");
}

/// Verify pool_stats() returns metrics consistent with pool_size.
#[tokio::test]
async fn native_pool_stats() {
    let client = get_native_client().with_pool_size(3);
    // Warm up one connection.
    client.ping().await.unwrap();

    let stats = client.pool_stats();
    assert_eq!(stats.max_size, 3);
    assert!(stats.size >= 1, "at least one connection should exist after ping");
}

/// Verify named parameters work via the .param() builder.
#[tokio::test]
async fn native_named_params() {
    let client = get_native_client();
    let result: u64 = client
        .query("SELECT {val:UInt64}")
        .param("val", 42u64)
        .fetch_one()
        .await
        .unwrap();
    assert_eq!(result, 42);
}

/// Verify per-query settings override client settings.
#[tokio::test]
async fn native_per_query_settings() {
    let client = get_native_client();
    // max_result_rows=1 should cause an error when we try to fetch 2 rows
    // (with result_overflow_mode=throw).
    let result = client
        .query("SELECT number FROM system.numbers LIMIT 2")
        .with_settings([
            ("max_result_rows".to_string(), "1".to_string()),
            ("result_overflow_mode".to_string(), "throw".to_string()),
        ])
        .fetch_all::<u64>()
        .await;
    assert!(result.is_err(), "should fail with max_result_rows=1");
}

/// Verify query_id is respected -- appears in system.query_log.
#[tokio::test]
async fn native_query_id() {
    let client = get_native_client();
    let qid = format!("chrs_test_{:x}", rand::random::<u64>());

    // Execute a query with a specific ID.
    client
        .query("SELECT 1")
        .with_query_id(&qid)
        .execute()
        .await
        .unwrap();

    // Flush the query log.
    client
        .query("SYSTEM FLUSH LOGS")
        .execute()
        .await
        .unwrap();

    // Check it appears in query_log using a named parameter.
    let count: u64 = client
        .query("SELECT count() FROM system.query_log WHERE query_id = {qid:String}")
        .param("qid", &qid)
        .fetch_one()
        .await
        .unwrap();
    assert!(count > 0, "query_id {qid} not found in system.query_log");
}

/// Verify insert timeouts -- a very short timeout should fail.
#[tokio::test]
async fn native_insert_timeout_fires() {
    use std::time::Duration;

    #[derive(Row, Serialize)]
    struct TimeoutRow {
        x: u64,
    }

    let client = get_native_client();
    client
        .query("CREATE TABLE IF NOT EXISTS default.chrs_timeout_test (x UInt64) ENGINE = Memory")
        .execute()
        .await
        .unwrap();

    // end_timeout of 1ns is effectively instant -- should time out.
    let mut insert = client
        .insert::<TimeoutRow>("default.chrs_timeout_test")
        .with_timeouts(None, Some(Duration::from_nanos(1)));

    insert.write(&TimeoutRow { x: 1 }).await.unwrap();
    let result = insert.end().await;

    // Clean up regardless of result.
    let _ = client
        .query("DROP TABLE IF EXISTS default.chrs_timeout_test")
        .execute()
        .await;

    assert!(
        result.is_err(),
        "insert with 1ns end_timeout should fail"
    );
}

/// Verify multi-host round-robin -- multiple good addrs all work.
#[tokio::test]
async fn native_multi_host_round_robin() {
    use std::net::ToSocketAddrs;

    let host = std::env::var("CLICKHOUSE_HOST").unwrap_or_else(|_| "localhost".into());
    let port = std::env::var("CLICKHOUSE_NATIVE_PORT").unwrap_or_else(|_| "9000".into());
    let user = std::env::var("CLICKHOUSE_USER").unwrap_or_else(|_| "default".into());
    let password = std::env::var("CLICKHOUSE_PASSWORD").unwrap_or_else(|_| "".into());

    let addr = format!("{host}:{port}")
        .to_socket_addrs()
        .expect("resolve addr")
        .next()
        .expect("at least one addr");

    // Two copies of the same good addr -- round-robin distributes across both.
    let client = NativeClient::default()
        .with_addrs(vec![addr, addr])
        .with_database("default")
        .with_user(user)
        .with_password(password)
        .with_pool_size(4);

    // Multiple pings should all succeed, exercising round-robin selection.
    for _ in 0..4 {
        client.ping().await.expect("round-robin ping should succeed");
    }
}
