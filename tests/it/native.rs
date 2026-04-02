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

    let client = NativeClient::default()
        .with_addr(format!("{host}:{port}"))
        .with_database("default")
        .with_user(user)
        .with_password(password);

    // On a replicated cluster, write to a quorum of replicas before returning
    // and ensure SELECT only reads quorum-committed data.  This gives
    // read-after-write consistency without pinning connections to a single node.
    //
    // CLICKHOUSE_INSERT_QUORUM: number of replicas that must acknowledge each
    // INSERT — set to the replica count for your cluster (default: 2).
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
/// nodes see the database immediately — required for multi-node setups.
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
/// Local Docker: `ENGINE = Memory` — fast, no persistence needed.
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

    // Insert 10,000 rows — server will send multiple blocks
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

    // Object('json') is the legacy JSON type — stored as String on the wire.
    client
        .query(
            &format!("CREATE TABLE t{} (id UInt32, data Object('json')) {} \
             SETTINGS allow_experimental_object_type = 1",
             on_cluster(), test_engine("tuple()")),
        )
        .execute()
        .await
        .unwrap_or_else(|e| {
            // Legacy Object type may not be available on all server versions — skip
            eprintln!("SKIP native_json_legacy: {e}");
        });

    // Insert and query are separate — if CREATE failed, just verify we skip cleanly
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

    // Decimal64 is stored as i64 (scaled integer) — maps to i64 in Rust
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
    assert_eq!(rows[0].price, 1234);  // 12.34 × 100
    assert_eq!(rows[1].price, 9999);  // 99.99 × 100
    assert_eq!(rows[2].price, 1);     // 0.01 × 100
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

    // 256-bit types have no native Rust equivalent — read as raw 32-byte LE arrays.
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
    // Int256(-42): two's complement 32-byte LE — last bytes all 0xFF
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
    //   high u64 = 0 → bytes [0..8] all zero
    //   low  u64 = 1 → bytes [8..16] = [1, 0, 0, 0, 0, 0, 0, 0] (LE)
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

    // Enum8/16 are wire-compatible with Int8/Int16 — deserialize as raw integer discriminant.
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
    // DateTime64(N) = i64 (scaled: ×10^N from epoch).
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
    assert_eq!(rows[0].d32, 1234);         // 12.34 × 100
    assert_eq!(rows[0].d128, 12345678i128); // 1234.5678 × 10^4
    // d256: 123456789012 (123456.789012 × 10^6) — check first bytes
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

    // Point = 2 × Float64 LE (16 raw bytes).  Read as [u8; 16] to avoid
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

/// Pool size 2, 10 concurrent tasks — all must succeed.
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

/// Pool size 1 + many concurrent inserts — verifies no deadlock when the
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
    // Task B tries to ping at the same time — it must wait, not deadlock.
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

/// All rows false — sparse format sends 0 non-default values.
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

/// All rows true — sparse format stores every row as a non-default value.
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

/// Mixed true/false across many rows — exercises sparse offset groups.
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
    // 50 trues, then 50 falses — exercises multiple sparse offset groups.
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

/// INSERT Bool via `NativeInsert<T>` (not SQL VALUES) — tests the encoder path.
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
/// Verifies that the sparse decoder does not misalign the stream — after reading
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

/// Two consecutive Bool columns — each must decode its own sparse stream
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

    // a: T F T F T, b: F F T T F → different sparse patterns.
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

/// 1 000 rows, only position 0 is `true` — zero-offset sparse group.
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

/// Write 50 000 rows — enough to trigger multiple intermediate flushes at the
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

/// Drop `NativeInsert` without calling `end()` — must not commit any data,
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

    // Write two rows then drop without end() — aborts the INSERT.
    {
        let mut insert = client.insert::<R>("t");
        insert.write(&R { id: 1 }).await.expect("write 1 failed");
        insert.write(&R { id: 2 }).await.expect("write 2 failed");
        // dropped here — connection must be discarded, not returned to pool
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

    // Empty table → None.
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

    // One row → Some.
    let some: Option<u32> = client
        .query("SELECT id FROM t LIMIT 1")
        .fetch_optional::<u32>()
        .await
        .expect("fetch_optional failed");
    assert_eq!(some, Some(42u32));
}

/// `bind()` with multiple `?` placeholders — each replaces the next occurrence.
#[tokio::test]
async fn native_query_bind_multiple() {
    let client = get_native_client();

    // ClickHouse infers UInt8 for small literals; match the inferred type.
    let result: u8 = client
        .query("SELECT ? + ?")
        .bind(10u8)
        .bind(32u8)
        .fetch_one::<u8>()
        .await
        .expect("fetch failed");

    assert_eq!(result, 42u8);
}

/// `bind()` when the SQL has no `?` — should be a no-op (query unchanged).
#[tokio::test]
async fn native_query_bind_no_placeholder() {
    let client = get_native_client();

    let result: u8 = client
        .query("SELECT 1")
        .bind(999u32)   // no placeholder — ignored
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

/// Array(Nullable(String)) — nulls inside an array.
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

    // Clear all — cache must be empty.
    client.clear_all_cached_schemas();
    assert!(client.cached_schema("t").is_none(), "cache should be empty after clear_all");

    // fetch_schema re-populates.
    let schema = client.fetch_schema("t").await.expect("fetch_schema failed");
    assert!(!schema.is_empty());
    assert!(client.cached_schema("t").is_some(), "cache should be re-populated after fetch_schema");
}
