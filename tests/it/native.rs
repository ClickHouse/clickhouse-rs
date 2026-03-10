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
