use std::net::{Ipv4Addr, Ipv6Addr};

use serde::{Deserialize, Serialize};

use clickhouse::Row;

#[tokio::test]
async fn smoke() {
    let client = prepare_database!();

    #[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Row)]
    struct MyRow {
        #[serde(with = "clickhouse::serde::ipv4")]
        ipv4: Ipv4Addr,
        ipv6: Ipv6Addr, // requires no annotations.
        #[serde(with = "clickhouse::serde::ipv4::option")]
        ipv4_opt: Option<Ipv4Addr>,
        ipv6_opt: Option<Ipv6Addr>, // requires no annotations.
    }

    client
        .query(
            "
            CREATE TABLE test(
                ipv4 IPv4,
                ipv6 IPv6,
                ipv4_opt Nullable(IPv4),
                ipv6_opt Nullable(IPv6),
            ) ENGINE = MergeTree ORDER BY ipv4
        ",
        )
        .execute()
        .await
        .unwrap();

    let original_row = MyRow {
        ipv4: Ipv4Addr::new(192, 168, 0, 1),
        ipv6: Ipv6Addr::new(0x2001, 0xdb8, 0, 0, 0, 0xafc8, 0x10, 0x1),
        ipv4_opt: Some(Ipv4Addr::new(192, 168, 0, 1)),
        ipv6_opt: Some(Ipv6Addr::new(0x2001, 0xdb8, 0, 0, 0, 0xafc8, 0x10, 0x1)),
    };

    let mut insert = client.insert::<MyRow>("test").await.unwrap();
    insert.write(&original_row).await.unwrap();
    insert.end().await.unwrap();

    let (row, row_ipv4_str, row_ipv6_str) = client
        .query("SELECT ?fields, toString(ipv4), toString(ipv6) FROM test")
        .fetch_one::<(MyRow, String, String)>()
        .await
        .unwrap();

    assert_eq!(row, original_row);
    assert_eq!(row_ipv4_str, original_row.ipv4.to_string());
    assert_eq!(row_ipv6_str, original_row.ipv6.to_string());
}

/// Live ClickHouse round-trip covering IPv4, IPv6, and FixedString(4) in
/// one table. Pins the documented annotation contract:
///
/// - `Ipv4Addr` requires `#[serde(with = "clickhouse::serde::ipv4")]`.
/// - `Ipv6Addr` round-trips by default; the `ipv6` helper is a
///   pass-through provided for symmetry.
/// - `[u8; N]` maps to `FixedString(N)` byte-for-byte; user supplies any
///   padding.
///
/// Schema validation is enabled by `default-validation` (the workspace
/// default). The byte-level invariants pinned by the rowbinary unit
/// tests in `src/rowbinary/tests.rs::ip_and_fixed_string` are
/// re-asserted end-to-end here against a real CH instance.
#[tokio::test]
async fn ipv4_ipv6_fixedstring_round_trip() {
    let client = prepare_database!();

    #[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Row)]
    struct MyRow {
        #[serde(with = "clickhouse::serde::ipv4")]
        ipv4: Ipv4Addr,
        ipv6_default: Ipv6Addr,
        #[serde(with = "clickhouse::serde::ipv6")]
        ipv6_via_helper: Ipv6Addr,
        fixed4: [u8; 4],
    }

    client
        .query(
            "
            CREATE TABLE test(
                ipv4            IPv4,
                ipv6_default    IPv6,
                ipv6_via_helper IPv6,
                fixed4          FixedString(4)
            ) ENGINE = MergeTree ORDER BY ipv4
        ",
        )
        .execute()
        .await
        .unwrap();

    let original = MyRow {
        ipv4: Ipv4Addr::new(192, 168, 0, 1),
        ipv6_default: Ipv6Addr::new(0x2001, 0xdb8, 0, 0, 0, 0xafc8, 0x10, 0x1),
        ipv6_via_helper: Ipv6Addr::new(0x2001, 0xdb8, 0, 0, 0, 0xafc8, 0x10, 0x1),
        fixed4: *b"BTC\0",
    };

    let mut insert = client.insert::<MyRow>("test").await.unwrap();
    insert.write(&original).await.unwrap();
    insert.end().await.unwrap();

    // Validation enabled (default). Fetch back the row and a server-side
    // string view of each column so a CH-side decoding regression
    // surfaces here, not just a Rust-side serde regression.
    let (row, ipv4_str, ipv6_def_str, ipv6_helper_str, fixed4_str) = client
        .query(
            "SELECT
                ?fields,
                toString(ipv4),
                toString(ipv6_default),
                toString(ipv6_via_helper),
                toString(fixed4)
             FROM test",
        )
        .fetch_one::<(MyRow, String, String, String, String)>()
        .await
        .unwrap();

    assert_eq!(row, original, "round-tripped row diverged from input");
    assert_eq!(ipv4_str, original.ipv4.to_string());
    assert_eq!(ipv6_def_str, original.ipv6_default.to_string());
    assert_eq!(ipv6_helper_str, original.ipv6_via_helper.to_string());
    // toString on FixedString preserves user padding bytes.
    assert_eq!(fixed4_str.as_bytes(), &original.fixed4);
}
