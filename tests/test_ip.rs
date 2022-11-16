use std::net::{Ipv4Addr, Ipv6Addr};

use serde::{Deserialize, Serialize};

use clickhouse::Row;

mod common;

#[common::named]
#[tokio::test]
async fn smoke() {
    let client = common::prepare_database!();

    #[derive(Debug, Row, Serialize, Deserialize)]
    struct MyRow {
        #[serde(with = "clickhouse::serde::ipv4")]
        ipv4: Ipv4Addr,
        ipv6: Ipv6Addr, // requires no annotations.
    }

    client
        .query("CREATE TABLE test(ipv4 IPv4, ipv6 IPv6) ENGINE = MergeTree ORDER BY ipv4")
        .execute()
        .await
        .unwrap();

    let ipv4 = Ipv4Addr::new(192, 168, 0, 1);
    let ipv6 = Ipv6Addr::new(0x2001, 0xdb8, 0, 0, 0, 0xafc8, 0x10, 0x1);

    let mut insert = client.insert("test").unwrap();
    insert.write(&MyRow { ipv4, ipv6 }).await.unwrap();
    insert.end().await.unwrap();

    let (row, row_ipv4_str, row_ipv6_str) = client
        .query("SELECT ?fields, toString(ipv4), toString(ipv6) FROM test")
        .fetch_one::<(MyRow, String, String)>()
        .await
        .unwrap();

    assert_eq!(row.ipv4, ipv4);
    assert_eq!(row_ipv4_str, ipv4.to_string());
    assert_eq!(row.ipv6, ipv6);
    assert_eq!(row_ipv6_str, ipv6.to_string());
}
