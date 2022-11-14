use serde::{Deserialize, Serialize};

use clickhouse::Row;

mod common;

#[common::named]
#[tokio::test]
async fn ip() {
    use std::net::{Ipv4Addr, Ipv6Addr};

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

#[cfg(feature = "uuid")]
#[common::named]
#[tokio::test]
async fn uuid() {
    use uuid::Uuid;

    let client = common::prepare_database!();

    #[derive(Debug, Row, Serialize, Deserialize)]
    struct MyRow {
        #[serde(with = "clickhouse::serde::uuid")]
        uuid: Uuid,
    }

    client
        .query("CREATE TABLE test(uuid UUID) ENGINE = MergeTree ORDER BY uuid")
        .execute()
        .await
        .unwrap();

    let uuid = Uuid::new_v4();
    println!("uuid: {}", uuid);

    let mut insert = client.insert("test").unwrap();
    insert.write(&MyRow { uuid }).await.unwrap();
    insert.end().await.unwrap();

    let (row, row_uuid_str) = client
        .query("SELECT ?fields, toString(uuid) FROM test")
        .fetch_one::<(MyRow, String)>()
        .await
        .unwrap();

    assert_eq!(row.uuid, uuid);
    assert_eq!(row_uuid_str, uuid.to_string());
}

#[cfg(feature = "time")]
#[common::named]
#[tokio::test]
async fn time() {
    use time::{macros::datetime, OffsetDateTime};

    let client = common::prepare_database!();

    #[derive(Debug, PartialEq, Eq, Row, Serialize, Deserialize)]
    struct MyRow {
        #[serde(with = "clickhouse::serde::time::datetime")]
        dt: OffsetDateTime,
        #[serde(with = "clickhouse::serde::time::datetime64::secs")]
        dt64s: OffsetDateTime,
        #[serde(with = "clickhouse::serde::time::datetime64::millis")]
        dt64ms: OffsetDateTime,
        #[serde(with = "clickhouse::serde::time::datetime64::micros")]
        dt64us: OffsetDateTime,
        #[serde(with = "clickhouse::serde::time::datetime64::nanos")]
        dt64ns: OffsetDateTime,
    }

    #[derive(Debug, Row, Deserialize)]
    struct MyRowStr {
        dt: String,
        dt64s: String,
        dt64ms: String,
        dt64us: String,
        dt64ns: String,
    }

    client
        .query(
            "
            CREATE TABLE test(
                dt DateTime,
                dt64s DateTime64(0),
                dt64ms DateTime64(3),
                dt64us DateTime64(6),
                dt64ns DateTime64(9)
            )
            ENGINE = MergeTree
            ORDER BY dt
        ",
        )
        .execute()
        .await
        .unwrap();

    let original_row = MyRow {
        dt: datetime!(2022-11-13 15:27:42 UTC),
        dt64s: datetime!(2022-11-13 15:27:42 UTC),
        dt64ms: datetime!(2022-11-13 15:27:42.123 UTC),
        dt64us: datetime!(2022-11-13 15:27:42.123456 UTC),
        dt64ns: datetime!(2022-11-13 15:27:42.123456789 UTC),
    };

    let mut insert = client.insert("test").unwrap();
    insert.write(&original_row).await.unwrap();
    insert.end().await.unwrap();

    let row = client
        .query("SELECT ?fields FROM test")
        .fetch_one::<MyRow>()
        .await
        .unwrap();

    let row_str = client
        .query("SELECT * APPLY(toString) FROM test")
        .fetch_one::<MyRowStr>()
        .await
        .unwrap();

    assert_eq!(row, original_row);
    assert_eq!(row_str.dt, &original_row.dt.to_string()[..19]);
    assert_eq!(row_str.dt64s, &original_row.dt64s.to_string()[..19]);
    assert_eq!(row_str.dt64ms, &original_row.dt64ms.to_string()[..23]);
    assert_eq!(row_str.dt64us, &original_row.dt64us.to_string()[..26]);
    assert_eq!(row_str.dt64ns, &original_row.dt64ns.to_string()[..29]);
}
