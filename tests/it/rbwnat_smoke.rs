use crate::decimals::*;
use crate::{create_simple_table, insert_and_select, SimpleRow};
use clickhouse::sql::Identifier;
use clickhouse::Row;
use serde::__private::from_utf8_lossy;
use serde::{Deserialize, Serialize};
use serde_repr::{Deserialize_repr, Serialize_repr};
use std::collections::HashMap;
use std::str::FromStr;
// TODO: these "e2e" tests should replace many other tests from `rbwnat.rs`

#[tokio::test]
async fn basic_types() {
    #[derive(Clone, Debug, Row, Serialize, Deserialize, PartialEq)]
    struct Data {
        uint8_val: u8,
        uint16_val: u16,
        uint32_val: u32,
        uint64_val: u64,
        uint128_val: u128,
        int8_val: i8,
        int16_val: i16,
        int32_val: i32,
        int64_val: i64,
        int128_val: i128,
        float32_val: f32,
        float64_val: f64,
        string_val: String,
    }

    let client = prepare_database!();
    client
        .query(
            "
            CREATE TABLE IF NOT EXISTS test (
              uint8_val UInt8,
              uint16_val UInt16,
              uint32_val UInt32,
              uint64_val UInt64,
              uint128_val UInt128,
              int8_val Int8,
              int16_val Int16,
              int32_val Int32,
              int64_val Int64,
              int128_val Int128,
              float32_val Float32,
              float64_val Float64,
              string_val String
            )
            ENGINE = MergeTree
            ORDER BY ()
            ",
        )
        .execute()
        .await
        .unwrap();

    let rows = vec![Data {
        uint8_val: 255,
        uint16_val: 65535,
        uint32_val: 4294967295,
        uint64_val: 18446744073709551615,
        uint128_val: 340282366920938463463374607431768211455,
        int8_val: -128,
        int16_val: -32768,
        int32_val: -2147483648,
        int64_val: -9223372036854775808,
        int128_val: -170141183460469231731687303715884105728,
        float32_val: 42.0,
        float64_val: 144.0,
        string_val: "test".to_string(),
    }];

    let result = insert_and_select(&client, "test", rows.clone()).await;
    assert_eq!(result, rows);
}

#[tokio::test]
async fn several_simple_rows() {
    let client = prepare_database!();
    create_simple_table(&client, "test").await;

    let rows = vec![
        SimpleRow::new(42, "foo".to_string()),
        SimpleRow::new(144, "bar".to_string()),
        SimpleRow::new(222, "baz".to_string()),
    ];

    let result = insert_and_select(&client, "test", rows.clone()).await;
    assert_eq!(result, rows);
}

#[tokio::test]
async fn arrays() {
    #[derive(Clone, Debug, Row, Serialize, Deserialize, PartialEq)]
    struct Data {
        id: u16,
        one_dim_array: Vec<u32>,
        two_dim_array: Vec<Vec<i64>>,
        three_dim_array: Vec<Vec<Vec<f64>>>,
        description: String,
    }

    let client = prepare_database!();
    client
        .query(
            "
            CREATE TABLE IF NOT EXISTS test (
                id UInt16,
                one_dim_array Array(UInt32),
                two_dim_array Array(Array(Int64)),
                three_dim_array Array(Array(Array(Float64))),
                description String
            )
            ENGINE = MergeTree
            ORDER BY ()
            ",
        )
        .execute()
        .await
        .unwrap();

    let rows = vec![Data {
        id: 42,
        one_dim_array: vec![1, 2],
        two_dim_array: vec![vec![1, 2], vec![3, 4]],
        three_dim_array: vec![
            vec![vec![1.1, 2.2], vec![3.3, 4.4]],
            vec![],
            vec![vec![5.5, 6.6], vec![7.7, 8.8]],
        ],
        description: "foobar".to_string(),
    }];

    let result = insert_and_select(&client, "test", rows.clone()).await;
    assert_eq!(result, rows);
}

#[tokio::test]
async fn maps() {
    #[derive(Clone, Debug, Row, Serialize, Deserialize, PartialEq)]
    struct Data {
        m1: HashMap<String, String>,
        m2: HashMap<u16, HashMap<String, i32>>,
    }

    let client = prepare_database!();
    client
        .query(
            "
            CREATE TABLE IF NOT EXISTS test (
                m1 Map(String, String),
                m2 Map(UInt16, Map(String, Int32))
            )
            ENGINE = MergeTree
            ORDER BY ()
            ",
        )
        .execute()
        .await
        .unwrap();

    let rows = vec![Data {
        m1: vec![
            ("key1".to_string(), "value1".to_string()),
            ("key2".to_string(), "value2".to_string()),
        ]
        .into_iter()
        .collect(),
        m2: vec![
            (
                42,
                vec![("foo".to_string(), 100), ("bar".to_string(), 200)]
                    .into_iter()
                    .collect(),
            ),
            (
                144,
                vec![("qaz".to_string(), 300), ("qux".to_string(), 400)]
                    .into_iter()
                    .collect(),
            ),
        ]
        .into_iter()
        .collect::<HashMap<u16, HashMap<String, i32>>>(),
    }];

    let result = insert_and_select(&client, "test", rows.clone()).await;
    assert_eq!(result, rows);
}

#[tokio::test]
async fn map_as_vec_of_tuples() {
    #[derive(Clone, Debug, Row, Serialize, Deserialize, PartialEq)]
    struct Data {
        m1: Vec<(i128, String)>,
        m2: Vec<(u16, Vec<(String, i32)>)>,
    }

    let client = prepare_database!();
    client
        .query(
            "
            CREATE TABLE IF NOT EXISTS test (
                m1 Map(Int128, String),
                m2 Map(UInt16, Map(String, Int32))
            )
            ENGINE = MergeTree
            ORDER BY ()
            ",
        )
        .execute()
        .await
        .unwrap();

    let rows = vec![Data {
        m1: vec![(100, "value1".to_string()), (200, "value2".to_string())],
        m2: vec![
            (
                42,
                vec![("foo".to_string(), 100), ("bar".to_string(), 200)]
                    .into_iter()
                    .collect(),
            ),
            (
                144,
                vec![("qaz".to_string(), 300), ("qux".to_string(), 400)]
                    .into_iter()
                    .collect(),
            ),
        ],
    }];

    let result = insert_and_select(&client, "test", rows.clone()).await;
    assert_eq!(result, rows)
}

#[tokio::test]
async fn enums() {
    #[derive(Clone, Debug, PartialEq, Serialize_repr, Deserialize_repr)]
    #[repr(i8)]
    enum MyEnum8 {
        Winter = -128,
        Spring = 0,
        Summer = 100,
        Autumn = 127,
    }

    #[derive(Clone, Debug, PartialEq, Serialize_repr, Deserialize_repr)]
    #[repr(i16)]
    enum MyEnum16 {
        North = -32768,
        East = 0,
        South = 144,
        West = 32767,
    }

    #[derive(Clone, Debug, PartialEq, Row, Serialize, Deserialize)]
    struct Data {
        id: u16,
        enum8: MyEnum8,
        enum16: MyEnum16,
    }

    let table_name = "test_rbwnat_enum";

    let client = prepare_database!();
    client
        .query(
            "
            CREATE OR REPLACE TABLE ?
            (
                id     UInt16,
                enum8  Enum8 ('Winter' = -128,   'Spring' = 0, 'Summer' = 100, 'Autumn' = 127),
                enum16 Enum16('North'  = -32768, 'East'   = 0, 'South'  = 144, 'West'   = 32767)
            ) ENGINE MergeTree ORDER BY id
            ",
        )
        .bind(Identifier(table_name))
        .execute()
        .await
        .unwrap();

    let rows = vec![
        Data {
            id: 1,
            enum8: MyEnum8::Spring,
            enum16: MyEnum16::East,
        },
        Data {
            id: 2,
            enum8: MyEnum8::Autumn,
            enum16: MyEnum16::North,
        },
        Data {
            id: 3,
            enum8: MyEnum8::Winter,
            enum16: MyEnum16::South,
        },
        Data {
            id: 4,
            enum8: MyEnum8::Summer,
            enum16: MyEnum16::West,
        },
    ];

    let result = insert_and_select(&client, table_name, rows.clone()).await;
    assert_eq!(result, rows);
}

#[tokio::test]
async fn nullable() {
    #[derive(Clone, Debug, Row, Serialize, Deserialize, PartialEq)]
    struct Data {
        a: u32,
        b: Option<i64>,
    }

    let client = prepare_database!();
    client
        .query(
            "
            CREATE TABLE IF NOT EXISTS test (
                a UInt32,
                b Nullable(Int64)
            )
            ENGINE = MergeTree
            ORDER BY a
            ",
        )
        .execute()
        .await
        .unwrap();

    let rows = vec![
        Data { a: 1, b: Some(2) },
        Data { a: 3, b: None },
        Data { a: 4, b: Some(5) },
    ];

    let result = insert_and_select(&client, "test", rows.clone()).await;
    assert_eq!(result, rows);
}

#[tokio::test]
async fn blob_string_with_serde_bytes() {
    #[derive(Clone, Debug, Row, Serialize, Deserialize, PartialEq)]
    struct Data {
        #[serde(with = "serde_bytes")]
        blob: Vec<u8>,
    }

    let client = prepare_database!();
    client
        .query(
            "
            CREATE TABLE IF NOT EXISTS test (
                blob String
            )
            ENGINE = MergeTree
            ORDER BY tuple()
            ",
        )
        .execute()
        .await
        .unwrap();

    let rows = vec![
        Data {
            blob: "foo".as_bytes().to_vec(),
        },
        Data {
            blob: "bar".as_bytes().to_vec(),
        },
        Data {
            blob: "qaz".as_bytes().to_vec(),
        },
    ];

    let result = insert_and_select(&client, "test", rows.clone()).await;
    assert_eq!(result, rows);
}

#[tokio::test]
async fn low_cardinality() {
    #[derive(Clone, Debug, Row, Serialize, Deserialize, PartialEq)]
    struct Data {
        a: u32,
        b: Option<i64>,
    }

    let client = prepare_database!();
    client
        .query(
            "
            CREATE TABLE IF NOT EXISTS test (
                a LowCardinality(UInt32),
                b LowCardinality(Nullable(Int64))
            )
            ENGINE = MergeTree
            ORDER BY a
            ",
        )
        .with_option("allow_suspicious_low_cardinality_types", "1")
        .execute()
        .await
        .unwrap();

    let rows = vec![
        Data { a: 1, b: Some(2) },
        Data { a: 3, b: None },
        Data { a: 4, b: Some(5) },
    ];

    let result = insert_and_select(&client, "test", rows.clone()).await;
    assert_eq!(result, rows);
}

#[tokio::test]
async fn serde_skip_struct_field() {
    #[derive(Clone, Debug, Row, Serialize, Deserialize, PartialEq)]
    struct Data {
        a: u32,
        #[serde(skip_serializing)]
        #[serde(skip_deserializing)]
        b: u32,
        c: u32,
    }

    let client = prepare_database!();
    client
        .query(
            "
            CREATE TABLE IF NOT EXISTS test (
                a UInt32,
                c UInt32
            )
            ENGINE = MergeTree
            ORDER BY a ASC
            ",
        )
        .execute()
        .await
        .unwrap();

    let result = insert_and_select(
        &client,
        "test",
        vec![
            Data {
                a: 42,
                b: 111, // b will be ignored
                c: 144,
            },
            Data {
                a: 100,
                b: 222,
                c: 200,
            },
        ],
    )
    .await;

    assert_eq!(
        result,
        vec![
            Data {
                a: 42,
                b: 0, // default value for u32
                c: 144
            },
            Data {
                a: 100,
                b: 0,
                c: 200
            },
        ]
    );
}

#[tokio::test]
#[cfg(feature = "time")]
async fn date_and_time() {
    use time::format_description::well_known::Iso8601;
    use time::Month::{February, January};
    use time::OffsetDateTime;

    #[derive(Clone, Debug, Row, Serialize, Deserialize, PartialEq)]
    struct Data {
        #[serde(with = "clickhouse::serde::time::date")]
        date: time::Date,
        #[serde(with = "clickhouse::serde::time::date32")]
        date32: time::Date,
        #[serde(with = "clickhouse::serde::time::datetime")]
        date_time: OffsetDateTime,
        #[serde(with = "clickhouse::serde::time::datetime64::secs")]
        date_time64_0: OffsetDateTime,
        #[serde(with = "clickhouse::serde::time::datetime64::millis")]
        date_time64_3: OffsetDateTime,
        #[serde(with = "clickhouse::serde::time::datetime64::micros")]
        date_time64_6: OffsetDateTime,
        #[serde(with = "clickhouse::serde::time::datetime64::nanos")]
        date_time64_9: OffsetDateTime,
    }

    let client = prepare_database!();
    client
        .query(
            "
            CREATE TABLE IF NOT EXISTS test (
                date Date,
                date32 Date32,
                date_time DateTime,
                date_time64_0 DateTime64(0),
                date_time64_3 DateTime64(3),
                date_time64_6 DateTime64(6),
                date_time64_9 DateTime64(9)
            )
            ENGINE = MergeTree
            ORDER BY tuple()
            ",
        )
        .execute()
        .await
        .unwrap();

    let data = vec![Data {
        date: time::Date::from_calendar_date(2023, January, 1).unwrap(),
        date32: time::Date::from_calendar_date(2023, February, 2).unwrap(),
        date_time: OffsetDateTime::parse("2023-01-03T12:00:00Z", &Iso8601::DEFAULT).unwrap(),
        date_time64_0: OffsetDateTime::parse("2023-01-04T13:00:00Z", &Iso8601::DEFAULT).unwrap(),
        date_time64_3: OffsetDateTime::parse("2023-01-05T14:00:00.123Z", &Iso8601::DEFAULT)
            .unwrap(),
        date_time64_6: OffsetDateTime::parse("2023-01-06T15:00:00.123456Z", &Iso8601::DEFAULT)
            .unwrap(),
        date_time64_9: OffsetDateTime::parse("2023-01-07T16:00:00.123456789Z", &Iso8601::DEFAULT)
            .unwrap(),
    }];

    let result = insert_and_select(&client, "test", data.clone()).await;
    assert_eq!(result, data);
}

#[tokio::test]
#[cfg(feature = "uuid")]
async fn uuid() {
    #[derive(Clone, Debug, Row, Serialize, Deserialize, PartialEq)]
    struct Data {
        id: u16,
        #[serde(with = "clickhouse::serde::uuid")]
        uuid: uuid::Uuid,
    }

    let client = prepare_database!();
    client
        .query(
            "
            CREATE TABLE IF NOT EXISTS test (
                id UInt16,
                uuid UUID
            )
            ENGINE = MergeTree
            ORDER BY id
            ",
        )
        .execute()
        .await
        .unwrap();

    let rows = vec![
        Data {
            id: 42,
            uuid: uuid::Uuid::from_str("550e8400-e29b-41d4-a716-446655440000").unwrap(),
        },
        Data {
            id: 144,
            uuid: uuid::Uuid::from_str("12345678-1234-5678-1234-567812345678").unwrap(),
        },
        Data {
            id: 255,
            uuid: uuid::Uuid::from_str("00000000-0000-0000-0000-000000000000").unwrap(),
        },
    ];

    let result = insert_and_select(&client, "test", rows.clone()).await;
    assert_eq!(result, rows);
}

#[tokio::test]
async fn ipv4_ipv6() {
    #[derive(Clone, Debug, Row, Serialize, Deserialize, PartialEq)]
    struct Data {
        id: u16,
        #[serde(with = "clickhouse::serde::ipv4")]
        ipv4: std::net::Ipv4Addr,
        ipv6: std::net::Ipv6Addr,
    }

    let client = prepare_database!();
    client
        .query(
            "
            CREATE TABLE IF NOT EXISTS test (
                id UInt16,
                ipv4 IPv4,
                ipv6 IPv6
            )
            ENGINE = MergeTree
            ORDER BY id
            ",
        )
        .execute()
        .await
        .unwrap();

    let rows = vec![Data {
        id: 42,
        ipv4: std::net::Ipv4Addr::new(192, 168, 0, 1),
        ipv6: std::net::Ipv6Addr::from_str("2001:db8:3333:4444:5555:6666:7777:8888").unwrap(),
    }];

    let result = insert_and_select(&client, "test", rows.clone()).await;
    assert_eq!(result, rows)
}

#[tokio::test]
async fn fixed_str() {
    #[derive(Clone, Debug, Row, Serialize, Deserialize, PartialEq)]
    struct Data {
        a: [u8; 4],
        b: [u8; 3],
    }

    let client = prepare_database!();
    client
        .query(
            "
            CREATE TABLE IF NOT EXISTS test (
                a FixedString(4),
                b FixedString(3)
            )
            ENGINE = MergeTree
            ORDER BY tuple()
            ",
        )
        .execute()
        .await
        .unwrap();

    let rows = vec![Data {
        a: [49, 50, 51, 52], // '1234'
        b: [55, 55, 55],     // '777'
    }];

    let result = insert_and_select(&client, "test", rows.clone()).await;
    assert_eq!(result, rows);
    assert_eq!(from_utf8_lossy(&result[0].a), "1234");
    assert_eq!(from_utf8_lossy(&result[0].b), "777");
}

#[tokio::test]
async fn decimals() {
    #[derive(Clone, Debug, Row, Serialize, Deserialize, PartialEq)]
    struct Data {
        decimal32_9_4: Decimal32,
        decimal64_18_8: Decimal64,
        decimal128_38_12: Decimal128,
    }

    let client = prepare_database!();
    client
        .query(
            "
            CREATE TABLE IF NOT EXISTS test (
                decimal32_9_4 Decimal32(4),
                decimal64_18_8 Decimal64(8),
                decimal128_38_12 Decimal128(12)
            )
            ENGINE = MergeTree
            ORDER BY tuple()
            ",
        )
        .execute()
        .await
        .unwrap();

    let rows = vec![Data {
        decimal32_9_4: Decimal32::from_str("42.1234").unwrap(),
        decimal64_18_8: Decimal64::from_str("144.56789012").unwrap(),
        decimal128_38_12: Decimal128::from_str("-17014118346046923173168730.37158841057").unwrap(),
    }];

    let result = insert_and_select(&client, "test", rows.clone()).await;
    assert_eq!(result, rows);
}

#[tokio::test]
async fn different_struct_field_order_same_types() {
    #[derive(Clone, Debug, Row, Serialize, Deserialize, PartialEq)]
    struct Data {
        c: String,
        a: String,
    }

    let client = prepare_database!();
    client
        .query(
            "
            CREATE OR REPLACE TABLE test (
                a String,
                c String
            ) ENGINE MergeTree ORDER BY a
            ",
        )
        .execute()
        .await
        .unwrap();

    let rows = vec![
        Data {
            c: "foo".to_string(),
            a: "bar".to_string(),
        },
        Data {
            c: "baz".to_string(),
            a: "qux".to_string(),
        },
    ];

    let result = insert_and_select(&client, "test", rows.clone()).await;
    assert_eq!(result, rows);
}

#[tokio::test]
async fn different_struct_field_order_different_types() {
    #[derive(Clone, Debug, Row, Serialize, Deserialize, PartialEq)]
    struct Data {
        b: u32,
        a: String,
        c: Vec<bool>,
    }

    let client = prepare_database!();
    client
        .query(
            "
            CREATE OR REPLACE TABLE test (
                a String,
                b UInt32,
                c Array(Bool)
            ) ENGINE MergeTree ORDER BY a
            ",
        )
        .execute()
        .await
        .unwrap();

    let rows = vec![
        Data {
            b: 42,
            a: "bar".to_string(),
            c: vec![false, true],
        },
        Data {
            b: 144,
            a: "foo".to_string(),
            c: vec![true, false, true],
        },
    ];

    let result = insert_and_select(&client, "test", rows.clone()).await;
    assert_eq!(result, rows);
}

#[tokio::test]
async fn different_struct_field_order_mixed_usage() {
    #[derive(Clone, Debug, Row, Serialize, Deserialize, PartialEq)]
    struct Data {
        c: String,
        a: String,
        sku: u32,
        id: u32,
        #[serde(skip_serializing)]
        #[serde(skip_deserializing)]
        ignored: u64,
        #[serde(rename = "b")]
        x: u64,
    }

    let client = prepare_database!();
    client
        .query(
            "
            CREATE OR REPLACE TABLE test (
                id UInt32,
                a String,
                b UInt64,
                c String,
                sku UInt32
            ) ENGINE MergeTree ORDER BY id
            ",
        )
        .execute()
        .await
        .unwrap();

    let rows = vec![
        Data {
            c: "foo".to_string(),
            a: "bar".to_string(),
            sku: 42,
            id: 1,
            ignored: 123, // skipped
            x: 100,       // serialized as 'b'
        },
        Data {
            c: "baz".to_string(),
            a: "qux".to_string(),
            sku: 144,
            id: 2,
            ignored: 777, // skipped
            x: 200,       // serialized as 'b'
        },
    ];

    let result = insert_and_select(&client, "test", rows.clone()).await;
    assert_eq!(
        result,
        vec![
            Data {
                c: "foo".to_string(),
                a: "bar".to_string(),
                sku: 42,
                id: 1,
                ignored: 0, // not deserialized, default value
                x: 100,     // deserialized from the db field 'b'
            },
            Data {
                c: "baz".to_string(),
                a: "qux".to_string(),
                sku: 144,
                id: 2,
                ignored: 0, // not deserialized, default value
                x: 200,     // deserialized from the db field 'b'
            },
        ]
    );
}
