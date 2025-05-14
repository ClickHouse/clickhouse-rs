use clickhouse::error::Error;
use clickhouse::output_format::OutputFormat;
use clickhouse_derive::Row;
use clickhouse_rowbinary::parse_columns_header;
use clickhouse_rowbinary::types::{Column, DataTypeNode};
use serde::{Deserialize, Serialize};
use std::str::FromStr;
use time::format_description::well_known::Iso8601;
use time::Month::{February, January};
use time::OffsetDateTime;

#[tokio::test]
async fn test_header_parsing() {
    let client = prepare_database!();
    client
        .query(
            "
                CREATE OR REPLACE TABLE visits
                (
                    CounterID UInt32,
                    StartDate Date,
                    Sign Int8,
                    IsNew UInt8,
                    VisitID UInt64,
                    UserID UInt64,
                    Goals Nested
                    (
                        ID UInt32,
                        Serial UInt32,
                        EventTime DateTime,
                        Price Int64,
                        OrderID String,
                        CurrencyID UInt32
                    )
                ) ENGINE = MergeTree ORDER BY ()
            ",
        )
        .execute()
        .await
        .unwrap();

    let mut cursor = client
        .query("SELECT * FROM visits LIMIT 0")
        .fetch_bytes("RowBinaryWithNamesAndTypes")
        .unwrap();

    let data = cursor.collect().await.unwrap();
    let result = parse_columns_header(&mut &data[..]).unwrap();
    assert_eq!(
        result,
        vec![
            Column {
                name: "CounterID".to_string(),
                data_type: DataTypeNode::UInt32
            },
            Column {
                name: "StartDate".to_string(),
                data_type: DataTypeNode::Date
            },
            Column {
                name: "Sign".to_string(),
                data_type: DataTypeNode::Int8
            },
            Column {
                name: "IsNew".to_string(),
                data_type: DataTypeNode::UInt8
            },
            Column {
                name: "VisitID".to_string(),
                data_type: DataTypeNode::UInt64
            },
            Column {
                name: "UserID".to_string(),
                data_type: DataTypeNode::UInt64
            },
            Column {
                name: "Goals.ID".to_string(),
                data_type: DataTypeNode::Array(Box::new(DataTypeNode::UInt32))
            },
            Column {
                name: "Goals.Serial".to_string(),
                data_type: DataTypeNode::Array(Box::new(DataTypeNode::UInt32))
            },
            Column {
                name: "Goals.EventTime".to_string(),
                data_type: DataTypeNode::Array(Box::new(DataTypeNode::DateTime(None)))
            },
            Column {
                name: "Goals.Price".to_string(),
                data_type: DataTypeNode::Array(Box::new(DataTypeNode::Int64))
            },
            Column {
                name: "Goals.OrderID".to_string(),
                data_type: DataTypeNode::Array(Box::new(DataTypeNode::String))
            },
            Column {
                name: "Goals.CurrencyID".to_string(),
                data_type: DataTypeNode::Array(Box::new(DataTypeNode::UInt32))
            }
        ]
    );
}

#[tokio::test]
async fn test_basic_types_deserialization() {
    #[derive(Debug, Row, Serialize, Deserialize, PartialEq)]
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

    let client = prepare_database!().with_fetch_format(OutputFormat::RowBinaryWithNamesAndTypes);
    let result = client
        .query(
            "
            SELECT
                255                                      :: UInt8   AS uint8_val,
                65535                                    :: UInt16  AS uint16_val,
                4294967295                               :: UInt32  AS uint32_val,
                18446744073709551615                     :: UInt64  AS uint64_val,
                340282366920938463463374607431768211455  :: UInt128 AS uint128_val,
                -128                                     :: Int8    AS int8_val,
                -32768                                   :: Int16   AS int16_val,
                -2147483648                              :: Int32   AS int32_val,
                -9223372036854775808                     :: Int64   AS int64_val,
                -170141183460469231731687303715884105728 :: Int128  AS int128_val,
                42.0                                     :: Float32 AS float32_val,
                144.0                                    :: Float64 AS float64_val,
                'test'                                   :: String  AS string_val
            ",
        )
        .fetch_one::<Data>()
        .await;

    assert_eq!(
        result.unwrap(),
        Data {
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
        }
    );
}

#[tokio::test]
async fn test_several_simple_rows() {
    #[derive(Debug, Row, Serialize, Deserialize, PartialEq)]
    struct Data {
        num: u64,
        str: String,
    }

    let client = prepare_database!().with_fetch_format(OutputFormat::RowBinaryWithNamesAndTypes);
    let result = client
        .query("SELECT number AS num, toString(number) AS str FROM system.numbers LIMIT 3")
        .fetch_all::<Data>()
        .await;

    assert_eq!(
        result.unwrap(),
        vec![
            Data {
                num: 0,
                str: "0".to_string(),
            },
            Data {
                num: 1,
                str: "1".to_string(),
            },
            Data {
                num: 2,
                str: "2".to_string(),
            },
        ]
    );
}

#[tokio::test]
async fn test_many_numbers() {
    #[derive(Row, Deserialize)]
    struct Data {
        no: u64,
    }

    let client = prepare_database!().with_fetch_format(OutputFormat::RowBinaryWithNamesAndTypes);
    let mut cursor = client
        .query("SELECT number FROM system.numbers_mt LIMIT 2000")
        .fetch::<Data>()
        .unwrap();

    let mut sum = 0;
    while let Some(row) = cursor.next().await.unwrap() {
        sum += row.no;
    }
    assert_eq!(sum, (0..2000).sum::<u64>());
}

#[tokio::test]
async fn test_array_deserialization() {
    #[derive(Debug, Row, Serialize, Deserialize, PartialEq)]
    struct Data {
        id: u16,
        one_dim_array: Vec<u32>,
        two_dim_array: Vec<Vec<i64>>,
        three_dim_array: Vec<Vec<Vec<f64>>>,
        description: String,
    }

    let client = prepare_database!().with_fetch_format(OutputFormat::RowBinaryWithNamesAndTypes);
    let result = client
        .query(
            "
            SELECT
                42                                                       :: UInt16                       AS id,
                [1, 2]                                                   :: Array(UInt32)                AS one_dim_array,
                [[1, 2], [3, 4]]                                         :: Array(Array(Int64))          AS two_dim_array,
                [[[1.1, 2.2], [3.3, 4.4]], [], [[5.5, 6.6], [7.7, 8.8]]] :: Array(Array(Array(Float64))) AS three_dim_array,
                'foobar'                                                 :: String                       AS description
            ",
        )
        .fetch_one::<Data>()
        .await;

    assert_eq!(
        result.unwrap(),
        Data {
            id: 42,
            one_dim_array: vec![1, 2],
            two_dim_array: vec![vec![1, 2], vec![3, 4]],
            three_dim_array: vec![
                vec![vec![1.1, 2.2], vec![3.3, 4.4]],
                vec![],
                vec![vec![5.5, 6.6], vec![7.7, 8.8]]
            ],
            description: "foobar".to_string(),
        }
    );
}

#[tokio::test]
async fn test_default_types_validation_nullable() {
    #[derive(Debug, Row, Serialize, Deserialize, PartialEq)]
    struct Data {
        n: Option<u32>,
    }

    let client = prepare_database!().with_fetch_format(OutputFormat::RowBinaryWithNamesAndTypes);
    let result = client
        .query("SELECT true AS b, 144 :: Int32 AS n2")
        .fetch_one::<Data>()
        .await;

    assert!(result.is_err());
    assert!(matches!(
        result.unwrap_err(),
        Error::DataTypeMismatch { .. }
    ));

    // FIXME: lack of derive PartialEq for Error prevents proper assertion
    // assert_eq!(result, Error::DataTypeMismatch {
    //     column_name: "n".to_string(),
    //     expected_type: "Nullable".to_string(),
    //     actual_type: "Bool".to_string(),
    //     columns: vec![...],
    // });
}

#[tokio::test]
#[cfg(feature = "time")]
async fn test_default_types_validation_custom_serde() {
    #[derive(Debug, Row, Serialize, Deserialize, PartialEq)]
    struct Data {
        #[serde(with = "clickhouse::serde::time::datetime64::millis")]
        n1: OffsetDateTime, // underlying is still Int64; should not compose it from two (U)Int32
    }

    let client = prepare_database!().with_fetch_format(OutputFormat::RowBinaryWithNamesAndTypes);
    let result = client
        .query("SELECT 42 :: UInt32 AS n1, 144 :: Int32 AS n2")
        .fetch_one::<Data>()
        .await;

    assert!(result.is_err());
    println!("{:?}", result);
    assert!(matches!(
        result.unwrap_err(),
        Error::DataTypeMismatch { .. }
    ));

    // FIXME: lack of derive PartialEq for Error prevents proper assertion
    // assert_eq!(result, Error::DataTypeMismatch {
    //     column_name: "n1".to_string(),
    //     expected_type: "Int64".to_string(),
    //     actual_type: "Int32".to_string(),
    //     columns: vec![...],
    // });
}

#[tokio::test]
async fn test_too_many_struct_fields() {
    #[derive(Debug, Row, Serialize, Deserialize, PartialEq)]
    struct Data {
        a: u32,
        b: u32,
        c: u32,
    }

    let client = prepare_database!().with_fetch_format(OutputFormat::RowBinaryWithNamesAndTypes);
    let result = client
        .query("SELECT 42 :: UInt32 AS a, 144 :: UInt32 AS b")
        .fetch_one::<Data>()
        .await;

    assert!(result.is_err());
    assert!(matches!(
        result.unwrap_err(),
        Error::DeserializeCallAfterEndOfStruct { .. }
    ));
}

#[tokio::test]
async fn test_serde_skip_deserializing() {
    #[derive(Debug, Row, Serialize, Deserialize, PartialEq)]
    struct Data {
        a: u32,
        #[serde(skip_deserializing)]
        b: u32,
        c: u32,
    }

    let client = prepare_database!().with_fetch_format(OutputFormat::RowBinaryWithNamesAndTypes);
    let result = client
        .query("SELECT 42 :: UInt32 AS a, 144 :: UInt32 AS c")
        .fetch_one::<Data>()
        .await;

    assert_eq!(
        result.unwrap(),
        Data {
            a: 42,
            b: 0, // default value
            c: 144,
        }
    );
}

#[tokio::test]
#[cfg(feature = "time")]
async fn test_date_time_types() {
    #[derive(Debug, Row, Serialize, Deserialize, PartialEq)]
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

    let client = prepare_database!().with_fetch_format(OutputFormat::RowBinaryWithNamesAndTypes);
    let result = client
        .query(
            "
            SELECT
                '2023-01-01'                    :: Date          AS date,
                '2023-02-02'                    :: Date32        AS date32,
                '2023-01-03 12:00:00'           :: DateTime      AS date_time,
                '2023-01-04 13:00:00'           :: DateTime64(0) AS date_time64_0,
                '2023-01-05 14:00:00.123'       :: DateTime64(3) AS date_time64_3,
                '2023-01-06 15:00:00.123456'    :: DateTime64(6) AS date_time64_6,
                '2023-01-07 16:00:00.123456789' :: DateTime64(9) AS date_time64_9
            ",
        )
        .fetch_one::<Data>()
        .await;

    assert_eq!(
        result.unwrap(),
        Data {
            date: time::Date::from_calendar_date(2023, January, 1).unwrap(),
            date32: time::Date::from_calendar_date(2023, February, 2).unwrap(),
            date_time: OffsetDateTime::parse("2023-01-03T12:00:00Z", &Iso8601::DEFAULT).unwrap(),
            date_time64_0: OffsetDateTime::parse("2023-01-04T13:00:00Z", &Iso8601::DEFAULT)
                .unwrap(),
            date_time64_3: OffsetDateTime::parse("2023-01-05T14:00:00.123Z", &Iso8601::DEFAULT)
                .unwrap(),
            date_time64_6: OffsetDateTime::parse("2023-01-06T15:00:00.123456Z", &Iso8601::DEFAULT)
                .unwrap(),
            date_time64_9: OffsetDateTime::parse(
                "2023-01-07T16:00:00.123456789Z",
                &Iso8601::DEFAULT
            )
            .unwrap(),
        }
    );
}

#[tokio::test]
async fn test_ipv4_ipv6() {
    #[derive(Debug, Row, Serialize, Deserialize, PartialEq)]
    struct Data {
        id: u16,
        #[serde(with = "clickhouse::serde::ipv4")]
        ipv4: std::net::Ipv4Addr,
        ipv6: std::net::Ipv6Addr,
    }

    let client = prepare_database!().with_fetch_format(OutputFormat::RowBinaryWithNamesAndTypes);
    let result = client
        .query(
            "
            SELECT
                42                                       :: UInt16 AS id,
                '192.168.0.1'                            :: IPv4   AS ipv4,
                '2001:db8:3333:4444:5555:6666:7777:8888' :: IPv6   AS ipv6
            ",
        )
        .fetch_all::<Data>()
        .await;

    assert_eq!(
        result.unwrap(),
        vec![Data {
            id: 42,
            ipv4: std::net::Ipv4Addr::new(192, 168, 0, 1),
            ipv6: std::net::Ipv6Addr::from_str("2001:db8:3333:4444:5555:6666:7777:8888").unwrap(),
        }]
    )
}

// FIXME: RBWNAT should allow for tracking the order of fields in the struct and in the database!
#[tokio::test]
#[ignore]
async fn test_different_struct_field_order() {
    #[derive(Debug, Row, Serialize, Deserialize, PartialEq)]
    struct Data {
        c: String,
        a: String,
    }

    let client = prepare_database!().with_fetch_format(OutputFormat::RowBinaryWithNamesAndTypes);
    let result = client
        .query("SELECT 'foo' AS a, 'bar' :: String AS c")
        .fetch_one::<Data>()
        .await;

    assert_eq!(
        result.unwrap(),
        Data {
            a: "foo".to_string(),
            c: "bar".to_string(),
        }
    );
}
