use std::str::FromStr;

use fixnum::typenum::{U12, U4, U8};
use fixnum::FixedPoint;
use rand::distributions::Alphanumeric;
use rand::seq::SliceRandom;
use rand::Rng;
use time::{Date, Month, OffsetDateTime, Time};

use clickhouse::sql::Identifier;
use clickhouse::{error::Result, Client};

// This example covers derivation of _simpler_ ClickHouse data types.
// See also: https://clickhouse.com/docs/en/sql-reference/data-types

#[tokio::main]
async fn main() -> Result<()> {
    let table_name = "chrs_data_types_derive";
    let client = Client::default().with_url("http://localhost:8123");

    client
        .query(
            "
            CREATE OR REPLACE TABLE ?
            (
                int8                 Int8,
                int16                Int16,
                int32                Int32,
                int64                Int64,
                int128               Int128,
                -- int256               Int256,
                uint8                UInt8,
                uint16               UInt16,
                uint32               UInt32,
                uint64               UInt64,
                uint128              UInt128,
                -- uint256              UInt256,
                float32              Float32,
                float64              Float64,
                boolean              Boolean,
                str                  String,
                blob_str             String,
                nullable_str         Nullable(String),
                low_car_str          LowCardinality(String),
                nullable_low_car_str LowCardinality(Nullable(String)),
                fixed_str            FixedString(16),
                uuid                 UUID,
                ipv4                 IPv4,
                ipv6                 IPv6,
                enum8                Enum8('Foo', 'Bar'),
                enum16               Enum16('Qaz' = 42, 'Qux' = 255),
                decimal32_9_4        Decimal(9, 4),
                decimal64_18_8       Decimal(18, 8),
                decimal128_38_12     Decimal(38, 12),
                -- decimal256_76_20           Decimal(76, 20),
                date                 Date,
                date32               Date32,
                datetime             DateTime,
                datetime_tz          DateTime('UTC'),
                datetime64_0         DateTime64(0),
                datetime64_3         DateTime64(3),
                datetime64_6         DateTime64(6),
                datetime64_9         DateTime64(9),
                datetime64_9_tz      DateTime64(9, 'UTC')
            ) ENGINE MergeTree ORDER BY ();
        ",
        )
        .bind(Identifier(table_name))
        .execute()
        .await?;

    let mut insert = client.insert(table_name)?;
    insert.write(&Row::new()).await?;
    insert.end().await?;

    let rows = client
        .query("SELECT ?fields FROM ?")
        .bind(Identifier(table_name))
        .fetch_all::<Row>()
        .await?;

    println!("{rows:#?}");
    Ok(())
}

#[derive(Clone, Debug, PartialEq)]
#[derive(clickhouse::Row, serde::Serialize, serde::Deserialize)]
pub struct Row {
    pub int8: i8,
    pub int16: i16,
    pub int32: i32,
    pub int64: i64,
    pub int128: i128,
    pub uint8: u8,
    pub uint16: u16,
    pub uint32: u32,
    pub uint64: u64,
    pub uint128: u128,
    pub float32: f32,
    pub float64: f64,
    pub boolean: bool,
    pub str: String,
    // Avoiding reading/writing strings as UTF-8 for blobs stored in a string column
    #[serde(with = "serde_bytes")]
    pub blob_str: Vec<u8>,
    pub nullable_str: Option<String>,
    // LowCardinality does not affect the struct definition
    pub low_car_str: String,
    // The same applies to a "nested" LowCardinality
    pub nullable_low_car_str: Option<String>,
    // FixedString is represented as raw bytes (similarly to `blob_str`, no UTF-8)
    pub fixed_str: [u8; 16],
    #[serde(with = "clickhouse::serde::uuid")]
    pub uuid: uuid::Uuid,
    #[serde(with = "clickhouse::serde::ipv4")]
    pub ipv4: std::net::Ipv4Addr,
    pub ipv6: std::net::Ipv6Addr,
    pub enum8: Enum8,
    pub enum16: Enum16,
    pub decimal32_9_4: Decimal32,
    pub decimal64_18_8: Decimal64,
    pub decimal128_38_12: Decimal128,
    #[serde(with = "clickhouse::serde::time::date")]
    pub date: Date,
    #[serde(with = "clickhouse::serde::time::date32")]
    pub date32: Date,
    #[serde(with = "clickhouse::serde::time::datetime")]
    pub datetime: OffsetDateTime,
    #[serde(with = "clickhouse::serde::time::datetime")]
    pub datetime_tz: OffsetDateTime,
    #[serde(with = "clickhouse::serde::time::datetime64::secs")]
    pub datetime64_0: OffsetDateTime,
    #[serde(with = "clickhouse::serde::time::datetime64::millis")]
    pub datetime64_3: OffsetDateTime,
    #[serde(with = "clickhouse::serde::time::datetime64::micros")]
    pub datetime64_6: OffsetDateTime,
    #[serde(with = "clickhouse::serde::time::datetime64::nanos")]
    pub datetime64_9: OffsetDateTime,
    #[serde(with = "clickhouse::serde::time::datetime64::nanos")]
    pub datetime64_9_tz: OffsetDateTime,
}

// See ClickHouse decimal sizes: https://clickhouse.com/docs/en/sql-reference/data-types/decimal
type Decimal32 = FixedPoint<i32, U4>; // Decimal(9, 4) = Decimal32(4)
type Decimal64 = FixedPoint<i64, U8>; // Decimal(18, 8) = Decimal64(8)
type Decimal128 = FixedPoint<i128, U12>; // Decimal(38, 12) = Decimal128(12)

#[derive(Clone, Debug, PartialEq)]
#[derive(serde_repr::Serialize_repr, serde_repr::Deserialize_repr)]
#[repr(u8)]
pub enum Enum8 {
    Foo = 1,
    Bar = 2,
}

#[derive(Clone, Debug, PartialEq)]
#[derive(serde_repr::Serialize_repr, serde_repr::Deserialize_repr)]
#[repr(u16)]
pub enum Enum16 {
    Qaz = 42,
    Qux = 255,
}

impl Row {
    pub fn new() -> Self {
        let mut rng = rand::thread_rng();
        Row {
            int8: rng.gen(),
            int16: rng.gen(),
            int32: rng.gen(),
            int64: rng.gen(),
            int128: rng.gen(),
            uint8: rng.gen(),
            uint16: rng.gen(),
            uint32: rng.gen(),
            uint64: rng.gen(),
            uint128: rng.gen(),
            float32: rng.gen(),
            float64: rng.gen(),
            boolean: rng.gen(),
            str: random_str(),
            blob_str: rng.gen::<[u8; 3]>().to_vec(),
            nullable_str: Some(random_str()),
            low_car_str: random_str(),
            nullable_low_car_str: Some(random_str()),
            fixed_str: rng.gen(),
            uuid: uuid::Uuid::new_v4(),
            ipv4: std::net::Ipv4Addr::from_str("172.195.0.1").unwrap(),
            ipv6: std::net::Ipv6Addr::from_str("::ffff:acc3:1").unwrap(),
            enum8: [Enum8::Foo, Enum8::Bar]
                .choose(&mut rng)
                .unwrap()
                .to_owned(),
            enum16: [Enum16::Qaz, Enum16::Qux]
                .choose(&mut rng)
                .unwrap()
                .to_owned(),
            // See also: https://clickhouse.com/docs/en/sql-reference/data-types/decimal
            decimal32_9_4: Decimal32::from_str("99999.9999").unwrap(),
            decimal64_18_8: Decimal64::from_str("9999999999.99999999").unwrap(),
            decimal128_38_12: Decimal128::from_str("99999999999999999999999999.999999999999")
                .unwrap(),
            // Allowed values ranges:
            // - Date   = [1970-01-01, 2149-06-06]
            // - Date32 = [1900-01-01, 2299-12-31]
            // See
            // - https://clickhouse.com/docs/en/sql-reference/data-types/date
            // - https://clickhouse.com/docs/en/sql-reference/data-types/date32
            date: Date::from_calendar_date(2149, Month::June, 6).unwrap(),
            date32: Date::from_calendar_date(2299, Month::December, 31).unwrap(),
            datetime: max_datetime(),
            datetime_tz: max_datetime(),
            datetime64_0: max_datetime64(),
            datetime64_3: max_datetime64(),
            datetime64_6: max_datetime64(),
            datetime64_9: max_datetime64_nanos(),
            datetime64_9_tz: max_datetime64_nanos(),
        }
    }
}

impl Default for Row {
    fn default() -> Self {
        Self::new()
    }
}

fn random_str() -> String {
    rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(3)
        .map(char::from)
        .collect()
}

fn max_datetime() -> OffsetDateTime {
    OffsetDateTime::from_unix_timestamp(u32::MAX.into()).unwrap()
}

// The allowed range for DateTime64(8) and lower is
// [1900-01-01 00:00:00, 2299-12-31 23:59:59.99999999] UTC
// See https://clickhouse.com/docs/en/sql-reference/data-types/datetime64
fn max_datetime64() -> OffsetDateTime {
    // 2262-04-11 23:47:16
    OffsetDateTime::new_utc(
        Date::from_calendar_date(2299, Month::December, 31).unwrap(),
        Time::from_hms_micro(23, 59, 59, 999_999).unwrap(),
    )
}

// DateTime64(8)/DateTime(9) allowed range is
// [1900-01-01 00:00:00, 2262-04-11 23:47:16] UTC
// See https://clickhouse.com/docs/en/sql-reference/data-types/datetime64
fn max_datetime64_nanos() -> OffsetDateTime {
    OffsetDateTime::new_utc(
        Date::from_calendar_date(2262, Month::April, 11).unwrap(),
        Time::from_hms_nano(23, 47, 15, 999_999_999).unwrap(),
    )
}
