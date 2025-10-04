#![cfg(all(feature = "chrono", feature = "time", feature = "uuid"))]

use clickhouse::data_types::blob::ChBlobString;
use clickhouse::data_types::chrono::{ChChronoDate, ChChronoDate32, ChChronoDateTime};
use clickhouse::data_types::fixed_string::ChFixedString;
use clickhouse::data_types::ipv4::ChIpv4;
use clickhouse::data_types::ipv6::ChIpv6;
use clickhouse::data_types::time::*;
use clickhouse::data_types::uuid::ChUuid;
use clickhouse::sql::Identifier;
use std::net::{Ipv4Addr, Ipv6Addr};
use time::{Date, Month, OffsetDateTime, Time};
use uuid::Uuid;

#[tokio::test]
async fn smoke() {
    #[derive(Clone, Debug, PartialEq)]
    #[derive(clickhouse::Row, serde::Serialize, serde::Deserialize)]
    struct Row {
        // Avoiding reading/writing strings as UTF-8 for blobs stored in a string column
        blob_str: ChBlobString,
        // FixedString is represented as raw bytes (similarly to `blob_str`, no UTF-8)
        fixed_str: ChFixedString<4>,

        uuid: ChUuid,
        ipv4: ChIpv4,
        ipv6: ChIpv6,

        // decimal32_9_4: Decimal32,
        // decimal64_18_8: Decimal64,
        // decimal128_38_12: Decimal128,
        time_date: ChDate,
        time_date32: ChDate32,
        time_datetime: ChDateTime,
        time_datetime64_0: ChDateTime64<0>,
        time_datetime64_3: ChDateTime64<3>,
        time_datetime64_6: ChDateTime64<6>,
        time_datetime64_9: ChDateTime64<9>,
        time_time: ChTime,
        time_time64_0: ChTime64<0>,
        time_time64_3: ChTime64<3>,
        time_time64_6: ChTime64<6>,
        time_time64_9: ChTime64<9>,

        chrono_date: ChChronoDate,
        chrono_date32: ChChronoDate32,
        chrono_datetime: ChChronoDateTime,
        // chrono_datetime64_0: ChChronoDateTime<0>,
        // chrono_datetime64_3: ChChronoDateTime<3>,
        // chrono_datetime64_6: ChChronoDateTime<6>,
        // chrono_datetime64_9: ChChronoDateTime<9>,
    }

    let client = prepare_database!();
    let table_name = "test_wrappers";

    client
        .query(
            "
            CREATE OR REPLACE TABLE ?
            (
                blob_str             String,
                fixed_str            FixedString(4),

                uuid                 UUID,
                ipv4                 IPv4,
                ipv6                 IPv6,

                -- decimal32_9_4        Decimal32(4),
                -- decimal64_18_8       Decimal64(8),
                -- decimal128_38_12     Decimal128(12),

                time_date            Date,
                time_date32          Date32,
                time_datetime        DateTime('UTC'),
                time_datetime64_0    DateTime64(0, 'UTC'),
                time_datetime64_3    DateTime64(3, 'UTC'),
                time_datetime64_6    DateTime64(6, 'UTC'),
                time_datetime64_9    DateTime64(9, 'UTC'),

                time_time            Time,
                time_time64_0        Time64(0),
                time_time64_3        Time64(3),
                time_time64_6        Time64(6),
                time_time64_9        Time64(9),

                chrono_date          Date,
                chrono_date32        Date32,
                chrono_datetime      DateTime('UTC'),
                -- chrono_datetime64_0  DateTime64(0, 'UTC'),
                -- chrono_datetime64_3  DateTime64(3, 'UTC'),
                -- chrono_datetime64_6  DateTime64(6, 'UTC'),
                -- chrono_datetime64_9  DateTime64(9, 'UTC'),

                -- chrono_time          Time,
                -- chrono_time64_0      Time64(0),
                -- chrono_time64_3      Time64(3),
                -- chrono_time64_6      Time64(6),
                -- chrono_time64_9      Time64(9)
            ) Engine=MergeTree ORDER BY ()
        ",
        )
        .bind(Identifier(table_name))
        .with_option("enable_time_time64_type", "1")
        .execute()
        .await
        .unwrap();

    let row = Row {
        fixed_str: [1, 2, 3, 4].into(),
        uuid: Uuid::new_v4().into(),
        ipv4: Ipv4Addr::from(255).into(),
        ipv6: Ipv6Addr::from(1111).into(),
        blob_str: "foo".to_string().into_bytes().into(),
        // Allowed values ranges:
        // - Date   = [1970-01-01, 2149-06-06]
        // - Date32 = [1900-01-01, 2299-12-31]
        // See
        // - https://clickhouse.com/docs/en/sql-reference/data-types/date
        // - https://clickhouse.com/docs/en/sql-reference/data-types/date32
        time_date: Date::from_calendar_date(2149, Month::June, 6)
            .unwrap()
            .into(),
        time_date32: Date::from_calendar_date(2299, Month::December, 31)
            .unwrap()
            .into(),
        time_datetime: OffsetDateTime::from_unix_timestamp(u32::MAX as i64)
            .unwrap()
            .into(),
        time_datetime64_0: max_datetime64(0).into(),
        time_datetime64_3: max_datetime64(999_000).into(),
        time_datetime64_6: max_datetime64(999_999).into(),
        time_datetime64_9: max_datetime64_nanos().into(),
        time_time: time::Duration::seconds(i32::MAX as i64).into(),
        time_time64_0: time::Duration::seconds(i64::MAX / 1_000_000_000).into(),
        time_time64_3: time::Duration::milliseconds(i64::MAX / 1_000_000).into(),
        time_time64_6: time::Duration::microseconds(i64::MAX / 1_000).into(),
        time_time64_9: time::Duration::nanoseconds(i64::MAX).into(),

        chrono_date: chrono::NaiveDate::from_ymd_opt(2149, 6, 6).unwrap().into(),
        chrono_date32: chrono::NaiveDate::from_ymd_opt(2299, 12, 31)
            .unwrap()
            .into(),
        chrono_datetime: chrono::DateTime::from_timestamp(u32::MAX.into(), 0)
            .unwrap()
            .into(),
        // TODO ...
    };

    let mut insert = client.insert::<Row>(table_name).await.unwrap();
    insert.write(&row).await.unwrap();
    insert.end().await.unwrap();

    let rows = client
        .query("SELECT * FROM ?")
        .bind(Identifier(table_name))
        .fetch_all::<Row>()
        .await
        .unwrap();
    assert_eq!(rows, vec![row]);
}

// The allowed range for DateTime64(8) and lower is
// [1900-01-01 00:00:00, 2299-12-31 23:59:59.99999999] UTC
// See https://clickhouse.com/docs/en/sql-reference/data-types/datetime64
fn max_datetime64(microseconds: u32) -> OffsetDateTime {
    // 2262-04-11 23:47:16
    OffsetDateTime::new_utc(
        Date::from_calendar_date(2299, Month::December, 31).unwrap(),
        Time::from_hms_micro(23, 59, 59, microseconds).unwrap(),
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
