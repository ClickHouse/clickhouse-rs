#![cfg(feature = "time")]

use std::ops::RangeBounds;

use rand::{distributions::Standard, Rng};
use serde::{Deserialize, Serialize};
use time::{macros::datetime, Date, OffsetDateTime};

use clickhouse::Row;

mod common;

#[common::named]
#[tokio::test]
async fn datetime() {
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

#[common::named]
#[tokio::test]
async fn date() {
    let client = common::prepare_database!();

    #[derive(Debug, Row, Serialize, Deserialize)]
    struct MyRow {
        #[serde(with = "clickhouse::serde::time::date")]
        date: Date,
    }

    client
        .query("CREATE TABLE test(date Date) ENGINE = MergeTree ORDER BY date")
        .execute()
        .await
        .unwrap();

    let mut insert = client.insert("test").unwrap();

    let dates = generate_dates(1970..2149, 100);
    for &date in &dates {
        insert.write(&MyRow { date }).await.unwrap();
    }
    insert.end().await.unwrap();

    let actual = client
        .query("SELECT ?fields, toString(date) FROM test ORDER BY date")
        .fetch_all::<(MyRow, String)>()
        .await
        .unwrap();

    assert_eq!(actual.len(), dates.len());

    for ((row, date_str), expected) in actual.iter().zip(dates) {
        assert_eq!(row.date, expected);
        assert_eq!(date_str, &expected.to_string());
    }
}

#[common::named]
#[tokio::test]
async fn date32() {
    let client = common::prepare_database!();

    #[derive(Debug, Row, Serialize, Deserialize)]
    struct MyRow {
        #[serde(with = "clickhouse::serde::time::date32")]
        date: Date,
    }

    client
        .query("CREATE TABLE test(date Date32) ENGINE = MergeTree ORDER BY date")
        .execute()
        .await
        .unwrap();

    let mut insert = client.insert("test").unwrap();

    let dates = generate_dates(1925..2283, 100); // TODO: 1900..=2299 for newer versions.
    for &date in &dates {
        insert.write(&MyRow { date }).await.unwrap();
    }
    insert.end().await.unwrap();

    let actual = client
        .query("SELECT ?fields, toString(date) FROM test ORDER BY date")
        .fetch_all::<(MyRow, String)>()
        .await
        .unwrap();

    assert_eq!(actual.len(), dates.len());

    for ((row, date_str), expected) in actual.iter().zip(dates) {
        assert_eq!(row.date, expected);
        assert_eq!(date_str, &expected.to_string());
    }
}

fn generate_dates(years: impl RangeBounds<i32>, count: usize) -> Vec<Date> {
    let mut rng = rand::thread_rng();
    let mut dates: Vec<_> = (&mut rng)
        .sample_iter(Standard)
        .filter(|date: &Date| years.contains(&date.year()))
        .take(count)
        .collect();

    dates.sort_unstable();
    dates
}
