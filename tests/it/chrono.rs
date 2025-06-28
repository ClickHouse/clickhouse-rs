#![cfg(feature = "chrono")]

use std::ops::RangeBounds;

use chrono::{DateTime, Datelike, NaiveDate, Utc};
use rand::{
    distr::{Distribution, StandardUniform},
    Rng,
};
use serde::{Deserialize, Serialize};

use clickhouse::Row;

#[tokio::test]
async fn datetime() {
    let client = prepare_database!();

    #[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Row)]
    struct MyRow {
        #[serde(with = "clickhouse::serde::chrono::datetime")]
        dt: DateTime<Utc>,
        #[serde(with = "clickhouse::serde::chrono::datetime::option")]
        dt_opt: Option<DateTime<Utc>>,
        #[serde(with = "clickhouse::serde::chrono::datetime64::secs")]
        dt64s: DateTime<Utc>,
        #[serde(with = "clickhouse::serde::chrono::datetime64::secs::option")]
        dt64s_opt: Option<DateTime<Utc>>,
        #[serde(with = "clickhouse::serde::chrono::datetime64::millis")]
        dt64ms: DateTime<Utc>,
        #[serde(with = "clickhouse::serde::chrono::datetime64::millis::option")]
        dt64ms_opt: Option<DateTime<Utc>>,
        #[serde(with = "clickhouse::serde::chrono::datetime64::micros")]
        dt64us: DateTime<Utc>,
        #[serde(with = "clickhouse::serde::chrono::datetime64::micros::option")]
        dt64us_opt: Option<DateTime<Utc>>,
        #[serde(with = "clickhouse::serde::chrono::datetime64::nanos")]
        dt64ns: DateTime<Utc>,
        #[serde(with = "clickhouse::serde::chrono::datetime64::nanos::option")]
        dt64ns_opt: Option<DateTime<Utc>>,
    }

    #[derive(Debug, Deserialize, Row)]
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
                dt          DateTime,
                dt_opt      Nullable(DateTime),
                dt64s       DateTime64(0),
                dt64s_opt   Nullable(DateTime64(0)),
                dt64ms      DateTime64(3),
                dt64ms_opt  Nullable(DateTime64(3)),
                dt64us      DateTime64(6),
                dt64us_opt  Nullable(DateTime64(6)),
                dt64ns      DateTime64(9),
                dt64ns_opt  Nullable(DateTime64(9))
            )
            ENGINE = MergeTree ORDER BY dt
        ",
        )
        .execute()
        .await
        .unwrap();
    let d = NaiveDate::from_ymd_opt(2022, 11, 13).unwrap();
    let dt_s = d.and_hms_opt(15, 27, 42).unwrap().and_utc();
    let dt_ms = d.and_hms_milli_opt(15, 27, 42, 123).unwrap().and_utc();
    let dt_us = d.and_hms_micro_opt(15, 27, 42, 123456).unwrap().and_utc();
    let dt_ns = d.and_hms_nano_opt(15, 27, 42, 123456789).unwrap().and_utc();

    let original_row = MyRow {
        dt: dt_s,
        dt_opt: Some(dt_s),
        dt64s: dt_s,
        dt64s_opt: Some(dt_s),
        dt64ms: dt_ms,
        dt64ms_opt: Some(dt_ms),
        dt64us: dt_us,
        dt64us_opt: Some(dt_us),
        dt64ns: dt_ns,
        dt64ns_opt: Some(dt_ns),
    };

    let mut insert = client.insert::<MyRow>("test").unwrap();
    insert.write(&original_row).await.unwrap();
    insert.end().await.unwrap();

    let row = client
        .query("SELECT ?fields FROM test")
        .fetch_one::<MyRow>()
        .await
        .unwrap();

    let row_str = client
        .query(
            "
            SELECT toString(dt)     AS dt,
                   toString(dt64s)  AS dt64s,
                   toString(dt64ms) AS dt64ms,
                   toString(dt64us) AS dt64us,
                   toString(dt64ns) AS dt64ns
              FROM test
        ",
        )
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

#[tokio::test]
async fn date() {
    let client = prepare_database!();

    #[derive(Debug, Serialize, Deserialize, Row)]
    struct MyRow {
        #[serde(with = "clickhouse::serde::chrono::date")]
        date: NaiveDate,
        #[serde(with = "clickhouse::serde::chrono::date::option")]
        date_opt: Option<NaiveDate>,
    }

    client
        .query(
            "
            CREATE TABLE test(
                date        Date,
                date_opt    Nullable(Date)
            ) ENGINE = MergeTree ORDER BY date
        ",
        )
        .execute()
        .await
        .unwrap();

    let mut insert = client.insert::<MyRow>("test").unwrap();

    let dates = generate_dates(1970..2149, 100);
    for &date in &dates {
        let original_row = MyRow {
            date,
            date_opt: Some(date),
        };

        insert.write(&original_row).await.unwrap();
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
        assert_eq!(row.date_opt, Some(expected));
        assert_eq!(date_str, &expected.to_string());
    }
}

#[tokio::test]
async fn date32() {
    let client = prepare_database!();

    #[derive(Debug, Serialize, Deserialize, Row)]
    struct MyRow {
        #[serde(with = "clickhouse::serde::chrono::date32")]
        date: NaiveDate,
        #[serde(with = "clickhouse::serde::chrono::date32::option")]
        date_opt: Option<NaiveDate>,
    }

    client
        .query(
            "
            CREATE TABLE test(
                date        Date32,
                date_opt    Nullable(Date32)
            ) ENGINE = MergeTree ORDER BY date
        ",
        )
        .execute()
        .await
        .unwrap();

    let mut insert = client.insert::<MyRow>("test").unwrap();

    let dates = generate_dates(1925..2283, 100); // TODO: 1900..=2299 for newer versions.
    for &date in &dates {
        let original_row = MyRow {
            date,
            date_opt: Some(date),
        };

        insert.write(&original_row).await.unwrap();
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
        assert_eq!(row.date_opt, Some(expected));
        assert_eq!(date_str, &expected.to_string());
    }
}

// Distribution isn't implemented for `chrono` types, but we can lift the implementation from the `time` crate: https://docs.rs/time/latest/src/time/rand.rs.html#14-20
struct NaiveDateWrapper(NaiveDate);

impl Distribution<NaiveDateWrapper> for StandardUniform {
    fn sample<R: Rng + ?Sized>(&self, rng: &mut R) -> NaiveDateWrapper {
        NaiveDateWrapper(
            NaiveDate::from_num_days_from_ce_opt(rng.random_range(
                NaiveDate::MIN.num_days_from_ce()..=NaiveDate::MAX.num_days_from_ce(),
            ))
            .unwrap(),
        )
    }
}

fn generate_dates(years: impl RangeBounds<i32>, count: usize) -> Vec<NaiveDate> {
    let mut rng = rand::rng();
    let mut dates: Vec<_> = (&mut rng)
        .sample_iter(StandardUniform)
        .filter_map(|date: NaiveDateWrapper| {
            if years.contains(&date.0.year()) {
                Some(date.0)
            } else {
                None
            }
        })
        .take(count)
        .collect();

    dates.sort_unstable();
    dates
}
