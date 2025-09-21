#![cfg(feature = "jiff")]

use std::ops::RangeBounds;

use jiff::{
    civil::{Date, DateTime, Time},
    tz::TimeZone,
    SignedDuration, Timestamp,
};
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
        #[serde(with = "clickhouse::serde::jiff::datetime")]
        dt: Timestamp,
        #[serde(with = "clickhouse::serde::jiff::datetime::option")]
        dt_opt: Option<Timestamp>,
        #[serde(with = "clickhouse::serde::jiff::datetime64::secs")]
        dt64s: Timestamp,
        #[serde(with = "clickhouse::serde::jiff::datetime64::secs::option")]
        dt64s_opt: Option<Timestamp>,
        #[serde(with = "clickhouse::serde::jiff::datetime64::millis")]
        dt64ms: Timestamp,
        #[serde(with = "clickhouse::serde::jiff::datetime64::millis::option")]
        dt64ms_opt: Option<Timestamp>,
        #[serde(with = "clickhouse::serde::jiff::datetime64::micros")]
        dt64us: Timestamp,
        #[serde(with = "clickhouse::serde::jiff::datetime64::micros::option")]
        dt64us_opt: Option<Timestamp>,
        #[serde(with = "clickhouse::serde::jiff::datetime64::nanos")]
        dt64ns: Timestamp,
        #[serde(with = "clickhouse::serde::jiff::datetime64::nanos::option")]
        dt64ns_opt: Option<Timestamp>,
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
    let d = DateTime::constant(2022, 11, 13, 0, 0, 0, 0)
        .to_zoned(TimeZone::UTC)
        .unwrap();
    let dt_s = d.with().hour(15).minute(27).second(42).build().unwrap();
    let dt_ms = dt_s.with().millisecond(123).build().unwrap();
    let dt_us = dt_ms.with().microsecond(456).build().unwrap();
    let dt_ns = dt_us.with().nanosecond(789).build().unwrap();

    let dt_s = dt_s.timestamp();
    let dt_ms = dt_ms.timestamp();
    let dt_us = dt_us.timestamp();
    let dt_ns = dt_ns.timestamp();

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
    assert_eq!(row_str.dt, original_row.dt.strftime("%F %T").to_string());
    assert_eq!(
        row_str.dt64s,
        original_row.dt64s.strftime("%F %T").to_string()
    );
    assert_eq!(
        row_str.dt64ms,
        original_row.dt64ms.strftime("%F %T%.f").to_string()
    );
    assert_eq!(
        row_str.dt64us,
        original_row.dt64us.strftime("%F %T%.f").to_string()
    );
    assert_eq!(
        row_str.dt64ns,
        original_row.dt64ns.strftime("%F %T%.f").to_string()
    );
}

#[tokio::test]
async fn date() {
    let client = prepare_database!();

    #[derive(Debug, Serialize, Deserialize, Row)]
    struct MyRow {
        #[serde(with = "clickhouse::serde::jiff::date")]
        date: Date,
        #[serde(with = "clickhouse::serde::jiff::date::option")]
        date_opt: Option<Date>,
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
        #[serde(with = "clickhouse::serde::jiff::date32")]
        date: Date,
        #[serde(with = "clickhouse::serde::jiff::date32::option")]
        date_opt: Option<Date>,
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

// Distribution isn't implemented for `jiff` types, but we can lift the implementation from the `time` crate: https://docs.rs/crate/time/0.3.44/source/src/rand09.rs#17-27
struct DateWrapper(Date);

impl Distribution<DateWrapper> for StandardUniform {
    fn sample<R: Rng + ?Sized>(&self, rng: &mut R) -> DateWrapper {
        // For some reason adding to a Date a SignedDuration that is larger than
        // 2932896 days causes an overflow.
        //
        // Causing this:
        // ```
        // Date::MIN + DATE::MAX.duration_since(Date::MIN)
        // ```
        // to fail.
        //
        // Let's just limit ourselves to years 1900 - 2299 (roughly 3506304 hours).
        DateWrapper(
            Date::constant(1900, 1, 1) + SignedDuration::from_hours(rng.random_range(0..=3506304)),
        )
    }
}

fn generate_dates(years: impl RangeBounds<i16>, count: usize) -> Vec<Date> {
    let mut rng = rand::rng();
    let mut dates: Vec<_> = (&mut rng)
        .sample_iter(StandardUniform)
        .filter_map(|date: DateWrapper| {
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

#[tokio::test]
async fn time_roundtrip() {
    let client = prepare_database!();

    client
        .query(
            r#"
            CREATE TABLE test_time (
                t0  Time,
                t1  Nullable(Time)
            ) ENGINE = MergeTree ORDER BY tuple()
            SETTINGS enable_time_time64_type = 1;
            "#,
        )
        .execute()
        .await
        .unwrap();

    #[derive(Debug, PartialEq, Serialize, Deserialize, Row)]
    struct TimeRow {
        #[serde(with = "clickhouse::serde::jiff::time")]
        t0: SignedDuration,
        #[serde(with = "clickhouse::serde::jiff::time::option")]
        t1: Option<SignedDuration>,
    }

    let time = Time::constant(12, 34, 56, 0);
    let duration = time.duration_since(Time::midnight());

    let row = TimeRow {
        t0: duration,
        t1: Some(duration),
    };

    let mut insert = client.insert::<TimeRow>("test_time").unwrap();
    insert.write(&row).await.unwrap();
    insert.end().await.unwrap();

    let fetched = client
        .query("SELECT ?fields FROM test_time")
        .fetch_one::<TimeRow>()
        .await
        .unwrap();

    assert_eq!(fetched, row);
}

#[tokio::test]
async fn time_negative_roundtrip() {
    let client = prepare_database!();

    client
        .query(
            r#"
            CREATE TABLE test_time_chrono_negative (
                t0  Time,
                t1  Nullable(Time)
            ) ENGINE = MergeTree ORDER BY tuple()
            SETTINGS enable_time_time64_type = 1;
            "#,
        )
        .execute()
        .await
        .unwrap();

    #[derive(Debug, PartialEq, Serialize, Deserialize, Row)]
    struct TimeRow {
        #[serde(with = "clickhouse::serde::jiff::time")]
        t0: SignedDuration,
        #[serde(with = "clickhouse::serde::jiff::time::option")]
        t1: Option<SignedDuration>,
    }

    // Create negative duration directly
    let negative_duration = SignedDuration::from_secs(-2 * 3600 - 15 * 60 - 30); // -02:15:30

    let row = TimeRow {
        t0: negative_duration,
        t1: Some(negative_duration),
    };

    let mut insert = client
        .insert::<TimeRow>("test_time_chrono_negative")
        .unwrap();
    insert.write(&row).await.unwrap();
    insert.end().await.unwrap();

    let fetched = client
        .query("SELECT ?fields FROM test_time_chrono_negative")
        .fetch_one::<TimeRow>()
        .await
        .unwrap();

    assert_eq!(fetched, row);
}

#[tokio::test]
async fn time64_roundtrip() {
    let client = prepare_database!();

    client
        .query(
            r#"
            CREATE TABLE test_time64 (
                t0      Time64(0),
                t0_opt  Nullable(Time64(0)),
                t3      Time64(3),
                t3_opt  Nullable(Time64(3)),
                t6      Time64(6),
                t6_opt  Nullable(Time64(6)),
                t9      Time64(9),
                t9_opt  Nullable(Time64(9))
            ) ENGINE = MergeTree
            ORDER BY tuple()
            SETTINGS enable_time_time64_type = 1;
            "#,
        )
        .execute()
        .await
        .unwrap();

    #[derive(Debug, PartialEq, Serialize, Deserialize, Row)]
    struct MyRow {
        #[serde(with = "clickhouse::serde::jiff::time64::secs")]
        t0: SignedDuration,
        #[serde(with = "clickhouse::serde::jiff::time64::secs::option")]
        t0_opt: Option<SignedDuration>,

        #[serde(with = "clickhouse::serde::jiff::time64::millis")]
        t3: SignedDuration,
        #[serde(with = "clickhouse::serde::jiff::time64::millis::option")]
        t3_opt: Option<SignedDuration>,

        #[serde(with = "clickhouse::serde::jiff::time64::micros")]
        t6: SignedDuration,
        #[serde(with = "clickhouse::serde::jiff::time64::micros::option")]
        t6_opt: Option<SignedDuration>,

        #[serde(with = "clickhouse::serde::jiff::time64::nanos")]
        t9: SignedDuration,
        #[serde(with = "clickhouse::serde::jiff::time64::nanos::option")]
        t9_opt: Option<SignedDuration>,
    }

    let time_s = Time::constant(12, 34, 56, 0);
    let time_ms = Time::constant(12, 34, 56, 789_000_000);
    let time_us = Time::constant(12, 34, 56, 789_123_000);
    let time_ns = Time::constant(12, 34, 56, 789_123_456);

    let dur_s = time_s.duration_since(Time::midnight());
    let dur_ms = time_ms.duration_since(Time::midnight());
    let dur_us = time_us.duration_since(Time::midnight());
    let dur_ns = time_ns.duration_since(Time::midnight());

    let original_row = MyRow {
        t0: dur_s,
        t0_opt: Some(dur_s),
        t3: dur_ms,
        t3_opt: Some(dur_ms),
        t6: dur_us,
        t6_opt: Some(dur_us),
        t9: dur_ns,
        t9_opt: Some(dur_ns),
    };

    let mut insert = client.insert::<MyRow>("test_time64").unwrap();
    insert.write(&original_row).await.unwrap();
    insert.end().await.unwrap();

    let fetched = client
        .query("SELECT ?fields FROM test_time64")
        .fetch_one::<MyRow>()
        .await
        .unwrap();

    assert_eq!(fetched, original_row);
}

#[tokio::test]
async fn time64_negative_roundtrip() {
    let client = prepare_database!();

    client
        .query(
            r#"
            CREATE TABLE test_time64_negative (
                t0      Time64(0),
                t0_opt  Nullable(Time64(0)),
                t3      Time64(3),
                t3_opt  Nullable(Time64(3)),
                t6      Time64(6),
                t6_opt  Nullable(Time64(6)),
                t9      Time64(9),
                t9_opt  Nullable(Time64(9))
            ) ENGINE = MergeTree
            ORDER BY tuple()
            SETTINGS enable_time_time64_type = 1;
            "#,
        )
        .execute()
        .await
        .unwrap();

    #[derive(Debug, PartialEq, Serialize, Deserialize, Row)]
    struct MyRow {
        #[serde(with = "clickhouse::serde::jiff::time64::secs")]
        t0: SignedDuration,
        #[serde(with = "clickhouse::serde::jiff::time64::secs::option")]
        t0_opt: Option<SignedDuration>,

        #[serde(with = "clickhouse::serde::jiff::time64::millis")]
        t3: SignedDuration,
        #[serde(with = "clickhouse::serde::jiff::time64::millis::option")]
        t3_opt: Option<SignedDuration>,

        #[serde(with = "clickhouse::serde::jiff::time64::micros")]
        t6: SignedDuration,
        #[serde(with = "clickhouse::serde::jiff::time64::micros::option")]
        t6_opt: Option<SignedDuration>,

        #[serde(with = "clickhouse::serde::jiff::time64::nanos")]
        t9: SignedDuration,
        #[serde(with = "clickhouse::serde::jiff::time64::nanos::option")]
        t9_opt: Option<SignedDuration>,
    }

    // Create negative durations directly
    let neg_base_seconds = -5 * 3600 - 15 * 60 - 30; // -18930 seconds (-05:15:30)

    let dur_s = SignedDuration::from_secs(neg_base_seconds);
    let dur_ms = dur_s - SignedDuration::from_millis(123);
    let dur_us = dur_s - SignedDuration::from_micros(123_456);
    let dur_ns = dur_s - SignedDuration::from_nanos(123_456_789);

    let negative_row = MyRow {
        t0: dur_s,
        t0_opt: Some(dur_s),
        t3: dur_ms,
        t3_opt: Some(dur_ms),
        t6: dur_us,
        t6_opt: Some(dur_us),
        t9: dur_ns,
        t9_opt: Some(dur_ns),
    };

    let mut insert = client.insert::<MyRow>("test_time64_negative").unwrap();
    insert.write(&negative_row).await.unwrap();
    insert.end().await.unwrap();

    let fetched = client
        .query("SELECT ?fields FROM test_time64_negative")
        .fetch_one::<MyRow>()
        .await
        .unwrap();

    assert_eq!(fetched, negative_row);
}
