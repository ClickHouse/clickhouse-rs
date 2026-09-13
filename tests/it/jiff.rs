use core::ops::RangeBounds;

use clickhouse_jiff::{
    Date16, Date32, SignedDuration32, SignedDuration64, Timestamp32, Timestamp64,
};
use jiff::{
    SignedDuration,
    civil::{Date, DateTime, Time},
    tz::TimeZone,
};
use rand::{
    Rng,
    distr::{Distribution, StandardUniform},
};
use serde::{Deserialize, Serialize};

use clickhouse::Row;

#[tokio::test]
async fn datetime() {
    let client = prepare_database!();

    #[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Row)]
    struct MyRow {
        dt: Timestamp32,
        dt_opt: Option<Timestamp32>,
        dt64s: Timestamp64<0>,
        dt64s_opt: Option<Timestamp64<0>>,
        dt64ms: Timestamp64<3>,
        dt64ms_opt: Option<Timestamp64<3>>,
        dt64us: Timestamp64<6>,
        dt64us_opt: Option<Timestamp64<6>>,
        dt64ns: Timestamp64<9>,
        dt64ns_opt: Option<Timestamp64<9>>,
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

    let dt = Timestamp32::try_from(dt_s.timestamp()).unwrap();
    let dt64s = Timestamp64::<_>::try_from(dt_s.timestamp()).unwrap();
    let dt64ms = Timestamp64::<_>::try_from(dt_ms.timestamp()).unwrap();
    let dt64us = Timestamp64::<_>::try_from(dt_us.timestamp()).unwrap();
    let dt64ns = Timestamp64::<_>::try_from(dt_ns.timestamp()).unwrap();

    let original_row = MyRow {
        dt,
        dt_opt: Some(dt),
        dt64s,
        dt64s_opt: Some(dt64s),
        dt64ms,
        dt64ms_opt: Some(dt64ms),
        dt64us,
        dt64us_opt: Some(dt64us),
        dt64ns,
        dt64ns_opt: Some(dt64ns),
    };

    let mut insert = client.insert::<MyRow>("test").await.unwrap();
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
    assert_eq!(
        row_str.dt,
        original_row.dt.into_inner().strftime("%F %T").to_string()
    );
    assert_eq!(
        row_str.dt64s,
        original_row
            .dt64s
            .into_inner()
            .strftime("%F %T")
            .to_string()
    );
    assert_eq!(
        row_str.dt64ms,
        original_row
            .dt64ms
            .into_inner()
            .strftime("%F %T%.f")
            .to_string()
    );
    assert_eq!(
        row_str.dt64us,
        original_row
            .dt64us
            .into_inner()
            .strftime("%F %T%.f")
            .to_string()
    );
    assert_eq!(
        row_str.dt64ns,
        original_row
            .dt64ns
            .into_inner()
            .strftime("%F %T%.f")
            .to_string()
    );
}

#[tokio::test]
async fn date() {
    let client = prepare_database!();

    #[derive(Debug, Serialize, Deserialize, Row)]
    struct MyRow {
        date: Date16,
        date_opt: Option<Date16>,
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

    let mut insert = client.insert::<MyRow>("test").await.unwrap();

    let dates = generate_dates(1970..2149, 100);
    for &date in &dates {
        let date = Date16::try_from(date).unwrap();
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
        assert_eq!(row.date.into_inner(), expected);
        assert_eq!(row.date_opt.map(|d| d.into_inner()), Some(expected));
        assert_eq!(date_str, &expected.to_string());
    }
}

#[tokio::test]
async fn date32() {
    let client = prepare_database!();

    #[derive(Debug, Serialize, Deserialize, Row)]
    struct MyRow {
        date: Date32,
        date_opt: Option<Date32>,
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

    let mut insert = client.insert::<MyRow>("test").await.unwrap();

    let dates = generate_dates(1925..2283, 100); // TODO: 1900..=2299 for newer versions.
    for &date in &dates {
        let date = Date32::try_from(date).unwrap();
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
        assert_eq!(row.date.into_inner(), expected);
        assert_eq!(row.date_opt.map(|d| d.into_inner()), Some(expected));
        assert_eq!(date_str, &expected.to_string());
    }
}

// Distribution isn't implemented for `jiff` types,
// let's implement it ourselves.
struct DateWrapper(Date);

impl Distribution<DateWrapper> for StandardUniform {
    fn sample<R: Rng + ?Sized>(&self, rng: &mut R) -> DateWrapper {
        // For some reason adding to a Date a SignedDuration that is larger than
        // 2932896 days causes an overflow.
        //
        // Causing this:
        // ```
        // Date::MIN + Date::MAX.duration_since(Date::MIN)
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
            CREATE TABLE test(
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
        t0: SignedDuration32,
        t1: Option<SignedDuration32>,
    }

    let time = Time::constant(12, 34, 56, 0);
    let duration = SignedDuration32::try_from(time.duration_since(Time::midnight())).unwrap();

    let row = TimeRow {
        t0: duration,
        t1: Some(duration),
    };

    let mut insert = client.insert::<TimeRow>("test").await.unwrap();
    insert.write(&row).await.unwrap();
    insert.end().await.unwrap();

    let fetched = client
        .query("SELECT ?fields FROM test")
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
            CREATE TABLE test(
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
        t0: SignedDuration32,
        t1: Option<SignedDuration32>,
    }

    // Create negative duration directly
    let negative_duration =
        SignedDuration32::try_from(SignedDuration::from_secs(-2 * 3600 - 15 * 60 - 30)).unwrap(); // -02:15:30

    let row = TimeRow {
        t0: negative_duration,
        t1: Some(negative_duration),
    };

    let mut insert = client.insert::<TimeRow>("test").await.unwrap();
    insert.write(&row).await.unwrap();
    insert.end().await.unwrap();

    let fetched = client
        .query("SELECT ?fields FROM test")
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
            CREATE TABLE test(
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
        t0: SignedDuration64<0>,
        t0_opt: Option<SignedDuration64<0>>,

        t3: SignedDuration64<3>,
        t3_opt: Option<SignedDuration64<3>>,

        t6: SignedDuration64<6>,
        t6_opt: Option<SignedDuration64<6>>,

        t9: SignedDuration64<9>,
        t9_opt: Option<SignedDuration64<9>>,
    }

    let time_s = Time::constant(12, 34, 56, 0);
    let time_ms = Time::constant(12, 34, 56, 789_000_000);
    let time_us = Time::constant(12, 34, 56, 789_123_000);
    let time_ns = Time::constant(12, 34, 56, 789_123_456);

    let dur_s = SignedDuration64::<_>::try_from(time_s.duration_since(Time::midnight())).unwrap();
    let dur_ms = SignedDuration64::<_>::try_from(time_ms.duration_since(Time::midnight())).unwrap();
    let dur_us = SignedDuration64::<_>::try_from(time_us.duration_since(Time::midnight())).unwrap();
    let dur_ns = SignedDuration64::<_>::try_from(time_ns.duration_since(Time::midnight())).unwrap();

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

    let mut insert = client.insert::<MyRow>("test").await.unwrap();
    insert.write(&original_row).await.unwrap();
    insert.end().await.unwrap();

    let fetched = client
        .query("SELECT ?fields FROM test")
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
            CREATE TABLE test(
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
        t0: SignedDuration64<0>,
        t0_opt: Option<SignedDuration64<0>>,

        t3: SignedDuration64<3>,
        t3_opt: Option<SignedDuration64<3>>,

        t6: SignedDuration64<6>,
        t6_opt: Option<SignedDuration64<6>>,

        t9: SignedDuration64<9>,
        t9_opt: Option<SignedDuration64<9>>,
    }

    // Create negative durations directly
    let neg_base_seconds = -5 * 3600 - 15 * 60 - 30; // -18930 seconds (-05:15:30)

    let dur_s = SignedDuration::from_secs(neg_base_seconds);
    let dur_ms = SignedDuration64::<_>::try_from(dur_s - SignedDuration::from_millis(123)).unwrap();
    let dur_us =
        SignedDuration64::<_>::try_from(dur_s - SignedDuration::from_micros(123_456)).unwrap();
    let dur_ns =
        SignedDuration64::<_>::try_from(dur_s - SignedDuration::from_nanos(123_456_789)).unwrap();

    let dur_s = SignedDuration64::<_>::try_from(dur_s).unwrap();

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

    let mut insert = client.insert::<MyRow>("test").await.unwrap();
    insert.write(&negative_row).await.unwrap();
    insert.end().await.unwrap();

    let fetched = client
        .query("SELECT ?fields FROM test")
        .fetch_one::<MyRow>()
        .await
        .unwrap();

    assert_eq!(fetched, negative_row);
}
