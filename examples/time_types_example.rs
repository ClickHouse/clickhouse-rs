use chrono::Duration;
use clickhouse::Client;
use serde::{Deserialize, Serialize};
use time::Time;

#[derive(Debug, Serialize, Deserialize, clickhouse::Row)]
struct TimeExample {
    #[serde(with = "clickhouse::serde::time::time")]
    time_field: Time,

    #[serde(with = "clickhouse::serde::time::time::option")]
    time_optional: Option<Time>,

    #[serde(with = "clickhouse::serde::time::time64::secs")]
    time64_seconds: Time,

    #[serde(with = "clickhouse::serde::time::time64::millis")]
    time64_millis: Time,

    #[serde(with = "clickhouse::serde::time::time64::micros")]
    time64_micros: Time,

    #[serde(with = "clickhouse::serde::time::time64::nanos")]
    time64_nanos: Time,
}

#[derive(Debug, Serialize, Deserialize, clickhouse::Row)]
struct TimeExampleChrono {
    #[serde(with = "clickhouse::serde::chrono::time")]
    time_field: Duration,

    #[serde(with = "clickhouse::serde::chrono::time::option")]
    time_optional: Option<Duration>,

    #[serde(with = "clickhouse::serde::chrono::time64::secs")]
    time64_seconds: Duration,

    #[serde(with = "clickhouse::serde::chrono::time64::millis")]
    time64_millis: Duration,

    #[serde(with = "clickhouse::serde::chrono::time64::micros")]
    time64_micros: Duration,

    #[serde(with = "clickhouse::serde::chrono::time64::nanos")]
    time64_nanos: Duration,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = Client::default();

    let create_table_sql = r#"
        CREATE TABLE IF NOT EXISTS time_example (
            time_field Time,
            time_optional Nullable(Time),
            time64_seconds Time64(0),
            time64_millis Time64(3),
            time64_micros Time64(6),
            time64_nanos Time64(9)
        ) ENGINE = MergeTree()
        ORDER BY time_field
    "#;

    client.query(create_table_sql).execute().await?;

    // Insert data using time crate
    let time_example = TimeExample {
        time_field: Time::from_hms(12, 34, 56).unwrap(),
        time_optional: Some(Time::from_hms(23, 59, 59).unwrap()),
        time64_seconds: Time::from_hms(1, 2, 3).unwrap(),
        time64_millis: Time::from_hms_milli(4, 5, 6, 123).unwrap(),
        time64_micros: Time::from_hms_micro(7, 8, 9, 456_789).unwrap(),
        time64_nanos: Time::from_hms_nano(10, 11, 12, 123_456_789).unwrap(),
    };

    let mut insert = client.insert::<TimeExample>("time_example")?;
    insert.write(&time_example).await?;
    insert.end().await?;

    // Insert data using chrono crate
    let time_example_chrono = TimeExampleChrono {
        time_field: Duration::seconds(13 * 3600 + 45 * 60),
        time_optional: Some(Duration::seconds(1)),
        time64_seconds: Duration::seconds(2 * 3600 + 3 * 60 + 4),
        time64_millis: Duration::seconds(5 * 3600 + 6 * 60 + 7) + Duration::milliseconds(456),
        time64_micros: Duration::seconds(8 * 3600 + 9 * 60 + 10) + Duration::microseconds(789_012),
        time64_nanos: Duration::seconds(11 * 3600 + 12 * 60 + 13)
            + Duration::nanoseconds(987_654_321),
    };

    let mut insert = client.insert::<TimeExampleChrono>("time_example")?;
    insert.write(&time_example_chrono).await?;
    insert.end().await?;

    // Insert chrono edge cases
    let edge_cases = vec![
        Duration::seconds(-999 * 3600 - 59 * 60 - 59), // Min
        Duration::zero(),                              // Midnight
        Duration::seconds(999 * 3600 + 59 * 60 + 59),  // Max
    ];

    for (i, edge) in edge_cases.into_iter().enumerate() {
        let data = TimeExampleChrono {
            time_field: edge,
            time_optional: Some(edge),
            time64_seconds: edge,
            time64_millis: edge,
            time64_micros: edge,
            time64_nanos: edge,
        };
        let mut insert = client.insert::<TimeExampleChrono>("time_example")?;
        insert.write(&data).await?;
        insert.end().await?;
        println!("Inserted edge case #{i}: {edge:?}");
    }

    // Query the data
    let rows: Vec<TimeExample> = client
        .query("SELECT * FROM time_example ORDER BY time_field")
        .fetch_all()
        .await?;
    for time_example in rows {
        println!("Time example: {time_example:?}");
    }

    println!("Time and Time64 types example completed successfully!");

    Ok(())
}
