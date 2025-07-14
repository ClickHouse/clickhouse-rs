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
    time_field: chrono::NaiveTime,
    
    #[serde(with = "clickhouse::serde::chrono::time::option")]
    time_optional: Option<chrono::NaiveTime>,
    
    #[serde(with = "clickhouse::serde::chrono::time64::secs")]
    time64_seconds: chrono::NaiveTime,
    
    #[serde(with = "clickhouse::serde::chrono::time64::millis")]
    time64_millis: chrono::NaiveTime,
    
    #[serde(with = "clickhouse::serde::chrono::time64::micros")]
    time64_micros: chrono::NaiveTime,
    
    #[serde(with = "clickhouse::serde::chrono::time64::nanos")]
    time64_nanos: chrono::NaiveTime,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create a connection pool
    let client = Client::default();
    
    // Create a client
    // let client = pool.get_handle()?; // This line is removed as per the edit hint
    
    // Create table with Time and Time64 columns
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
        time64_micros: Time::from_hms_micro(7, 8, 9, 456789).unwrap(),
        time64_nanos: Time::from_hms_nano(10, 11, 12, 123456789).unwrap(),
    };
    
    let mut insert = client.insert::<TimeExample>("time_example")?;
    insert.write(&time_example).await?;
    insert.end().await?;
    
    // Insert data using chrono crate
    let time_example_chrono = TimeExampleChrono {
        time_field: chrono::NaiveTime::from_hms_opt(13, 45, 67).unwrap(),
        time_optional: Some(chrono::NaiveTime::from_hms_opt(0, 0, 1).unwrap()),
        time64_seconds: chrono::NaiveTime::from_hms_opt(2, 3, 4).unwrap(),
        time64_millis: chrono::NaiveTime::from_hms_milli_opt(5, 6, 7, 456).unwrap(),
        time64_micros: chrono::NaiveTime::from_hms_micro_opt(8, 9, 10, 789012).unwrap(),
        time64_nanos: chrono::NaiveTime::from_hms_nano_opt(11, 12, 13, 987654321).unwrap(),
    };
    
    let mut insert = client.insert::<TimeExampleChrono>("time_example")?;
    insert.write(&time_example_chrono).await?;
    insert.end().await?;
    
    // Query the data
    let rows: Vec<TimeExample> = client
        .query("SELECT * FROM time_example ORDER BY time_field")
        .fetch_all()
        .await?;
    for time_example in rows {
        println!("Time example: {:?}", time_example);
    }
    
    println!("✅ Time and Time64 types example completed successfully!");
    
    Ok(())
} 