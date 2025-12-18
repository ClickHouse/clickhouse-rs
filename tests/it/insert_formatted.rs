use crate::{SimpleRow, create_simple_table, fetch_rows};
use bytes::Bytes;
use clickhouse::Client;
use clickhouse_macros::Row;
use serde::Deserialize;
use std::cmp;
use tokio::io::AsyncWriteExt;

/// First 1000 records of the [NYC taxi dataset] in `TabSeparated` format.
///
/// [NYC taxi dataset]: https://clickhouse.com/docs/getting-started/example-datasets/nyc-taxi#create-the-table-trips
const TAXI_DATA_TSV: &[u8] = include_bytes!("fixtures/nyc-taxi_trips_0_head_1000.tsv");

#[tokio::test]
async fn empty_insert() {
    let table_name = "insert_empty";
    let query_id = uuid::Uuid::new_v4().to_string();

    let client = prepare_database!();
    create_simple_table(&client, table_name).await;

    let insert = client
        .insert_formatted_with(format!("INSERT INTO {table_name} FORMAT TabSeparated"))
        .with_option("query_id", query_id);
    insert.end().await.unwrap();

    let rows = fetch_rows::<SimpleRow>(&client, table_name).await;
    assert!(rows.is_empty())
}

#[tokio::test]
async fn insert() {
    let client = prepare_database!()
        // Separate test for compression
        .with_compression(clickhouse::Compression::None);

    create_table(&client).await;

    let bytes = Bytes::copy_from_slice(TAXI_DATA_TSV);

    let mut insert =
        client.insert_formatted_with("INSERT INTO nyc_taxi_trips_small FORMAT TabSeparated");

    insert.send(bytes).await.unwrap();

    insert.end().await.unwrap();

    verify_insert(&client).await;
}

#[tokio::test]
#[cfg(feature = "lz4")]
async fn insert_compressed() {
    use clickhouse::insert_formatted::CompressedData;

    let client = prepare_database!();

    create_table(&client).await;

    let data = CompressedData::from_slice(TAXI_DATA_TSV);

    let mut insert =
        client.insert_formatted_with("INSERT INTO nyc_taxi_trips_small FORMAT TabSeparated");

    insert.send_compressed(data).await.unwrap();

    insert.end().await.unwrap();

    verify_insert(&client).await;
}

#[tokio::test]
async fn insert_buffered() {
    let client = prepare_database!();

    create_table(&client).await;

    let mut data = TAXI_DATA_TSV;
    let capacity = 8192;

    let mut insert = client
        .insert_formatted_with("INSERT INTO nyc_taxi_trips_small FORMAT TabSeparated")
        .buffered_with_capacity(capacity);

    // Cycle different read sizes in an attempt to break the buffer
    let read_sizes = [1, 10, 100, 1000, 1024, capacity];

    while !data.is_empty() {
        for size in read_sizes {
            if data.is_empty() {
                break;
            }

            let written = insert
                .write(&data[..cmp::min(size, data.len())])
                .await
                .unwrap();
            assert_ne!(written, 0);
            data = &data[written..];
        }
    }

    insert.end().await.unwrap();

    verify_insert(&client).await;
}

async fn create_table(client: &Client) {
    client
        .query(
            r#"
            CREATE TABLE IF NOT EXISTS nyc_taxi_trips_small (
                trip_id             UInt32,
                pickup_datetime     DateTime,
                dropoff_datetime    DateTime,
                pickup_longitude    Nullable(Float64),
                pickup_latitude     Nullable(Float64),
                dropoff_longitude   Nullable(Float64),
                dropoff_latitude    Nullable(Float64),
                passenger_count     UInt8,
                trip_distance       Float32,
                fare_amount         Float32,
                extra               Float32,
                tip_amount          Float32,
                tolls_amount        Float32,
                total_amount        Float32,
                payment_type        Enum('CSH' = 1, 'CRE' = 2, 'NOC' = 3, 'DIS' = 4, 'UNK' = 5),
                pickup_ntaname      LowCardinality(String),
                dropoff_ntaname     LowCardinality(String)
            )
            ENGINE = MergeTree
            PRIMARY KEY (pickup_datetime, dropoff_datetime)
            "#,
        )
        .execute()
        .await
        .unwrap();
}

async fn verify_insert(client: &Client) {
    #[derive(Row, Deserialize)]
    struct Results {
        min_trip_id: u32,
        max_trip_id: u32,
        avg_trip_distance: f64,
        count: u64,
    }

    let results = client
        .query(
            "SELECT \
                min(trip_id) min_trip_id, \
                max(trip_id) max_trip_id, \
                avg(trip_distance) avg_trip_distance,\
                count(*) count \
            FROM nyc_taxi_trips_small",
        )
        .fetch_one::<Results>()
        .await
        .unwrap();

    assert_eq!(results.min_trip_id, 1199999902);
    assert_eq!(results.max_trip_id, 1200019742);
    assert_eq!(results.avg_trip_distance, 2.983289997249842);
    assert_eq!(results.count, 1000);
}
