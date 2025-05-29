#![cfg(feature = "time")]

use clickhouse::{Client, Compression, Row};
use criterion::black_box;
use serde::Deserialize;
use serde_repr::Deserialize_repr;
use time::OffsetDateTime;

#[derive(Debug, Clone, Deserialize_repr)]
#[repr(i8)]
pub enum PaymentType {
    CSH = 1,
    CRE = 2,
    NOC = 3,
    DIS = 4,
    UNK = 5,
}

#[derive(Debug, Clone, Row, Deserialize)]
#[allow(dead_code)]
pub struct TripSmall {
    trip_id: u32,
    #[serde(with = "clickhouse::serde::time::datetime")]
    pickup_datetime: OffsetDateTime,
    #[serde(with = "clickhouse::serde::time::datetime")]
    dropoff_datetime: OffsetDateTime,
    pickup_longitude: Option<f64>,
    pickup_latitude: Option<f64>,
    dropoff_longitude: Option<f64>,
    dropoff_latitude: Option<f64>,
    passenger_count: u8,
    trip_distance: f32,
    fare_amount: f32,
    extra: f32,
    tip_amount: f32,
    tolls_amount: f32,
    total_amount: f32,
    payment_type: PaymentType,
    pickup_ntaname: String,
    dropoff_ntaname: String,
}

async fn bench(name: &str, compression: Compression) {
    let start = std::time::Instant::now();
    let (last_trip_id, dec_mbytes, rec_mbytes) = do_bench(compression).await;
    assert_eq!(last_trip_id, 3630387815532582);
    let elapsed = start.elapsed();
    let throughput = dec_mbytes / elapsed.as_secs_f64();
    println!("{name:>8}  {elapsed:>7.3?}  {throughput:>4.0} MiB/s  {rec_mbytes:>4.0} MiB");
}

async fn do_bench(compression: Compression) -> (u64, f64, f64) {
    let client = Client::default()
        .with_compression(compression)
        .with_url("http://localhost:8123");

    let mut cursor = client
        .query("SELECT * FROM nyc_taxi.trips_small ORDER BY trip_id DESC")
        .fetch::<TripSmall>()
        .unwrap();

    let mut sum = 0;
    while let Some(row) = cursor.next().await.unwrap() {
        sum += row.trip_id as u64;
        black_box(&row);
    }

    let dec_bytes = cursor.decoded_bytes();
    let dec_mbytes = dec_bytes as f64 / 1024.0 / 1024.0;
    let recv_bytes = cursor.received_bytes();
    let recv_mbytes = recv_bytes as f64 / 1024.0 / 1024.0;
    (sum, dec_mbytes, recv_mbytes)
}

#[tokio::main]
async fn main() {
    println!("compress  elapsed  throughput  received");
    bench("none", Compression::None).await;
    bench("lz4", Compression::Lz4).await;
}
