#![cfg(feature = "time")]

use crate::common_select::{
    do_select_bench, print_header, print_results, BenchmarkRow, WithAccessType, WithId,
};
use clickhouse::validation_mode::ValidationMode;
use clickhouse::{Compression, Row};
use serde::Deserialize;
use serde_repr::Deserialize_repr;
use time::OffsetDateTime;

mod common_select;

#[derive(Debug, Clone, Deserialize_repr)]
#[repr(i8)]
pub enum PaymentType {
    CSH = 1,
    CRE = 2,
    NOC = 3,
    DIS = 4,
    UNK = 5,
}

/// Uses just `visit_seq` since the order of the fields matches the database schema.
#[derive(Row, Deserialize)]
#[allow(dead_code)]
struct TripSmallSeqAccess {
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

/// Uses `visit_map` to deserialize instead of `visit_seq`,
/// since the fields definition is correct, but the order is wrong.
#[derive(Row, Deserialize)]
#[allow(dead_code)]
struct TripSmallMapAccess {
    pickup_ntaname: String,
    dropoff_ntaname: String,
    trip_id: u32,
    passenger_count: u8,
    trip_distance: f32,
    fare_amount: f32,
    extra: f32,
    tip_amount: f32,
    tolls_amount: f32,
    total_amount: f32,
    payment_type: PaymentType,
    #[serde(with = "clickhouse::serde::time::datetime")]
    pickup_datetime: OffsetDateTime,
    #[serde(with = "clickhouse::serde::time::datetime")]
    dropoff_datetime: OffsetDateTime,
    pickup_longitude: Option<f64>,
    pickup_latitude: Option<f64>,
    dropoff_longitude: Option<f64>,
    dropoff_latitude: Option<f64>,
}

impl_benchmark_row!(TripSmallSeqAccess, trip_id, "seq");
impl_benchmark_row!(TripSmallMapAccess, trip_id, "map");

async fn bench<'a, T: BenchmarkRow<'a>>(compression: Compression, validation_mode: ValidationMode) {
    let stats = do_select_bench::<T>(
        "SELECT * FROM nyc_taxi.trips_small ORDER BY trip_id DESC",
        compression,
        validation_mode,
    )
    .await;
    assert_eq!(stats.result, 3630387815532582);
    print_results::<T>(&stats, compression, validation_mode);
}

#[tokio::main]
async fn main() {
    print_header(Some("  access"));
    bench::<TripSmallSeqAccess>(Compression::None, ValidationMode::First(1)).await;
    bench::<TripSmallSeqAccess>(Compression::None, ValidationMode::Each).await;
    bench::<TripSmallMapAccess>(Compression::None, ValidationMode::Each).await;
    #[cfg(feature = "lz4")]
    {
        bench::<TripSmallSeqAccess>(Compression::Lz4, ValidationMode::First(1)).await;
        bench::<TripSmallSeqAccess>(Compression::Lz4, ValidationMode::Each).await;
        bench::<TripSmallMapAccess>(Compression::Lz4, ValidationMode::Each).await;
    }
}
