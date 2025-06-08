#![allow(dead_code)]

use clickhouse::query::RowCursor;
use clickhouse::validation_mode::ValidationMode;
use clickhouse::{Client, Compression, Row};
use criterion::black_box;
use serde::Deserialize;
use std::time::{Duration, Instant};

pub(crate) trait WithId {
    fn id(&self) -> u64;
}
pub(crate) trait WithAccessType {
    const ACCESS_TYPE: &'static str;
}
pub(crate) trait BenchmarkRow<'a>: Row + Deserialize<'a> + WithId + WithAccessType {}

#[macro_export]
macro_rules! impl_benchmark_row {
    ($type:ty, $id_field:ident, $access_type:literal) => {
        impl WithId for $type {
            fn id(&self) -> u64 {
                self.$id_field as u64
            }
        }

        impl WithAccessType for $type {
            const ACCESS_TYPE: &'static str = $access_type;
        }

        impl<'a> BenchmarkRow<'a> for $type {}
    };
}

#[macro_export]
macro_rules! impl_benchmark_row_no_access_type {
    ($type:ty, $id_field:ident) => {
        impl WithId for $type {
            fn id(&self) -> u64 {
                self.$id_field
            }
        }

        impl WithAccessType for $type {
            const ACCESS_TYPE: &'static str = "";
        }

        impl<'a> BenchmarkRow<'a> for $type {}
    };
}

pub(crate) fn print_header(add: Option<&str>) {
    let add = add.unwrap_or("");
    println!("compress  validation    elapsed  throughput  received{add}");
}

pub(crate) fn print_results<'a, T: BenchmarkRow<'a>>(
    stats: &BenchmarkStats<u64>,
    compression: Compression,
    validation_mode: ValidationMode,
) {
    let BenchmarkStats {
        throughput_mbytes_sec,
        received_mbytes,
        elapsed,
        ..
    } = stats;
    let validation_mode = match validation_mode {
        ValidationMode::First(n) => format!("First({})", n),
        ValidationMode::Each => "Each".to_string(),
        _ => panic!("Unexpected validation mode"),
    };
    let compression = match compression {
        Compression::None => "none",
        #[cfg(feature = "lz4")]
        Compression::Lz4 => "lz4",
        _ => panic!("Unexpected compression mode"),
    };
    let access = if T::ACCESS_TYPE.is_empty() {
        ""
    } else {
        let access_type = T::ACCESS_TYPE;
        &format!("  {access_type:>6}")
    };
    println!("{compression:>8}  {validation_mode:>10}  {elapsed:>9.3?}  {throughput_mbytes_sec:>4.0} MiB/s  {received_mbytes:>4.0} MiB{access}");
}

pub(crate) async fn fetch_cursor<'a, T: BenchmarkRow<'a>>(
    compression: Compression,
    validation_mode: ValidationMode,
    query: &str,
) -> RowCursor<T> {
    let client = Client::default()
        .with_compression(compression)
        .with_validation_mode(validation_mode)
        .with_url("http://localhost:8123");
    client.query(query).fetch::<T>().unwrap()
}

pub(crate) async fn do_select_bench<'a, T: BenchmarkRow<'a>>(
    query: &str,
    compression: Compression,
    validation_mode: ValidationMode,
) -> BenchmarkStats<u64> {
    let start = Instant::now();
    let mut cursor = fetch_cursor::<T>(compression, validation_mode, query).await;

    let mut sum = 0;
    while let Some(row) = cursor.next().await.unwrap() {
        sum += row.id();
        black_box(&row);
    }

    BenchmarkStats::new(&cursor, &start, sum)
}

pub(crate) struct BenchmarkStats<R> {
    pub(crate) throughput_mbytes_sec: f64,
    pub(crate) decoded_mbytes: f64,
    pub(crate) received_mbytes: f64,
    pub(crate) elapsed: Duration,
    // RustRover is unhappy with pub(crate)
    pub result: R,
}

impl<R> BenchmarkStats<R> {
    pub(crate) fn new<T>(cursor: &RowCursor<T>, start: &Instant, result: R) -> Self {
        let elapsed = start.elapsed();
        let dec_bytes = cursor.decoded_bytes();
        let decoded_mbytes = dec_bytes as f64 / 1024.0 / 1024.0;
        let recv_bytes = cursor.received_bytes();
        let received_mbytes = recv_bytes as f64 / 1024.0 / 1024.0;
        let throughput_mbytes_sec = decoded_mbytes / elapsed.as_secs_f64();
        BenchmarkStats {
            throughput_mbytes_sec,
            decoded_mbytes,
            received_mbytes,
            elapsed,
            result,
        }
    }
}
