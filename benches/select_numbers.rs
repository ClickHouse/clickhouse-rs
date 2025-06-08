use serde::Deserialize;

use crate::common_select::{
    do_select_bench, print_header, print_results, BenchmarkRow, WithAccessType, WithId,
};
use clickhouse::validation_mode::ValidationMode;
use clickhouse::{Compression, Row};

mod common_select;

#[derive(Row, Deserialize)]
struct Data {
    number: u64,
}

impl_benchmark_row_no_access_type!(Data, number);

async fn bench(compression: Compression, validation_mode: ValidationMode) {
    let stats = do_select_bench::<Data>(
        "SELECT number FROM system.numbers_mt LIMIT 500000000",
        compression,
        validation_mode,
    )
    .await;
    assert_eq!(stats.result, 124999999750000000);
    print_results::<Data>(&stats, compression, validation_mode);
}

#[tokio::main]
async fn main() {
    print_header(None);
    bench(Compression::None, ValidationMode::First(1)).await;
    bench(Compression::None, ValidationMode::Each).await;
    #[cfg(feature = "lz4")]
    {
        bench(Compression::Lz4, ValidationMode::First(1)).await;
        bench(Compression::Lz4, ValidationMode::Each).await;
    }
}
