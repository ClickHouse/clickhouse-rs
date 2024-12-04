use clickhouse::format::OutputFormat;

#[tokio::test]
async fn fetch_raw_with_error() {
    let client = prepare_database!();

    let mut raw_cursor = client
        .query("SELECT sleepEachRow(0.01) AS s FROM system.numbers LIMIT 30")
        .with_option("max_block_size", "1")
        .with_option("max_execution_time", "0.015")
        .fetch_raw(OutputFormat::JSONEachRow)
        .unwrap();

    let err = match raw_cursor.next().await {
        Ok(res) => Ok(res),
        Err(err) => Err(err),
    };

    println!("{err:?}");
    assert!(matches!(err, Err(clickhouse::error::Error::BadResponse(_))));
}
