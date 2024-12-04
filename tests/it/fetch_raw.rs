use clickhouse::format::OutputFormat;

#[tokio::test]
async fn fetch_raw_with_error() {
    let client = prepare_database!();

    let mut raw_cursor = client
        .query("SELECT sleepEachRow(0.05) AS s FROM system.numbers LIMIT 30")
        .with_option("max_block_size", "1")
        .with_option("max_execution_time", "0.08")
        .fetch_raw(OutputFormat::JSONEachRow)
        .unwrap();

    let err = raw_cursor.next().await;

    println!("{err:?}");
    assert!(matches!(err, Err(clickhouse::error::Error::BadResponse(_))));
}
