use clickhouse::{Client, Compression};

#[tokio::test]
async fn deferred() {
    let client = prepare_database!();
    let results = max_execution_time(client).await;
    assert_eq!(results, vec![0u64, 1u64]);
}

#[tokio::test]
async fn wait_end_of_query() {
    let client = prepare_database!();
    let results = max_execution_time(client.with_option("wait_end_of_query", "1")).await;
    // the entire response is buffered before sending to the client (and it fails)
    // so we don't get any results in this case
    assert_eq!(results, Vec::<u64>::new());
}

async fn max_execution_time(client: Client) -> Vec<u64> {
    #[derive(Debug, clickhouse::Row, serde::Deserialize)]
    struct Res {
        number: u64,
        _sleep: u8,
    }

    // TODO: check different `timeout_overflow_mode`
    let mut cursor = client
        .with_compression(Compression::None)
        // reducing max_block_size to force the server to stream one row at a time
        .with_option("max_block_size", "1")
        // with sleepEachRow(0.01) this ensures that the query will fail after the second row
        .with_option("max_execution_time", "0.025")
        .query("SELECT number, sleepEachRow(0.01) AS sleep FROM system.numbers LIMIT 3")
        .fetch::<Res>()
        .unwrap();

    let mut results: Vec<u64> = Vec::new();
    let err = loop {
        match cursor.next().await {
            Ok(Some(res)) => {
                println!("no: {:?}", res);
                results.push(res.number);
            }
            Ok(None) => panic!("DB exception hasn't been found"),
            Err(err) => break err,
        }
    };

    assert!(err.to_string().contains("TIMEOUT_EXCEEDED"));
    results
}

#[cfg(feature = "lz4")]
#[tokio::test]
async fn deferred_lz4() {
    let client = prepare_database!().with_compression(Compression::Lz4);

    client
        .query("CREATE TABLE test(no UInt32) ENGINE = MergeTree ORDER BY no")
        .execute()
        .await
        .unwrap();

    #[derive(serde::Serialize, clickhouse::Row)]
    struct Row {
        no: u32,
    }

    let part_count = 100;
    let part_size = 100_000;

    // Due to compression we need more complex test here: write a lot of big parts.
    for i in 0..part_count {
        let mut insert = client.insert("test").unwrap();

        for j in 0..part_size {
            let row = Row {
                no: i * part_size + j,
            };

            insert.write(&row).await.unwrap();
        }

        insert.end().await.unwrap();
    }

    let mut cursor = client
        .with_option("max_execution_time", "0.1")
        .query("SELECT no FROM test")
        .fetch::<u32>()
        .unwrap();

    let mut i = 0;

    let err = loop {
        match cursor.next().await {
            Ok(Some(_)) => i += 1,
            Ok(None) => panic!("DB exception hasn't been found"),
            Err(err) => break err,
        }
    };

    assert_ne!(i, 0); // we're interested only in errors during processing
    assert!(err.to_string().contains("TIMEOUT_EXCEEDED"));
}
