use clickhouse::{Client, Compression};

#[tokio::test]
async fn deferred() {
    let client = prepare_database!();
    max_execution_time(client, false).await;
}

#[tokio::test]
async fn wait_end_of_query() {
    let client = prepare_database!();
    max_execution_time(client, true).await;
}

async fn max_execution_time(mut client: Client, wait_end_of_query: bool) {
    if wait_end_of_query {
        client = client.with_option("wait_end_of_query", "1")
    }

    // TODO: check different `timeout_overflow_mode`
    let mut cursor = client
        .with_compression(Compression::None)
        .with_option("max_execution_time", "0.1")
        .query("SELECT toUInt8(65 + number % 5) FROM system.numbers LIMIT 100000000")
        .fetch::<u8>()
        .unwrap();

    let mut i = 0u64;

    let err = loop {
        match cursor.next().await {
            Ok(Some(no)) => {
                // Check that we haven't parsed something extra.
                assert_eq!(no, (65 + i % 5) as u8);
                i += 1;
            }
            Ok(None) => panic!("DB exception hasn't been found"),
            Err(err) => break err,
        }
    };

    assert!(wait_end_of_query ^ (i != 0));
    assert!(err.to_string().contains("TIMEOUT_EXCEEDED"));
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
