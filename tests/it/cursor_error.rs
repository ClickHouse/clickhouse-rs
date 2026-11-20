use clickhouse::{Client, Compression};

#[tokio::test]
async fn wait_end_of_query() {
    let client = prepare_database!();
    let scenarios = vec![
        // wait_end_of_query=?, expected_rows
        (false, 3), // server returns some rows before throwing an error
        (true, 0),  // server throws an error immediately
    ];
    for (wait_end_of_query, expected_rows) in scenarios {
        let result = max_execution_time(client.clone(), wait_end_of_query).await;
        assert_eq!(
            result, expected_rows,
            "wait_end_of_query: {wait_end_of_query}, expected_rows: {expected_rows}"
        );
    }
}

async fn max_execution_time(mut client: Client, wait_end_of_query: bool) -> u8 {
    if wait_end_of_query {
        client = client.with_setting("wait_end_of_query", "1")
    }

    // TODO: check different `timeout_overflow_mode`
    let mut cursor = client
        .with_compression(Compression::None)
        // fails on the 4th row
        .with_setting("max_execution_time", "0.1")
        // force streaming one row in a chunk
        .with_setting("max_block_size", "1")
        .query("SELECT sleepEachRow(0.03) AS s FROM system.numbers LIMIT 5")
        .fetch::<u8>()
        .unwrap();

    let mut i = 0;
    let err = loop {
        match cursor.next().await {
            Ok(Some(_)) => i += 1,
            Ok(None) => panic!("DB exception hasn't been found"),
            Err(err) => break err,
        }
    };

    let err_s = err.to_string();
    assert!(
        err_s.contains("TIMEOUT_EXCEEDED"),
        "expected TIMEOUT_EXCEEDED in error string, got {err_s:?}; original: {err:?}"
    );

    i
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
        let mut insert = client.insert::<Row>("test").await.unwrap();

        for j in 0..part_size {
            let row = Row {
                no: i * part_size + j,
            };

            insert.write(&row).await.unwrap();
        }

        insert.end().await.unwrap();
    }

    let mut cursor = client
        .with_setting("max_execution_time", "0.1")
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

    let err_s = err.to_string();
    assert!(
        err_s.contains("TIMEOUT_EXCEEDED"),
        "expected TIMEOUT_EXCEEDED in error string, got {err_s:?}; original: {err:?}"
    );
}

#[tokio::test]
async fn data_resembling_old_exception() {
    let client = prepare_database!();

    // Ends the response with text a pre-25.11 server would have written for an error.
    let text = "Code: 202. DB::Exception: Too many simultaneous queries. \
                (TOO_MANY_SIMULTANEOUS_QUERIES) (version 24.10.1.2812 (official build))\n";
    let fetched = client
        .query("SELECT ?")
        .bind(text)
        .fetch_one::<String>()
        .await
        .unwrap();
    assert_eq!(fetched, text);
}

#[derive(Debug, PartialEq, serde::Deserialize, clickhouse::Row)]
struct FormatRow {
    id: u64,
    version: u64,
}

const FORMAT_QUERY: &str = "SELECT number AS id, number * 10 AS version FROM numbers(3)";

// The format that `fetch()` asks for.
fn fetch_format(validation: bool) -> &'static str {
    if validation {
        "RowBinaryWithNamesAndTypes"
    } else {
        "RowBinary"
    }
}

#[track_caller]
fn assert_format_error<T: std::fmt::Debug>(
    result: clickhouse::error::Result<T>,
    query: &str,
    format: &str,
    validation: bool,
) {
    let err = result.expect_err(query).to_string();
    let expected = format!(
        "the server sent the result in the `{format}` format, but `fetch()` can read only `{}`",
        fetch_format(validation)
    );
    assert!(err.starts_with(&expected), "{query}: {err}");
}

#[tokio::test]
async fn fetch_with_format_clause() {
    let client = prepare_database!();

    for validation in [true, false] {
        let client = client.clone().with_validation(validation);
        let other = fetch_format(!validation);
        let other_clause = format!("FORMAT {other}");

        for (clause, format) in [
            ("FORMAT JSONEachRow", "JSONEachRow"),
            ("FORMAT CSV", "CSV"),
            ("FORMAT jsoneachrow", "jsoneachrow"),
            ("FORMAT JSONEachRow SETTINGS max_threads = 1", "JSONEachRow"),
            ("FORMAT Null", "Null"),
            (&other_clause, other),
        ] {
            let query = format!("{FORMAT_QUERY} {clause}");

            let result = client.query(&query).fetch_all::<FormatRow>().await;
            assert_format_error(result, &query, format, validation);

            let result = client.query(&query).fetch_one::<FormatRow>().await;
            assert_format_error(result, &query, format, validation);

            let result = client.query(&query).fetch_optional::<FormatRow>().await;
            assert_format_error(result, &query, format, validation);

            let mut cursor = client.query(&query).fetch::<FormatRow>().unwrap();
            assert_format_error(cursor.next().await, &query, format, validation);
        }
    }
}

#[tokio::test]
async fn queries_after_format_clause_error() {
    let client = prepare_database!();
    let expected = (0..3)
        .map(|id| FormatRow {
            id,
            version: id * 10,
        })
        .collect::<Vec<_>>();

    for validation in [true, false] {
        let client = client.clone().with_validation(validation);
        let format = fetch_format(validation);

        let query = format!("{FORMAT_QUERY} FORMAT JSONEachRow");
        let result = client.query(&query).fetch_all::<FormatRow>().await;
        assert_format_error(result, &query, "JSONEachRow", validation);

        // The same client still runs queries after the error.
        // A `FORMAT` clause with the format that `fetch()` asks for, in any case, is not an error.
        for query in [
            FORMAT_QUERY.to_string(),
            format!("{FORMAT_QUERY} FORMAT {format}"),
            format!("{FORMAT_QUERY} FORMAT {}", format.to_lowercase()),
        ] {
            let rows = client.query(&query).fetch_all::<FormatRow>().await;
            assert_eq!(rows.unwrap(), expected, "{query}");
        }

        // `fetch_bytes()` returns the data in the format that the query sets.
        let bytes = client
            .query(&query)
            .fetch_bytes(format)
            .unwrap()
            .collect()
            .await
            .unwrap();
        assert_eq!(
            bytes,
            "{\"id\":0,\"version\":0}\n{\"id\":1,\"version\":10}\n{\"id\":2,\"version\":20}\n"
        );
    }
}

#[tokio::test]
async fn fetch_with_default_format_setting() {
    let client = prepare_database!();

    for validation in [true, false] {
        let client = client.clone().with_validation(validation);

        let result = client
            .clone()
            .with_setting("default_format", "JSONEachRow")
            .query(FORMAT_QUERY)
            .fetch_all::<FormatRow>()
            .await;
        assert_format_error(result, FORMAT_QUERY, "JSONEachRow", validation);

        let query = format!("{FORMAT_QUERY} SETTINGS default_format = 'JSONEachRow'");
        let result = client.query(&query).fetch_all::<FormatRow>().await;
        assert_format_error(result, &query, "JSONEachRow", validation);
    }
}

#[tokio::test]
async fn fetch_native_with_other_format() {
    let client = prepare_database!();
    let expected = "the server sent the result in the `JSONEachRow` format, but `fetch_native()` \
                    can read only `Native`. Remove the `FORMAT` clause or the `default_format` \
                    setting, or use `fetch_bytes()` to read the raw bytes";

    let query = format!("{FORMAT_QUERY} FORMAT JSONEachRow");
    let mut cursor = client.query(&query).fetch_native().unwrap();
    let Err(err) = cursor.next().await else {
        panic!("{query}: expected an error");
    };
    assert_eq!(err.to_string(), expected, "{query}");

    let mut cursor = client
        .clone()
        .with_setting("default_format", "JSONEachRow")
        .query(FORMAT_QUERY)
        .fetch_native()
        .unwrap();
    let Err(err) = cursor.next().await else {
        panic!("{FORMAT_QUERY}: expected an error");
    };
    assert_eq!(err.to_string(), expected, "{FORMAT_QUERY}");

    // The same client still runs queries after the error.
    // A `FORMAT Native` clause, in any case, is not an error.
    for query in [
        FORMAT_QUERY.to_string(),
        format!("{FORMAT_QUERY} FORMAT Native"),
        format!("{FORMAT_QUERY} FORMAT native"),
    ] {
        let mut cursor = client.query(&query).fetch_native().unwrap();
        let block = cursor.next().await.unwrap().expect(&query);
        assert_eq!(block.num_rows(), 3, "{query}");
    }
}
