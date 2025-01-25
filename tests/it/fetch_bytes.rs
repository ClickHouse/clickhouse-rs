#![cfg(feature = "futures03")]

use futures::{AsyncBufReadExt, StreamExt};
use std::str::from_utf8;

#[tokio::test]
async fn fetch_bytes_single_chunk() {
    let client = prepare_database!();

    let mut raw_cursor = client
        .query("SELECT number FROM system.numbers LIMIT 3")
        .fetch_bytes("CSV")
        .unwrap();

    let mut total_chunks = 0;
    let mut buffer = Vec::<u8>::new();
    while let Some(chunk) = raw_cursor.next().await {
        buffer.extend(chunk.unwrap());
        total_chunks += 1;
    }

    assert_eq!(from_utf8(&buffer).unwrap(), "0\n1\n2\n",);
    assert_eq!(total_chunks, 1);
}

#[tokio::test]
async fn fetch_bytes_multiple_chunks() {
    let client = prepare_database!();

    let mut bytes_cursor = client
        .query("SELECT number FROM system.numbers LIMIT 3")
        // each number will go into a separate chunk
        .with_option("max_block_size", "1")
        .fetch_bytes("CSV")
        .unwrap();

    let mut total_chunks = 0;
    let mut buffer = Vec::<u8>::new();
    while let Some(data) = bytes_cursor.next().await {
        buffer.extend(data.unwrap());
        total_chunks += 1;
    }

    assert_eq!(from_utf8(&buffer).unwrap(), "0\n1\n2\n",);
    assert_eq!(total_chunks, 3);
}

#[tokio::test]
async fn fetch_bytes_with_error() {
    let client = prepare_database!();

    let mut bytes_cursor = client
        .query("SELECT sleepEachRow(0.05) AS s FROM system.numbers LIMIT 30")
        .with_option("max_block_size", "1")
        .with_option("max_execution_time", "0.01")
        .fetch_bytes("JSONEachRow")
        .unwrap();

    let err = bytes_cursor.next().await.unwrap();
    println!("{:?}", err);
    assert!(matches!(err, Err(clickhouse::error::Error::BadResponse(_))));
}

#[tokio::test]
async fn fetch_bytes_lines_single_chunk() {
    let client = prepare_database!();

    let mut lines_cursor = client
        .query("SELECT number FROM system.numbers LIMIT 3")
        .fetch_bytes("CSV")
        .unwrap()
        .lines();

    let mut buffer = Vec::<String>::new();
    while let Some(data) = lines_cursor.next().await {
        buffer.push(data.unwrap());
    }

    assert_eq!(buffer, vec!["0", "1", "2"],);
}

#[tokio::test]
async fn fetch_bytes_lines_multiple_chunks() {
    let client = prepare_database!();

    let mut lines_cursor = client
        .query("SELECT number FROM system.numbers LIMIT 3")
        // each number will go into a separate chunk
        .with_option("max_block_size", "1")
        .fetch_bytes("CSV")
        .unwrap()
        .lines();

    let mut buffer = Vec::<String>::new();
    while let Some(data) = lines_cursor.next().await {
        buffer.push(data.unwrap());
    }

    assert_eq!(buffer, vec!["0", "1", "2"],);
}
