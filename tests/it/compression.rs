use serde::{Deserialize, Serialize};

use clickhouse::{Client, Compression, Row};

async fn check(client: Client) {
    #[derive(Debug, Row, Serialize, Deserialize)]
    struct MyRow<'a> {
        no: u32,
        name: &'a str,
    }

    client
        .query(
            "
            CREATE TABLE test(no UInt32, name LowCardinality(String))
            ENGINE = MergeTree
            ORDER BY no
        ",
        )
        .execute()
        .await
        .unwrap();

    let mut insert = client.insert("test").unwrap();
    for i in 0..200_000 {
        insert.write(&MyRow { no: i, name: "foo" }).await.unwrap();
    }
    insert.end().await.unwrap();

    // Check data.

    let (sum_no, sum_len) = client
        .query("SELECT sum(no), sum(length(name)) FROM test")
        .fetch_one::<(u64, u64)>()
        .await
        .unwrap();

    assert_eq!(sum_no, 19_999_900_000);
    assert_eq!(sum_len, 600_000);
}

#[tokio::test]
async fn none() {
    let client = prepare_database!().with_compression(Compression::None);
    check(client).await;
}

#[cfg(feature = "lz4")]
#[tokio::test]
async fn lz4() {
    let client = prepare_database!().with_compression(Compression::Lz4);
    check(client).await;
}

#[cfg(feature = "lz4")]
#[tokio::test]
async fn lz4_hc() {
    let client = prepare_database!().with_compression(Compression::Lz4Hc(4));
    check(client).await;
}
