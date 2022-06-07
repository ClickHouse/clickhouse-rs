use serde::{Deserialize, Serialize};

use clickhouse::{Compression, Row};

mod common;

async fn it_inserts(name: &str, compression: Compression) {
    let client = common::prepare_database(name)
        .await
        .with_compression(compression);

    #[derive(Debug, Row, Serialize, Deserialize)]
    struct MyRow<'a> {
        no: u32,
        name: &'a str,
    }

    client
        .query(
            "
            CREATE TABLE some(no UInt32, name LowCardinality(String))
            ENGINE = MergeTree
            ORDER BY no
        ",
        )
        .execute()
        .await
        .unwrap();

    let mut inserter = client.inserter("some").unwrap();

    for i in 0..200_000 {
        inserter.write(&MyRow { no: i, name: "foo" }).await.unwrap();
        inserter.commit().await.unwrap();
    }

    inserter.end().await.unwrap();

    // Check data.

    let (sum_no, sum_len) = client
        .query("SELECT sum(no), sum(length(name)) FROM some")
        .fetch_one::<(u64, u64)>()
        .await
        .unwrap();

    assert_eq!(sum_no, 19_999_900_000);
    assert_eq!(sum_len, 600_000);
}

#[tokio::test]
async fn it_inserts_compression_none() {
    it_inserts("it_inserts_compression_none", Compression::None).await;
}

#[tokio::test]
async fn it_inserts_compression_lz4() {
    it_inserts("it_inserts_compression_lz4", Compression::Lz4).await;
}

#[tokio::test]
async fn it_inserts_compression_lz4_hc() {
    it_inserts("it_inserts_compression_lz4hc", Compression::Lz4Hc(4)).await;
}
