#![cfg(feature = "test-util")]

use crate::SimpleRow;
use clickhouse::{test, Client};
use clickhouse_types::data_types::Column;
use clickhouse_types::DataTypeNode;
use std::time::Duration;

async fn test_provide() {
    let mock = test::Mock::new();
    let client = Client::default().with_url(mock.url());
    let expected = vec![SimpleRow::new(1, "one"), SimpleRow::new(2, "two")];
    let columns = vec![
        Column::new("id".to_string(), DataTypeNode::UInt64),
        Column::new("data".to_string(), DataTypeNode::String),
    ];
    mock.add(test::handlers::provide(&columns, &expected));

    let actual = crate::fetch_rows::<SimpleRow>(&client, "doesn't matter").await;
    assert_eq!(actual, expected);
}

#[tokio::test]
async fn provide() {
    test_provide().await;

    // Same but with the advanced time.
    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(100_000)).await;
    test_provide().await;
}
