#![cfg(feature = "test-util")]

use crate::SimpleRow;
use clickhouse::{test, Client};
use std::time::Duration;

async fn test_provide() {
    let mock = test::Mock::new();
    let client = Client::default()
        .with_url(mock.url())
        .with_disabled_validation();
    let expected = vec![SimpleRow::new(1, "one"), SimpleRow::new(2, "two")];

    mock.add(test::handlers::provide(&expected));

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
