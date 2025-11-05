use crate::get_client;
use bstr::ByteSlice;
use clickhouse::query::{BytesCursor, Query};
use std::str::from_utf8;

#[tokio::test]
async fn fetch_one() {
    let client = &get_client();
    let run_query = async |query: Query| -> String { query.fetch_one().await.unwrap() };

    let query = select_readonly_setting_query(client);
    let initial_readonly_value = run_query(query).await;
    assert_eq!(
        initial_readonly_value, "1",
        "initial `fetch_one` readonly setting value should be 1"
    );

    let query = select_readonly_setting_query(client).with_option("readonly", "0");
    let disabled_readonly_value = run_query(query).await;
    assert_eq!(
        disabled_readonly_value, "0",
        "`fetch_one` modified readonly setting value should be 0"
    );

    let query = select_readonly_setting_query(client).with_option("readonly", "1");
    let same_readonly_value = run_query(query).await;
    assert_eq!(
        same_readonly_value, "1",
        "`fetch_one` should allow setting the same readonly setting value"
    );
}

#[tokio::test]
async fn fetch_all() {
    let client = &get_client();
    let run_query = async |query: Query| -> Vec<String> { query.fetch_all().await.unwrap() };

    let query = select_readonly_setting_query(client);
    let initial_readonly_value = run_query(query).await;
    assert_eq!(
        initial_readonly_value,
        vec!["1"],
        "initial `fetch_all` readonly setting value should be 1"
    );

    let query = select_readonly_setting_query(client).with_option("readonly", "0");
    let disabled_readonly_value = run_query(query).await;
    assert_eq!(
        disabled_readonly_value,
        vec!["0"],
        "`fetch_all` modified readonly setting value should be 0"
    );

    let query = select_readonly_setting_query(client).with_option("readonly", "1");
    let same_readonly_value = run_query(query).await;
    assert_eq!(
        same_readonly_value,
        vec!["1"],
        "`fetch_all` should allow setting the same readonly setting value"
    );
}

#[tokio::test]
async fn fetch_optional() {
    let client = &get_client();
    let run_query =
        async |query: Query| -> Option<String> { query.fetch_optional().await.unwrap() };

    let query = select_readonly_setting_query(client);
    let initial_readonly_value = run_query(query).await;
    assert_eq!(
        initial_readonly_value.as_deref(),
        Some("1"),
        "initial `fetch_optional` readonly setting value should be 1"
    );

    let query = select_readonly_setting_query(client).with_option("readonly", "0");
    let disabled_readonly_value = run_query(query).await;
    assert_eq!(
        disabled_readonly_value.as_deref(),
        Some("0"),
        "`fetch_optional` modified readonly setting value should be 0"
    );

    let query = select_readonly_setting_query(client).with_option("readonly", "1");
    let same_readonly_value = run_query(query).await;
    assert_eq!(
        same_readonly_value.as_deref(),
        Some("1"),
        "`fetch_optional` should allow setting the same readonly setting value"
    );
}

#[tokio::test]
async fn fetch_bytes() {
    let client = &get_client();
    let run_query = async |query: Query| -> Vec<u8> {
        let bytes = query
            .fetch_bytes("LineAsString")
            .unwrap()
            .collect()
            .await
            .unwrap();
        bytes.to_vec()
    };

    let query = select_readonly_setting_query(client);
    let initial_readonly_value = run_query(query).await;
    assert_eq!(
        initial_readonly_value, b"1\n",
        "initial `fetch_bytes` readonly setting value should be 1"
    );

    let query = select_readonly_setting_query(client).with_option("readonly", "0");
    let disabled_readonly_value = run_query(query).await;
    assert_eq!(
        disabled_readonly_value, b"0\n",
        "`fetch_bytes` modified readonly setting value should be 0"
    );

    let query = select_readonly_setting_query(client).with_option("readonly", "1");
    let same_readonly_value = run_query(query).await;
    assert_eq!(
        same_readonly_value, b"1\n",
        "`fetch_bytes` should allow setting the same readonly setting value"
    );
}

#[tokio::test]
async fn fetch() {
    #[derive(clickhouse::Row, serde::Deserialize)]
    struct SystemSettingsRow {
        value: String,
    }

    let client = &get_client();
    let run_query = async |query: Query| -> SystemSettingsRow {
        let mut cursor = query.fetch::<SystemSettingsRow>().unwrap();

        let row = cursor.next().await.unwrap();
        assert!(row.is_some());

        let row = row.unwrap();
        assert!(
            cursor.next().await.unwrap().is_none(),
            "expected only one row"
        );
        row
    };

    let query = select_readonly_setting_query(client);
    let initial_readonly_row = run_query(query).await;
    assert_eq!(
        initial_readonly_row.value, "1",
        "initial `fetch` readonly setting value should be 1"
    );

    let query = select_readonly_setting_query(client).with_option("readonly", "0");
    let disabled_readonly_row = run_query(query).await;
    assert_eq!(
        disabled_readonly_row.value, "0",
        "`fetch` modified readonly setting value should be 0"
    );

    let query = select_readonly_setting_query(client).with_option("readonly", "1");
    let same_readonly_row = run_query(query).await;
    assert_eq!(
        same_readonly_row.value, "1",
        "`fetch` should allow setting the same readonly setting value"
    );
}

fn select_readonly_setting_query(client: &clickhouse::Client) -> Query {
    client.query("SELECT value FROM system.settings WHERE name = 'readonly'")
}
