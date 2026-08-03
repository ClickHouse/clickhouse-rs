use clickhouse::native::BlockBuilder;
use crate::get_client_with_session;

#[tokio::test]
async fn mixed_types() {
    let client = get_client_with_session();

    client.query(
        "CREATE TEMPORARY TABLE foo(
            number Int32,
            text String,
            nullable_number Nullable(UInt64),
            nullable_text Nullable(String),
            number_text_tuple (Int64, String),
            nullable_tuple Nullable((Int64, String))
        )"
    )
        .execute()
        .await
        .unwrap();

    let mut builder = BlockBuilder::new();

    let column = builder
        .upsert_column_with("number")
        .unwrap();

    column.
}
