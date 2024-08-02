use crate::{create_simple_table, fetch_simple_rows, SimpleRow};

#[tokio::test]
async fn settings_override() {
    let table_name = "insert_settings";
    let query_id = uuid::Uuid::new_v4().to_string();
    let client = prepare_database!();
    create_simple_table(&client, table_name).await;

    let row = SimpleRow::new(42, "foo");

    let mut insert = client
        .insert(table_name)
        .unwrap()
        .with_option("async_insert", "1")
        .with_option("query_id", &query_id);

    insert.write(&row).await.unwrap();
    insert.end().await.unwrap();

    // flush query_log
    client.query("SYSTEM FLUSH LOGS").execute().await.unwrap();

    let result = client
        .query(
            "
                SELECT Settings['async_insert'] = '1'
                FROM system.query_log
                WHERE query_id = ?
                AND type = 'QueryFinish'
                AND query_kind = 'Insert'
                ",
        )
        .bind(&query_id)
        .fetch_one::<bool>()
        .await;

    assert!(
        result.unwrap(),
        "INSERT statement settings should contain async_insert = 1"
    );

    let rows = fetch_simple_rows(&client, table_name).await;
    assert_eq!(rows, vec!(row))
}
