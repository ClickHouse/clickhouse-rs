use crate::{create_simple_table, flush_query_log, SimpleRow};
use clickhouse::sql::Identifier;
use clickhouse::Client;

const PKG_VER: &str = env!("CARGO_PKG_VERSION");
const RUST_VER: &str = env!("CARGO_PKG_RUST_VERSION");
const OS: &str = std::env::consts::OS;

#[tokio::test]
async fn default_user_agent() {
    let table_name = "chrs_default_user_agent";
    let client = prepare_database!();
    let expected_user_agent = format!("clickhouse-rs/{PKG_VER} (lv:rust/{RUST_VER}, os:{OS})");
    assert_queries_user_agents(&client, table_name, &expected_user_agent).await;
}

#[tokio::test]
async fn user_agent_with_app_name() {
    let table_name = "chrs_user_agent_with_app_name";
    let client = prepare_database!().with_app_name("my-app");
    let expected_user_agent =
        format!("my-app clickhouse-rs/{PKG_VER} (lv:rust/{RUST_VER}, os:{OS})");
    assert_queries_user_agents(&client, table_name, &expected_user_agent).await;
}

async fn assert_queries_user_agents(client: &Client, table_name: &str, expected_user_agent: &str) {
    let row = SimpleRow::new(42, "foo");

    create_simple_table(client, table_name).await;

    let mut insert = client.insert(table_name).unwrap();
    insert.write(&row).await.unwrap();
    insert.end().await.unwrap();

    let rows = client
        .query("SELECT ?fields FROM ?")
        .bind(Identifier(table_name))
        .fetch_all::<SimpleRow>()
        .await
        .unwrap();

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0], row);

    flush_query_log(client).await;

    let recorded_user_agents = client
        .query(&format!(
            "
            SELECT http_user_agent
            FROM system.query_log
            WHERE type = 'QueryFinish'
            AND (
              query LIKE 'SELECT%FROM%{table_name}%'
              OR
              query LIKE 'INSERT%INTO%{table_name}%'
            )
            ORDER BY event_time_microseconds DESC
            LIMIT 2
            "
        ))
        .fetch_all::<String>()
        .await
        .unwrap();

    assert_eq!(recorded_user_agents.len(), 2);
    assert_eq!(recorded_user_agents[0], expected_user_agent);
    assert_eq!(recorded_user_agents[1], expected_user_agent);
}
