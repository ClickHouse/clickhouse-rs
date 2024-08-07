use clickhouse::sql::Identifier;
use clickhouse::Client;
use clickhouse_derive::Row;
use serde::{Deserialize, Serialize};
use std::env;

// This example requires three environment variables with your instance credentials to be set
//
// - CLICKHOUSE_URL (e.g., https://myservice.clickhouse.cloud:8443)
// - CLICKHOUSE_USER
// - CLICKHOUSE_PASSWORD
//
// Additionally, `rustls-tls` or `native-tls` cargo features should be enabled.

async fn cloud_usage() -> clickhouse::error::Result<()> {
    let table_name = "chrs_cloud";

    let client = Client::default()
        .with_url(read_env_var(CLICKHOUSE_URL))
        .with_user(read_env_var(CLICKHOUSE_USER))
        .with_password(read_env_var(CLICKHOUSE_PASSWORD));

    // `wait_end_of_query` is required in this case, as we want these DDLs to be executed
    // on the entire Cloud cluster before we receive the response.
    // See https://clickhouse.com/docs/en/interfaces/http/#response-buffering
    client
        .query("DROP TABLE IF EXISTS ?")
        .bind(Identifier(table_name))
        .with_option("wait_end_of_query", "1")
        .execute()
        .await?;

    // Note that you could just use MergeTree with CH Cloud, and omit the `ON CLUSTER` clause.
    // The same applies to other engines as well;
    // e.g., ReplacingMergeTree will become SharedReplacingMergeTree and so on.
    // See https://clickhouse.com/docs/en/cloud/reference/shared-merge-tree#enabling-sharedmergetree
    client
        .query("CREATE TABLE ? (id Int32, name String) ENGINE MergeTree ORDER BY id")
        .bind(Identifier(table_name))
        .with_option("wait_end_of_query", "1")
        .execute()
        .await?;

    let mut insert = client.insert(table_name)?;
    insert
        .write(&Data {
            id: 42,
            name: "foo".into(),
        })
        .await?;
    insert.end().await?;

    let data = client
        .query("SELECT ?fields FROM ?")
        .bind(Identifier(table_name))
        // This setting is optional; use it when you need strong consistency guarantees on the reads
        // See https://clickhouse.com/docs/en/cloud/reference/shared-merge-tree#consistency
        .with_option("select_sequential_consistency", "1")
        .fetch_all::<Data>()
        .await?;

    println!("Stored data: {data:?}");
    Ok(())
}

#[tokio::main]
async fn main() -> clickhouse::error::Result<()> {
    if cfg!(any(feature = "rustls-tls", feature = "native-tls")) {
        cloud_usage().await?;
    } else {
        println!("This example requires either `rustls-tls` or `native-tls` cargo features")
    }
    Ok(())
}

#[derive(Debug, Serialize, Deserialize, Row)]
struct Data {
    id: u32,
    name: String,
}

const CLICKHOUSE_URL: &str = "CLICKHOUSE_URL";
const CLICKHOUSE_USER: &str = "CLICKHOUSE_USER";
const CLICKHOUSE_PASSWORD: &str = "CLICKHOUSE_PASSWORD";

fn read_env_var(key: &str) -> String {
    env::var(key).expect(&format!("{key} env variable should be set"))
}
