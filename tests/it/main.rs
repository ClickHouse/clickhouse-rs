//! ## Integration tests
//!
//! - The `wait_end_of_query` setting that is used for all DDLs forces HTTP response buffering.
//!   We will get the response only when the DDL is executed on every cluster node.
//!   See also: https://clickhouse.com/docs/en/interfaces/http/#response-buffering
//!
//! - When tests are executed against ClickHouse Cloud, `ENGINE = MergeTree` is
//!   automatically replaced with `ENGINE = SharedMergeTree` by the server. In
//!   this case, no modifications to the DDLs are required, unlike on-premise
//!   clusters.
//!
//! - `CLICKHOUSE_TEST_ENVIRONMENT` env variable determines whether we are going
//!   to use a local ClickHouse instance in Docker (`local`, default value) or
//!   ClickHouse Cloud (`cloud`). NB: `cloud` will require one of the TLS
//!   features to be enabled.
//!
//! - ClickHouse server credentials are set via `CLICKHOUSE_CLOUD_HOST`,
//!   `CLICKHOUSE_CLOUD_USER`, and `CLICKHOUSE_CLOUD_PASSWORD`. Specific
//!   Cloud-only tests might also require JWT access token, which should be
//!   provided via `CLICKHOUSE_CLOUD_JWT_ACCESS_TOKEN`.
//!
//! - Created database names should match the following template:
//!   `chrs__{...}__{unix_millis}`. This allows to simply clean up the databases
//!   from the Cloud instance based on its creation time. See
//!   [`_priv::make_db_name`].

use clickhouse::{sql::Identifier, Client, Row};
use serde::{Deserialize, Serialize};

macro_rules! prepare_database {
    () => {
        crate::_priv::prepare_database({
            fn f() {}
            fn type_name_of_val<T>(_: T) -> &'static str {
                std::any::type_name::<T>()
            }
            type_name_of_val(f)
        })
        .await
    };
}

macro_rules! check_cloud_test_env {
    () => {
        match std::env::var("CLICKHOUSE_TEST_ENVIRONMENT") {
            Ok(test_env) if test_env == "cloud" => (),
            _ => {
                eprintln!("Skipping test as it is Cloud only");
                return;
            }
        }
    };
}

pub(crate) fn get_client() -> Client {
    let client = Client::default();
    match std::env::var("CLICKHOUSE_TEST_ENVIRONMENT") {
        Ok(test_env) if test_env == "cloud" => client
            .with_url(get_cloud_url())
            .with_user("default")
            .with_password(require_env_var("CLICKHOUSE_CLOUD_PASSWORD")),
        _ => client.with_url("http://localhost:8123"),
    }
}

pub(crate) fn require_env_var(name: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| panic!("{name} environment variable is not set"))
}

pub(crate) fn get_cloud_url() -> String {
    let hostname = require_env_var("CLICKHOUSE_CLOUD_HOST");
    format!("https://{hostname}:8443")
}

#[derive(Debug, Row, Serialize, Deserialize, PartialEq)]
struct SimpleRow {
    id: u64,
    data: String,
}

impl SimpleRow {
    fn new(id: u64, data: impl ToString) -> Self {
        Self {
            id,
            data: data.to_string(),
        }
    }
}

async fn create_simple_table(client: &Client, table_name: &str) {
    client
        .query("CREATE TABLE ?(id UInt64, data String) ENGINE = MergeTree ORDER BY id")
        .with_option("wait_end_of_query", "1")
        .bind(Identifier(table_name))
        .execute()
        .await
        .unwrap();
}

async fn fetch_rows<T>(client: &Client, table_name: &str) -> Vec<T>
where
    T: Row + for<'b> Deserialize<'b>,
{
    client
        .query("SELECT ?fields FROM ?")
        .bind(Identifier(table_name))
        .fetch_all::<T>()
        .await
        .unwrap()
}

async fn flush_query_log(client: &Client) {
    client.query("SYSTEM FLUSH LOGS").execute().await.unwrap();
}

mod chrono;
mod cloud_jwt;
mod compression;
mod cursor_error;
mod cursor_stats;
mod fetch_bytes;
mod insert;
mod inserter;
mod int128;
mod ip;
mod mock;
mod nested;
mod query;
mod time;
mod user_agent;
mod uuid;
mod variant;
mod watch;

mod _priv {
    use super::*;
    use std::time::SystemTime;

    pub(crate) async fn prepare_database(fn_path: &str) -> Client {
        let db_name = make_db_name(fn_path);
        let client = get_client();

        client
            .query("DROP DATABASE IF EXISTS ?")
            .with_option("wait_end_of_query", "1")
            .bind(Identifier(&db_name))
            .execute()
            .await
            .unwrap_or_else(|err| panic!("cannot drop db {db_name}, cause: {err}"));

        client
            .query("CREATE DATABASE ?")
            .with_option("wait_end_of_query", "1")
            .bind(Identifier(&db_name))
            .execute()
            .await
            .unwrap_or_else(|err| panic!("cannot create db {db_name}, cause: {err}"));

        println!("Created database {db_name}");
        client.with_database(db_name)
    }

    // `it::compression::lz4::{{closure}}::f` ->
    // `chrs__compression__lz4__{unix_millis}`
    fn make_db_name(fn_path: &str) -> String {
        assert!(fn_path.starts_with("it::"));
        let mut iter = fn_path.split("::").skip(1);
        let module = iter.next().unwrap();
        let test = iter.next().unwrap();
        let now_unix_millis = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_millis();
        format!("chrs__{module}__{test}__{now_unix_millis}")
    }
}
