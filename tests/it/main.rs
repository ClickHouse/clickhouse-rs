use clickhouse::{sql, sql::Identifier, Client, Row};
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
mod compression;
mod cursor_error;
mod cursor_stats;
mod insert;
mod inserter;
mod ip;
mod mock;
mod nested;
mod query;
mod time;
mod user_agent;
mod uuid;
mod variant;
mod watch;

const HOST: &str = "localhost:8123";

mod _priv {
    use super::*;

    pub(crate) async fn prepare_database(fn_path: &str) -> Client {
        let name = make_db_name(fn_path);
        let client = Client::default().with_url(format!("http://{HOST}"));

        client
            .query("DROP DATABASE IF EXISTS ?")
            .bind(sql::Identifier(&name))
            .execute()
            .await
            .expect("cannot drop db");

        client
            .query("CREATE DATABASE ?")
            .bind(sql::Identifier(&name))
            .execute()
            .await
            .expect("cannot create db");

        client.with_database(name)
    }

    // `it::compression::lz4::{{closure}}::f` -> `chrs__compression__lz4`
    fn make_db_name(fn_path: &str) -> String {
        assert!(fn_path.starts_with("it::"));
        let mut iter = fn_path.split("::").skip(1);
        let module = iter.next().unwrap();
        let test = iter.next().unwrap();
        format!("chrs__{module}__{test}")
    }
}
