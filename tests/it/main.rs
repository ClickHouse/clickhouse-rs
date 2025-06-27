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

macro_rules! assert_panic_on_fetch_with_client {
    ($client:ident, $msg_parts:expr, $query:expr) => {
        use futures::FutureExt;
        let async_panic =
            std::panic::AssertUnwindSafe(async { $client.query($query).fetch_all::<Data>().await });
        let result = async_panic.catch_unwind().await;
        assert!(result.is_err());
        let panic_msg = *result.unwrap_err().downcast::<String>().unwrap();
        for &msg in $msg_parts {
            assert!(
                panic_msg.contains(msg),
                "panic message:\n{panic_msg}\ndid not contain the expected part:\n{msg}"
            );
        }
    };
}

macro_rules! assert_panic_on_fetch {
    ($msg_parts:expr, $query:expr) => {
        use futures::FutureExt;
        let client = get_client();
        let async_panic =
            std::panic::AssertUnwindSafe(async { client.query($query).fetch_all::<Data>().await });
        let result = async_panic.catch_unwind().await;
        assert!(
            result.is_err(),
            "expected a panic, but got a result instead: {:?}",
            result.unwrap()
        );
        let panic_msg = *result.unwrap_err().downcast::<String>().unwrap();
        for &msg in $msg_parts {
            assert!(
                panic_msg.contains(msg),
                "panic message:\n{panic_msg}\ndid not contain the expected part:\n{msg}"
            );
        }
    };
}

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
        if !crate::is_cloud_test_env() {
            eprintln!("Skipping test as it is not Cloud only");
            return;
        }
    };
}

pub(crate) fn get_client() -> Client {
    let client = Client::default();
    if is_cloud_test_env() {
        client
            .with_url(get_cloud_url())
            .with_user("default")
            .with_password(require_env_var("CLICKHOUSE_CLOUD_PASSWORD"))
    } else {
        client.with_url("http://localhost:8123")
    }
}

pub(crate) fn is_cloud_test_env() -> bool {
    matches!(std::env::var("CLICKHOUSE_TEST_ENVIRONMENT"), Ok(test_env) if test_env == "cloud")
}

pub(crate) fn require_env_var(name: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| panic!("{name} environment variable is not set"))
}

pub(crate) fn get_cloud_url() -> String {
    let hostname = require_env_var("CLICKHOUSE_CLOUD_HOST");
    format!("https://{hostname}:8443")
}

#[derive(Clone, Debug, Row, Serialize, Deserialize, PartialEq)]
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

pub(crate) async fn create_simple_table(client: &Client, table_name: &str) {
    client
        .query("CREATE TABLE ?(id UInt64, data String) ENGINE = MergeTree ORDER BY id")
        .with_option("wait_end_of_query", "1")
        .bind(Identifier(table_name))
        .execute()
        .await
        .unwrap();
}

pub(crate) async fn fetch_rows<T>(client: &Client, table_name: &str) -> Vec<T>
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

pub(crate) async fn flush_query_log(client: &Client) {
    client.query("SYSTEM FLUSH LOGS").execute().await.unwrap();
}

pub(crate) async fn execute_statements(client: &Client, statements: &[&str]) {
    for statement in statements {
        client
            .query(statement)
            .with_option("wait_end_of_query", "1")
            .execute()
            .await
            .unwrap_or_else(|err| panic!("cannot execute statement '{statement}', cause: {err}"));
    }
}

pub(crate) async fn insert_and_select<T>(
    client: &Client,
    table_name: &str,
    data: impl IntoIterator<Item = T>,
) -> Vec<T>
where
    T: Row + Serialize + for<'de> Deserialize<'de>,
{
    let mut insert = client.insert(table_name).await.unwrap();
    for row in data.into_iter() {
        insert.write(&row).await.unwrap();
    }
    insert.end().await.unwrap();

    client
        .query("SELECT ?fields FROM ? ORDER BY () ASC")
        .bind(Identifier(table_name))
        .fetch_all::<T>()
        .await
        .unwrap()
}

pub(crate) mod geo_types {
    // See https://clickhouse.com/docs/en/sql-reference/data-types/geo
    pub(crate) type Point = (f64, f64);
    pub(crate) type Ring = Vec<Point>;
    pub(crate) type Polygon = Vec<Ring>;
    pub(crate) type MultiPolygon = Vec<Polygon>;
    pub(crate) type LineString = Vec<Point>;
    pub(crate) type MultiLineString = Vec<LineString>;
}

pub(crate) mod decimals {
    use fixnum::typenum::{U12, U4, U8};
    use fixnum::FixedPoint;

    // See ClickHouse decimal sizes: https://clickhouse.com/docs/en/sql-reference/data-types/decimal
    pub(crate) type Decimal32 = FixedPoint<i32, U4>; // Decimal(9, 4) = Decimal32(4)
    pub(crate) type Decimal64 = FixedPoint<i64, U8>; // Decimal(18, 8) = Decimal64(8)
    pub(crate) type Decimal128 = FixedPoint<i128, U12>; // Decimal(38, 12) = Decimal128(12)
}

mod chrono;
mod cloud_jwt;
mod compression;
mod cursor_error;
mod cursor_stats;
mod fetch_bytes;
mod https_errors;
mod insert;
mod inserter;
mod int128;
mod ip;
mod mock;
mod nested;
mod query;
mod rbwnat_header;
mod rbwnat_smoke;
mod rbwnat_validation;
mod time;
mod user_agent;
mod uuid;
mod variant;

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

    /// `it::compression::lz4::{{closure}}::f` is transformed to:
    /// * For tests that use local CH instance: `chrs__compression__lz4`
    /// * For Cloud tests (e.g., JWT):          `chrs__compression__lz4__{unix_millis}`
    fn make_db_name(fn_path: &str) -> String {
        assert!(fn_path.starts_with("it::"));
        let mut iter = fn_path.split("::").skip(1);
        let module = iter.next().unwrap();
        let test = iter.next().unwrap();
        if is_cloud_test_env() {
            let now_unix_millis = SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_millis();
            format!("chrs__{module}__{test}__{now_unix_millis}")
        } else {
            format!("chrs__{module}__{test}")
        }
    }
}
