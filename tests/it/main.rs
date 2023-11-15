use clickhouse::{sql, Client};

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

mod compression;
mod cursor_error;
mod inserter;
mod ip;
mod nested;
mod query;
mod time;
mod uuid;
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
