use clickhouse::{sql, Client};
use function_name::named;

macro_rules! prepare_database {
    () => {
        crate::_priv::prepare_database(file!(), function_name!()).await
    };
}

mod compression;
mod cursor_error;
mod ip;
mod nested;
mod query;
mod time;
mod uuid;
mod wa_37420;
mod watch;

const HOST: &str = "localhost:8123";

mod _priv {
    use super::*;

    pub(crate) async fn prepare_database(file_path: &str, fn_name: &str) -> Client {
        let name = make_db_name(file_path, fn_name);
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

    fn make_db_name(file_path: &str, fn_name: &str) -> String {
        let (_, basename) = file_path.rsplit_once('/').expect("invalid file's path");
        let prefix = basename.strip_suffix(".rs").expect("invalid file's path");
        format!("{prefix}__{fn_name}")
    }
}
