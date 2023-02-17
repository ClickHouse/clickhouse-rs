#![allow(dead_code, unused_imports, unused_macros)]

use clickhouse::{sql, Client};

const HOST: &str = "localhost:8123";

macro_rules! prepare_database {
    () => {
        common::_priv::prepare_database(file!(), function_name!()).await
    };
}

pub(crate) use {function_name::named, prepare_database};
pub(crate) mod _priv {
    use super::*;

    pub async fn prepare_database(file_path: &str, fn_name: &str) -> Client {
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
