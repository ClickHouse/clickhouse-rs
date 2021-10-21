use clickhouse::{sql, Client};

const HOST: &str = "localhost:8123";

pub async fn prepare_database(name: &str) -> Client {
    let client = Client::default().with_url(format!("http://{}", HOST));

    client
        .query("DROP DATABASE IF EXISTS ?")
        .bind(sql::Identifier(name))
        .execute()
        .await
        .expect("cannot drop db");

    client
        .query("CREATE DATABASE ?")
        .bind(sql::Identifier(name))
        .execute()
        .await
        .expect("cannot create db");

    client.with_database(String::from(name))
}
