use serde::{Deserialize, Serialize};

use clickhouse::{sql, Client, Row};

const HOST: &str = "localhost:8123";

async fn prepare(name: &str) -> Client {
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

#[tokio::test]
async fn it_writes_then_reads() {
    #[derive(Debug, Row, Serialize, Deserialize)]
    struct MyRow<'a> {
        no: u32,
        name: &'a str,
    }

    let client = prepare("it_writes_then_reads").await;

    // Create a table.
    client
        .query(
            "
            CREATE TABLE some(no UInt32, name LowCardinality(String))
            ENGINE = MergeTree
            ORDER BY no
        ",
        )
        .execute()
        .await
        .expect("cannot create a table");

    // Write to the table.
    let mut insert = client.insert("some").expect("cannot insert");
    for i in 0..1000 {
        insert
            .write(&MyRow { no: i, name: "foo" })
            .await
            .expect("cannot write()");
    }

    insert.end().await.expect("cannot end()");

    // Read from the table.
    let mut cursor = client
        .query("SELECT ?fields FROM some WHERE no BETWEEN ? AND ?")
        .bind(500)
        .bind(504)
        .fetch::<MyRow<'_>>()
        .expect("cannot fetch");

    let mut i = 500;

    while let Some(row) = cursor.next().await.expect("cannot next()") {
        assert_eq!(row.no, i);
        assert_eq!(row.name, "foo");
        i += 1;
    }
}
