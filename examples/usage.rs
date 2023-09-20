use std::time::Duration;

use serde::{Deserialize, Serialize};

use clickhouse::{error::Result, sql, Client, Row};

#[derive(Debug, Row, Serialize, Deserialize)]
struct MyRow<'a> {
    no: u32,
    name: &'a str,
}

#[derive(Debug, Row, Serialize, Deserialize)]
struct MyRowOwned {
    no: u32,
    name: String,
}

async fn ddl(client: &Client) -> Result<()> {
    client.query("DROP TABLE IF EXISTS some").execute().await?;
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
}

async fn insert(client: &Client) -> Result<()> {
    let mut insert = client.insert()?;
    for i in 0..1000 {
        insert.write(&MyRow { no: i, name: "foo" }).await?;
    }

    insert.end().await
}

async fn inserter(client: &Client) -> Result<()> {
    let mut inserter = client
        .inserter()?
        .with_max_entries(100_000)
        .with_period(Some(Duration::from_secs(15)));

    for i in 0..1000 {
        if i == 500 {
            inserter.set_max_entries(300);
        }

        inserter.write(&MyRow { no: i, name: "foo" }).await?;
        inserter.commit().await?;
    }

    inserter.end().await?;
    Ok(())
}

async fn fetch(client: &Client) -> Result<()> {
    let mut cursor = client
        .query("SELECT ?fields FROM some WHERE name = ? AND no BETWEEN ? AND ?")
        .bind("foo")
        .bind(500)
        .bind(504)
        .fetch::<MyRow<'_>>()?;

    while let Some(row) = cursor.next().await? {
        println!("{row:?}");
    }

    Ok(())
}

async fn fetch_all(client: &Client) -> Result<()> {
    let vec = client
        .query("SELECT ?fields FROM ? WHERE no BETWEEN ? AND ?")
        .bind(sql::Identifier("some"))
        .bind(500)
        .bind(504)
        .fetch_all::<MyRowOwned>()
        .await?;

    println!("{vec:?}");

    Ok(())
}

async fn delete(client: &Client) -> Result<()> {
    client
        .clone()
        .with_option("mutations_sync", "1")
        .query("ALTER TABLE some DELETE WHERE no >= ?")
        .bind(500)
        .execute()
        .await?;

    Ok(())
}

async fn select_count(client: &Client) -> Result<()> {
    let count = client
        .query("SELECT count() FROM some")
        .fetch_one::<u64>()
        .await?;

    println!("count() = {count}");

    Ok(())
}

#[cfg(feature = "watch")]
async fn watch(client: &Client) -> Result<()> {
    let mut cursor = client
        .watch("SELECT max(no) no, argMax(name, some.no) name FROM some")
        .fetch::<MyRow<'_>>()?;

    let (version, row) = cursor.next().await?.unwrap();
    println!("version={version}, row={row:?}");

    let mut insert = client.insert("some")?;
    let row = MyRow {
        no: row.no + 1,
        name: "bar",
    };
    insert.write(&row).await?;
    insert.end().await?;

    let (version, row) = cursor.next().await?.unwrap();
    println!("version={version}, row={row:?}");

    // Or you can request only events without data.
    let mut cursor = client
        // It's possible to specify a view name.
        .watch("lv_f2ac5347c013c5b9a6c1aab7192dd97c2748daa0")
        .limit(10)
        .only_events()
        .fetch()?;

    println!("{:?}", cursor.next().await);

    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let client = Client::default().with_url("http://localhost:8123");

    ddl(&client).await?;
    insert(&client).await?;
    inserter(&client).await?;
    select_count(&client).await?;
    fetch(&client).await?;
    fetch_all(&client).await?;
    delete(&client).await?;
    select_count(&client).await?;

    #[cfg(feature = "watch")]
    watch(&client).await?;

    Ok(())
}
