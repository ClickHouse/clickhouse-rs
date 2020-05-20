use serde::{Deserialize, Serialize};

use ch_client::{error::Result, Client, Reflection};

#[derive(Debug, Reflection, Serialize, Deserialize)]
struct Row<'a> {
    no: u32,
    name: &'a str,
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
    let mut insert = client.insert("some")?;
    for i in 0..1000 {
        insert.write(&Row { no: i, name: "foo" }).await?;
    }

    insert.end().await
}

async fn select(client: &Client) -> Result<()> {
    let mut cursor = client
        .query("SELECT ?fields FROM some WHERE no BETWEEN ? AND ?")
        .bind(500)
        .bind(504)
        .fetch::<Row<'_>>()?;

    while let Some(row) = cursor.next().await? {
        println!("{:?}", row);
    }

    Ok(())
}

async fn delete(client: &Client) -> Result<()> {
    client
        .query("ALTER TABLE some DELETE WHERE no >= ?")
        .bind(500)
        .execute()
        .await?;

    // Mutations are async, so we are waiting for some time.
    tokio::time::delay_for(tokio::time::Duration::from_millis(100)).await;
    Ok(())
}

async fn select_count(client: &Client) -> Result<()> {
    #[derive(Reflection, Deserialize)]
    struct Row {
        count: u64,
    }

    let mut cursor = client.query("SELECT count() FROM some").fetch::<Row>()?;
    let row = cursor.next().await?.unwrap();
    println!("count() = {}", row.count);

    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let client = Client::default().with_url("http://localhost:8123");

    ddl(&client).await?;
    insert(&client).await?;
    select_count(&client).await?;
    select(&client).await?;
    delete(&client).await?;
    select_count(&client).await?;

    Ok(())
}
