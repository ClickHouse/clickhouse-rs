use serde::{Deserialize, Serialize};

use ch_client::{error::Result, Client, Reflection};

#[derive(Debug, Reflection, Serialize, Deserialize)]
struct Row<'a> {
    no: u32,
    name: &'a str,
}

async fn insert(client: &Client) -> Result<()> {
    let mut insert = client.insert("some")?;
    for i in 0..1000 {
        insert.write(&Row { no: i, name: "foo" }).await?;
    }

    insert.end().await?;
    Ok(())
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

#[tokio::main]
async fn main() -> Result<()> {
    let client = Client::default().with_url("http://localhost:8123");

    insert(&client).await?;
    select(&client).await?;

    Ok(())
}
