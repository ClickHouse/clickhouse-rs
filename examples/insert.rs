use serde::Serialize;

use ch_client::{error::Result, Client, Reflection};

#[derive(Reflection, Serialize)]
struct Row<'a> {
    no: u32,
    name: &'a str,
}

#[tokio::main]
async fn main() -> Result<()> {
    let client = Client::default().with_url("http://localhost:8123");

    let mut insert = client.insert("some")?;
    for i in 0..1000 {
        insert.write(&Row { no: i, name: "foo" }).await?;
    }

    insert.end().await?;
    Ok(())
}
