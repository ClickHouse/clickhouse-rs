use clickhouse::{error::Result, test, Client, Row};
use futures::stream;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq)]
#[derive(Serialize, Deserialize, Row)]
struct SomeRow {
    no: u32,
}

async fn make_select(client: &Client) -> Result<Vec<SomeRow>> {
    client
        .query("SELECT ?fields FROM `who cares`")
        .fetch_all::<SomeRow>()
        .await
}

async fn make_insert(client: &Client, data: &[SomeRow]) -> Result<()> {
    let mut insert = client.insert("who cares")?;
    for row in data {
        insert.write(row).await?;
    }
    insert.end().await
}

#[tokio::main]
async fn main() {
    let mock = test::Mock::new();
    let client = Client::default().with_url(mock.url());

    let list = vec![SomeRow { no: 1 }, SomeRow { no: 2 }];

    // How to test SELECTs.
    mock.add(test::handlers::provide(stream::iter(list.clone())));
    let rows = make_select(&client).await.unwrap();
    assert_eq!(rows, list);

    // How to test failures.
    mock.add(test::handlers::failure(test::status::FORBIDDEN));
    let reason = make_select(&client).await;
    assert_eq!(format!("{:?}", reason), r#"Err(BadResponse("Forbidden"))"#);

    // How to test INSERTs.
    let recording = mock.add(test::handlers::record());
    make_insert(&client, &list).await.unwrap();

    let rows: Vec<SomeRow> = recording.collect().await;
    assert_eq!(rows, list);
}
