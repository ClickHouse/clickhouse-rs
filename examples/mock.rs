use clickhouse::{error::Result, test, Client, Reflection};
use futures::stream;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq)]
#[derive(Serialize, Deserialize, Reflection)]
struct Row {
    no: u32,
}

async fn make_request(client: &Client) -> Result<Vec<Row>> {
    client
        .query("SELECT ?fields FROM `who cares`")
        .fetch_all::<Row>()
        .await
}

async fn test_select() {
    let mock = test::Mock::new();
    let client = Client::default().with_url(mock.url());

    let list = vec![Row { no: 1 }, Row { no: 2 }];
    mock.add(test::OnSelect::new().success(stream::iter(list.clone())));
    let rows = make_request(&client).await.unwrap();
    assert_eq!(rows, list);

    mock.add(test::OnSelect::new().failure(test::status::FORBIDDEN));
    let reason = make_request(&client).await;
    assert_eq!(format!("{:?}", reason), r#"Err(BadResponse("Forbidden"))"#);
}

#[tokio::main]
async fn main() {
    test_select().await;
}
