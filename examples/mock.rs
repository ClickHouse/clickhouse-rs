use clickhouse::{test, Client, Reflection};
use futures::stream;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq)]
#[derive(Serialize, Deserialize, Reflection)]
struct Row {
    no: u32,
}

async fn test_select() {
    let mock = test::Mock::new();

    mock.add(test::OnSelect::new().success(stream::iter(vec![Row { no: 1 }, Row { no: 2 }])));

    let client = Client::default().with_url(mock.url());
    let rows = client
        .query("SELECT ?fields FROM `who cares?`")
        .fetch_all::<Row>()
        .await
        .unwrap();

    assert_eq!(rows, vec![Row { no: 1 }, Row { no: 2 }]);
}

#[tokio::main]
async fn main() {
    test_select().await;
}
