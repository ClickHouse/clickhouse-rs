use clickhouse::{error::Result, test, Client, Row};
use futures::stream;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq)]
#[derive(Serialize, Deserialize, Row)]
struct SomeRow {
    no: u32,
}

async fn make_create(client: &Client) -> Result<()> {
    client.query("CREATE TABLE test").execute().await
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

#[cfg(feature = "watch")]
async fn make_watch(client: &Client) -> Result<(u64, SomeRow)> {
    client
        .watch("SELECT max(no) no FROM test")
        .fetch_one::<SomeRow>()
        .await
}

#[cfg(feature = "watch")]
async fn make_watch_only_events(client: &Client) -> Result<u64> {
    client
        .watch("SELECT max(no) no FROM test")
        .only_events()
        .fetch_one()
        .await
}

#[tokio::main]
async fn main() {
    let mock = test::Mock::new();
    let client = Client::default().with_url(mock.url());
    let list = vec![SomeRow { no: 1 }, SomeRow { no: 2 }];

    // How to test DDL.
    let recording = mock.add(test::handlers::record_ddl());
    make_create(&client).await.unwrap();
    assert!(recording.query().await.contains("CREATE TABLE"));

    // How to test SELECT.
    mock.add(test::handlers::provide(stream::iter(list.clone())));
    let rows = make_select(&client).await.unwrap();
    assert_eq!(rows, list);

    // How to test failures.
    mock.add(test::handlers::failure(test::status::FORBIDDEN));
    let reason = make_select(&client).await;
    assert_eq!(format!("{reason:?}"), r#"Err(BadResponse("Forbidden"))"#);

    // How to test INSERT.
    let recording = mock.add(test::handlers::record());
    make_insert(&client, &list).await.unwrap();
    let rows: Vec<SomeRow> = recording.collect().await;
    assert_eq!(rows, list);

    // How to test WATCH.
    #[cfg(feature = "watch")]
    {
        // Check `CREATE LIVE VIEW` (for `watch(query)` case only).
        let recording = mock.add(test::handlers::record_ddl());
        mock.add(test::handlers::watch(stream::iter(
            list.into_iter().map(|row| (42, row)),
        )));
        let (version, row) = make_watch(&client).await.unwrap();
        assert!(recording.query().await.contains("CREATE LIVE VIEW"));
        assert_eq!(version, 42);
        assert_eq!(row, SomeRow { no: 1 });

        // `EVENTS`.
        let recording = mock.add(test::handlers::record_ddl());
        mock.add(test::handlers::watch_only_events(stream::iter(3..5)));
        let version = make_watch_only_events(&client).await.unwrap();
        assert!(recording.query().await.contains("CREATE LIVE VIEW"));
        assert_eq!(version, 3);
    }
}
