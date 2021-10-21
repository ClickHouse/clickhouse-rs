use serde::{Deserialize, Serialize};

use clickhouse::Row;

mod common;

#[tokio::test]
async fn it_watches_changes() {
    let client = common::prepare_database("it_watches_changes").await;

    #[derive(Debug, PartialEq, Row, Serialize, Deserialize)]
    struct MyRow {
        num: u32,
    }

    // Create a table and live views.
    client
        .query(
            "
            CREATE TABLE some(num UInt32)
            ENGINE = MergeTree
            ORDER BY num
        ",
        )
        .execute()
        .await
        .unwrap();

    let mut cursor1 = client
        .watch("SELECT num FROM some ORDER BY num")
        .limit(1)
        .fetch::<MyRow>()
        .unwrap();

    let mut cursor2 = client
        .watch("SELECT sum(num) FROM some")
        .fetch::<u64>()
        .unwrap();

    // Insert first batch.
    let mut insert = client.insert("some").unwrap();
    insert.write(&MyRow { num: 1 }).await.unwrap();
    insert.write(&MyRow { num: 2 }).await.unwrap();
    insert.end().await.unwrap();

    assert_eq!(cursor1.next().await.unwrap(), Some((1, MyRow { num: 1 })));
    assert_eq!(cursor1.next().await.unwrap(), Some((1, MyRow { num: 2 })));
    assert_eq!(cursor2.next().await.unwrap(), Some((1, 3)));

    // Insert second batch.
    let mut insert = client.insert("some").unwrap();
    insert.write(&MyRow { num: 3 }).await.unwrap();
    insert.write(&MyRow { num: 4 }).await.unwrap();
    insert.end().await.unwrap();

    assert_eq!(cursor1.next().await.unwrap(), Some((2, MyRow { num: 1 })));
    assert_eq!(cursor1.next().await.unwrap(), Some((2, MyRow { num: 2 })));
    assert_eq!(cursor1.next().await.unwrap(), Some((2, MyRow { num: 3 })));
    assert_eq!(cursor1.next().await.unwrap(), Some((2, MyRow { num: 4 })));
    assert_eq!(cursor2.next().await.unwrap(), Some((2, 10)));

    // Insert third batch.
    let mut insert = client.insert("some").unwrap();
    insert.write(&MyRow { num: 5 }).await.unwrap();
    insert.write(&MyRow { num: 6 }).await.unwrap();
    insert.end().await.unwrap();

    assert_eq!(cursor1.next().await.unwrap(), None);
    assert_eq!(cursor2.next().await.unwrap(), Some((3, 21)));
}
