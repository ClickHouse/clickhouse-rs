use rand::random;
use serde::{Deserialize, Serialize};

use clickhouse::Row;

#[tokio::test]
async fn u128() {
    let client = prepare_database!();

    #[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Row)]
    struct MyRow {
        id: u128,
        value: String,
    }

    client
        .query(
            "
            CREATE TABLE test(
                id UInt128,
                value String,
            ) ENGINE = MergeTree ORDER BY id
        ",
        )
        .execute()
        .await
        .unwrap();

    let (id0, id1, id2) = (random(), random(), random());
    println!("ids: {id0}, {id1}, {id2}");

    let original_rows = vec![
        MyRow {
            id: id0,
            value: "test_0".to_string(),
        },
        MyRow {
            id: id1,
            value: "test_1".to_string(),
        },
        MyRow {
            id: id2,
            value: "test_2".to_string(),
        },
    ];

    let mut insert = client.insert::<MyRow>("test").unwrap();
    for row in &original_rows {
        insert.write(row).await.unwrap();
    }
    insert.end().await.unwrap();

    let rows = client
        .query("SELECT ?fields FROM test WHERE id IN ? ORDER BY value")
        .bind(vec![id0, id2])
        .fetch_all::<MyRow>()
        .await
        .unwrap();

    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0], original_rows[0]);
    assert_eq!(rows[1], original_rows[2]);
}

#[tokio::test]
async fn i128() {
    let client = prepare_database!();

    #[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Row)]
    struct MyRow {
        id: i128,
        value: String,
    }

    client
        .query(
            "
            CREATE TABLE test(
                id Int128,
                value String,
            ) ENGINE = MergeTree ORDER BY id
        ",
        )
        .execute()
        .await
        .unwrap();

    let (id0, id1, id2) = (random(), random(), random());
    println!("ids: {id0}, {id1}, {id2}");

    let original_rows = vec![
        MyRow {
            id: id0,
            value: "test_0".to_string(),
        },
        MyRow {
            id: id1,
            value: "test_1".to_string(),
        },
        MyRow {
            id: id2,
            value: "test_2".to_string(),
        },
    ];

    let mut insert = client.insert::<MyRow>("test").unwrap();
    for row in &original_rows {
        insert.write(row).await.unwrap();
    }
    insert.end().await.unwrap();

    let rows = client
        .query("SELECT ?fields FROM test WHERE id IN ? ORDER BY value")
        .bind(vec![id0, id2])
        .fetch_all::<MyRow>()
        .await
        .unwrap();

    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0], original_rows[0]);
    assert_eq!(rows[1], original_rows[2]);
}
