use crate::get_client;
use arrow::array::types::Int32Type;
use arrow::array::{PrimitiveArray, RecordBatch, StringArray, create_array};
use arrow::datatypes::{DataType, Field, Schema};
use std::sync::Arc;

use clickhouse_ext_arrow::{ArrowClientExt, ArrowQueryExt};

#[tokio::test]
async fn basic_query() {
    // `record_batch!()` sets all columns to be nullable
    let expected = RecordBatch::try_new(
        Schema::new(vec![
            Field::new("number", DataType::UInt64, false),
            Field::new("name", DataType::Utf8, false),
        ])
        .into(),
        vec![
            create_array!(UInt64, [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]),
            create_array!(
                Utf8,
                [
                    "test_0", "test_1", "test_2", "test_3", "test_4", "test_5", "test_6", "test_7",
                    "test_8", "test_9"
                ]
            ),
        ],
    )
    .unwrap();

    let client = get_client();

    let mut cursor = client
        .query("SELECT number, 'test_' || number as name FROM system.numbers LIMIT 10")
        .fetch_arrow()
        .unwrap();

    let actual = cursor.collect_merged().await.unwrap();

    assert_eq!(actual, expected);
}

#[tokio::test]
async fn insert() {
    let client = prepare_database!();

    client
        .query(
            "CREATE TABLE arrow_insert_test(bar Int32, baz String) ENGINE = MergeTree ORDER BY bar",
        )
        .execute()
        .await
        .unwrap();

    let batch_size = 100;
    let num_batches = 100;
    let mut next_id = 1..;

    let mut batches = Vec::new();
    let schema = Arc::new(Schema::new(vec![
        Field::new("bar", DataType::Int32, false),
        Field::new("baz", DataType::Utf8, false),
    ]));

    for batch in 1..=num_batches {
        let bars: PrimitiveArray<Int32Type> = (0..batch_size)
            .zip(&mut next_id)
            .map(|(_, id)| id)
            .collect();

        let bazzes: StringArray = bars
            .iter()
            .filter_map(|bar| {
                let bar = bar?;
                Some(format!("batch_{batch}_bar_{bar}"))
            })
            .collect::<Vec<String>>()
            .into();

        batches.push(
            RecordBatch::try_new(schema.clone(), vec![Arc::new(bars), Arc::new(bazzes)]).unwrap(),
        );
    }

    let mut insert = client.insert_arrow("arrow_insert_test").unwrap();

    for batch in batches {
        insert.write(&batch).await.unwrap();
    }

    insert.end().await.unwrap();

    let result = client
        .query(
            "SELECT \
         count(*) AS row_count, \
         first_value(bar) AS min_bar, \
         first_value(baz) AS min_baz,
         last_value(bar) AS max_bar, \
         last_value(baz) AS max_baz \
         FROM (SELECT * FROM arrow_insert_test ORDER BY bar)",
        )
        .fetch_arrow()
        .unwrap()
        .collect_merged()
        .await
        .unwrap();

    let expected_count = batch_size * num_batches;

    let expected = RecordBatch::try_new(
        Schema::new(vec![
            Field::new("row_count", DataType::UInt64, false),
            Field::new("min_bar", DataType::Int32, false),
            Field::new("min_baz", DataType::Utf8, false),
            Field::new("max_bar", DataType::Int32, false),
            Field::new("max_baz", DataType::Utf8, false),
        ])
        .into(),
        vec![
            create_array!(UInt64, [expected_count]),
            create_array!(Int32, [1]),
            create_array!(Utf8, ["batch_1_bar_1"]),
            create_array!(Int32, [expected_count as i32]),
            create_array!(Utf8, ["batch_100_bar_10000"]),
        ],
    )
    .unwrap();

    assert_eq!(result, expected);
}

#[tokio::test]
async fn query_empty_response() {
    let client = get_client();

    let mut cursor = client.query("SYSTEM FLUSH LOGS").fetch_arrow().unwrap();

    let batch = cursor.next().await.unwrap();
    assert_eq!(batch, None);
    assert_eq!(cursor.schema(), None);
}
