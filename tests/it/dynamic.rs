use serde::{Deserialize, Serialize};

use clickhouse::dynamic::Dynamic;
use clickhouse::Row;

// See also: https://clickhouse.com/docs/en/sql-reference/data-types/dynamic

#[tokio::test]
async fn dynamic_data_type() {
    let client = prepare_database!().with_option("allow_experimental_dynamic_type", "1");

    client
        .query(
            "
            CREATE OR REPLACE TABLE test_dyn
            (id UInt64, data Dynamic)
            ENGINE = MergeTree
            ORDER BY ()",
        )
        .execute()
        .await
        .unwrap();

    let rows = get_rows();
    let mut insert = client.insert("test_dyn").unwrap();
    for row in &rows {
        insert.write(row).await.unwrap();
    }
    insert.end().await.unwrap();

    // Read from the table.
    let result_rows = client
        .query("SELECT ?fields FROM test_dyn")
        .fetch_all::<MyRow>()
        .await
        .unwrap();

    assert_eq!(result_rows.len(), rows.len());
    rows.iter()
        .zip(result_rows.iter())
        .for_each(|(row, result_row)| {
            assert_eq!(row, result_row);
        });
}

#[derive(Debug, PartialEq, Row, Serialize, Deserialize)]
struct MyRow {
    id: u64,
    data: Dynamic,
}

fn get_rows() -> Vec<MyRow> {
    vec![
        MyRow {
            id: 0,
            data: Dynamic::Boolean(true),
        },
        MyRow {
            id: 1,
            data: Dynamic::Int8(1),
        },
        // MyRow {
        //     id: 2,
        //     data: Dynamic::Int16(2),
        // },
        // MyRow {
        //     id: 3,
        //     data: Dynamic::Int32(3),
        // },
        // MyRow {
        //     id: 4,
        //     data: Dynamic::Int64(4),
        // },
        // MyRow {
        //     id: 5,
        //     data: Dynamic::Int128(5),
        // },
        // MyRow {
        //     id: 6,
        //     data: Dynamic::UInt8(6),
        // },
        // MyRow {
        //     id: 7,
        //     data: Dynamic::UInt16(7),
        // },
        // MyRow {
        //     id: 8,
        //     data: Dynamic::UInt32(8),
        // },
        // MyRow {
        //     id: 9,
        //     data: Dynamic::UInt64(9),
        // },
        // MyRow {
        //     id: 10,
        //     data: Dynamic::UInt128(10),
        // },
        // MyRow {
        //     id: 11,
        //     data: Dynamic::Float32(100.5),
        // },
        // MyRow {
        //     id: 12,
        //     data: Dynamic::Float64(200.1),
        // },
        MyRow {
            id: 13,
            data: Dynamic::String("foobar".to_string()),
        },
        // MyRow {
        //     id: 14,
        //     data: Dynamic::Nullable(None),
        // }, MyRow {
        //     id: 15,
        //     data: Dynamic::Nullable(Some(Box::new(Dynamic::Int8(15)))),
        // }, MyRow {
        //     id: 16,
        //     data: Dynamic::Nullable(Some(Box::new(Dynamic::String("baz".to_string())))),
        // },
        // MyRow {
        //     id: 17,
        //     data: Dynamic::Array(vec![Dynamic::Int8(1), Dynamic::Int8(2), Dynamic::Int8(3)]),
        // },
    ]
}
