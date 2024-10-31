#![cfg(feature = "time")]

use serde::{Deserialize, Serialize};
use time::Month::January;

use clickhouse::Row;

// See also: https://clickhouse.com/docs/en/sql-reference/data-types/variant

#[tokio::test]
async fn variant_data_type() {
    let client = prepare_database!();

    // NB: Inner Variant types are _always_ sorted alphabetically,
    // and should be defined in _exactly_ the same order in the enum.
    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    enum MyRowVariant {
        Array(Vec<i16>),
        Boolean(bool),
        // attributes should work in this case, too
        #[serde(with = "clickhouse::serde::time::date")]
        Date(time::Date),
        FixedString([u8; 6]),
        Float32(f32),
        Float64(f64),
        Int128(i128),
        Int16(i16),
        Int32(i32),
        Int64(i64),
        Int8(i8),
        String(String),
        UInt128(u128),
        UInt16(i16),
        UInt32(u32),
        UInt64(u64),
        UInt8(i8),
    }

    #[derive(Debug, PartialEq, Row, Serialize, Deserialize)]
    struct MyRow {
        var: MyRowVariant,
    }

    // No matter the order of the definition on the Variant types, it will always be sorted as follows:
    // Variant(Array(UInt16), Bool, FixedString(6), Float32, Float64, Int128, Int16, Int32, Int64, Int8, String, UInt128, UInt16, UInt32, UInt64, UInt8)
    client
        .query(
            "
            CREATE OR REPLACE TABLE test_var
            (
                `var` Variant(
                    Array(UInt16),
                    Bool,
                    Date,
                    FixedString(6),
                    Float32, Float64,
                    Int128, Int16, Int32, Int64, Int8,
                    String,
                    UInt128, UInt16, UInt32, UInt64, UInt8
                )
            )
            ENGINE = MergeTree
            ORDER BY ()",
        )
        .with_option("allow_experimental_variant_type", "1")
        .with_option("allow_suspicious_variant_types", "1")
        .execute()
        .await
        .unwrap();

    let rows = vec![
        MyRow {
            var: MyRowVariant::Array(vec![1, 2]),
        },
        MyRow {
            var: MyRowVariant::Boolean(true),
        },
        MyRow {
            var: MyRowVariant::Date(time::Date::from_calendar_date(2021, January, 1).unwrap()),
        },
        MyRow {
            var: MyRowVariant::FixedString(*b"foobar"),
        },
        MyRow {
            var: MyRowVariant::Float32(100.5),
        },
        MyRow {
            var: MyRowVariant::Float64(200.1),
        },
        MyRow {
            var: MyRowVariant::Int8(2),
        },
        MyRow {
            var: MyRowVariant::Int16(3),
        },
        MyRow {
            var: MyRowVariant::Int32(4),
        },
        MyRow {
            var: MyRowVariant::Int64(5),
        },
        MyRow {
            var: MyRowVariant::Int128(6),
        },
        MyRow {
            var: MyRowVariant::String("my_string".to_string()),
        },
        MyRow {
            var: MyRowVariant::UInt8(7),
        },
        MyRow {
            var: MyRowVariant::UInt16(8),
        },
        MyRow {
            var: MyRowVariant::UInt32(9),
        },
        MyRow {
            var: MyRowVariant::UInt64(10),
        },
        MyRow {
            var: MyRowVariant::UInt128(11),
        },
    ];

    // Write to the table.
    let mut insert = client.insert("test_var").unwrap();
    for row in &rows {
        insert.write(row).await.unwrap();
    }
    insert.end().await.unwrap();

    // Read from the table.
    let result_rows = client
        .query("SELECT ?fields FROM test_var")
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
