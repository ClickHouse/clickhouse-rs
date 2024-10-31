#![cfg(feature = "time")]

use clickhouse::Row;
use serde::Deserialize;
use time::Month::January;

// See also: https://clickhouse.com/docs/en/sql-reference/data-types/variant

#[tokio::test]
async fn variant_data_type() {
    let client = prepare_database!();

    // NB: Inner Variant types are _always_ sorted alphabetically,
    // and should be defined in _exactly_ the same order in the enum.
    #[derive(Debug, PartialEq, Deserialize)]
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

    #[derive(Debug, Row, Deserialize)]
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

    // Write to the table.
    client
        .query(
            "
            INSERT INTO test_var VALUES
                ([1, 2]),
                (true),
                ('2021-01-01' :: Date),
                ('foobar' :: FixedString(6)),
                (100.5 :: Float32), (200.1 :: Float64),
                (2 :: Int8), (3 :: Int16), (4 :: Int32), (5 :: Int64), (6 :: Int128),
                ('my_string' :: String),
                (7 :: UInt8), (8 :: UInt16), (9 :: UInt32), (10 :: UInt64), (11 :: UInt128)",
        )
        .execute()
        .await
        .unwrap();

    // Read from the table.
    let rows = client
        .query("SELECT ?fields FROM test_var")
        .fetch_all::<MyRow>()
        .await
        .unwrap();

    let expected = vec![
        MyRowVariant::Array(vec![1, 2]),
        MyRowVariant::Boolean(true),
        MyRowVariant::Date(time::Date::from_calendar_date(2021, January, 1).unwrap()),
        MyRowVariant::FixedString(*b"foobar"),
        MyRowVariant::Float32(100.5),
        MyRowVariant::Float64(200.1),
        MyRowVariant::Int8(2),
        MyRowVariant::Int16(3),
        MyRowVariant::Int32(4),
        MyRowVariant::Int64(5),
        MyRowVariant::Int128(6),
        MyRowVariant::String("my_string".to_string()),
        MyRowVariant::UInt8(7),
        MyRowVariant::UInt16(8),
        MyRowVariant::UInt32(9),
        MyRowVariant::UInt64(10),
        MyRowVariant::UInt128(11),
    ];

    assert_eq!(rows.len(), expected.len());
    rows.iter()
        .zip(expected.iter())
        .for_each(|(row, expected)| {
            assert_eq!(row.var, *expected);
        });
}
