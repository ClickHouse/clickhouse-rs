// https://github.com/ClickHouse/clickhouse-rs/issues/392
// Variant is implicitly nullable: 0xFF discriminator = NULL.

use serde::{Deserialize, Serialize};

use clickhouse::Row;

#[tokio::test]
async fn variant_null_values() {
    let client = prepare_database!();

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    enum SimpleVariant {
        String(String),
        UInt64(u64),
    }

    #[derive(Debug, PartialEq, Row, Serialize, Deserialize)]
    struct MyRow {
        id: u32,
        var: Option<SimpleVariant>,
    }

    client
        .query(
            "
            CREATE OR REPLACE TABLE test_var_null
            (
                `id` UInt32,
                `var` Variant(String, UInt64)
            )
            ENGINE = MergeTree
            ORDER BY id",
        )
        .with_option("allow_experimental_variant_type", "1")
        .execute()
        .await
        .unwrap();

    // insert via SQL so we get actual NULLs in the Variant column
    client
        .query("INSERT INTO test_var_null VALUES (1, 'hello'), (2, NULL), (3, 42), (4, NULL)")
        .execute()
        .await
        .unwrap();

    let rows = client
        .query("SELECT ?fields FROM test_var_null ORDER BY id")
        .fetch_all::<MyRow>()
        .await
        .unwrap();

    assert_eq!(
        rows,
        vec![
            MyRow {
                id: 1,
                var: Some(SimpleVariant::String("hello".to_string())),
            },
            MyRow { id: 2, var: None },
            MyRow {
                id: 3,
                var: Some(SimpleVariant::UInt64(42)),
            },
            MyRow { id: 4, var: None },
        ]
    );
}

// verifies that a NULL Variant value can be read without a table,
// using an inline cast expression (e.g. `NULL::Variant(String, UInt64)`).
#[tokio::test]
async fn variant_null_cast() {
    let client = prepare_database!();

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    enum SimpleVariant {
        String(String),
        UInt64(u64),
    }

    #[derive(Debug, PartialEq, Row, Deserialize)]
    struct Wrapper {
        #[serde(rename = "v")]
        v: Option<SimpleVariant>,
    }

    let result = client
        .query("SELECT NULL::Variant(String, UInt64) AS v")
        .fetch_one::<Wrapper>()
        .await
        .unwrap();

    assert_eq!(result.v, None);
}
