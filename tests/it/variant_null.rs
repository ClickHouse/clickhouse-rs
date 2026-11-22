// https://github.com/ClickHouse/clickhouse-rs/issues/392
// Variant is implicitly nullable: 0xFF discriminator = NULL.

use serde::{Deserialize, Serialize};

use clickhouse::{Client, Row};

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
        .with_setting("allow_experimental_variant_type", "1")
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
        .with_setting("allow_experimental_variant_type", "1")
        .fetch_one::<Wrapper>()
        .await
        .unwrap();

    assert_eq!(result.v, None);
}

/// Regression test for nested Variant inside an Array.
/// Before the inner-validator `null_encoding` impl, `Vec<Option<MyVariant>>`
/// over `Array(Variant(...))` would consume a discriminator byte as a
/// Nullable flag and corrupt the input stream.
#[tokio::test]
async fn variant_null_in_array() {
    let client = prepare_database!();

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    enum SimpleVariant {
        String(String),
        UInt64(u64),
    }

    #[derive(Debug, PartialEq, Row, Deserialize)]
    struct MyRow {
        id: u32,
        vars: Vec<Option<SimpleVariant>>,
    }

    client
        .query(
            "
            CREATE OR REPLACE TABLE test_var_null_array
            (
                `id` UInt32,
                `vars` Array(Variant(String, UInt64))
            )
            ENGINE = MergeTree
            ORDER BY id",
        )
        .with_setting("allow_experimental_variant_type", "1")
        .execute()
        .await
        .unwrap();

    // Mix NULL and non-NULL discriminators in the same array so the
    // deserializer has to switch paths element-by-element.
    client
        .query(
            "INSERT INTO test_var_null_array VALUES \
             (1, ['hello', NULL, 42, NULL, 'world'])",
        )
        .execute()
        .await
        .unwrap();

    let rows = client
        .query("SELECT ?fields FROM test_var_null_array ORDER BY id")
        .fetch_all::<MyRow>()
        .await
        .unwrap();

    assert_eq!(
        rows,
        vec![MyRow {
            id: 1,
            vars: vec![
                Some(SimpleVariant::String("hello".to_string())),
                None,
                Some(SimpleVariant::UInt64(42)),
                None,
                Some(SimpleVariant::String("world".to_string())),
            ],
        }]
    );
}

/// Regression test for nested Variant inside a Tuple.
/// Exercises the `Tuple(elements)` branch of the inner validator's
/// `null_encoding`.
#[tokio::test]
async fn variant_null_in_tuple() {
    let client = prepare_database!();

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    enum SimpleVariant {
        String(String),
        UInt64(u64),
    }

    #[derive(Debug, PartialEq, Row, Deserialize)]
    struct MyRow {
        id: u32,
        pair: (u32, Option<SimpleVariant>),
    }

    client
        .query(
            "
            CREATE OR REPLACE TABLE test_var_null_tuple
            (
                `id` UInt32,
                `pair` Tuple(UInt32, Variant(String, UInt64))
            )
            ENGINE = MergeTree
            ORDER BY id",
        )
        .with_setting("allow_experimental_variant_type", "1")
        .execute()
        .await
        .unwrap();

    client
        .query(
            "INSERT INTO test_var_null_tuple VALUES \
             (1, (10, 'hello')), (2, (20, NULL)), (3, (30, 42))",
        )
        .execute()
        .await
        .unwrap();

    let rows = client
        .query("SELECT ?fields FROM test_var_null_tuple ORDER BY id")
        .fetch_all::<MyRow>()
        .await
        .unwrap();

    assert_eq!(
        rows,
        vec![
            MyRow {
                id: 1,
                pair: (10, Some(SimpleVariant::String("hello".to_string()))),
            },
            MyRow {
                id: 2,
                pair: (20, None),
            },
            MyRow {
                id: 3,
                pair: (30, Some(SimpleVariant::UInt64(42))),
            },
        ]
    );
}

// The tests below write `Option<T>` to `Variant` columns with `Client::insert`.
// A `Variant` column has no Nullable marker byte. NULL is the discriminator 0xFF.
// Each test reads the data back and also asks the server how it parsed the data,
// so that a defect on both the write path and the read path cannot hide.

#[derive(Debug, PartialEq, Serialize, Deserialize)]
enum InsertVariant {
    String(String),
    UInt64(u64),
}

#[derive(Debug, PartialEq, Row, Serialize, Deserialize)]
struct InsertVariantRow {
    id: u32,
    var: Option<InsertVariant>,
}

async fn insert_and_read_back(client: &Client, rows: &[InsertVariantRow]) {
    client
        .query(
            "
            CREATE OR REPLACE TABLE test_var_null_insert
            (
                `id` UInt32,
                `var` Variant(String, UInt64)
            )
            ENGINE = MergeTree
            ORDER BY id",
        )
        .with_setting("allow_experimental_variant_type", "1")
        .execute()
        .await
        .unwrap();

    let mut insert = client
        .insert::<InsertVariantRow>("test_var_null_insert")
        .await
        .unwrap();
    for row in rows {
        insert.write(row).await.unwrap();
    }
    insert.end().await.unwrap();

    let fetched = client
        .query("SELECT ?fields FROM test_var_null_insert ORDER BY id")
        .fetch_all::<InsertVariantRow>()
        .await
        .unwrap();
    assert_eq!(fetched, rows);
}

async fn variant_types_on_server(client: &Client) -> Vec<String> {
    client
        .query("SELECT toString(variantType(var)) FROM test_var_null_insert ORDER BY id")
        .fetch_all::<String>()
        .await
        .unwrap()
}

#[tokio::test]
async fn variant_null_insert_some() {
    let client = prepare_database!();
    let rows = [
        InsertVariantRow {
            id: 1,
            var: Some(InsertVariant::String("hello".to_string())),
        },
        InsertVariantRow {
            id: 2,
            var: Some(InsertVariant::UInt64(42)),
        },
    ];

    insert_and_read_back(&client, &rows).await;
    assert_eq!(variant_types_on_server(&client).await, ["String", "UInt64"]);
}

#[tokio::test]
async fn variant_null_insert_none() {
    let client = prepare_database!();
    let rows = [
        InsertVariantRow { id: 1, var: None },
        InsertVariantRow { id: 2, var: None },
    ];

    insert_and_read_back(&client, &rows).await;
    assert_eq!(variant_types_on_server(&client).await, ["None", "None"]);
}

#[tokio::test]
async fn variant_null_insert_mixed() {
    let client = prepare_database!();
    let rows = [
        InsertVariantRow {
            id: 1,
            var: Some(InsertVariant::String("hello".to_string())),
        },
        InsertVariantRow { id: 2, var: None },
        InsertVariantRow {
            id: 3,
            var: Some(InsertVariant::UInt64(42)),
        },
        InsertVariantRow { id: 4, var: None },
    ];

    insert_and_read_back(&client, &rows).await;
    assert_eq!(
        variant_types_on_server(&client).await,
        ["String", "None", "UInt64", "None"]
    );
}

#[tokio::test]
async fn variant_null_insert_in_array() {
    let client = prepare_database!();

    #[derive(Debug, PartialEq, Row, Serialize, Deserialize)]
    struct MyRow {
        id: u32,
        vars: Vec<Option<InsertVariant>>,
    }

    client
        .query(
            "
            CREATE OR REPLACE TABLE test_var_null_insert_array
            (
                `id` UInt32,
                `vars` Array(Variant(String, UInt64))
            )
            ENGINE = MergeTree
            ORDER BY id",
        )
        .with_setting("allow_experimental_variant_type", "1")
        .execute()
        .await
        .unwrap();

    let rows = [
        MyRow {
            id: 1,
            vars: vec![
                Some(InsertVariant::String("hello".to_string())),
                None,
                Some(InsertVariant::UInt64(42)),
                None,
            ],
        },
        MyRow {
            id: 2,
            vars: vec![None],
        },
    ];

    let mut insert = client
        .insert::<MyRow>("test_var_null_insert_array")
        .await
        .unwrap();
    for row in &rows {
        insert.write(row).await.unwrap();
    }
    insert.end().await.unwrap();

    let on_server = client
        .query("SELECT toString(vars) FROM test_var_null_insert_array ORDER BY id")
        .fetch_all::<String>()
        .await
        .unwrap();
    assert_eq!(on_server, ["['hello',NULL,42,NULL]", "[NULL]"]);

    let fetched = client
        .query("SELECT ?fields FROM test_var_null_insert_array ORDER BY id")
        .fetch_all::<MyRow>()
        .await
        .unwrap();
    assert_eq!(fetched, rows);
}

#[tokio::test]
async fn variant_null_insert_in_tuple() {
    let client = prepare_database!();

    #[derive(Debug, PartialEq, Row, Serialize, Deserialize)]
    struct MyRow {
        id: u32,
        pair: (u32, Option<InsertVariant>),
    }

    client
        .query(
            "
            CREATE OR REPLACE TABLE test_var_null_insert_tuple
            (
                `id` UInt32,
                `pair` Tuple(UInt32, Variant(String, UInt64))
            )
            ENGINE = MergeTree
            ORDER BY id",
        )
        .with_setting("allow_experimental_variant_type", "1")
        .execute()
        .await
        .unwrap();

    let rows = [
        MyRow {
            id: 1,
            pair: (10, Some(InsertVariant::String("hello".to_string()))),
        },
        MyRow {
            id: 2,
            pair: (20, None),
        },
        MyRow {
            id: 3,
            pair: (30, Some(InsertVariant::UInt64(42))),
        },
    ];

    let mut insert = client
        .insert::<MyRow>("test_var_null_insert_tuple")
        .await
        .unwrap();
    for row in &rows {
        insert.write(row).await.unwrap();
    }
    insert.end().await.unwrap();

    let on_server = client
        .query("SELECT toString(pair) FROM test_var_null_insert_tuple ORDER BY id")
        .fetch_all::<String>()
        .await
        .unwrap();
    assert_eq!(on_server, ["(10,'hello')", "(20,NULL)", "(30,42)"]);

    let fetched = client
        .query("SELECT ?fields FROM test_var_null_insert_tuple ORDER BY id")
        .fetch_all::<MyRow>()
        .await
        .unwrap();
    assert_eq!(fetched, rows);
}
