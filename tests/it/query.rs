use serde::{Deserialize, Serialize};

use clickhouse::{Row, error::Error, query::QI, sql::Identifier};
//new changes
// use clickhouse::{Row, error::Error};
#[tokio::test]
async fn smoke() {
    let client = prepare_database!();

    #[derive(Debug, Row, Serialize, Deserialize)]
    struct MyRow<'a> {
        no: u32,
        name: &'a str,
    }

    // Create a table.
    client
        .query(
            "
            CREATE TABLE test(no UInt32, name LowCardinality(String))
            ENGINE = MergeTree
            ORDER BY no
        ",
        )
        .execute()
        .await
        .unwrap();

    // Write to the table.
    let mut insert = client.insert::<MyRow<'_>>("test").await.unwrap();
    for i in 0..1000 {
        insert.write(&MyRow { no: i, name: "foo" }).await.unwrap();
    }

    insert.end().await.unwrap();

    // Read from the table.
    let mut cursor = client
        .query_with_flags::<{ QI::FIELDS | QI::BIND }>(
            "SELECT ?fields FROM test WHERE name = ? AND no BETWEEN ? AND ?.2",
        )
        .bind("foo")
        .bind(500)
        .bind((42, 504))
        .fetch::<MyRow<'_>>()
        .unwrap();

    let mut i = 500;

    while let Some(row) = cursor.next().await.unwrap() {
        assert_eq!(row.no, i);
        assert_eq!(row.name, "foo");
        i += 1;
    }
}

#[tokio::test]
async fn fetch_one_and_optional() {
    let client = prepare_database!();

    client
        .query("CREATE TABLE test(n String) ENGINE = MergeTree ORDER BY n")
        .execute()
        .await
        .unwrap();

    let q = "SELECT * FROM test";
    let got_string = client.query(q).fetch_optional::<String>().await.unwrap();
    assert_eq!(got_string, None);

    let got_string = client.query(q).fetch_one::<String>().await;
    assert!(matches!(got_string, Err(Error::RowNotFound)));

    #[derive(Serialize, Row)]
    struct Row {
        n: String,
    }

    let mut insert = client.insert::<Row>("test").await.unwrap();
    insert.write(&Row { n: "foo".into() }).await.unwrap();
    insert.write(&Row { n: "bar".into() }).await.unwrap();
    insert.end().await.unwrap();

    let got_string = client.query(q).fetch_optional::<String>().await.unwrap();
    assert_eq!(got_string, Some("bar".into()));

    let got_string = client.query(q).fetch_one::<String>().await.unwrap();
    assert_eq!(got_string, "bar");
}

#[tokio::test]
async fn server_side_param() {
    let client = prepare_database!();

    let result = client
        .query("SELECT plus({val1: Int32}, {val2: Int32}) AS result")
        .param("val1", 42)
        .param("val2", 144)
        .fetch_one::<i64>()
        .await
        .expect("failed to fetch Int64");
    assert_eq!(result, 186);

    let result = client
        .query("SELECT {val1: String} AS result")
        .param("val1", "string")
        .fetch_one::<String>()
        .await
        .expect("failed to fetch string");
    assert_eq!(result, "string");

    let result = client
        .query("SELECT {val1: String} AS result")
        .param("val1", "\x01\x02\x03\\ \"\'")
        .fetch_one::<String>()
        .await
        .expect("failed to fetch string");
    assert_eq!(result, "\x01\x02\x03\\ \"\'");

    let result = client
        .query("SELECT {val1: Array(String)} AS result")
        .param("val1", vec!["a", "bc"])
        .fetch_one::<Vec<String>>()
        .await
        .expect("failed to fetch string");
    assert_eq!(result, &["a", "bc"]);
}

// See #19.
#[tokio::test]
async fn long_query() {
    let client = prepare_database!();

    client
        .query("CREATE TABLE test(n String) ENGINE = MergeTree ORDER BY n")
        .execute()
        .await
        .unwrap();

    let long_string = "A".repeat(100_000);

    let got_string = client
        .query_with_flags::<{ QI::BIND }>("select ?")
        .bind(&long_string)
        .fetch_one::<String>()
        .await
        .unwrap();

    assert_eq!(got_string, long_string);
}

// See #22.
#[tokio::test]
async fn big_borrowed_str() {
    let client = prepare_database!();

    #[derive(Debug, Row, Serialize, Deserialize)]
    struct MyRow<'a> {
        no: u32,
        body: &'a str,
    }

    client
        .query("CREATE TABLE test(no UInt32, body String) ENGINE = MergeTree ORDER BY no")
        .execute()
        .await
        .unwrap();

    let long_string = "A".repeat(10000);

    let mut insert = client.insert::<MyRow<'_>>("test").await.unwrap();
    insert
        .write(&MyRow {
            no: 0,
            body: &long_string,
        })
        .await
        .unwrap();
    insert.end().await.unwrap();

    let mut cursor = client
        .query("SELECT ?fields FROM test")
        .fetch::<MyRow<'_>>()
        .unwrap();

    let row = cursor.next().await.unwrap().unwrap();
    assert_eq!(row.body, long_string);
}

// See #31.
#[tokio::test]
async fn all_floats() {
    let client = prepare_database!();

    client
        .query("CREATE TABLE test(no UInt32, f Float64) ENGINE = MergeTree ORDER BY no")
        .execute()
        .await
        .unwrap();

    #[derive(Row, Serialize)]
    struct Row {
        no: u32,
        f: f64,
    }

    let mut insert = client.insert::<Row>("test").await.unwrap();
    insert.write(&Row { no: 0, f: 42.5 }).await.unwrap();
    insert.write(&Row { no: 1, f: 43.5 }).await.unwrap();
    insert.end().await.unwrap();

    let vec = client
        .query("SELECT f FROM test")
        .fetch_all::<f64>()
        .await
        .unwrap();

    assert_eq!(vec, &[42.5, 43.5]);
}

#[tokio::test]
async fn keeps_client_options() {
    let (client_setting_name, client_setting_value) = ("max_block_size", "1000");
    let (query_setting_name, query_setting_value) = ("date_time_input_format", "basic");

    let client = prepare_database!().with_option(client_setting_name, client_setting_value);

    let value = client
        .query_with_flags::<{ QI::BIND }>(
            "SELECT value FROM system.settings WHERE name = ? OR name = ? ORDER BY name",
        )
        .bind(query_setting_name)
        .bind(client_setting_name)
        .with_option(query_setting_name, query_setting_value)
        .fetch_all::<String>()
        .await
        .unwrap();

    // should keep the client options
    assert_eq!(value, vec!(query_setting_value, client_setting_value));
}

#[tokio::test]
async fn overrides_client_options() {
    let (setting_name, setting_value, override_value) = ("max_block_size", "1000", "2000");

    let client = prepare_database!().with_option(setting_name, setting_value);

    let value = client
        .query_with_flags::<{ QI::BIND }>("SELECT value FROM system.settings WHERE name = ?")
        .bind(setting_name)
        .with_option(setting_name, override_value)
        .fetch_one::<String>()
        .await
        .unwrap();

    // should override the client options
    assert_eq!(value, override_value);
}

#[tokio::test]
async fn prints_query() {
    let client = prepare_database!();

    let q = client.query("SELECT ?fields FROM test WHERE a = ? AND b < ?");
    assert_eq!(
        format!("{}", q.sql_display()),
        "SELECT ?fields FROM test WHERE a = ? AND b < ?"
    );
}

#[tokio::test]
async fn query_flags_normal_query_with_bind_should_error() {
    let client = prepare_database!();

    // Test that using .bind() with regular .query() should skip binding and produce warning
    let result = client
        .query("SELECT ? as value")
        .bind(42)
        .fetch_one::<i32>()
        .await;

    // Should fail because ? becomes NULL and we can't cast NULL to i32
    assert!(result.is_err());
}

#[tokio::test]
async fn query_flags_fields_without_fields_flag_should_error() {
    let client = prepare_database!();

    #[derive(Debug, Row, Deserialize, PartialEq)]
    struct TestRow {
        value: u8,
    }

    // Test that using ?fields with only BIND flag should fail when ?fields is not substituted
    // This tests the SQL generation without fetch auto-binding
    let result = client
        .query_with_flags::<{ QI::BIND }>("INSERT INTO test (?fields) VALUES (42)")
        .execute()
        .await;

    // Should fail because ?fields is skipped and we get "INSERT INTO test () VALUES (42)" which is invalid SQL
    assert!(result.is_err());
}

#[tokio::test]
async fn query_flags_happy_case_bind_only() {
    let client = prepare_database!();

    // Test successful binding with BIND flag
    let result = client
        .query_with_flags::<{ QI::BIND }>("SELECT ? as value")
        .bind(42u8)
        .fetch_one::<u8>()
        .await
        .unwrap();

    assert_eq!(result, 42);
}

#[tokio::test]
async fn query_flags_happy_case_fields_only() {
    let client = prepare_database!();

    #[derive(Debug, Row, Deserialize, PartialEq)]
    struct TestRow {
        value: u8,
    }

    // Test successful fields substitution with FIELDS flag
    let result = client
        .query_with_flags::<{ QI::FIELDS }>("SELECT ?fields FROM (SELECT 42 as value)")
        .fetch_one::<TestRow>()
        .await
        .unwrap();

    assert_eq!(result, TestRow { value: 42 });
}

#[tokio::test]
async fn query_flags_happy_case_both_flags() {
    let client = prepare_database!();

    #[derive(Debug, Row, Deserialize, PartialEq)]
    struct TestRow {
        value: u8,
    }

    // Test successful combination of both ?fields and ? with both flags
    let result = client
        .query_with_flags::<{ QI::FIELDS | QI::BIND }>("SELECT ?fields FROM (SELECT ? as value)")
        .bind(42u8)
        .fetch_one::<TestRow>()
        .await
        .unwrap();

    assert_eq!(result, TestRow { value: 42 });
}

#[tokio::test]
async fn query_flags_multiple_binds_with_flag() {
    let client = prepare_database!();

    // Test multiple bind parameters with BIND flag
    let result = client
        .query_with_flags::<{ QI::BIND }>("SELECT ? + ? as sum")
        .bind(10u8)
        .bind(32u8)
        .fetch_one::<u16>()
        .await
        .unwrap();

    assert_eq!(result, 42);
}

#[tokio::test]
async fn query_flags_bind_with_identifier() {
    let client = prepare_database!();
    let table_name = "test_table";

    // Create a test table
    client
        .query_with_flags::<{ QI::BIND }>(
            "CREATE TABLE ? (id UInt32, value String) ENGINE = Memory",
        )
        .bind(Identifier(table_name))
        .execute()
        .await
        .unwrap();

    // Insert test data
    client
        .query_with_flags::<{ QI::BIND }>("INSERT INTO ? VALUES (1, 'test')")
        .bind(Identifier(table_name))
        .execute()
        .await
        .unwrap();

    // Query with identifier binding
    let count = client
        .query_with_flags::<{ QI::BIND }>("SELECT count() FROM ?")
        .bind(Identifier(table_name))
        .fetch_one::<u64>()
        .await
        .unwrap();

    assert_eq!(count, 1);
}

#[tokio::test]
async fn query_flags_none_with_fields_and_bind() {
    let client = prepare_database!();

    #[derive(Debug, Row, Deserialize, PartialEq)]
    struct TestRow {
        value: u8,
    }

    // Test that QI::NONE skips both ?fields and ? - this should fail because ?fields becomes empty
    let result = client
        .query_with_flags::<{ QI::NONE }>("SELECT ?fields, ? as extra FROM (SELECT 42 as value)")
        .bind(999) // This should be ignored
        .execute() // Use execute to avoid fetch auto-binding fields
        .await;

    // Should fail because ?fields is skipped making "SELECT , NULL as extra FROM ..." which is invalid SQL
    assert!(result.is_err());
}
