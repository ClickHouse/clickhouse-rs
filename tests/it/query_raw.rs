use clickhouse::Row;
use serde::{Deserialize, Serialize};

#[derive(Row, Deserialize, Debug)]
struct PersonName<'a> {
    name: &'a str,
}

#[derive(Row, Deserialize, Debug)]
struct PersonInfo {
    name: String,
    age: u32,
}

#[tokio::test]
async fn verify_raw_query_basic_functionality() {
    let client = prepare_database!();

    // The key test: verify that ? characters don't cause binding errors
    let result = client
        .query_raw("SELECT 1 WHERE 'test?' = 'test?'")
        .fetch_bytes("TSV")
        .unwrap();

    let mut data = Vec::new();
    let mut cursor = result;
    while let Some(chunk) = cursor.next().await.unwrap() {
        data.extend_from_slice(&chunk);
    }
    let response = String::from_utf8(data).unwrap();

    // Should return "1\n" - proving the query executed successfully
    assert_eq!(response.trim(), "1");

    // Contrast: regular query with ? should fail
    let regular_result = client
        .query("SELECT 1 WHERE 'test?' = 'test?'")
        .fetch_bytes("TSV");

    // This should fail because ? is treated as a bind parameter
    assert!(regular_result.is_err());
    if let Err(error) = regular_result {
        let error_msg = error.to_string();
        assert!(error_msg.contains("unbound"));
    }
}

#[tokio::test]
async fn fetch_with_single_field_struct() {
    let client = prepare_database!();

    client
        .query("CREATE TABLE test_users(name String) ENGINE = Memory")
        .execute()
        .await
        .unwrap();

    client
        .query_raw("INSERT INTO test_users VALUES ('Alice?'), ('Bob??'), ('Charlie???')")
        .execute()
        .await
        .unwrap();

    // Test raw query with struct fetching
    let sql = "SELECT name FROM test_users ORDER BY name";

    let mut cursor = client.query_raw(sql).fetch::<PersonName<'_>>().unwrap();

    let mut names = Vec::new();
    while let Some(PersonName { name }) = cursor.next().await.unwrap() {
        names.push(name.to_string());
    }

    assert_eq!(names, vec!["Alice?", "Bob??", "Charlie???"]);
}

#[tokio::test]
async fn fetch_with_multi_field_struct() {
    let client = prepare_database!();

    // Create a test table
    client
        .query("CREATE TABLE test_persons(name String, age UInt32) ENGINE = Memory")
        .execute()
        .await
        .unwrap();

    // Insert test data with question marks in names
    client
        .query_raw("INSERT INTO test_persons VALUES ('What?', 25), ('How??', 30), ('Why???', 35)")
        .execute()
        .await
        .unwrap();

    // Test raw query with multi-field struct
    let sql = "SELECT name, age FROM test_persons ORDER BY age";

    let mut cursor = client.query_raw(sql).fetch::<PersonInfo>().unwrap();

    let mut persons = Vec::new();
    while let Some(person) = cursor.next().await.unwrap() {
        persons.push((person.name.clone(), person.age));
    }

    assert_eq!(
        persons,
        vec![
            ("What?".to_string(), 25),
            ("How??".to_string(), 30),
            ("Why???".to_string(), 35)
        ]
    );
}

#[tokio::test]
async fn compare_raw_vs_regular_query_with_structs() {
    let client = prepare_database!();

    client
        .query("CREATE TABLE test_comparison(name String) ENGINE = Memory")
        .execute()
        .await
        .unwrap();

    client
        .query_raw("INSERT INTO test_comparison VALUES ('Alice?')")
        .execute()
        .await
        .unwrap();

    // Regular query with ? should fail due to unbound parameter
    let regular_result = client
        .query("SELECT name FROM test_comparison WHERE name = 'Alice?'")
        .fetch::<PersonName<'_>>();

    assert!(regular_result.is_err());
    if let Err(error) = regular_result {
        let error_msg = error.to_string();
        assert!(error_msg.contains("unbound"));
    }

    // Raw query with ? should succeed )
    let raw_result = client
        .query_raw("SELECT name FROM test_comparison WHERE name = 'Alice?'")
        .fetch::<PersonName<'_>>()
        .unwrap();

    let mut names = Vec::new();
    let mut cursor = raw_result;
    while let Some(PersonName { name }) = cursor.next().await.unwrap() {
        names.push(name.to_string());
    }

    assert_eq!(names, vec!["Alice?"]);
}

#[tokio::test]
async fn mixed_question_mark() {
    let client = prepare_database!();

    // Test various question mark patterns with bytes fetch to avoid format issues
    let patterns = vec![
        ("SELECT 1 WHERE 'test?' = 'test?'", "?"),
        ("SELECT 2 WHERE 'test??' = 'test??'", "??"),
        ("SELECT 3 WHERE 'test???' = 'test???'", "???"),
        (
            "SELECT 4 WHERE 'What? How?? Why???' = 'What? How?? Why???'",
            "mixed",
        ),
    ];

    for (sql, pattern_type) in patterns {
        let result = client.query_raw(sql).fetch_bytes("TSV").unwrap();

        let mut data = Vec::new();
        let mut cursor = result;
        while let Some(chunk) = cursor.next().await.unwrap() {
            data.extend_from_slice(&chunk);
        }
        let response = String::from_utf8(data).unwrap();

        // Should return the expected number
        assert!(
            !response.trim().is_empty(),
            "Query should return data for pattern: {}",
            pattern_type
        );
    }
}

#[tokio::test]
async fn question_marks_in_comments() {
    let client = prepare_database!();

    // Test question marks in SQL comments - should work without binding
    let result = client
        .query_raw("SELECT 1 /* What? How?? Why??? */ WHERE 1=1")
        .fetch_bytes("TSV")
        .unwrap();

    let mut data = Vec::new();
    let mut cursor = result;
    while let Some(chunk) = cursor.next().await.unwrap() {
        data.extend_from_slice(&chunk);
    }
    let response = String::from_utf8(data).unwrap();

    assert_eq!(response.trim(), "1");
}

#[tokio::test]
async fn contrast_with_regular_query() {
    let client = prepare_database!();

    // This should fail with regular query because of unbound parameter
    let result = client
        .query("SELECT 1 WHERE 'test?' = 'test?'")
        .fetch_bytes("TSV");

    // Regular query should fail due to unbound ?
    assert!(result.is_err());
    if let Err(error) = result {
        let error_msg = error.to_string();
        assert!(error_msg.contains("unbound"));
    }

    // But raw query should succeed
    let raw_result = client
        .query_raw("SELECT 1 WHERE 'test?' = 'test?'")
        .fetch_bytes("TSV")
        .unwrap();

    let mut data = Vec::new();
    let mut cursor = raw_result;
    while let Some(chunk) = cursor.next().await.unwrap() {
        data.extend_from_slice(&chunk);
    }
    let response = String::from_utf8(data).unwrap();

    assert_eq!(response.trim(), "1");
}

#[tokio::test]
async fn complex_sql_with_question_marks() {
    use clickhouse::Row;
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Row, Serialize, Deserialize)]
    struct TestResult {
        question: String,
        confusion: String,
        bewilderment: String,
        answer: String,
    }

    let client = prepare_database!();

    // Test a more complex SQL query with question marks in various contexts
    let sql = r#"
        SELECT 
            'What is this?' as question,
            'How does this work??' as confusion,
            'Why would you do this???' as bewilderment,
            CASE 
                WHEN 1=1 THEN 'Yes?'
                ELSE 'No??'
            END as answer
        WHERE 'test?' LIKE '%?'
    "#;

    let result = client.query_raw(sql).fetch_one::<TestResult>().await;

    assert!(result.is_ok());
    let row = result.unwrap();
    assert_eq!(row.question, "What is this?");
    assert_eq!(row.confusion, "How does this work??");
    assert_eq!(row.bewilderment, "Why would you do this???");
    assert_eq!(row.answer, "Yes?");
}

#[tokio::test]
async fn query_matches_log() {
    use uuid::Uuid;

    // setup
    let client = prepare_database!();
    let query_id = Uuid::new_v4().to_string(); // unique per run
    let sql = "SELECT 1 WHERE 'x?' = 'x?'"; // raw statement to verify

    // execute with explicit query_id
    client
        .query_raw(sql)
        .with_option("query_id", &query_id)
        .execute()
        .await
        .expect("executing raw SQL failed");

    crate::flush_query_log(&client).await;

    // read log row *inline*
    let log_sql = format!(
        "SELECT query \
         FROM system.query_log \
         WHERE query_id = '{}' LIMIT 1",
        query_id
    );

    let logged_sql: String = client
        .query_raw(&log_sql)
        .fetch_one()
        .await
        .expect("log entry not found");

    // assertion
    assert_eq!(
        logged_sql.trim(),
        sql.trim(),
        "Logged SQL differs from the statement sent"
    );
}
