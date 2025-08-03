#[tokio::test]
async fn query_with_semicolon() {
    let client = prepare_database!();
    let value = client.query("SELECT 1;").fetch_one::<u8>().await.unwrap();
    assert_eq!(value, 1);
}

#[tokio::test]
async fn query_with_tail_comment() {
    let client = prepare_database!();
    let value = client.query("SELECT 1 \n --comment").fetch_one::<u8>().await.unwrap();
    assert_eq!(value, 1);
}

#[tokio::test]
async fn query_with_head_comment() {
    let client = prepare_database!();
    let value = client.query("--comment\nSELECT 1").fetch_one::<u8>().await.unwrap();
    assert_eq!(value, 1);
}

#[tokio::test]
async fn query_with_head_and_tail_comment() {
    let client = prepare_database!();
    let value = client.query("--comment\nSELECT 1\n--comment").fetch_one::<u8>().await.unwrap();
    assert_eq!(value, 1);
}

#[tokio::test]
async fn query_with_mid_comment() {
    let client = prepare_database!();
    let value = client.query("SELECT \n--comment\n 1").fetch_one::<u8>().await.unwrap();
    assert_eq!(value, 1);
}

#[tokio::test]
async fn query_with_tail_newline() {
    let client = prepare_database!();
    let value = client.query("SELECT 1\n").fetch_one::<u8>().await.unwrap();
    assert_eq!(value, 1);
}

#[tokio::test]
async fn query_with_tail_space() {
    let client = prepare_database!();
    let value = client.query("SELECT 1 ").fetch_one::<u8>().await.unwrap();
    assert_eq!(value, 1);
}

#[tokio::test]
async fn query_with_tail_tab() {
    let client = prepare_database!();
    let value = client.query("SELECT 1\t").fetch_one::<u8>().await.unwrap();
    assert_eq!(value, 1);
}

#[tokio::test]
async fn query_with_tail_carriage_return() {
    let client = prepare_database!();
    let value = client.query("SELECT 1\r").fetch_one::<u8>().await.unwrap();
    assert_eq!(value, 1);
}

#[tokio::test]
async fn query_with_tail_carriage_return_newline() {
    let client = prepare_database!();
    let value = client.query("SELECT 1\r\n").fetch_one::<u8>().await.unwrap();
    assert_eq!(value, 1);
}

#[tokio::test]
async fn query_with_tail_carriage_return_space() {
    let client = prepare_database!();
    let value = client.query("SELECT 1\r ").fetch_one::<u8>().await.unwrap();
    assert_eq!(value, 1);
}
