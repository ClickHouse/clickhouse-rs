use crate::get_client_with_session;
use clickhouse::native::BlockBuilder;
#[tokio::test]
async fn mixed_types_empty() {
    mixed_types(0).await
}

#[tokio::test]
async fn mixed_types_10() {
    mixed_types(10).await
}

#[tokio::test]
async fn mixed_types_100() {
    mixed_types(100).await
}

#[tokio::test]
async fn mixed_types_1000() {
    mixed_types(1000).await
}

async fn mixed_types(num_rows: u64) {
    let client = get_client_with_session();

    client
        .query(
            "\
            CREATE TEMPORARY TABLE foo(
                number Int32,
                text String,
                nullable_number Nullable(UInt64),
                nullable_text Nullable(String),
                number_tuple Tuple(UInt32, Int64),
                number_text_tuple Tuple(Int64, String),
                nullable_tuple Tuple(Nullable(Int64), Nullable(String))
            )",
        )
        .execute()
        .await
        .unwrap();

    let numbers = (0..(num_rows as i32)).collect::<Vec<_>>();
    let texts = (0..(num_rows as usize))
        .map(|i| {
            if i == 0 {
                return "".to_string();
            }

            format!("{i:.i$}")
        })
        .collect::<Vec<_>>();

    let mut builder = BlockBuilder::new();

    builder
        .upsert_column::<i32>("number")
        .unwrap()
        .add_all(&numbers)
        .unwrap();

    builder
        .upsert_column::<String>("text")
        .unwrap()
        .add_all(&texts)
        .unwrap();

    builder
        .upsert_column::<Option<i32>>("nullable_number")
        .unwrap()
        .add_all(numbers.iter().map(|&i| (i % 2 == 0).then_some(i)))
        .unwrap();

    builder
        .upsert_column::<Option<String>>("nullable_text")
        .unwrap()
        .add_all(
            numbers
                .iter()
                .zip(&texts)
                .map(|(&number, text)| (number % 2 != 0).then_some(text)),
        )
        .unwrap();

    builder
        .upsert_column::<(i32, i64)>("number_tuple")
        .unwrap()
        .add_all(numbers.iter().map(|&i| (i, i as i64)))
        .unwrap();

    builder
        .upsert_column::<(i64, String)>("number_text_tuple")
        .unwrap()
        .add_all(
            numbers
                .iter()
                .zip(&texts)
                .map(|(&number, text)| (number as i64, text)),
        )
        .unwrap();

    builder
        .upsert_column::<(Option<i64>, Option<String>)>("nullable_tuple")
        .unwrap()
        .add_all(numbers.iter().zip(&texts).map(|(&number, text)| {
            (
                (number % 2 == 0).then_some(number as i64),
                (number % 2 != 0).then_some(text),
            )
        }))
        .unwrap();

    let block_in = builder.build().unwrap();

    let mut insert = client.insert_native("foo");

    insert.write(&block_in).await.unwrap();

    insert.end().await.unwrap();

    let mut cursor = client.query("SELECT * FROM foo").fetch_native().unwrap();

    let Some(block_out) = cursor.next().await.unwrap() else {
        assert_eq!(num_rows, 0, "expected block, got none");
        return;
    };

    let mut number_iter = block_out["number"].iter::<i32>().unwrap();

    for (res, &expected) in number_iter.by_ref().zip(&numbers) {
        let actual = res.unwrap();
        assert_eq!(actual, expected);
    }

    if let Some(res) = number_iter.next() {
        panic!("unexpected value {res:?}");
    }

    let mut text_iter = block_out["text"].iter::<String>().unwrap();

    for (res, expected) in text_iter.by_ref().zip(&texts) {
        let actual = res.unwrap();
        assert_eq!(actual, *expected);
    }

    if let Some(res) = text_iter.next() {
        panic!("unexpected value {res:?}");
    }

    let mut nullable_text_iter = block_out["nullable_text"].iter::<Option<String>>().unwrap();

    for (i, (res, expected)) in nullable_text_iter.by_ref().zip(&texts).enumerate() {
        let actual = res.unwrap();

        if !i.is_multiple_of(2) {
            assert_eq!(actual.as_ref(), Some(expected));
        } else {
            assert_eq!(actual, None);
        }
    }

    if let Some(res) = nullable_text_iter.next() {
        panic!("unexpected value {res:?}");
    }

    let mut number_tuple_iter = block_out["number_tuple"].iter::<(i32, i64)>().unwrap();

    for (res, &expected) in number_tuple_iter.by_ref().zip(&numbers) {
        let (actual_int32, actual_int64) = res.unwrap();
        assert_eq!(actual_int32, expected);
        assert_eq!(actual_int64, expected as i64);
    }

    if let Some(res) = number_tuple_iter.next() {
        panic!("unexpected value {res:?}");
    }

    let mut number_text_tuple_iter = block_out["number_text_tuple"]
        .iter::<(i64, String)>()
        .unwrap();

    for ((res, &expected_number), expected_text) in
        number_text_tuple_iter.by_ref().zip(&numbers).zip(&texts)
    {
        let (actual_number, actual_text) = res.unwrap();
        assert_eq!(actual_number, expected_number as i64);
        assert_eq!(actual_text, *expected_text);
    }

    if let Some(res) = number_text_tuple_iter.next() {
        panic!("unexpected value {res:?}");
    }

    let mut nullable_tuple_iter = block_out["nullable_tuple"]
        .iter::<(Option<i64>, Option<String>)>()
        .unwrap();

    for ((res, &expected_number), expected_text) in
        nullable_tuple_iter.by_ref().zip(&numbers).zip(&texts)
    {
        let (actual_number, actual_text) = res.unwrap();

        assert_eq!(
            actual_number,
            (expected_number % 2 == 0).then_some(expected_number as i64)
        );
        assert_eq!(
            actual_text.as_ref(),
            (expected_number % 2 != 0).then_some(expected_text)
        );
    }

    if let Some(res) = nullable_tuple_iter.next() {
        panic!("unexpected value {res:?}");
    }
}
