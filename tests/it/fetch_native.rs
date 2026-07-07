use crate::get_client;

#[tokio::test]
async fn mixed_types() {
    let client = get_client();

    let mut cursor = client
        .query(
            "SELECT
            number,
            leftPad(toString(number), number, '.') AS text,
            number % 2 == 0 ?? number : NULL AS nullable_number,
            number %2 != 0 ?? text : NULL AS nullable_text,
            arrayMap(x -> toUInt64(x), arrayEnumerate(arrayWithConstant(number, toUInt64(0)))) AS number_array
            -- arrayMap(x -> rightPad(toString(number), number, '-'), number_array) AS text_array
        FROM system.numbers
        LIMIT 10",
        )
        .fetch_native()
        .unwrap();

    let block = cursor.next().await.unwrap().expect("expected one block");

    assert_eq!(block.num_rows(), 10);

    assert_eq!(block.columns().len(), 5);

    assert_eq!(block[0].name(), "number");

    let mut number_iter = block["number"].iter::<u64>().unwrap();

    for (number, row) in number_iter.by_ref().zip(0u64..10) {
        assert_eq!(number.unwrap(), row);
    }

    assert!(number_iter.next().is_none());

    let mut text_iter = block["text"].iter::<&str>().unwrap();

    for (text, row) in text_iter.by_ref().zip(0u64..10) {
        let text = text.unwrap();
        assert_eq!(text.len() as u64, row, "incorrect text: {text:?}");
    }

    assert!(text_iter.next().is_none());

    let mut nullable_number_iter = block["nullable_number"].iter::<Option<u64>>().unwrap();

    for (opt_number, row) in nullable_number_iter.by_ref().zip(0u64..10) {
        let opt_number = opt_number.unwrap();

        if row % 2 == 0 {
            assert_eq!(opt_number, Some(row));
        } else {
            assert_eq!(opt_number, None);
        }
    }

    assert!(nullable_number_iter.next().is_none());

    let mut nullable_text_iter = block["nullable_text"].iter::<Option<&str>>().unwrap();

    for (opt_text, row) in nullable_text_iter.by_ref().zip(0u64..10) {
        let opt_text = opt_text.unwrap();

        if row % 2 != 0 {
            assert_eq!(
                opt_text.unwrap().len() as u64,
                row,
                "incorrect nullable text: {opt_text:?}"
            );
        } else {
            assert_eq!(opt_text, None);
        }
    }

    let mut number_array_iter = block["number_array"].iter::<Vec<u64>>().unwrap();

    for (number_array, row) in number_array_iter.by_ref().zip(0u64..10) {
        let number_array = number_array.unwrap();

        assert_eq!(number_array.len() as u64, row);

        assert!(
            number_array.iter().copied().eq(0u64..row),
            "invalid number_array: {number_array:?}"
        );
    }
}
