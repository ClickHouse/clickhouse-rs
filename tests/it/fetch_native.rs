use crate::get_client;
use std::collections::HashMap;

#[tokio::test]
async fn mixed_types() {
    const NUM_ROWS: u64 = 10;

    let client = get_client();

    let mut cursor = client
        .query(
            "SELECT
            number,
            leftPad(toString(number), number, '.') AS text,
            number % 2 == 0 ?? number : NULL AS nullable_number,
            number % 2 != 0 ?? text : NULL AS nullable_text,
            tuple(number, text) AS number_text_tuple,
            tuple(nullable_number, nullable_text) AS nullable_tuple,
            arrayMap(x -> toUInt64(x), arrayEnumerate(arrayWithConstant(number, toUInt64(0)))) AS number_array,
            arrayMap(x -> rightPad(toString(x), x, '-'), number_array) AS text_array,
            arrayZip(number_array, text_array) AS number_text_tuple_array,
            mapFromArrays(number_array, text_array) AS number_to_text_map,
            toLowCardinality(text) as low_cardinality_text,
            toLowCardinality(nullable_text) as low_cardinality_nullable
        FROM system.numbers
        LIMIT {limit:UInt64}",
        )
        .param("limit", NUM_ROWS)
        .fetch_native()
        .unwrap();

    let block = cursor.next().await.unwrap().expect("expected one block");

    assert_eq!(block.num_rows() as u64, NUM_ROWS);

    assert_eq!(block.columns().len(), 12);

    assert_eq!(block[0].name(), "number");

    let mut number_iter = block["number"].iter::<u64>().unwrap();

    for (number, row) in number_iter.by_ref().zip(0u64..NUM_ROWS) {
        assert_eq!(number.unwrap(), row);
    }

    assert!(number_iter.next().is_none());

    let mut text_iter = block["text"].iter::<&str>().unwrap();

    for (text, row) in text_iter.by_ref().zip(0u64..NUM_ROWS) {
        let text = text.unwrap();

        assert_eq!(text.len() as u64, row);

        if row > 0 {
            assert_eq!(text, format!("{row:.>width$}", width = row as usize));
        }
    }

    assert!(text_iter.next().is_none());

    let mut nullable_number_iter = block["nullable_number"].iter::<Option<u64>>().unwrap();

    for (opt_number, row) in nullable_number_iter.by_ref().zip(0u64..NUM_ROWS) {
        let opt_number = opt_number.unwrap();

        if row % 2 == 0 {
            assert_eq!(opt_number, Some(row));
        } else {
            assert_eq!(opt_number, None);
        }
    }

    assert!(nullable_number_iter.next().is_none());

    let mut nullable_text_iter = block["nullable_text"].iter::<Option<&str>>().unwrap();

    for (opt_text, row) in nullable_text_iter.by_ref().zip(0u64..NUM_ROWS) {
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

    assert!(nullable_text_iter.next().is_none());

    let mut tuple_iter = block["number_text_tuple"].iter::<(u64, &str)>().unwrap();

    for (tuple, row) in tuple_iter.by_ref().zip(0u64..NUM_ROWS) {
        let (number, text) = tuple.unwrap();

        assert_eq!(number, row);
        assert_eq!(text.len() as u64, row);

        if row > 0 {
            assert_eq!(text, format!("{row:.>width$}", width = row as usize));
        }
    }

    assert!(tuple_iter.next().is_none());

    let mut nullable_tuple_iter = block["nullable_tuple"]
        .iter::<(Option<u64>, Option<&str>)>()
        .unwrap();

    for (tuple, row) in nullable_tuple_iter.by_ref().zip(0u64..NUM_ROWS) {
        let (opt_number, opt_text) = tuple.unwrap();

        if row % 2 == 0 {
            assert_eq!(opt_number, Some(row));
        } else {
            assert_eq!(opt_number, None);
        }

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

    for (number_array, row) in number_array_iter.by_ref().zip(0u64..NUM_ROWS) {
        let number_array = number_array.unwrap();

        assert_eq!(number_array.len() as u64, row);

        assert!(
            number_array.iter().copied().eq(1u64..=row),
            "invalid number_array for row #{row}: {number_array:?}"
        );
    }

    assert!(number_array_iter.next().is_none());

    let mut text_array_iter = block["text_array"].iter::<Vec<&str>>().unwrap();

    for (text_array, row) in text_array_iter.by_ref().zip(0u64..NUM_ROWS) {
        let text_array = text_array.unwrap();

        assert_eq!(text_array.len() as u64, row);

        for (text, len) in text_array.iter().zip(1..=row) {
            assert_eq!(*text, format!("{len:-<width$}", width = len as usize));
        }
    }

    assert!(text_array_iter.next().is_none());

    let mut map_iter = block["number_to_text_map"]
        .iter::<HashMap<u64, String>>()
        .unwrap();

    for (map, row) in map_iter.by_ref().zip(0u64..NUM_ROWS) {
        let map = map.unwrap();

        assert_eq!(map.len() as u64, row);

        for i in 1..=row {
            assert_eq!(map[&i], format!("{i:-<width$}", width = i as usize))
        }
    }

    assert!(map_iter.next().is_none());

    // Should be identical to `text` column
    let mut lc_text_iter = block["low_cardinality_text"].iter::<&str>().unwrap();

    for (text, row) in lc_text_iter.by_ref().zip(0u64..NUM_ROWS) {
        let text = text.unwrap();

        assert_eq!(text.len() as u64, row);

        if row > 0 {
            assert_eq!(text, format!("{row:.>width$}", width = row as usize));
        }
    }

    assert!(lc_text_iter.next().is_none());

    // Should be identical to `nullable_text` column
    let mut lc_nullable_text_iter = block["low_cardinality_nullable"]
        .iter::<Option<&str>>()
        .unwrap();

    for (opt_text, row) in lc_nullable_text_iter.by_ref().zip(0u64..NUM_ROWS) {
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

    assert!(lc_nullable_text_iter.next().is_none());
}
