use crate::get_client;

#[tokio::test]
async fn fixed_width_types() {
    let client = get_client();

    let mut cursor = client.query(
        "SELECT number, leftPad(toString(number), number, '.') text FROM system.numbers LIMIT 10"
    )
        .fetch_native()
        .unwrap();

    let block = cursor.next().await.unwrap().expect("expected one block");

    assert_eq!(block.num_rows(), 10);

    assert_eq!(block.columns().len(), 2);

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
}
