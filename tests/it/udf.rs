use bytes::Bytes;
use clickhouse::{Row, udf::Executor};
use serde::{Deserialize, Serialize};

// The Rust names differ from the SQL aliases. Only the field order and types matter.
#[derive(Row, Deserialize)]
struct Input<'a> {
    number: u32,
    #[serde(borrow, with = "serde_bytes")]
    data: &'a [u8],
}

#[derive(Row, Serialize)]
struct Output<'a> {
    number: u32,
    #[serde(with = "serde_bytes")]
    data: &'a [u8],
    length: u64,
}

#[tokio::test]
async fn executable_udf_wire_roundtrip() {
    let client = prepare_database!();
    let mut cursor = client
        .query(
            "SELECT * FROM (
            SELECT toUInt32(258) AS id, unhex('FF000A') AS payload
            UNION ALL SELECT toUInt32(259), ''
         ) ORDER BY id",
        )
        .fetch_bytes("RowBinary")
        .unwrap();
    let mut wire = b"3\n".to_vec();
    while let Some(bytes) = cursor.next().await.unwrap() {
        wire.extend(bytes);
    }
    // Append a third row encoded locally to the two rows produced by ClickHouse.
    wire.extend(260_u32.to_le_bytes());
    wire.extend(b"\x02ab");

    let mut output = Vec::new();
    Executor::new(wire.as_slice(), &mut output)
        .run::<Input<'_>, Output<'_>, _>(|row| {
            Ok(Output {
                number: row.number,
                data: row.data,
                length: row.data.len() as u64,
            })
        })
        .unwrap();

    client
        .query("CREATE TABLE udf_output (id UInt32, payload String, length UInt64) ENGINE = Memory")
        .execute()
        .await
        .unwrap();
    let mut insert = client.insert_formatted_with("INSERT INTO udf_output FORMAT RowBinary");
    insert.send(Bytes::from(output)).await.unwrap();
    insert.end().await.unwrap();
    let rows = client
        .query("SELECT id, hex(payload), length FROM udf_output ORDER BY id")
        .fetch_all::<(u32, String, u64)>()
        .await
        .unwrap();
    assert_eq!(
        rows,
        [
            (258, "FF000A".into(), 3),
            (259, "".into(), 0),
            (260, "6162".into(), 2),
        ]
    );
}
