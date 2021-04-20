use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Serialize, Deserialize)]
struct Timestamp32(u32);

#[derive(Debug, PartialEq, Serialize, Deserialize)]
struct Timestamp64(u64);

#[derive(Debug, PartialEq, Serialize, Deserialize)]
struct FixedPoint64(i64);

#[derive(Debug, PartialEq, Serialize, Deserialize)]
struct FixedPoint128(i128);

#[derive(Debug, PartialEq, Serialize, Deserialize)]
struct Sample<'a> {
    int8: i8,
    int32: i32,
    int64: i64,
    uint8: u8,
    uint32: u32,
    uint64: u64,
    float32: f32,
    float64: f64,
    datetime: Timestamp32,
    datetime64: Timestamp64,
    decimal64: FixedPoint64,
    decimal128: FixedPoint128,
    string: &'a str,
    #[serde(with = "serde_bytes")]
    blob: &'a [u8],
    optional_decimal64: Option<FixedPoint64>,
    optional_datetime: Option<Timestamp32>,
    fixed_string: [u8; 4],
    array: Vec<i8>,
    boolean: bool,
}

fn sample() -> Sample<'static> {
    Sample {
        int8: -42,
        int32: -3242,
        int64: -6442,
        uint8: 42,
        uint32: 3242,
        uint64: 6442,
        float32: 42.42,
        float64: 42.42,
        datetime: Timestamp32(2_301_990_162),
        datetime64: Timestamp64(2_301_990_162_123),
        decimal64: FixedPoint64(4242 * 10_000_000),
        decimal128: FixedPoint128(4242 * 10_000_000),
        string: "01234",
        blob: &[0, 1, 2, 3, 4],
        optional_decimal64: None,
        optional_datetime: Some(Timestamp32(2_301_990_162)),
        fixed_string: [b'B', b'T', b'C', 0],
        array: vec![-42, 42, -42, 42],
        boolean: true,
    }
}

fn sample_serialized() -> Vec<u8> {
    vec![
        // [Int8] -42
        0xd6, /**/
        // [Int32] -3242
        0x56, 0xf3, 0xff, 0xff, /**/
        // [Int64] -6442
        0xd6, 0xe6, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, /**/
        // [UInt8] 42
        0x2a, /**/
        // [UInt32] 3242
        0xaa, 0x0c, 0x00, 0x00, /**/
        // [UInt64] 6442
        0x2a, 0x19, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, /**/
        // [Float32] 42.42
        0x14, 0xae, 0x29, 0x42, /**/
        // [Float64] 42.42
        0xf6, 0x28, 0x5c, 0x8f, 0xc2, 0x35, 0x45, 0x40, /**/
        // [DateTime] 2042-12-12 12:42:42
        //       (ts: 2301990162)
        0x12, 0x95, 0x35, 0x89, /**/
        // [DateTime64(3)] 2042-12-12 12:42:42'123
        //       (ts: 2301990162123)
        0xcb, 0x4e, 0x4e, 0xf9, 0x17, 0x02, 0x00, 0x00, /**/
        // [Decimal64(9)] 42.420000000
        0x00, 0xd5, 0x6d, 0xe0, 0x09, 0x00, 0x00, 0x00, /**/
        // [Decimal128(9)] 42.420000000
        0x00, 0xd5, 0x6d, 0xe0, 0x09, 0x00, 0x00, 0x00, /**/
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, /**/
        // [String] 5 "01234"
        0x05, 0x30, 0x31, 0x32, 0x33, 0x34, /**/
        // [String] 5 [0, 1, 2, 3, 4]
        0x05, 0x00, 0x01, 0x02, 0x03, 0x04, /**/
        // [Nullable(Decimal64(9))] NULL
        0x01, /**/
        // [Nullable(DateTime)] 2042-12-12 12:42:42
        //       (ts: 2301990162)
        0x00, 0x12, 0x95, 0x35, 0x89, /**/
        // [FixedString(4)] [b'B', b'T', b'C', 0]
        0x42, 0x54, 0x43, 0x00, /**/
        // [Array(Int32)] [-42, 42, -42, 42]
        0x04, 0xd6, 0x2a, 0xd6, 0x2a, /**/
        // [Boolean] true
        0x01, /**/
    ]
}

#[test]
fn it_serializes() {
    let mut actual = Vec::new();
    super::serialize_into(&mut actual, &sample()).unwrap();
    assert_eq!(actual, sample_serialized());
}

#[test]
fn it_deserializes() {
    use bytes::buf::Buf;

    let input = sample_serialized();
    let mut temp_buf = [0; 1024];

    for i in 0..input.len() {
        let (left, right) = input.split_at(i);

        // It shouldn't panic.
        let _: Result<Sample<'_>, _> = super::deserialize_from(left, &mut temp_buf);
        let _: Result<Sample<'_>, _> = super::deserialize_from(right, &mut temp_buf);

        let buf = left.chain(right);
        let actual: Sample<'_> = super::deserialize_from(buf, &mut temp_buf).unwrap();
        assert_eq!(actual, sample());
    }
}
