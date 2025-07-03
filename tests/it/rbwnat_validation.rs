use crate::{execute_statements, geo_types::*, get_client};
use clickhouse_derive::Row;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use serde_repr::{Deserialize_repr, Serialize_repr};
use clickhouse::sql::Identifier;

#[tokio::test]
async fn fetch_primitive_row() {
    let client = get_client();
    let result = client
        .query("SELECT count() FROM (SELECT * FROM system.numbers LIMIT 3)")
        .fetch_one::<u64>()
        .await;
    assert_eq!(result.unwrap(), 3);
}

#[tokio::test]
async fn fetch_primitive_row_schema_mismatch() {
    type Data = i32; // expected type is UInt64
    assert_panic_on_fetch!(
        &["primitive", "UInt64", "i32"],
        "SELECT count() FROM (SELECT * FROM system.numbers LIMIT 3)"
    );
}

#[tokio::test]
async fn fetch_vector_row() {
    let client = get_client();
    let result = client
        .query("SELECT [1, 2, 3] :: Array(UInt32)")
        .fetch_one::<Vec<u32>>()
        .await;
    assert_eq!(result.unwrap(), vec![1, 2, 3]);
}

#[tokio::test]
async fn fetch_vector_row_schema_mismatch_nested_type() {
    type Data = Vec<i128>; // expected type for Array(UInt32) is Vec<u32>
    assert_panic_on_fetch!(
        &["vector", "UInt32", "i128"],
        "SELECT [1, 2, 3] :: Array(UInt32)"
    );
}

#[tokio::test]
async fn fetch_tuple_row() {
    let client = get_client();
    let result = client
        .query("SELECT 42 :: UInt32 AS a, 'foo' :: String AS b")
        .fetch_one::<(u32, String)>()
        .await;
    assert_eq!(result.unwrap(), (42, "foo".to_string()));
}

#[tokio::test]
async fn fetch_tuple_row_schema_mismatch_first_element() {
    type Data = (i128, String); // expected u32 instead of i128
    assert_panic_on_fetch!(
        &["tuple", "UInt32", "i128"],
        "SELECT 42 :: UInt32 AS a, 'foo' :: String AS b"
    );
}

#[tokio::test]
async fn fetch_tuple_row_schema_mismatch_second_element() {
    type Data = (u32, i64); // expected String instead of i64
    assert_panic_on_fetch!(
        &["tuple", "String", "i64"],
        "SELECT 42 :: UInt32 AS a, 'foo' :: String AS b"
    );
}

#[tokio::test]
async fn fetch_tuple_row_schema_mismatch_missing_element() {
    type Data = (u32, String); // expected to have the third element as i64
    assert_panic_on_fetch!(
        &[
            "database schema has 3 columns",
            "tuple definition has 2 fields"
        ],
        "SELECT 42 :: UInt32 AS a, 'foo' :: String AS b, 144 :: Int64 AS c"
    );
}

#[tokio::test]
async fn fetch_tuple_row_schema_mismatch_too_many_elements() {
    type Data = (u32, String, i128); // i128 should not be there
    assert_panic_on_fetch!(
        &[
            "database schema has 2 columns",
            "tuple definition has 3 fields"
        ],
        "SELECT 42 :: UInt32 AS a, 'foo' :: String AS b"
    );
}

#[tokio::test]
async fn fetch_tuple_row_with_struct() {
    #[derive(Debug, Row, Serialize, Deserialize, PartialEq)]
    struct Data {
        a: u32,
        b: String,
    }

    let client = get_client();
    let result = client
        .query("SELECT 42 :: UInt32 AS a, 'foo' :: String AS b, 144 :: UInt64 AS c")
        .fetch_one::<(Data, u64)>()
        .await;
    assert_eq!(
        result.unwrap(),
        (
            Data {
                a: 42,
                b: "foo".to_string()
            },
            144
        )
    );
}

#[tokio::test]
async fn fetch_tuple_row_with_struct_schema_mismatch() {
    #[derive(Debug, Row, Serialize, Deserialize, PartialEq)]
    struct _Data {
        a: u64, // expected type is u32
        b: String,
    }
    type Data = (_Data, u64);
    assert_panic_on_fetch!(
        &["tuple", "UInt32", "u64"],
        "SELECT 42 :: UInt32 AS a, 'foo' :: String AS b, 144 :: UInt64 AS c"
    );
}

#[tokio::test]
async fn fetch_tuple_row_with_struct_schema_mismatch_too_many_struct_fields() {
    #[derive(Debug, Row, Serialize, Deserialize, PartialEq)]
    struct _Data {
        a: u32,
        b: String,
        c: u64, // this field should not be here
    }
    type Data = (_Data, u64);
    assert_panic_on_fetch!(
        &["3 columns", "4 fields"],
        "SELECT 42 :: UInt32 AS a, 'foo' :: String AS b, 144 :: UInt64 AS c"
    );
}

#[tokio::test]
async fn fetch_tuple_row_with_struct_schema_mismatch_too_many_fields() {
    #[derive(Debug, Row, Serialize, Deserialize, PartialEq)]
    struct _Data {
        a: u32,
        b: String,
    }
    type Data = (_Data, u64, u64); // one too many u64
    assert_panic_on_fetch!(
        &["3 columns", "4 fields"],
        "SELECT 42 :: UInt32 AS a, 'foo' :: String AS b, 144 :: UInt64 AS c"
    );
}

#[tokio::test]
async fn fetch_tuple_row_with_struct_schema_mismatch_too_few_struct_fields() {
    #[derive(Debug, Row, Serialize, Deserialize, PartialEq)]
    struct _Data {
        a: u32, // the second field is missing now
    }
    type Data = (_Data, u64);
    assert_panic_on_fetch!(
        &["3 columns", "2 fields"],
        "SELECT 42 :: UInt32 AS a, 'foo' :: String AS b, 144 :: UInt64 AS c"
    );
}

#[tokio::test]
async fn fetch_tuple_row_with_struct_schema_mismatch_too_few_fields() {
    #[derive(Debug, Row, Serialize, Deserialize, PartialEq)]
    struct _Data {
        a: u32,
        b: String,
    }
    type Data = (_Data, u64); // another u64 is missing here
    assert_panic_on_fetch!(
        &["4 columns", "3 fields"],
        "SELECT 42 :: UInt32 AS a, 'foo' :: String AS b, 144 :: UInt64 AS c, 255 :: UInt64 AS d"
    );
}

#[tokio::test]
async fn basic_types() {
    #[derive(Debug, Row, Serialize, Deserialize, PartialEq)]
    struct Data {
        uint8_val: u8,
        uint16_val: u16,
        uint32_val: u32,
        uint64_val: u64,
        uint128_val: u128,
        int8_val: i8,
        int16_val: i16,
        int32_val: i32,
        int64_val: i64,
        int128_val: i128,
        float32_val: f32,
        float64_val: f64,
        string_val: String,
    }

    let client = get_client();
    let result = client
        .query(
            "
            SELECT
                255                                      :: UInt8   AS uint8_val,
                65535                                    :: UInt16  AS uint16_val,
                4294967295                               :: UInt32  AS uint32_val,
                18446744073709551615                     :: UInt64  AS uint64_val,
                340282366920938463463374607431768211455  :: UInt128 AS uint128_val,
                -128                                     :: Int8    AS int8_val,
                -32768                                   :: Int16   AS int16_val,
                -2147483648                              :: Int32   AS int32_val,
                -9223372036854775808                     :: Int64   AS int64_val,
                -170141183460469231731687303715884105728 :: Int128  AS int128_val,
                42.0                                     :: Float32 AS float32_val,
                144.0                                    :: Float64 AS float64_val,
                'test'                                   :: String  AS string_val
            ",
        )
        .fetch_one::<Data>()
        .await;

    assert_eq!(
        result.unwrap(),
        Data {
            uint8_val: 255,
            uint16_val: 65535,
            uint32_val: 4294967295,
            uint64_val: 18446744073709551615,
            uint128_val: 340282366920938463463374607431768211455,
            int8_val: -128,
            int16_val: -32768,
            int32_val: -2147483648,
            int64_val: -9223372036854775808,
            int128_val: -170141183460469231731687303715884105728,
            float32_val: 42.0,
            float64_val: 144.0,
            string_val: "test".to_string(),
        }
    );
}

#[tokio::test]
async fn borrowed_data() {
    #[derive(Debug, Row, Serialize, Deserialize, PartialEq)]
    struct Data<'a> {
        str: &'a str,
        array: Vec<&'a str>,
        tuple: (&'a str, &'a str),
        str_opt: Option<&'a str>,
        vec_map_str: Vec<(&'a str, &'a str)>,
        vec_map_f32: Vec<(&'a str, f32)>,
        vec_map_nested: Vec<(&'a str, Vec<(&'a str, &'a str)>)>,
        hash_map_str: HashMap<&'a str, &'a str>,
        hash_map_f32: HashMap<&'a str, f32>,
        hash_map_nested: HashMap<&'a str, HashMap<&'a str, &'a str>>,
    }

    let client = get_client();
    let mut cursor = client
        .query(
            "
            SELECT * FROM
            (
            SELECT
                'a'                                     :: String                           AS str,
                ['b', 'c']                              :: Array(String)                    AS array,
                ('d', 'e')                              :: Tuple(String, String)            AS tuple,
                NULL                                    :: Nullable(String)                 AS str_opt,
                map('key1', 'value1', 'key2', 'value2') :: Map(String, String)              AS hash_map_str,
                map('key3', 100, 'key4', 200)           :: Map(String, Float32)             AS hash_map_f32,
                map('n1', hash_map_str)                 :: Map(String, Map(String, String)) AS hash_map_nested,
                hash_map_str                                                                AS vec_map_str,
                hash_map_f32                                                                AS vec_map_f32,
                hash_map_nested                                                             AS vec_map_nested
            UNION ALL
            SELECT
                'f'                                     :: String                           AS str,
                ['g', 'h']                              :: Array(String)                    AS array,
                ('i', 'j')                              :: Tuple(String, String)            AS tuple,
                'k'                                     :: Nullable(String)                 AS str_opt,
                map('key4', 'value4', 'key5', 'value5') :: Map(String, String)              AS hash_map_str,
                map('key6', 300, 'key7', 400)           :: Map(String, Float32)             AS hash_map_f32,
                map('n2', hash_map_str)                 :: Map(String, Map(String, String)) AS hash_map_nested,
                hash_map_str                                                                AS vec_map_str,
                hash_map_f32                                                                AS vec_map_f32,
                hash_map_nested                                                             AS vec_map_nested
            )
            ORDER BY str
            ",
        )
        .fetch::<Data<'_>>()
        .unwrap();

    assert_eq!(
        cursor.next().await.unwrap().unwrap(),
        Data {
            str: "a",
            array: vec!["b", "c"],
            tuple: ("d", "e"),
            str_opt: None,
            vec_map_str: vec![("key1", "value1"), ("key2", "value2")],
            vec_map_f32: vec![("key3", 100.0), ("key4", 200.0)],
            vec_map_nested: vec![("n1", vec![("key1", "value1"), ("key2", "value2")])],
            hash_map_str: HashMap::from([("key1", "value1"), ("key2", "value2")]),
            hash_map_f32: HashMap::from([("key3", 100.0), ("key4", 200.0)]),
            hash_map_nested: HashMap::from([(
                "n1",
                HashMap::from([("key1", "value1"), ("key2", "value2")]),
            )]),
        }
    );

    assert_eq!(
        cursor.next().await.unwrap().unwrap(),
        Data {
            str: "f",
            array: vec!["g", "h"],
            tuple: ("i", "j"),
            str_opt: Some("k"),
            vec_map_str: vec![("key4", "value4"), ("key5", "value5")],
            vec_map_f32: vec![("key6", 300.0), ("key7", 400.0)],
            vec_map_nested: vec![("n2", vec![("key4", "value4"), ("key5", "value5")])],
            hash_map_str: HashMap::from([("key4", "value4"), ("key5", "value5")]),
            hash_map_f32: HashMap::from([("key6", 300.0), ("key7", 400.0)]),
            hash_map_nested: HashMap::from([(
                "n2",
                HashMap::from([("key4", "value4"), ("key5", "value5")]),
            )]),
        },
    );

    assert!(cursor.next().await.unwrap().is_none());
}

#[tokio::test]
async fn many_numbers() {
    #[derive(Row, Deserialize)]
    struct Data {
        number: u64,
    }

    let client = get_client();
    let mut cursor = client
        .query("SELECT number FROM system.numbers_mt LIMIT 2000")
        .fetch::<Data>()
        .unwrap();

    let mut sum = 0;
    while let Some(row) = cursor.next().await.unwrap() {
        sum += row.number;
    }
    assert_eq!(sum, (0..2000).sum::<u64>());
}

#[tokio::test]
async fn map_as_vec_of_tuples_schema_mismatch() {
    #[derive(Debug, Row, Serialize, Deserialize, PartialEq)]
    struct Data {
        m: Vec<(u16, Vec<(String, i32)>)>,
    }

    assert_panic_on_fetch!(
        &["Data.m", "Map(Int64, String)", "Int64", "u16"],
        "SELECT map(100, 'value1', 200, 'value2') :: Map(Int64, String) AS m"
    );
}

#[tokio::test]
async fn map_as_vec_of_tuples_schema_mismatch_nested() {
    type Inner = Vec<(i32, i64)>; // the value should be i128 instead of i64

    #[derive(Debug, Row, Serialize, Deserialize, PartialEq)]
    struct Data {
        m: Vec<(u16, Vec<(String, Inner)>)>,
    }

    assert_panic_on_fetch!(
        &[
            "Data.m",
            "Map(UInt16, Map(String, Map(Int32, Int128)))",
            "Int128",
            "i64"
        ],
        "SELECT map(42, map('foo', map(144, 255)))
                :: Map(UInt16, Map(String, Map(Int32, Int128))) AS m"
    );
}

#[tokio::test]
async fn enums() {
    #[derive(Debug, PartialEq, Serialize_repr, Deserialize_repr)]
    #[repr(i8)]
    enum MyEnum8 {
        Winter = -128,
        Spring = 0,
        Summer = 100,
        Autumn = 127,
    }

    #[derive(Debug, PartialEq, Serialize_repr, Deserialize_repr)]
    #[repr(i16)]
    enum MyEnum16 {
        North = -32768,
        East = 0,
        South = 144,
        West = 32767,
    }

    #[derive(Debug, PartialEq, Row, Serialize, Deserialize)]
    struct Data {
        id: u16,
        enum8: MyEnum8,
        enum16: MyEnum16,
    }

    let table_name = "test_rbwnat_enum";

    let client = prepare_database!();
    client
        .query(
            "
            CREATE OR REPLACE TABLE ?
            (
                id     UInt16,
                enum8  Enum8 ('Winter' = -128,   'Spring' = 0, 'Summer' = 100, 'Autumn' = 127),
                enum16 Enum16('North'  = -32768, 'East'   = 0, 'South'  = 144, 'West'   = 32767)
            ) ENGINE MergeTree ORDER BY id
            ",
        )
        .bind(Identifier(table_name))
        .execute()
        .await
        .unwrap();

    let expected = vec![
        Data {
            id: 1,
            enum8: MyEnum8::Spring,
            enum16: MyEnum16::East,
        },
        Data {
            id: 2,
            enum8: MyEnum8::Autumn,
            enum16: MyEnum16::North,
        },
        Data {
            id: 3,
            enum8: MyEnum8::Winter,
            enum16: MyEnum16::South,
        },
        Data {
            id: 4,
            enum8: MyEnum8::Summer,
            enum16: MyEnum16::West,
        },
    ];

    let mut insert = client.insert::<Data>(table_name).await.unwrap();
    for row in &expected {
        insert.write(row).await.unwrap()
    }
    insert.end().await.unwrap();

    let result = client
        .query("SELECT * FROM ? ORDER BY id ASC")
        .bind(Identifier(table_name))
        .fetch_all::<Data>()
        .await
        .unwrap();

    assert_eq!(result, expected);
}

#[tokio::test]
async fn nullable() {
    #[derive(Debug, Row, Serialize, Deserialize, PartialEq)]
    struct Data {
        a: u32,
        b: Option<i64>,
    }

    let client = get_client();
    let result = client
        .query(
            "
            SELECT * FROM (
                SELECT 1 :: UInt32 AS a, 2    :: Nullable(Int64) AS b
                UNION ALL
                SELECT 3 :: UInt32 AS a, NULL :: Nullable(Int64) AS b
                UNION ALL
                SELECT 4 :: UInt32 AS a, 5    :: Nullable(Int64) AS b
            )
            ORDER BY a ASC
            ",
        )
        .fetch_all::<Data>()
        .await;

    assert_eq!(
        result.unwrap(),
        vec![
            Data { a: 1, b: Some(2) },
            Data { a: 3, b: None },
            Data { a: 4, b: Some(5) },
        ]
    );
}

#[tokio::test]
async fn invalid_nullable() {
    #[derive(Debug, Row, Serialize, Deserialize, PartialEq)]
    struct Data {
        n: Option<u32>,
    }
    assert_panic_on_fetch!(
        &["Data.n", "Array(UInt32)", "Option<T>"],
        "SELECT array(42) :: Array(UInt32) AS n"
    );
}

#[tokio::test]
async fn invalid_low_cardinality() {
    #[derive(Debug, Row, Serialize, Deserialize, PartialEq)]
    struct Data {
        a: u32,
    }
    let client = get_client().with_option("allow_suspicious_low_cardinality_types", "1");
    assert_panic_on_fetch_with_client!(
        client,
        &["Data.a", "LowCardinality(Int32)", "u32"],
        "SELECT 144 :: LowCardinality(Int32) AS a"
    );
}

#[tokio::test]
async fn invalid_nullable_low_cardinality() {
    #[derive(Debug, Row, Serialize, Deserialize, PartialEq)]
    struct Data {
        a: Option<u32>,
    }
    let client = get_client().with_option("allow_suspicious_low_cardinality_types", "1");
    assert_panic_on_fetch_with_client!(
        client,
        &["Data.a", "LowCardinality(Nullable(Int32))", "u32"],
        "SELECT 144 :: LowCardinality(Nullable(Int32)) AS a"
    );
}

#[tokio::test]
#[cfg(feature = "time")]
async fn invalid_serde_with() {
    #[derive(Debug, Row, Serialize, Deserialize, PartialEq)]
    struct Data {
        #[serde(with = "clickhouse::serde::time::datetime64::millis")]
        n1: time::OffsetDateTime, // underlying is still Int64; should not compose it from two (U)Int32
    }
    assert_panic_on_fetch!(&["Data.n1", "UInt32", "i64"], "SELECT 42 :: UInt32 AS n1");
}

#[tokio::test]
async fn too_many_struct_fields() {
    #[derive(Debug, Row, Serialize, Deserialize, PartialEq)]
    struct Data {
        a: u32,
        b: u32,
        c: u32,
    }
    assert_panic_on_fetch!(
        &["2 columns", "3 fields"],
        "SELECT 42 :: UInt32 AS a, 144 :: UInt32 AS b"
    );
}

#[tokio::test]
async fn fixed_str_too_long() {
    #[derive(Debug, Row, Serialize, Deserialize, PartialEq)]
    struct Data {
        a: [u8; 4],
        b: [u8; 3],
    }
    assert_panic_on_fetch!(
        &["Data.a", "FixedString(5)", "with length 4"],
        "SELECT '12345' :: FixedString(5) AS a, '777' :: FixedString(3) AS b"
    );
}

#[tokio::test]
async fn tuple() {
    #[derive(Debug, Row, Serialize, Deserialize, PartialEq)]
    struct Data {
        a: (u32, String),
        b: (i128, HashMap<u16, String>),
    }

    let client = get_client();
    let result = client
        .query(
            "
            SELECT
                (42, 'foo')            :: Tuple(UInt32, String)              AS a,
                (144, map(255, 'bar')) :: Tuple(Int128, Map(UInt16, String)) AS b
            ",
        )
        .fetch_one::<Data>()
        .await;

    assert_eq!(
        result.unwrap(),
        Data {
            a: (42, "foo".to_string()),
            b: (144, vec![(255, "bar".to_string())].into_iter().collect()),
        }
    );
}

#[tokio::test]
async fn tuple_invalid_definition() {
    #[derive(Debug, Row, Serialize, Deserialize, PartialEq)]
    struct Data {
        a: (u32, String),
        b: (i128, HashMap<u16, String>),
    }
    // Map key is UInt64 instead of UInt16 requested in the struct
    assert_panic_on_fetch!(
        &[
            "Data.b",
            "Tuple(Int128, Map(UInt64, String))",
            "UInt64 as u16"
        ],
        "
        SELECT
            (42, 'foo')            :: Tuple(UInt32, String)              AS a,
            (144, map(255, 'bar')) :: Tuple(Int128, Map(UInt64, String)) AS b
        "
    );
}

#[tokio::test]
async fn tuple_too_many_elements_in_the_schema() {
    #[derive(Debug, Row, Serialize, Deserialize, PartialEq)]
    struct Data {
        a: (u32, String),
        b: (i128, HashMap<u16, String>),
    }
    // too many elements in the db type definition
    assert_panic_on_fetch!(
        &[
            "Data.a",
            "Tuple(UInt32, String, Bool)",
            "remaining elements: Bool"
        ],
        "
        SELECT
            (42, 'foo', true)      :: Tuple(UInt32, String, Bool)        AS a,
            (144, map(255, 'bar')) :: Tuple(Int128, Map(UInt16, String)) AS b
        "
    );
}

#[tokio::test]
async fn tuple_too_many_elements_in_the_struct() {
    #[derive(Debug, Row, Serialize, Deserialize, PartialEq)]
    struct Data {
        a: (u32, String, bool),
        b: (i128, HashMap<u16, String>),
    }
    // too many elements in the struct enum
    assert_panic_on_fetch!(
        &["Data.a", "Tuple(UInt32, String)", "deserialize bool"],
        "
        SELECT
            (42, 'foo')            :: Tuple(UInt32, String)              AS a,
            (144, map(255, 'bar')) :: Tuple(Int128, Map(UInt16, String)) AS b
        "
    );
}

#[tokio::test]
async fn deeply_nested_validation_incorrect_fixed_string() {
    #[derive(Debug, Row, Serialize, Deserialize, PartialEq)]
    struct Data {
        id: u32,
        col: Vec<Vec<HashMap<u32, Vec<[u8; 2]>>>>,
    }
    // Struct has FixedString(2) instead of FixedString(1)
    assert_panic_on_fetch!(
        &["Data.col", "FixedString(1)", "with length 2"],
        "
        SELECT
            42                                     :: UInt32                                           AS id,
            array(array(map(42, array('1', '2')))) :: Array(Array(Map(UInt32, Array(FixedString(1))))) AS col
        "
    );
}

#[tokio::test]
async fn geo() {
    #[derive(Clone, Debug, PartialEq)]
    #[derive(Row, serde::Serialize, serde::Deserialize)]
    struct Data {
        id: u32,
        point: Point,
        ring: Ring,
        polygon: Polygon,
        multi_polygon: MultiPolygon,
        line_string: LineString,
        multi_line_string: MultiLineString,
    }

    let client = get_client();
    let result = client
        .query(
            "
            SELECT
                42                                               :: UInt32          AS id,
                (1.0, 2.0)                                       :: Point           AS point,
                [(3.0, 4.0), (5.0, 6.0)]                         :: Ring            AS ring,
                [[(7.0, 8.0), (9.0, 10.0)], [(11.0, 12.0)]]      :: Polygon         AS polygon,
                [[[(13.0, 14.0), (15.0, 16.0)], [(17.0, 18.0)]]] :: MultiPolygon    AS multi_polygon,
                [(19.0, 20.0), (21.0, 22.0)]                     :: LineString      AS line_string,
                [[(23.0, 24.0), (25.0, 26.0)], [(27.0, 28.0)]]   :: MultiLineString AS multi_line_string
            ",
        )
        .fetch_one::<Data>()
        .await;

    assert_eq!(
        result.unwrap(),
        Data {
            id: 42,
            point: (1.0, 2.0),
            ring: vec![(3.0, 4.0), (5.0, 6.0)],
            polygon: vec![vec![(7.0, 8.0), (9.0, 10.0)], vec![(11.0, 12.0)]],
            multi_polygon: vec![vec![vec![(13.0, 14.0), (15.0, 16.0)], vec![(17.0, 18.0)]]],
            line_string: vec![(19.0, 20.0), (21.0, 22.0)],
            multi_line_string: vec![vec![(23.0, 24.0), (25.0, 26.0)], vec![(27.0, 28.0)]],
        }
    );
}

// TODO: there are two panics; one about schema mismatch,
//  another about not all Tuple elements being deserialized
//  not easy to assert, same applies to the other Geo types
#[ignore]
#[tokio::test]
async fn geo_invalid_point() {
    #[derive(Debug, Row, Serialize, Deserialize, PartialEq)]
    struct Data {
        id: u32,
        pt: (i32, i32),
    }
    assert_panic_on_fetch!(
        &["Data.pt", "Point", "Float64 as i32"],
        "
            SELECT
                42         :: UInt32 AS id,
                (1.0, 2.0) :: Point  AS pt
        "
    );
}

#[tokio::test]
/// See https://github.com/ClickHouse/clickhouse-rs/issues/100
async fn issue_100() {
    {
        #[derive(Debug, Row, Serialize, Deserialize, PartialEq)]
        struct Data {
            n: i8,
        }
        assert_panic_on_fetch!(
            &["Data.n", "Nullable(Bool)", "i8"],
            "SELECT NULL :: Nullable(Bool) AS n"
        );
    }

    {
        #[derive(Debug, Row, Serialize, Deserialize, PartialEq)]
        struct Data {
            n: u8,
        }
        assert_panic_on_fetch!(
            &["Data.n", "Nullable(Bool)", "u8"],
            "SELECT NULL :: Nullable(Bool) AS n"
        );
    }

    {
        #[derive(Debug, Row, Serialize, Deserialize, PartialEq)]
        struct Data {
            n: bool,
        }
        assert_panic_on_fetch!(
            &["Data.n", "Nullable(Bool)", "bool"],
            "SELECT NULL :: Nullable(Bool) AS n"
        );
    }
}

// TODO: unignore after insert implementation uses RBWNAT, too
#[ignore]
#[tokio::test]
/// See https://github.com/ClickHouse/clickhouse-rs/issues/109#issuecomment-2243197221
async fn issue_109_1() {
    #[derive(Debug, Serialize, Deserialize, Row)]
    struct Data {
        #[serde(skip_deserializing)]
        en_id: String,
        journey: u32,
        drone_id: String,
        call_sign: String,
    }
    let client = prepare_database!();
    execute_statements(
        &client,
        &[
            "
            CREATE TABLE issue_109 (
                drone_id  String,
                call_sign String,
                journey   UInt32,
                en_id     String,
            )
            ENGINE = MergeTree
            ORDER BY (drone_id)
            ",
            "
            INSERT INTO issue_109 VALUES
                ('drone_1', 'call_sign_1', 1, 'en_id_1'),
                ('drone_2', 'call_sign_2', 2, 'en_id_2'),
                ('drone_3', 'call_sign_3', 3, 'en_id_3')
            ",
        ],
    )
    .await;

    let data = client
        .query("SELECT journey, drone_id, call_sign FROM issue_109")
        .fetch_all::<Data>()
        .await
        .unwrap();
    let mut insert = client.insert::<Data>("issue_109").await.unwrap();
    for (id, elem) in data.iter().enumerate() {
        let elem = Data {
            en_id: format!("ABC-{}", id),
            journey: elem.journey,
            drone_id: elem.drone_id.clone(),
            call_sign: elem.call_sign.clone(),
        };
        insert.write(&elem).await.unwrap();
    }
    insert.end().await.unwrap();
}

#[tokio::test]
async fn issue_112() {
    #[derive(Debug, Row, Serialize, Deserialize, PartialEq)]
    struct Data {
        a: bool,
        b: bool,
    }

    assert_panic_on_fetch!(
        &["Data.a", "Nullable(Bool)", "bool"],
        "WITH (SELECT true) AS a, (SELECT true) AS b SELECT ?fields"
    );
}

#[tokio::test]
/// See https://github.com/ClickHouse/clickhouse-rs/issues/113
async fn issue_113() {
    #[derive(Debug, Row, Serialize, Deserialize, PartialEq)]
    struct Data {
        a: u64,
        b: f64,
        c: f64,
    }
    let client = prepare_database!();
    execute_statements(&client, &[
        "
        CREATE TABLE issue_113_1(
            id UInt32
        )
        ENGINE MergeTree
        ORDER BY id
        ",
        "
        CREATE TABLE issue_113_2(
            id  UInt32,
            pos Float64
        )
        ENGINE MergeTree
        ORDER BY id
        ",
        "INSERT INTO issue_113_1 VALUES (1), (2), (3), (4), (5), (6), (7), (8), (9), (10)",
        "INSERT INTO issue_113_2 VALUES (1, 100.5), (2, 200.2), (3, 300.3), (4, 444.4), (5, 555.5)",
    ]).await;

    // Struct should have had Option<f64> instead of f64
    assert_panic_on_fetch_with_client!(
        client,
        &["Data.b", "Nullable(Float64)", "f64"],
        "
        SELECT
            COUNT(*)                                                 AS a,
            (COUNT(*) / (SELECT COUNT(*) FROM issue_113_1)) * 100.0  AS b,
            AVG(pos)                                                 AS c
        FROM issue_113_2
        "
    );
}

#[tokio::test]
#[cfg(feature = "time")]
/// See https://github.com/ClickHouse/clickhouse-rs/issues/114
async fn issue_114() {
    #[derive(Row, Deserialize, Debug, PartialEq)]
    struct Data {
        #[serde(with = "clickhouse::serde::time::date")]
        date: time::Date,
        arr: Vec<HashMap<String, String>>,
    }

    let client = get_client();
    let result = client
        .query(
            "
            SELECT
                '2023-05-01'                           :: Date                       AS date,
                array(map('k1', 'v1'), map('k2', 'v2')) :: Array(Map(String, String)) AS arr
            ",
        )
        .fetch_one::<Data>()
        .await;

    assert_eq!(
        result.unwrap(),
        Data {
            date: time::Date::from_calendar_date(2023, time::Month::May, 1).unwrap(),
            arr: vec![
                HashMap::from([("k1".to_owned(), "v1".to_owned())]),
                HashMap::from([("k2".to_owned(), "v2".to_owned())]),
            ],
        }
    );
}

#[tokio::test]
#[cfg(feature = "time")]
/// See https://github.com/ClickHouse/clickhouse-rs/issues/173
async fn issue_173() {
    #[derive(Debug, Serialize, Deserialize, Row)]
    struct Data {
        log_id: String,
        #[serde(with = "clickhouse::serde::time::datetime")]
        ts: time::OffsetDateTime,
    }

    let client = prepare_database!().with_option("date_time_input_format", "best_effort");

    execute_statements(&client, &[
        "
        CREATE OR REPLACE TABLE logs (
          log_id      String,
          timestamp   DateTime('Europe/Berlin')
        )
        ENGINE = MergeTree()
        PRIMARY KEY (log_id, timestamp)
        ",
        "INSERT INTO logs VALUES ('56cde52f-5f34-45e0-9f08-79d6f582e913', '2024-11-05T11:52:52+01:00')",
        "INSERT INTO logs VALUES ('0e967129-6271-44f2-967b-0c8d11a60fdc', '2024-11-05T11:59:21+01:00')",
    ]).await;

    // panics as we fetch `ts` two times: one from `?fields` macro, and the second time explicitly
    // the resulting dataset will, in fact, contain 3 columns instead of 2:
    assert_panic_on_fetch_with_client!(
        client,
        &["3 columns", "2 fields"],
        "SELECT ?fields, toUnixTimestamp(timestamp) AS ts FROM logs ORDER by ts DESC"
    );
}

#[tokio::test]
/// See https://github.com/ClickHouse/clickhouse-rs/issues/185
async fn issue_185() {
    #[derive(Row, Deserialize, Debug, PartialEq)]
    struct Data {
        pk: u32,
        decimal_col: Option<String>,
    }

    let client = prepare_database!();
    execute_statements(
        &client,
        &[
            "
            CREATE TABLE issue_185(
                pk UInt32,
                decimal_col Nullable(Decimal(10, 4)))
            ENGINE MergeTree
            ORDER BY pk
            ",
            "INSERT INTO issue_185 VALUES (1, 1.1), (2, 2.2), (3, 3.3)",
        ],
    )
    .await;

    assert_panic_on_fetch_with_client!(
        client,
        &["Data.decimal_col", "Decimal(10, 4)", "String"],
        "SELECT ?fields FROM issue_185"
    );
}

#[tokio::test]
#[cfg(feature = "chrono")]
async fn issue_218() {
    #[derive(Row, Serialize, Deserialize, Debug)]
    struct Data {
        max_time: chrono::DateTime<chrono::Utc>,
    }

    let client = prepare_database!();
    execute_statements(
        &client,
        &["
            CREATE TABLE IF NOT EXISTS issue_218 (
               my_time DateTime64(3, 'UTC') CODEC(Delta, ZSTD),
            ) ENGINE = MergeTree
            ORDER BY my_time
            "],
    )
    .await;

    // FIXME: It is not a super clear panic as it hints about `&str`,
    //  and not about the missing attribute for `chrono::DateTime`.
    //  Still better than a `premature end of input` error, though.
    assert_panic_on_fetch_with_client!(
        client,
        &["Data.max_time", "DateTime64(3, 'UTC')", "&str"],
        "SELECT max(my_time) AS max_time FROM issue_218"
    );
}

#[tokio::test]
async fn variant_wrong_definition() {
    #[derive(Debug, Deserialize, PartialEq)]
    enum MyVariant {
        Str(String),
        U32(u32),
    }

    #[derive(Debug, Row, Deserialize, PartialEq)]
    struct Data {
        id: u8,
        var: MyVariant,
    }

    let client = get_client().with_option("allow_experimental_variant_type", "1");

    assert_panic_on_fetch_with_client!(
        client,
        &["Data.var", "Variant(String, UInt16)", "u32"],
        "
        SELECT * FROM (
            SELECT 0 :: UInt8 AS id, 'foo' :: Variant(String, UInt16) AS var
            UNION ALL
            SELECT 1 :: UInt8 AS id, 144   :: Variant(String, UInt16) AS var
        ) ORDER BY id ASC
        "
    );
}

#[tokio::test]
async fn decimal32_wrong_size() {
    #[derive(Debug, Row, Serialize, Deserialize, PartialEq)]
    struct Data {
        decimal32: i16,
    }

    assert_panic_on_fetch!(
        &["Data.decimal32", "Decimal(9, 4)", "i16"],
        "SELECT 42 :: Decimal32(4) AS decimal32"
    );
}

#[tokio::test]
async fn decimal64_wrong_size() {
    #[derive(Debug, Row, Serialize, Deserialize, PartialEq)]
    struct Data {
        decimal64: i32,
    }

    assert_panic_on_fetch!(
        &["Data.decimal64", "Decimal(18, 8)", "i32"],
        "SELECT 144 :: Decimal64(8) AS decimal64"
    );
}

#[tokio::test]
async fn decimal128_wrong_size() {
    #[derive(Debug, Row, Serialize, Deserialize, PartialEq)]
    struct Data {
        decimal128: i64,
    }

    assert_panic_on_fetch!(
        &["Data.decimal128", "Decimal(38, 12)", "i64"],
        "SELECT -17014118346046923173168730.37158841057 :: Decimal128(12) AS decimal128"
    );
}

#[tokio::test]
async fn different_struct_field_order_same_types() {
    #[derive(Debug, Row, Deserialize, PartialEq)]
    struct Data {
        c: String,
        a: String,
    }

    let client = get_client();
    let result = client
        .query("SELECT 'foo' AS a, 'bar' :: String AS c")
        .fetch_one::<Data>()
        .await;

    assert_eq!(
        result.unwrap(),
        Data {
            c: "bar".to_string(),
            a: "foo".to_string(),
        }
    );
}

#[tokio::test]
async fn different_struct_field_order_different_types() {
    #[derive(Debug, Row, Deserialize, PartialEq)]
    struct Data {
        b: u32,
        a: String,
        c: Vec<bool>,
    }

    let client = get_client();
    let result = client
        .query(
            "
            SELECT array(true, false, true) AS c,
                   42 :: UInt32             AS b,
                   'foo'                    AS a
            ",
        )
        .fetch_one::<Data>()
        .await;

    assert_eq!(
        result.unwrap(),
        Data {
            c: vec![true, false, true],
            b: 42,
            a: "foo".to_string(),
        }
    );
}
