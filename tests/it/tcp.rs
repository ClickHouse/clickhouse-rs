//! Tests of the native TCP transport against the HTTP transport.
//!
//! Every query of the corpus runs over both transports on the same server,
//! and the responses must be byte-for-byte equal.
//!
//! The server is taken from `CLICKHOUSE_TEST_URL` (if it is a `tcp://` URL) or
//! `CLICKHOUSE_TEST_TCP_URL` (`tcp://localhost:9000` by default), and
//! `CLICKHOUSE_TEST_HTTP_URL` (`http://localhost:8123` by default) must be the same server.

use clickhouse::error::Error;
use clickhouse::{Client, Compression};

fn tcp_url() -> String {
    let url = crate::local_url();

    if url.starts_with("tcp") {
        url
    } else {
        std::env::var("CLICKHOUSE_TEST_TCP_URL").unwrap_or_else(|_| "tcp://localhost:9000".into())
    }
}

fn http_url() -> String {
    let url = crate::local_url();

    if url.starts_with("http") {
        url
    } else {
        std::env::var("CLICKHOUSE_TEST_HTTP_URL").unwrap_or_else(|_| "http://localhost:8123".into())
    }
}

macro_rules! check_local_env {
    () => {
        if crate::test_env() != crate::TestEnv::Local {
            eprintln!("Skipping test: it requires a local server");
            return;
        }
    };
}

fn http_client() -> Client {
    Client::default()
        .with_url(http_url())
        .with_compression(Compression::None)
}

fn tcp_client() -> Client {
    Client::default().with_url(tcp_url())
}

/// A query of the corpus, with the settings that it needs.
struct Case {
    name: &'static str,
    sql: &'static str,
    settings: &'static [(&'static str, &'static str)],
}

const fn case(name: &'static str, sql: &'static str) -> Case {
    Case {
        name,
        sql,
        settings: &[],
    }
}

const fn case_with(
    name: &'static str,
    sql: &'static str,
    settings: &'static [(&'static str, &'static str)],
) -> Case {
    Case {
        name,
        sql,
        settings,
    }
}

const VARIANT: &[(&str, &str)] = &[("allow_experimental_variant_type", "1")];
const LOW_CARDINALITY: &[(&str, &str)] = &[("allow_suspicious_low_cardinality_types", "1")];
const TIME: &[(&str, &str)] = &[("enable_time_time64_type", "1")];
const SMALL_BLOCKS: &[(&str, &str)] = &[("max_block_size", "7")];

const CORPUS: &[Case] = &[
    case("empty result", "SELECT number FROM system.numbers LIMIT 0"),
    case(
        "empty result of many types",
        "SELECT 1::UInt8 a, 'x' b, [1] c, map('a', 1) d, (1, 'x') e WHERE 0",
    ),
    case("constant", "SELECT 1"),
    case(
        "unsigned integers",
        "SELECT number::UInt8 a, (number * 1000)::UInt16 b, (number * 100000)::UInt32 c, (number * 1e15)::UInt64 d, (number * 1e30)::UInt128 e, (number * 1e60)::UInt256 f FROM numbers(10)",
    ),
    case(
        "signed integers",
        "SELECT (number - 5)::Int8 a, (number * -1000)::Int16 b, (number * -100000)::Int32 c, (number * -1e15)::Int64 d, (number * -1e30)::Int128 e, (number * -1e60)::Int256 f FROM numbers(10)",
    ),
    case(
        "integer limits",
        "SELECT 255::UInt8, 65535::UInt16, 4294967295::UInt32, 18446744073709551615::UInt64, -128::Int8, -32768::Int16, -2147483648::Int32, -9223372036854775808::Int64, toInt256('-57896044618658097711785492504343953926634992332820282019728792003956564819968'), toUInt256('115792089237316195423570985008687907853269984665640564039457584007913129639935')",
    ),
    case(
        "floats",
        "SELECT number / 3 AS a, (number / 7)::Float32 AS b, nan, inf, -inf, -0.0, 1e308, 1e-308::Float64 FROM numbers(5)",
    ),
    case(
        "bool",
        "SELECT number % 2 = 0 AS a, true, false FROM numbers(4)",
    ),
    case(
        "decimals",
        "SELECT toDecimal32(number / 3, 4) a, toDecimal64(-number / 7, 10) b, toDecimal128(number * 1e20, 12) c, toDecimal256(number * -1e40, 30) d FROM numbers(5)",
    ),
    case(
        "strings",
        "SELECT toString(number) a, repeat('x', number) b, '' c, 'üñíçødé 雪 🎉' d, '\\0\\n\\t\\'\\\\' e FROM numbers(6)",
    ),
    case(
        "binary strings",
        "SELECT unhex('00ff80fe7f') AS a, char(0, 200, 255) AS b",
    ),
    case("long string", "SELECT repeat('abcdefghij', 300000) AS s"),
    case(
        "fixed strings",
        "SELECT toFixedString(toString(number), 3) a, toFixedString('', 1) b, toFixedString('üñ', 8) c FROM numbers(5)",
    ),
    case(
        "uuid",
        "SELECT toUUID('12345678-1234-5678-1234-567812345678') a, generateUUIDv4(number) IS NOT NULL b, toUUID('00000000-0000-0000-0000-000000000000') c FROM numbers(3)",
    ),
    case(
        "dates",
        "SELECT toDate('2024-02-29') + number a, toDate32('1900-01-01') + number b, toDate('1970-01-01') c, toDate32('2299-12-31') d FROM numbers(3)",
    ),
    case(
        "date times",
        "SELECT toDateTime('2024-01-01 12:34:56', 'UTC') + number a, toDateTime('2024-01-01 00:00:00', 'Asia/Tokyo') b, toDateTime(0) c FROM numbers(3)",
    ),
    case(
        "date times 64",
        "SELECT toDateTime64('2024-01-01 12:34:56.123456789', 9, 'UTC') a, toDateTime64('1900-01-01 00:00:00.001', 3) b, toDateTime64(number, 6, 'Europe/Berlin') c FROM numbers(3)",
    ),
    case_with(
        "time",
        "SELECT toTime('12:34:56') a, toTime64('-01:02:03.456', 3) b",
        TIME,
    ),
    case(
        "ip addresses",
        "SELECT toIPv4('1.2.3.4') a, toIPv6('2001:db8::8a2e:370:7334') b, toIPv6('::ffff:1.2.3.4') c, toIPv4('255.255.255.255') d",
    ),
    case(
        "enums",
        "SELECT CAST(number % 3, 'Enum8(\\'a\\' = 0, \\'b\\' = 1, \\'c\\' = 2)') a, CAST(number % 2 * 1000 - 500, 'Enum16(\\'x\\' = -500, \\'y\\' = 500)') b FROM numbers(4)",
    ),
    case(
        "intervals",
        "SELECT toIntervalSecond(number) a, toIntervalDay(-number) b, toIntervalYear(1) c FROM numbers(3)",
    ),
    case(
        "nullable",
        "SELECT if(number % 2 = 0, NULL, number) a, if(number % 3 = 0, NULL, toString(number)) b, if(number = 1, NULL, toFixedString('ab', 2)) c, if(number = 2, NULL, toDecimal64(number, 3)) d, NULL::Nullable(UUID) e FROM numbers(6)",
    ),
    case(
        "all nulls",
        "SELECT NULL::Nullable(String) a, NULL::Nullable(Int32) b FROM numbers(3)",
    ),
    case(
        "low cardinality",
        "SELECT toLowCardinality(toString(number % 3)) a, toLowCardinality('') b FROM numbers(10)",
    ),
    case(
        "low cardinality nullable",
        "SELECT toLowCardinality(if(number % 3 = 0, NULL, toString(number % 4))) a FROM numbers(10)",
    ),
    case_with(
        "low cardinality of numbers",
        "SELECT toLowCardinality(number % 5) a, toLowCardinality(toFixedString(toString(number % 2), 1)) b, toLowCardinality(toDate('2024-01-01') + number % 2) c FROM numbers(8)",
        LOW_CARDINALITY,
    ),
    case(
        "low cardinality with many values",
        "SELECT toLowCardinality(toString(number)) a FROM numbers(70000)",
    ),
    case(
        "arrays",
        "SELECT range(number) a, arrayMap(x -> toString(x), range(number % 3)) b, [] :: Array(UInt8) c, [[1, 2], [], [3]] d FROM numbers(5)",
    ),
    case(
        "arrays of nullable",
        "SELECT [NULL, number, NULL] a, [if(number = 0, NULL, 'x')] b FROM numbers(3)",
    ),
    case(
        "arrays of low cardinality",
        "SELECT [toLowCardinality(toString(number)), toLowCardinality('x')] a, arrayMap(x -> toLowCardinality(toString(x)), range(number)) b FROM numbers(4)",
    ),
    case(
        "deeply nested arrays",
        "SELECT [[[number, 1], []], [[2]]] a, [[['a', 'b']], [[]]] b FROM numbers(3)",
    ),
    case(
        "maps",
        "SELECT map('a', number, 'b', number * 2) a, map() :: Map(String, UInt8) b, mapFromArrays(range(number), arrayMap(x -> toString(x), range(number))) c FROM numbers(4)",
    ),
    case(
        "maps of complex types",
        "SELECT map(toLowCardinality('k'), [number, NULL]) a, map(number, (toString(number), [1])) b, map('x', map('y', number)) c FROM numbers(3)",
    ),
    case(
        "tuples",
        "SELECT (number, toString(number)) a, (1, (2, 'x'), [3]) b, tuple(if(number = 1, NULL, number)) c FROM numbers(3)",
    ),
    case(
        "tuple of low cardinality",
        "SELECT (toLowCardinality(toString(number)), number) a FROM numbers(5)",
    ),
    case(
        "arrays of tuples",
        "SELECT [(number, 'a'), (number + 1, 'b')] a, arrayMap(x -> (x, toString(x)), range(number)) b FROM numbers(4)",
    ),
    case(
        "geo",
        "SELECT (1.5, 2.5)::Point a, [(1.0, 2.0), (3.0, 4.0)]::Ring b, [[(1.0, 2.0)], [(3.0, 4.0), (5.0, 6.0)]]::Polygon c, [[[(1.0, 2.0)]]]::MultiPolygon d, [(0.0, 0.0), (1.0, 1.0)]::LineString e, [[(0.0, 1.0)]]::MultiLineString f FROM numbers(3)",
    ),
    case(
        "geo of many rows",
        "SELECT (number * 1.0, number * 2.0)::Point p FROM numbers(100)",
    ),
    case_with(
        "variant",
        "SELECT number::Variant(UInt64, String, Array(UInt64)) a, if(number % 3 = 0, NULL, if(number % 3 = 1, toString(number), number))::Variant(String, UInt64) b FROM numbers(6)",
        VARIANT,
    ),
    case_with(
        "variant of one type",
        "SELECT 'x'::Variant(String, UInt64) a FROM numbers(3)",
        VARIANT,
    ),
    case_with(
        "variant with only nulls",
        "SELECT NULL::Variant(String, UInt64) a FROM numbers(3)",
        VARIANT,
    ),
    case_with(
        "variant with nested types",
        "SELECT [number::Variant(UInt64, String)] a, map('k', toString(number)::Variant(UInt64, String)) b, (number::Variant(UInt64, String), 1) c FROM numbers(4)",
        VARIANT,
    ),
    case(
        "simple aggregate function",
        "SELECT CAST(number, 'SimpleAggregateFunction(sum, UInt64)') a, CAST(toString(number), 'SimpleAggregateFunction(any, String)') b FROM numbers(3)",
    ),
    case(
        "bfloat16",
        "SELECT toBFloat16(number / 3) a, toBFloat16(-1.5) b FROM numbers(3)",
    ),
    case(
        "many rows",
        "SELECT number, toString(number) s, number % 7 = 0 b FROM numbers(200000)",
    ),
    case_with(
        "many small blocks",
        "SELECT number, [number], toLowCardinality(toString(number % 3)) FROM numbers(100)",
        SMALL_BLOCKS,
    ),
    case(
        "wide row",
        "SELECT number, repeat('x', 1000) a, repeat('y', 2000) b, range(500) c FROM numbers(2000)",
    ),
    case(
        "system table",
        "SELECT name, type, default_kind FROM system.columns WHERE database = 'system' AND table = 'one'",
    ),
    case(
        "nothing",
        "SELECT NULL AS a, [] AS b, [NULL] AS c, map() AS d FROM numbers(3)",
    ),
    case(
        "named tuples",
        "SELECT CAST((number, toString(number)), 'Tuple(a UInt64, `b c` String)') AS t, [t] AS ts FROM numbers(3)",
    ),
    case_with(
        "nested",
        "SELECT CAST([(1, 'a'), (2, 'b')], 'Nested(id UInt32, value String)') AS n",
        &[("flatten_nested", "0")],
    ),
];

/// Queries whose responses cannot be converted from the native protocol.
const UNSUPPORTED: &[Case] = &[
    case_with(
        "dynamic",
        "SELECT number::Dynamic AS d FROM numbers(3)",
        &[("allow_experimental_dynamic_type", "1")],
    ),
    case(
        "aggregate function state",
        "SELECT sumState(number) AS s FROM numbers(3)",
    ),
    case_with(
        "JSON without the string setting",
        "SELECT '{\"a\": 1}'::JSON AS j",
        &[("allow_experimental_json_type", "1")],
    ),
];

/// The formats in which both transports return data.
const FORMATS: &[&str] = &[
    "RowBinaryWithNamesAndTypes",
    "RowBinaryWithNames",
    "RowBinary",
    "Native",
];

async fn fetch_bytes(client: &Client, case: &Case, format: &str) -> Result<Vec<u8>, Error> {
    let mut query = client.query(case.sql);

    for (name, value) in case.settings {
        query = query.with_setting(*name, *value);
    }

    Ok(query.fetch_bytes(format)?.collect().await?.to_vec())
}

/// Compare the responses of both transports for every query and format.
async fn compare_corpus(tcp: &Client) {
    let http = http_client();
    let mut compared = 0;

    for case in CORPUS {
        for &format in FORMATS {
            let expected = fetch_bytes(&http, case, format).await;
            let actual = fetch_bytes(tcp, case, format).await;

            match (expected, actual) {
                (Ok(expected), Ok(actual)) => {
                    assert!(
                        expected == actual,
                        "{} ({format}): responses differ\nHTTP ({} bytes): {:?}\nTCP  ({} bytes): {:?}",
                        case.name,
                        expected.len(),
                        &expected[..expected.len().min(200)],
                        actual.len(),
                        &actual[..actual.len().min(200)],
                    );
                    compared += 1;
                }
                // The server does not support the query (e.g. a type of a newer version).
                (Err(_), Err(_)) => {
                    eprintln!("{} ({format}): not supported by the server", case.name)
                }
                // Old servers do not support some formats, which the client converts itself.
                (Err(e), Ok(_)) if e.to_string().contains("Unknown format") => {
                    eprintln!(
                        "{} ({format}): format not supported by the server",
                        case.name
                    );
                }
                (Err(e), Ok(_)) => panic!("{} ({format}): only HTTP failed: {e}", case.name),
                (Ok(_), Err(e)) => panic!("{} ({format}): only TCP failed: {e}", case.name),
            }
        }
    }

    // Older servers do not support some types; most cases must be supported by any version.
    let total = CORPUS.len() * FORMATS.len();
    assert!(
        compared * 10 >= total * 7,
        "only {compared} of {total} responses compared"
    );
}

#[tokio::test]
async fn corpus_matches_http() {
    check_local_env!();
    compare_corpus(&tcp_client().with_compression(Compression::None)).await;
}

#[cfg(feature = "lz4")]
#[tokio::test]
async fn corpus_matches_http_lz4() {
    check_local_env!();
    compare_corpus(&tcp_client().with_compression(Compression::Lz4)).await;
}

#[cfg(feature = "zstd")]
#[tokio::test]
async fn corpus_matches_http_zstd() {
    check_local_env!();
    compare_corpus(&tcp_client().with_compression(Compression::zstd())).await;
}

/// `JSON` is returned as strings by both transports with `output_format_binary_write_json_as_string`.
#[tokio::test]
async fn json_as_string_matches_http() {
    check_local_env!();

    let case = case_with(
        "json",
        "SELECT ('{\"a\": ' || toString(number) || ', \"b\": {\"c\": [1, 2]}}')::JSON AS j, [j] AS js FROM numbers(3)",
        &[
            ("allow_experimental_json_type", "1"),
            ("output_format_binary_write_json_as_string", "1"),
        ],
    );

    let http = http_client();
    let tcp = tcp_client();

    for format in ["RowBinaryWithNamesAndTypes", "RowBinary"] {
        let Ok(expected) = fetch_bytes(&http, &case, format).await else {
            eprintln!("JSON as string is not supported by the server");
            return;
        };

        let actual = fetch_bytes(&tcp, &case, format).await.unwrap();
        assert_eq!(expected, actual, "{format}");
    }
}

/// Data that cannot be converted fails with an error instead of wrong data.
#[tokio::test]
async fn unsupported_types_fail_with_error() {
    check_local_env!();

    let http = http_client();
    let tcp = tcp_client();

    for case in UNSUPPORTED {
        // Skip types that the server does not support.
        if fetch_bytes(&http, case, "RowBinary").await.is_err() {
            eprintln!("{}: not supported by the server", case.name);
            continue;
        }

        for format in FORMATS {
            let error = fetch_bytes(&tcp, case, format).await.unwrap_err();
            assert!(
                matches!(error, Error::DataFormat(_)),
                "{} ({format}): unexpected error {error:?}",
                case.name
            );
        }

        // The connection stays usable after the error.
        let value: u8 = tcp.query("SELECT 42").fetch_one().await.unwrap();
        assert_eq!(value, 42);
    }
}

/// A table definition and a query that fills it, to test all paths of `INSERT`.
struct InsertCase {
    name: &'static str,
    columns: &'static str,
    select: &'static str,
    settings: &'static [(&'static str, &'static str)],
}

const fn insert_case(
    name: &'static str,
    columns: &'static str,
    select: &'static str,
) -> InsertCase {
    InsertCase {
        name,
        columns,
        select,
        settings: &[],
    }
}

const INSERT_CORPUS: &[InsertCase] = &[
    insert_case(
        "numbers",
        "a UInt8, b Int16, c UInt32, d Int64, e UInt128, f Int256, g Float32, h Float64, i Bool",
        "SELECT number, -number * 100, number * 1000, -number * 1e12, number * 1e30, -number * 1e60, number / 3, number / 7, number % 2 FROM numbers(100)",
    ),
    insert_case(
        "strings",
        "a String, b FixedString(4), c LowCardinality(String), d LowCardinality(Nullable(String)), e Nullable(String)",
        "SELECT repeat('é', number % 5), toFixedString(toString(number % 1000), 4), toString(number % 3), if(number % 4 = 0, NULL, toString(number % 2)), if(number % 5 = 0, NULL, unhex('00ff')) FROM numbers(100)",
    ),
    insert_case(
        "dates and times",
        "a Date, b Date32, c DateTime('UTC'), d DateTime64(6, 'Asia/Tokyo'), e Nullable(DateTime64(3))",
        "SELECT toDate('2024-01-01') + number, toDate32('1950-01-01') + number, toDateTime(number * 1000, 'UTC'), toDateTime64(number, 6, 'Asia/Tokyo'), if(number % 2 = 0, NULL, toDateTime64(number, 3)) FROM numbers(50)",
    ),
    insert_case(
        "other scalars",
        "a UUID, b IPv4, c IPv6, d Enum8('x' = 1, 'y' = 2), e Decimal(38, 10), f Decimal(9, 2), g Decimal(76, 20)",
        "SELECT generateUUIDv4(number), toIPv4(number), toIPv6('::1'), 1 + number % 2, number / 3, -number / 7, number * 1e30 FROM numbers(50)",
    ),
    insert_case(
        "containers",
        "a Array(UInt64), b Array(Nullable(String)), c Map(String, Array(UInt8)), d Tuple(UInt8, String, Array(Nullable(UInt8))), e Array(Array(String)), f Map(LowCardinality(String), UInt64)",
        "SELECT range(number % 5), [NULL, toString(number)], map(toString(number), range(number % 3)), (number, toString(number), [NULL, number]), [[toString(number)], []], map(toString(number % 3), number) FROM numbers(60)",
    ),
    insert_case(
        "geo",
        "a Point, b Ring, c Polygon, d MultiPolygon, e LineString",
        "SELECT (number * 1.0, number * 2.0), [(number * 1.0, 1.0)], [[(0.0, number * 1.0)]], [[[(number * 1.0, 0.0)]]], [(1.0, 2.0), (number * 1.0, 3.0)] FROM numbers(20)",
    ),
    InsertCase {
        name: "variant",
        columns: "a Variant(UInt64, String, Array(UInt64)), b Array(Variant(String, UInt8))",
        select: "SELECT multiIf(number % 4 = 0, NULL, number % 4 = 1, number::Variant(UInt64, String, Array(UInt64)), number % 4 = 2, toString(number)::Variant(UInt64, String, Array(UInt64)), arrayMap(x -> toUInt64(x), range(number % 3))::Variant(UInt64, String, Array(UInt64))), [toString(number)::Variant(String, UInt8), (number % 200)::UInt8::Variant(String, UInt8)] FROM numbers(40)",
        settings: VARIANT,
    },
    insert_case(
        "large",
        "a UInt64, b String, c Array(UInt32)",
        "SELECT number, repeat('z', number % 100), range(number % 10) FROM numbers(300000)",
    ),
];

/// Insert the rows of `case.select` in `format` with the native transport and with the HTTP
/// transport, reading the data from the HTTP transport, and compare the tables.
///
/// For binary formats, the tables must also be equal to the result of `INSERT ... SELECT`.
/// Text formats can lose the precision of floats.
///
/// Returns `Ok(false)` if the server does not support the types of the case.
async fn check_insert(tcp: &Client, case: &InsertCase, format: &str) -> Result<bool, String> {
    let http = http_client().with_database(tcp.database().unwrap_or("default"));
    let with_settings = |mut query: clickhouse::query::Query| {
        for (name, value) in case.settings {
            query = query.with_setting(*name, *value);
        }
        query
    };

    for table in ["expected", "via_http", "via_tcp"] {
        let created = with_settings(http.query(&format!(
            "CREATE OR REPLACE TABLE {table} (id UInt64, {}) ENGINE = MergeTree ORDER BY id",
            case.columns
        )))
        .execute()
        .await;

        if let Err(e) = created {
            eprintln!("{} ({format}): not supported by the server: {e}", case.name);
            return Ok(false);
        }
    }

    let select = format!(
        "SELECT row_number() OVER () AS id, * FROM ({})",
        case.select
    );

    if let Err(e) = with_settings(http.query(&format!("INSERT INTO expected {select}")))
        .execute()
        .await
    {
        eprintln!("{} ({format}): not supported by the server: {e}", case.name);
        return Ok(false);
    }

    let data = with_settings(http.query("SELECT * FROM expected ORDER BY id"))
        .fetch_bytes(format)
        .map_err(|e| e.to_string())?
        .collect()
        .await
        .map_err(|e| format!("cannot fetch {format}: {e}"))?;

    // The server may not support the format, e.g. `RowBinaryWithNames` of older versions.
    let mut results = Vec::new();

    for (client, table) in [(&http, "via_http"), (tcp, "via_tcp")] {
        results.push(insert_data(client, table, format, case, &data).await);
    }

    match (&results[0], &results[1]) {
        (Ok(()), Ok(())) => {}
        (Err(http_error), Err(tcp_error)) => {
            eprintln!(
                "{} ({format}): not supported by the server: {http_error}; {tcp_error}",
                case.name
            );
            return Ok(false);
        }
        (Err(e), Ok(())) => return Err(format!("only HTTP INSERT failed: {e}")),
        (Ok(()), Err(e)) => return Err(format!("only TCP INSERT failed: {e}")),
    }

    let fetch = async |table: &str| {
        http.query(&format!("SELECT * FROM {table} ORDER BY id"))
            .fetch_bytes("RowBinaryWithNamesAndTypes")
            .unwrap()
            .collect()
            .await
            .unwrap()
    };

    let (expected, via_http, via_tcp) = (
        fetch("expected").await,
        fetch("via_http").await,
        fetch("via_tcp").await,
    );

    if via_tcp != via_http {
        return Err(format!(
            "TCP and HTTP inserted different data: {} bytes vs {} bytes",
            via_tcp.len(),
            via_http.len()
        ));
    }

    let binary = format.starts_with("RowBinary") || format == "Native";

    if expected.len() <= 100 || (binary && via_tcp != expected) {
        return Err(format!(
            "inserted data differs from INSERT SELECT: {} bytes vs {} bytes",
            via_tcp.len(),
            expected.len()
        ));
    }

    Ok(true)
}

async fn insert_data(
    client: &Client,
    table: &str,
    format: &str,
    case: &InsertCase,
    data: &[u8],
) -> Result<(), String> {
    let mut insert = client.insert_formatted_with(format!("INSERT INTO {table} FORMAT {format}"));
    for (name, value) in case.settings {
        insert = insert.with_setting(*name, *value);
    }

    // Send the data in pieces that split rows, to test incomplete rows in chunks.
    for chunk in data.chunks(1000) {
        insert
            .send(bytes::Bytes::copy_from_slice(chunk))
            .await
            .map_err(|e| format!("cannot send to {table}: {e}"))?;
    }

    insert
        .end()
        .await
        .map_err(|e| format!("INSERT into {table} failed: {e}"))
}

const INSERT_FORMATS: &[&str] = &[
    "RowBinary",
    "RowBinaryWithNames",
    "RowBinaryWithNamesAndTypes",
    "Native",
    "TabSeparated",
    "JSONEachRow",
];

async fn check_insert_corpus(tcp: Client) {
    let mut compared = 0;

    for case in INSERT_CORPUS {
        for &format in INSERT_FORMATS {
            match check_insert(&tcp, case, format).await {
                Ok(true) => compared += 1,
                Ok(false) => {}
                Err(e) => panic!("{} ({format}): {e}", case.name),
            }
        }
    }

    // Older servers do not support some types; most cases must be supported by any version.
    let total = INSERT_CORPUS.len() * INSERT_FORMATS.len();
    assert!(
        compared * 2 >= total,
        "only {compared} of {total} inserts compared"
    );
}

#[tokio::test]
async fn insert_corpus_matches_insert_select() {
    check_local_env!();
    let client = prepare_database!();
    check_insert_corpus(
        tcp_client()
            .with_database(client.database().unwrap())
            .with_compression(Compression::None),
    )
    .await;
}

#[cfg(feature = "lz4")]
#[tokio::test]
async fn insert_corpus_matches_insert_select_lz4() {
    check_local_env!();
    let client = prepare_database!();
    check_insert_corpus(
        tcp_client()
            .with_database(client.database().unwrap())
            .with_compression(Compression::Lz4),
    )
    .await;
}

/// A `SET` statement must not change other queries, like with the HTTP interface without a session.
#[tokio::test]
async fn set_statement_does_not_leak_into_pooled_connections() {
    check_local_env!();
    let client = tcp_client();

    let default: String = client
        .query("SELECT toString(getSetting('max_threads'))")
        .fetch_one()
        .await
        .unwrap();

    for _ in 0..3 {
        client.query("SET max_threads = 3").execute().await.unwrap();

        let value: String = client
            .query("SELECT toString(getSetting('max_threads'))")
            .fetch_one()
            .await
            .unwrap();
        assert_eq!(value, default);
    }
}

/// `session_id` reserves a connection for the session, like the sessions of the HTTP interface.
#[tokio::test]
async fn sessions() {
    check_local_env!();
    let session_id = |id: &str| format!("chrs_tcp_session_{id}_{:x}", rand::random::<u64>());
    let first = tcp_client().with_setting("session_id", session_id("a"));
    let second = tcp_client().with_setting("session_id", session_id("b"));

    first.query("SET max_threads = 3").execute().await.unwrap();
    first
        .query("CREATE TEMPORARY TABLE tmp (x UInt8)")
        .execute()
        .await
        .unwrap();
    first
        .query("INSERT INTO tmp VALUES (7)")
        .execute()
        .await
        .unwrap();

    let threads = |client: Client| async move {
        client
            .query("SELECT toUInt64(getSetting('max_threads'))")
            .fetch_one::<u64>()
            .await
            .unwrap()
    };

    assert_eq!(threads(first.clone()).await, 3);
    assert_ne!(threads(second.clone()).await, 3);
    assert_eq!(
        first
            .query("SELECT x FROM tmp")
            .fetch_all::<u8>()
            .await
            .unwrap(),
        [7]
    );
    assert!(
        second
            .query("SELECT x FROM tmp")
            .fetch_all::<u8>()
            .await
            .is_err()
    );

    // A session can be used by one query at a time.
    let mut cursor = first
        .query("SELECT number FROM system.numbers")
        .fetch::<u64>()
        .unwrap();
    assert_eq!(cursor.next().await.unwrap(), Some(0));
    let error = first.query("SELECT 1").execute().await.unwrap_err();
    assert!(error.to_string().contains("SESSION_IS_LOCKED"), "{error}");
    drop(cursor);

    // The session is available again when the query is canceled.
    let mut result = Err(error);
    for _ in 0..100 {
        result = first.query("SELECT x FROM tmp").fetch_all::<u8>().await;
        if result.is_ok() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    assert_eq!(result.unwrap(), [7]);

    // `session_check = 1` requires an existing session.
    let error = tcp_client()
        .with_setting("session_id", session_id("missing"))
        .with_setting("session_check", "1")
        .query("SELECT 1")
        .execute()
        .await
        .unwrap_err();
    assert!(error.to_string().contains("SESSION_NOT_FOUND"), "{error}");

    // `close_session = 1` ends the session after the query.
    first
        .query("SELECT 1")
        .with_setting("close_session", "1")
        .execute()
        .await
        .unwrap();
    assert!(
        first
            .query("SELECT x FROM tmp")
            .fetch_all::<u8>()
            .await
            .is_err()
    );
}

/// A dropped cursor cancels the query on the server.
#[tokio::test]
async fn dropped_cursor_cancels_query() {
    check_local_env!();
    let query_id = format!("chrs_tcp_cancel_{:x}", rand::random::<u64>());
    let client = tcp_client();

    let mut cursor = client
        .query("SELECT number FROM system.numbers")
        .with_setting("query_id", &query_id)
        .with_setting("max_block_size", "1000")
        .fetch::<u64>()
        .unwrap();

    for expected in 0..10_000 {
        assert_eq!(cursor.next().await.unwrap(), Some(expected));
    }
    drop(cursor);

    let observer = http_client();
    let mut running = u64::MAX;

    for _ in 0..100 {
        running = observer
            .query("SELECT count() FROM system.processes WHERE query_id = ?")
            .bind(&query_id)
            .fetch_one()
            .await
            .unwrap();

        if running == 0 {
            break;
        }

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }

    assert_eq!(running, 0, "the query is still running");

    // The client keeps working.
    let value: u8 = client.query("SELECT 7").fetch_one().await.unwrap();
    assert_eq!(value, 7);
}

/// An exception in the middle of the result is returned after the rows sent before it.
#[tokio::test]
async fn exception_after_rows() {
    check_local_env!();
    let client = tcp_client();

    let mut cursor = client
        .query("SELECT throwIf(number = 50000, 'boom') AS x, number FROM system.numbers")
        .with_setting("max_block_size", "1000")
        .fetch::<(u8, u64)>()
        .unwrap();

    let mut rows = 0;
    let error = loop {
        match cursor.next().await {
            Ok(Some((0, number))) => {
                assert_eq!(number, rows);
                rows += 1;
            }
            Ok(other) => panic!("unexpected row: {other:?}"),
            Err(error) => break error,
        }
    };

    assert!(rows > 0 && rows <= 50000, "{rows} rows");
    let message = error.to_string();
    assert!(message.contains("Code: 395"), "{message}");
    assert!(message.contains("boom"), "{message}");
    assert!(
        message.contains("(FUNCTION_THROW_IF_VALUE_IS_NON_ZERO)"),
        "{message}"
    );

    // The connection can be used again after a server exception.
    let value: u8 = client.query("SELECT 1").fetch_one().await.unwrap();
    assert_eq!(value, 1);
}

/// Parameters with the names of settings work with every server version.
#[tokio::test]
async fn parameters_named_like_settings() {
    check_local_env!();

    for client in [tcp_client(), http_client()] {
        let result = client
            .query("SELECT {limit:UInt64}, {page: String}, '{limit:UInt64}'")
            .param("limit", 42)
            .param("page", "it's")
            .fetch_one::<(u64, String, String)>()
            .await;

        // The native protocol of servers older than 22.x has no query parameters.
        if let Err(Error::Unsupported(message)) = &result {
            assert!(message.contains("query parameters"), "{message}");
            continue;
        }

        let (limit, page, text) = result.unwrap();

        assert_eq!(
            (limit, page.as_str(), text.as_str()),
            (42, "it's", "{limit:UInt64}")
        );
    }
}

/// Settings are applied like with the HTTP interface, including errors for invalid settings.
#[tokio::test]
async fn settings_match_http() {
    check_local_env!();

    let settings = [
        ("max_threads", "3"),
        ("max_memory_usage", "10G"),
        ("max_execution_time", "1.5"),
        ("join_algorithm", "hash,parallel_hash"),
        ("date_time_output_format", "iso"),
        ("output_format_json_quote_64bit_integers", "0"),
        ("log_comment", "it's \\ a comment"),
        ("max_block_size", "abc"),
        ("no_such_setting", "1"),
        ("custom_chrs", "value"),
    ];

    for (name, value) in settings {
        let get = async |client: Client| {
            client
                .query("SELECT toString(getSetting(?))")
                .bind(name)
                .with_setting(name, value)
                .fetch_one::<String>()
                .await
                .map_err(|error| error.to_string())
        };

        let expected = get(http_client()).await;
        let actual = get(tcp_client()).await;

        match (&expected, &actual) {
            (Ok(expected), Ok(actual)) => assert_eq!(expected, actual, "{name} = {value}"),
            (Err(expected), Err(actual)) => {
                // The same error code
                let code = |message: &str| {
                    let (_, rest) = message.split_once("Code: ")?;
                    let digits: String = rest.chars().take_while(char::is_ascii_digit).collect();
                    Some(digits)
                };
                assert!(code(expected).is_some(), "{name} = {value}: {expected}");
                assert_eq!(code(expected), code(actual), "{name} = {value}");
            }
            _ => panic!("{name} = {value}: HTTP {expected:?}, TCP {actual:?}"),
        }
    }
}

#[tokio::test]
async fn connection_errors() {
    check_local_env!();

    let error = tcp_client()
        .with_user("default")
        .with_password("wrong password")
        .query("SELECT 1")
        .execute()
        .await
        .unwrap_err();
    assert!(
        error.to_string().contains("AUTHENTICATION_FAILED"),
        "{error}"
    );

    let error = tcp_client()
        .with_database("chrs_no_such_database")
        .query("SELECT 1")
        .execute()
        .await
        .unwrap_err();
    assert!(error.to_string().contains("UNKNOWN_DATABASE"), "{error}");

    // Nothing listens on this port.
    let error = Client::default()
        .with_url("tcp://127.0.0.1:1")
        .query("SELECT 1")
        .execute()
        .await
        .unwrap_err();
    assert!(matches!(error, Error::Network(_)), "{error:?}");

    let result = Client::default()
        .with_url("tcp://127.0.0.1:1")
        .query("SELECT 1")
        .fetch_bytes("CSV");
    assert!(
        matches!(result, Err(Error::Unsupported(_))),
        "unexpected result"
    );
}

/// An `INSERT` that is dropped before `end()` inserts nothing.
#[tokio::test]
async fn aborted_insert() {
    check_local_env!();
    let client = prepare_database!();
    crate::create_simple_table(&client, "test").await;
    let tcp = tcp_client().with_database(client.database().unwrap());

    let mut insert = tcp.insert::<crate::SimpleRow>("test").await.unwrap();
    for i in 0..10 {
        insert.write(&crate::SimpleRow::new(i, "x")).await.unwrap();
    }
    drop(insert);

    // A failing `INSERT`: the server cannot parse the values.
    let mut insert = tcp.insert_formatted_with("INSERT INTO test FORMAT RowBinary");
    insert
        .send(bytes::Bytes::from_static(&[1, 2, 3]))
        .await
        .unwrap();
    assert!(insert.end().await.is_err());

    let count: u64 = tcp
        .query("SELECT count() FROM test")
        .fetch_one()
        .await
        .unwrap();
    assert_eq!(count, 0);
}

/// The query ID and the user agent reach the server.
#[tokio::test]
async fn query_id_and_client_name() {
    check_local_env!();
    let query_id = format!("chrs_tcp_query_id_{:x}", rand::random::<u64>());
    let client = tcp_client().with_product_info("chrs-test", "1.2.3");

    client
        .query("SELECT 1")
        .with_setting("query_id", &query_id)
        .execute()
        .await
        .unwrap();

    let (interface, client_name): (u8, String) =
        query_log("toUInt8(interface), client_name", &query_id).await;

    // `interface = 1` is TCP.
    assert_eq!(interface, 1);
    assert!(
        client_name.starts_with("chrs-test/1.2.3 clickhouse-rs/"),
        "{client_name}"
    );
}

/// The client requests its compression method if it differs from the default method of the server,
/// unless the user cannot change settings.
#[tokio::test]
async fn compression_negotiation() {
    check_local_env!();
    let admin = prepare_database!();
    let database = admin.database().unwrap().to_owned();

    let user = format!("chrs_tcp_readonly_{:x}", rand::random::<u64>());
    let password = format!("CHRS_{:x}", rand::random::<u64>());
    for statement in [
        format!(
            "CREATE USER {user} IDENTIFIED WITH sha256_password BY '{password}' DEFAULT DATABASE {database} SETTINGS readonly = 1"
        ),
        format!("GRANT SELECT ON {database}.* TO {user}"),
        "CREATE TABLE numbers (n UInt64) ENGINE = MergeTree ORDER BY n".to_owned(),
        "INSERT INTO numbers SELECT number FROM system.numbers LIMIT 100000".to_owned(),
    ] {
        admin.query(&statement).execute().await.unwrap();
    }

    let version: String = admin.query("SELECT version()").fetch_one().await.unwrap();
    let mut parts = version
        .split('.')
        .map(|part| part.parse::<u32>().unwrap_or(0));
    let zstd_default = (parts.next().unwrap_or(0), parts.next().unwrap_or(0)) >= (26, 9);

    let mut modes: Vec<(Compression, &str)> = vec![(Compression::None, "")];
    #[cfg(feature = "lz4")]
    modes.push((Compression::Lz4, "LZ4"));
    #[cfg(feature = "zstd")]
    modes.push((Compression::Zstd(5), "ZSTD"));

    for (compression, method) in modes {
        for readonly in [false, true] {
            let client = tcp_client()
                .with_database(&database)
                .with_compression(compression);
            let client = if readonly {
                client.with_user(&user).with_password(&password)
            } else {
                client
            };

            let query_id = format!("chrs_tcp_compression_{:x}", rand::random::<u64>());
            let sum: u64 = client
                .query("SELECT sum(n) FROM numbers")
                .with_setting("query_id", &query_id)
                .fetch_one()
                .await
                .unwrap_or_else(|e| panic!("{compression:?} (readonly: {readonly}): {e}"));
            assert_eq!(sum, 4_999_950_000);

            // The log has the settings with values that differ from the defaults.
            let (requested_method, requested_level): (String, String) = query_log(
                "Settings['network_compression_method'], Settings['network_zstd_compression_level']",
                &query_id,
            )
            .await;

            // LZ4 is the default before 26.9, ZSTD(3) since 26.9; ZSTD(5) is always requested.
            let expected = match (method, readonly) {
                (_, true) | ("", _) => ("", ""),
                ("LZ4", false) if zstd_default => ("LZ4", ""),
                ("LZ4", false) => ("", ""),
                (_, false) if zstd_default => ("", "5"),
                (_, false) => ("ZSTD", "5"),
            };
            assert_eq!(
                (requested_method.as_str(), requested_level.as_str()),
                expected,
                "{compression:?} (readonly: {readonly})"
            );
        }
    }
}

/// Read `columns` of the `QueryFinish` entry of the query from `system.query_log`.
///
/// Some servers write the entry after the end of the response, so the lookup is repeated.
async fn query_log<T>(columns: &str, query_id: &str) -> T
where
    T: clickhouse::RowOwned + clickhouse::RowRead,
{
    let client = http_client();

    for _ in 0..50 {
        crate::flush_query_log(&client).await;

        let row = client
            .query(&format!(
                "SELECT {columns} FROM system.query_log WHERE query_id = ? AND type = 'QueryFinish'"
            ))
            .bind(query_id)
            .fetch_optional::<T>()
            .await
            .unwrap();

        if let Some(row) = row {
            return row;
        }

        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }

    panic!("no query_log entry for {query_id}");
}

/// Scalar types for the generated corpus, with the minimum server version (`major * 100 + minor`).
const FUZZ_SCALARS: &[(&str, u32)] = &[
    ("UInt8", 0),
    ("UInt16", 0),
    ("UInt32", 0),
    ("UInt64", 0),
    ("UInt128", 0),
    ("UInt256", 0),
    ("Int8", 0),
    ("Int16", 0),
    ("Int32", 0),
    ("Int64", 0),
    ("Int128", 0),
    ("Int256", 0),
    ("Float32", 0),
    ("Float64", 0),
    ("BFloat16", 2411),
    ("Bool", 0),
    ("Decimal32(3)", 0),
    ("Decimal64(5)", 0),
    ("Decimal128(10)", 0),
    ("Decimal256(20)", 0),
    ("String", 0),
    ("FixedString(5)", 0),
    ("UUID", 0),
    ("Date", 0),
    ("Date32", 0),
    ("DateTime('UTC')", 0),
    ("DateTime64(3, 'Asia/Tokyo')", 0),
    ("IPv4", 0),
    ("IPv6", 0),
    ("Enum8('a' = 1, 'b' = -2, 'c d' = 3)", 0),
    ("Enum16('x' = -1000, 'y' = 1000)", 0),
];

/// Wrappers of the scalar types; `T` is replaced with the scalar type.
const FUZZ_WRAPPERS: &[&str] = &[
    "T",
    "Nullable(T)",
    "Array(T)",
    "Array(Nullable(T))",
    "Array(Array(T))",
    "Map(String, T)",
    "Map(LowCardinality(String), Array(Nullable(T)))",
    "Tuple(T, String)",
    "Tuple(a T, b Nullable(T), c Array(T))",
    "LowCardinality(T)",
    "LowCardinality(Nullable(T))",
    "Array(LowCardinality(Nullable(T)))",
    "Map(T, LowCardinality(String))",
];

async fn server_version() -> u32 {
    let version: String = http_client()
        .query("SELECT version()")
        .fetch_one()
        .await
        .unwrap();
    let mut parts = version
        .split('.')
        .map(|part| part.parse::<u32>().unwrap_or(0));
    parts.next().unwrap_or(0) * 100 + parts.next().unwrap_or(0)
}

/// All combinations of scalar types and wrappers that the server supports.
async fn fuzz_types() -> Vec<String> {
    let version = server_version().await;

    FUZZ_SCALARS
        .iter()
        .filter(|(_, since)| version >= *since)
        .flat_map(|(scalar, _)| {
            FUZZ_WRAPPERS
                .iter()
                .map(|wrapper| wrapper.replace('T', scalar))
        })
        .collect()
}

/// Random data of every generated type, compared between the transports in every format.
#[tokio::test]
async fn generated_corpus_matches_http() {
    check_local_env!();

    let http = http_client();
    let clients = [
        tcp_client().with_compression(Compression::None),
        tcp_client(),
    ];

    let mut compared = 0;
    let mut skipped = 0;

    for (index, data_type) in fuzz_types().await.into_iter().enumerate() {
        let structure = format!("id UInt32, c {data_type}");
        let sql = format!(
            "SELECT * FROM generateRandom({}, {index}, 8, 4) LIMIT {}",
            crate::tcp_quote(&structure),
            1 + index % 300
        );
        let block_size = (1 + index % 5 * 40).to_string();

        for &format in FORMATS {
            let fetch = async |client: &Client| -> Result<Vec<u8>, Error> {
                Ok(client
                    .query(&sql)
                    .with_setting("allow_suspicious_low_cardinality_types", "1")
                    .with_setting("max_block_size", &block_size)
                    // With several threads, `generateRandom` and `LIMIT` return different rows.
                    .with_setting("max_threads", "1")
                    .fetch_bytes(format)?
                    .collect()
                    .await?
                    .to_vec())
            };

            let Ok(expected) = fetch(&http).await else {
                // The server does not support the type or the format.
                skipped += 1;
                continue;
            };

            for client in &clients {
                let actual = fetch(client)
                    .await
                    .unwrap_or_else(|e| panic!("{data_type} ({format}): only TCP failed: {e}"));
                assert!(
                    actual == expected,
                    "{data_type} ({format}): responses differ ({} vs {} bytes)",
                    actual.len(),
                    expected.len()
                );
                compared += 1;
            }
        }
    }

    eprintln!("compared {compared} responses, {skipped} skipped");
    assert!(compared >= 600, "only {compared} responses compared");
}

/// Random data of every generated type inserted with the native transport in the binary formats.
#[tokio::test]
async fn generated_inserts_match_http() {
    check_local_env!();
    let admin = prepare_database!();
    let database = admin.database().unwrap().to_owned();
    let http = http_client().with_database(&database);
    let tcp = tcp_client().with_database(&database);

    let mut compared = 0;

    for (index, data_type) in fuzz_types().await.into_iter().enumerate() {
        let structure = format!("id UInt32, c {data_type}");
        let settings = [("allow_suspicious_low_cardinality_types", "1")];
        let execute = async |client: &Client, sql: String| {
            let mut query = client.query(&sql);
            for (name, value) in settings {
                query = query.with_setting(name, value);
            }
            query.execute().await
        };

        let created =
            async {
                for table in ["expected", "actual"] {
                    execute(&http, format!(
                    "CREATE OR REPLACE TABLE {table} ({structure}) ENGINE = MergeTree ORDER BY id"
                ))
                .await?;
                }
                execute(&http, format!(
                "INSERT INTO expected SELECT * FROM generateRandom({}, {index}, 8, 4) LIMIT {}",
                crate::tcp_quote(&structure),
                1 + index % 300
            ))
            .await
            };

        if created.await.is_err() {
            continue;
        }

        for format in ["RowBinary", "RowBinaryWithNamesAndTypes", "Native"] {
            execute(&http, "TRUNCATE TABLE actual".to_owned())
                .await
                .unwrap();

            let data = http
                .query("SELECT * FROM expected ORDER BY id")
                .fetch_bytes(format)
                .unwrap()
                .collect()
                .await
                .unwrap();

            let mut insert =
                tcp.insert_formatted_with(format!("INSERT INTO actual FORMAT {format}"));
            for (name, value) in settings {
                insert = insert.with_setting(name, value);
            }
            for chunk in data.chunks(777) {
                insert
                    .send(bytes::Bytes::copy_from_slice(chunk))
                    .await
                    .unwrap();
            }
            insert
                .end()
                .await
                .unwrap_or_else(|e| panic!("{data_type} ({format}): {e}"));

            let fetch = async |table: &str| {
                http.query(&format!("SELECT * FROM {table} ORDER BY id"))
                    .fetch_bytes("RowBinaryWithNamesAndTypes")
                    .unwrap()
                    .collect()
                    .await
                    .unwrap()
            };

            assert!(
                fetch("expected").await == fetch("actual").await,
                "{data_type} ({format}): inserted data differs"
            );
            compared += 1;
        }
    }

    eprintln!("compared {compared} inserts");
    assert!(compared >= 300, "only {compared} inserts compared");
}
