//! Tests that need a ClickHouse server on `localhost:8123`, such as the
//! server of "Run the tests locally" in README.md. Set
//! `CLICKHOUSE_TEST_RUNNER_LIVE=1` to run them; without it they pass without
//! doing anything.

use crate::run;

const LIVE_ENV: &str = "CLICKHOUSE_TEST_RUNNER_LIVE";
const URL: &str = "http://localhost:8123";

fn live() -> bool {
    if std::env::var(LIVE_ENV).as_deref() == Ok("1") {
        return true;
    }
    eprintln!(
        "skipped: set {LIVE_ENV}=1 to run the tests against a ClickHouse server on localhost"
    );
    false
}

/// The server's own TabSeparated text for a query.
fn server_tsv(query: &str) -> Vec<u8> {
    server_output(query, "TabSeparated")
}

/// The server's own text for a query without a FORMAT clause, in `format`.
fn server_output(query: &str, format: &str) -> Vec<u8> {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        let bytes = clickhouse::Client::default()
            .with_url(URL)
            .query_raw(query)
            .fetch_bytes(format)
            .unwrap()
            .collect()
            .await
            .unwrap_or_else(|e| panic!("server error for {query}: {e}"));
        bytes.to_vec()
    })
}

/// Runs the shim with `--query` and returns stdout, stderr and the exit code.
fn shim(query: &str) -> (String, String, i32) {
    let (out, err, code) = shim_bytes(query);
    (String::from_utf8_lossy(&out).into_owned(), err, code)
}

fn shim_bytes(query: &str) -> (Vec<u8>, String, i32) {
    let argv = vec![
        "--host".to_owned(),
        "localhost".to_owned(),
        "--query".to_owned(),
        query.to_owned(),
    ];
    let (mut out, mut err) = (Vec::new(), Vec::new());
    let code = run(&argv, &mut std::io::empty(), &mut out, &mut err);
    (out, String::from_utf8_lossy(&err).into_owned(), code)
}

/// For each expression, the text that the shim writes from the values that
/// clickhouse-rs decoded must be the same as the server's text.
#[test]
fn render_matches_server() {
    if !live() {
        return;
    }
    let exprs = [
        // Integers.
        "toInt8(-1), toUInt8(255), toInt64(-9223372036854775808), toUInt64(18446744073709551615)",
        "toInt128(-170141183460469231731687303715884105728), toUInt128(340282366920938463463374607431768211455)",
        "toInt256(-57896044618658097711785492504343953926634992332820282019728792003956564819968), toUInt256(115792089237316195423570985008687907853269984665640564039457584007913129639935)",
        "toInt256(-1), toInt256(0), toUInt256(10000000000000000000), toInt256('-12345678901234567890123')",
        // Floats.
        "arrayJoin([1e15, 1e21, 1.5e-7, 0.1 + 0.2, nan, inf, -inf, -0., 5e-324, 1e-6, 123456789012345680000])",
        "toFloat32(0.1), toFloat32(3.4028235e38), toFloat32(16777216), toFloat32(1e-7), toFloat32(-0.)",
        "toBFloat16(1.5), toBFloat16(0.1), toBFloat16(1e20), toBFloat16(-3.14159)",
        // `0/0` and `sqrt(-1)` are NaN with the sign bit set; the server writes `nan`.
        "0/0, -(0/0), sqrt(-1), [0/0], tuple(0/0), toFloat32(0/0), toBFloat16(0/0), toNullable(0/0)",
        // Decimals.
        "toDecimal32(1.50, 2), toDecimal64(-0.001, 5), toDecimal128(100, 3), toDecimal256('1.5', 10)",
        "toDecimal32(0, 4), toDecimal64(-12345.6789, 4), toDecimal128('-0.00000000000000000001', 20)",
        "toDecimal256('-123456789012345678901234567890123456789.123456789', 30), toDecimal256(0, 0)",
        // Strings.
        "'a\\tb\\\\c\\nd''e\\0\\b\\f\\v\\x01\\r', toFixedString('ab', 4), '', 'ü€😀'",
        "unhex('FF00C3'), [unhex('FF')], toFixedString(unhex('FF'), 2)",
        // NULL and Bool.
        "true, false, CAST(NULL AS Nullable(String)), toNullable(1), CAST(NULL AS Nullable(Int32))",
        // Dates and times.
        "toDate('2020-01-02'), toDate32('1900-01-01'), toDate32('2299-12-31'), toDate('2149-06-06'), toDate('1970-01-01')",
        "toDateTime('2020-01-01 10:00:00', 'UTC'), toDateTime(0, 'UTC'), toDateTime(4294967295, 'UTC')",
        // A top-level `DateTime('tz')` comes as `DateTime` (see [native-datetime-timezone-lost] in
        // allowlist.txt); a nested one keeps its time zone.
        "[toDateTime('2020-01-01 10:00:00', 'Asia/Tokyo')], toNullable(toDateTime('2020-06-01 10:00:00', 'Europe/Amsterdam')), tuple(toDateTime('2020-01-01 10:00:00', 'America/New_York'))",
        "toDateTime64('2020-01-01 00:00:00.120', 3), toDateTime64('2020-01-01 00:00:00', 9, 'Asia/Tokyo'), toDateTime64('1969-12-31 23:59:59.99', 2, 'UTC')",
        "toDateTime64('1900-01-01 00:00:00', 3, 'UTC'), toDateTime64('2299-12-31 23:59:59.999999', 6, 'UTC'), toDateTime64('1677-09-21 00:12:44', 0, 'UTC'), toDateTime64('2262-04-11 23:47:16.854775807', 9, 'UTC'), toDateTime64('1677-09-21 00:12:44', 9, 'UTC')",
        "toDateTime64('1600-01-01 00:00:00', 3, 'UTC'), toDateTime64('2500-01-01 00:00:00', 3, 'UTC')",
        // Daylight saving time after 2099 (chrono-tz has no transitions there).
        "[toDateTime('2105-07-28 04:08:00', 'Europe/Amsterdam'), toDateTime('2101-03-01 04:08:00', 'Europe/Amsterdam')], toDateTime64('2200-07-28 04:08:00', 0, 'Europe/Amsterdam'), toNullable(toDateTime64('2299-07-28 04:08:00', 3, 'America/New_York'))",
        // Fixed-offset time zones (`Fixed/UTC±HH:MM:SS`; `-` is west of UTC).
        "toDateTime64(0, 3, 'Fixed/UTC+14:00:00'), toDateTime64(0, 3, 'Fixed/UTC-14:00:00'), [toDateTime(0, 'Fixed/UTC+05:30:00')], tuple(toDateTime(1700000000, 'Fixed/UTC-08:00:00')), toNullable(toDateTime64(1700000000.5, 1, 'Fixed/UTC+05:45:00'))",
        // Values outside 0000-01-01 00:00:00 .. 9999-12-31 23:59:59 in the time zone of the
        // type: the server writes the nearest end of that range, with the fraction of the value.
        "reinterpret(toInt64(9223372036854775806), 'DateTime64(6, \\'UTC\\')'), reinterpret(toInt64(-9223372036854775808), 'DateTime64(3, \\'UTC\\')'), reinterpret(toInt64(9223372036854775807), 'DateTime64(0, \\'Asia/Tokyo\\')'), reinterpret(toInt64(-9223372036854775807), 'DateTime64(9, \\'America/New_York\\')')",
        "reinterpret(toInt64(253402304399), 'DateTime64(0, \\'Asia/Tokyo\\')'), reinterpret(toInt64(253402304399), 'DateTime64(0, \\'America/New_York\\')'), reinterpret(toInt64(-62167222800), 'DateTime64(0, \\'America/New_York\\')'), reinterpret(toInt64(-62167219200123), 'DateTime64(3, \\'UTC\\')'), reinterpret(toInt64(253402300800500), 'DateTime64(3, \\'UTC\\')')",
        "[reinterpret(toInt64(9223372036854775806), 'DateTime64(6, \\'Asia/Kolkata\\')')], toNullable(reinterpret(toInt64(-9223372036854775807), 'DateTime64(2, \\'Europe/Moscow\\')'))",
        "toTime('12:34:56'), toTime('-1:02:03'), toTime64('12:00:00.5', 3), toTime64('-00:00:01.1234567', 7), toTime64('00:00:01.12345678', 8), toTime64('100:00:00.123456789', 9)",
        "toTime(3600000), toTime64(3600000.5, 2), toTime(-3600000)",
        // UUID and IP addresses.
        "toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba0'), toUUID('00000000-0000-0000-0000-000000000000')",
        "toIPv4('1.2.3.4'), toIPv4('255.255.255.255'), toIPv6('::ffff:1.2.3.4'), toIPv6('::1.2.3.4'), toIPv6('2001:db8::1'), toIPv6('::'), toIPv6('1:0:0:2:0:0:0:3')",
        // Arrays, tuples and maps.
        "[1, 2], ['a''b', NULL], [[1], []], [toDate('2020-01-01')], [toDateTime64('2020-01-01 00:00:00.5', 2)]",
        "(1, 'x', CAST(NULL AS Nullable(UInt8))), [(1, 'a')], (toInt128(1), toUInt256(2)), tuple(1)",
        "CAST((1, NULL) AS Tuple(Int32, LowCardinality(Nullable(String)))), [toLowCardinality(toNullable('x')), NULL]",
        "CAST((1, 'x') AS Nullable(Tuple(UInt8, String))), CAST(NULL AS Nullable(Tuple(UInt8, String)))",
        "[CAST((1, NULL) AS Tuple(UInt8, Nullable(String)))], tuple(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16)",
        "map('k', 1, 'a', 2, 'z', 3, 'b', 4), map(1, [2]), map('x', map('y', 1, 'a', 2)), map([1], 'a'), map(map(1, 2), 'b')",
        "[map('b', 1, 'a', 2)], (map('z', 1, 'y', 2), 1), map('k', NULL, 'j', 2)",
        "CAST('a' AS Enum8('a' = 1, 'b' = 2)), [CAST('b' AS Enum16('a' = -1000, 'b' = 2))], CAST('x''y' AS Enum8('x''y' = -128))",
        "toLowCardinality('x'), toLowCardinality(toNullable('y')), [toLowCardinality('z')], toLowCardinality(toNullable(1))",
        "INTERVAL 3 DAY, toIntervalSecond(-5), toIntervalMonth(1)",
        "(1.5, 2.5)::Point",
        "sumSimpleState(number), anySimpleState(toString(number)) FROM numbers(3)",
        // A tuple in a `SimpleAggregateFunction` has the encoding of the tuple.
        "CAST(([1, 2], [3, 4]) AS SimpleAggregateFunction(sumMap, Tuple(Array(UInt8), Array(UInt64))))",
        "CAST([(1, 'a'), (2, NULL)] AS SimpleAggregateFunction(groupArrayArray, Array(Tuple(UInt8, Nullable(String)))))",
        // Nullable with a null-map byte other than 1 (the server can send 2, 3, ...).
        "if(number % 5, NULL, toInt8OrZero(toString(number))) FROM numbers(6)",
        "number, toString(number), if(number % 2, NULL, number) FROM numbers(3)",
        "number, repeat('x', number % 7) FROM numbers(200000) WHERE number % 49999 = 0",
        "toLowCardinality(toString(number % 3)) FROM numbers(10)",
        // A column or alias named `format` is not the FORMAT clause.
        "1 AS format ORDER BY format DESC",
        "1 AS format ORDER BY format ASC",
        "format FROM (SELECT 1 AS format)",
        "1 AS format FROM numbers(1)",
        // `?` is not a bind placeholder.
        "'?', 1 ? 2 : 3",
    ];
    let mut failures = Vec::new();
    for expr in exprs {
        let query = format!("SELECT {expr}");
        let want = server_tsv(&query);
        let (got, err, code) = shim_bytes(&query);
        if code != 0 || got != want {
            failures.push(format!(
                "{query}\n  server: {:?}\n  shim:   {:?} (exit code {code}, stderr {err:?})",
                String::from_utf8_lossy(&want),
                String::from_utf8_lossy(&got),
            ));
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

/// The TSV variants of the output format.
#[test]
fn output_formats() {
    if !live() {
        return;
    }
    let cases = [
        (
            "SELECT 1 AS a, 'x\\ty' AS `b c` FORMAT TSVWithNames",
            "a\tb c\n1\tx\\ty\n",
        ),
        (
            "SELECT 1 AS a, 'x\\ty' AS b FORMAT TabSeparatedWithNamesAndTypes",
            "a\tb\nUInt8\tString\n1\tx\\ty\n",
        ),
        ("SELECT 'x\\ty' FORMAT TSVRaw", "x\ty\n"),
        ("SELECT ['x\\ty'] FORMAT TSVRaw", "['x\\ty']\n"),
        ("SELECT 1 FORMAT Null", ""),
        ("SELECT 1 FORMAT `Null`", ""),
        ("SELECT 1 FORMAT tsv SETTINGS max_threads = 1", "1\n"),
    ];
    for (query, want) in cases {
        let (got, err, code) = shim(query);
        assert_eq!((got.as_str(), code), (want, 0), "{query}: {err}");
    }
    let (_, err, code) = shim("SELECT 1 FORMAT JSONEachRow");
    assert_eq!(code, 1);
    assert!(
        err.contains("output format JSONEachRow is not supported"),
        "{err}"
    );
}

/// The header lines (names, types): a Raw format writes them without
/// escapes, as its rows. Contrast: TSVWithNames escapes the header.
#[test]
fn header_of_format() {
    if !live() {
        return;
    }
    let select = "SELECT 1 AS `a\\tb\\\\c'd`, 'x' AS `é\\nf`, \
                  CAST('a' AS Enum8('a\\tb' = 1, 'a' = 2)) AS e";
    let formats = [
        "TSVWithNames",
        "TSVWithNamesAndTypes",
        "TSVRawWithNames",
        "TabSeparatedRawWithNamesAndTypes",
        "RawWithNames",
    ];
    let mut failures = Vec::new();
    for format in formats {
        let query = format!("{select} FORMAT {format}");
        let want = server_output(select, format);
        let (got, err, code) = shim_bytes(&query);
        if code != 0 || got != want {
            failures.push(format!(
                "{format}\n  server: {:?}\n  shim:   {:?} (exit code {code}, stderr {err:?})",
                String::from_utf8_lossy(&want),
                String::from_utf8_lossy(&got),
            ));
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

/// `SET`, `USE` and temporary tables last for the whole invocation, also
/// after an error that a test hint expects.
#[test]
fn session_state() {
    if !live() {
        return;
    }
    let script = [
        "SET max_threads = 3;",
        "SELECT throwIf(1); -- { serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }",
        "SELECT getSetting('max_threads');",
        "CREATE TEMPORARY TABLE t (x UInt8);",
        "INSERT INTO t VALUES (1), (2);",
        "INSERT INTO t FORMAT TSV",
        "3",
        "",
        "INSERT INTO t FORMAT CSV",
        "4",
        "5",
        "",
        "SELECT sum(x) FROM t;",
        "USE system;",
        "SELECT currentDatabase();",
        "SELECT 1 FROM one;",
    ]
    .join("\n");
    let (got, err, code) = shim(&script);
    assert_eq!((got.as_str(), code), ("3\n15\nsystem\n1\n", 0), "{err}");
}

#[test]
fn errors() {
    if !live() {
        return;
    }
    // An unexpected error stops the script, writes the error name to
    // stderr, and sets the exit code to the error code.
    let (got, err, code) = shim("SELECT 1; SELECT * FROM no_such_table_xyz; SELECT 2;");
    assert_eq!(got, "1\n");
    assert_eq!(code, 60);
    assert!(err.contains("UNKNOWN_TABLE"), "{err}");

    // A query that succeeds although the hint expects an error.
    let (_, err, code) = shim("SELECT 1; -- { serverError UNKNOWN_TABLE }");
    assert_ne!(code, 0);
    assert!(err.contains("expected serverError UNKNOWN_TABLE"), "{err}");

    // A different error than the hint expects.
    let (_, err, code) = shim("SELECT * FROM no_such_table_xyz; -- { serverError SYNTAX_ERROR }");
    assert_eq!(code, 60, "stderr: {err}");

    // A syntax error satisfies a clientError hint, and is not echoed.
    let (got, err, code) =
        shim("-- { echo }\nSELECT 1;\nSELEC 2; -- { clientError SYNTAX_ERROR }\nSELECT 3;");
    assert_eq!(
        (got.as_str(), code),
        ("-- { echo }\nSELECT 1;\n1\nSELECT 3;\n3\n", 0),
        "{err}"
    );

    // A syntax error in the argument of `formatQuery` comes after the
    // statement parsed, so clickhouse-client echoes the statement.
    let script =
        "-- { echo }\nSELECT formatQuery('SELECT 1 +'); -- { serverError SYNTAX_ERROR }\nSELECT 3;";
    let (got, err, code) = shim(script);
    assert_eq!(
        (got.as_str(), code),
        (format!("{script}\n3\n").as_str(), 0),
        "{err}"
    );

    // A Poco exception has no `Code: N.` prefix in its text
    // (`Poco::Exception. Code: 1000, ...`), but satisfies its hint.
    let (got, err, code) = shim(
        "SELECT * FROM url('http://127.0.0.1:1337/? HTTP/1.1\\r\\nTest: test', CSV, 'c String'); -- { serverError POCO_EXCEPTION }\nSELECT 1;",
    );
    assert_eq!((got.as_str(), code), ("1\n", 0), "{err}");

    // An exception in the middle of the result.
    let (_, err, code) =
        shim("SELECT throwIf(number = 100000) FROM numbers(200000) SETTINGS max_block_size = 1000");
    assert_eq!(code, 395 % 256, "stderr: {err}");
}

/// Statement forms that the ClickHouse lexer accepts: `//` comments, Unicode
/// whitespace before the keyword, and `KILL`, which returns rows.
#[test]
fn statement_forms() {
    if !live() {
        return;
    }
    let (got, err, code) = shim("SELECT 1; // μs\nSELECT 2 // x;\n;");
    assert_eq!((got.as_str(), code), ("1\n2\n", 0), "{err}");

    let (got, err, code) =
        shim("\u{0085}\u{00A0} \u{2000}\u{2028}SELECT\u{2029}\u{3000}3\u{FEFF};");
    assert_eq!((got.as_str(), code), ("3\n", 0), "{err}");

    // The mutation cannot finish ('x' is not a number), so it waits until
    // KILL MUTATION stops it, and KILL MUTATION writes it as a row.
    let script = [
        "DROP TABLE IF EXISTS shim_live_kill;",
        "CREATE TABLE shim_live_kill (v String) ENGINE = MergeTree ORDER BY tuple();",
        "INSERT INTO shim_live_kill VALUES ('x');",
        "ALTER TABLE shim_live_kill MODIFY COLUMN v UInt64 SETTINGS mutations_sync = 2; -- { serverError UNFINISHED }",
        "KILL MUTATION WHERE database = currentDatabase() AND table = 'shim_live_kill';",
        "DROP TABLE shim_live_kill;",
    ]
    .join("\n");
    let (got, err, code) = shim(&script);
    assert_eq!(code, 0, "{err}");
    assert!(
        got.starts_with("waiting\tdefault\tshim_live_kill\tmutation_2.txt\t")
            && got.lines().count() == 1,
        "{got:?}"
    );
}

/// After `SET dialect`, every statement runs as a query, without its
/// trailing semicolon.
#[test]
fn other_dialect() {
    if !live() {
        return;
    }
    let script = "SET allow_experimental_kusto_dialect = 1;\nSET dialect = 'kusto';\nprint x = 1 + 2;\nSET dialect = 'clickhouse';\nSELECT 4;";
    let (got, err, code) = shim(script);
    assert_eq!((got.as_str(), code), ("3\n4\n", 0), "{err}");
}
