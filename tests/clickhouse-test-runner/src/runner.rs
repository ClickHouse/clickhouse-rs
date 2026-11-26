//! Runs the statements of one invocation through clickhouse-rs, on one
//! session.
//!
//! clickhouse-rs speaks HTTP. A session over HTTP is a `session_id`: the
//! client sends the same `session_id` setting with every request, so `SET`,
//! `USE` and temporary tables last for the whole invocation, also after an
//! error. The shim sends nothing that `clickhouse-client` does not send,
//! because some tests read their own queries from `system.query_log`.

use crate::args::{Options, http_port};
use crate::format::{OutputFormat, returns_rows, split_output_format, unquote_identifier};
use crate::log::InvocationLog;
use crate::render::{Context, render_cell, write_string};
use crate::split::{
    Statement, is_word_byte, leading_keyword, leading_word_is_call, skip_quoted,
    skip_whitespace_and_comments,
};
use crate::value::decode_column;
use clickhouse::Client;
use clickhouse::error::Error as ClientError;
use std::collections::BTreeMap;
use std::hash::{BuildHasher, Hasher};
use std::io::Write;

const SYNTAX_ERROR: i32 = 62;

/// Why a statement or the invocation failed.
#[derive(Debug)]
pub(crate) enum Failure {
    /// A server exception (`Code: N. DB::Exception: ...`).
    Server { code: i32, message: String },
    /// A result that does not match the test hint.
    Hint { message: String, code: Option<i32> },
    /// Any other error: of the client, of the shim, or of the output.
    Other(String),
}

impl From<ClientError> for Failure {
    fn from(e: ClientError) -> Self {
        match &e {
            ClientError::BadResponse(message) => match server_error_code(message) {
                Some(code) => Failure::Server {
                    code,
                    message: message.clone(),
                },
                None => Failure::Other(e.to_string()),
            },
            _ => Failure::Other(format!("clickhouse-rs: {}", error_chain(&e))),
        }
    }
}

/// The text of an error and of its sources, without the repetitions (an
/// error often writes its source into its own text).
pub(crate) fn error_chain(e: &dyn std::error::Error) -> String {
    let mut text = e.to_string();
    let mut source = e.source();
    while let Some(s) = source {
        let s_text = s.to_string();
        if !text.contains(&s_text) {
            text.push_str(": ");
            text.push_str(&s_text);
        }
        source = s.source();
    }
    text
}

impl From<std::io::Error> for Failure {
    fn from(e: std::io::Error) -> Self {
        Failure::Other(format!("write stdout: {e}"))
    }
}

/// Reads the error code of a server exception. The client gives the text
/// of the exception (`Code: 60. DB::Exception: ...`), or only `Code: 60`
/// from the `X-ClickHouse-Exception-Code` header when the body is empty.
/// An exception that is not a `DB::Exception` has a prefix before the code
/// (`Poco::Exception. Code: 1000, e.code() = 0, ...`; the forms of
/// `getCurrentExceptionMessage` in ClickHouse's `src/Common/Exception.cpp`).
pub(crate) fn server_error_code(message: &str) -> Option<i32> {
    const PREFIXES: [&str; 3] = [
        "Poco::Exception. ",
        "std::exception. ",
        "Unknown exception. ",
    ];
    let message = message.trim_start();
    let message = PREFIXES
        .iter()
        .find_map(|p| message.strip_prefix(p))
        .unwrap_or(message);
    let rest = message.strip_prefix("Code: ")?;
    let end = rest
        .bytes()
        .position(|c| !c.is_ascii_digit())
        .unwrap_or(rest.len());
    rest[..end].parse().ok()
}

/// Tells if a `SYNTAX_ERROR` comes from a function that parses its argument
/// (`formatQuery`, `formatQuerySingleLine`), not from the statement itself.
/// The statement then parsed, and the server adds the context of the
/// function to the message: `In scope ...` (analyzer) or `While processing
/// ...` (old analyzer). A statement that does not parse has no such context.
fn is_syntax_error_of_function(message: &str) -> bool {
    message.contains(": In scope ") || message.contains(": While processing ")
}

pub(crate) struct Runner<'a> {
    opts: &'a Options,
    client: Client,
    log: &'a mut InvocationLog,
    ctx: Context,
    /// The command-line settings that go with each query, as
    /// clickhouse-client sends them. A `SET` of the same setting in the test
    /// removes it from here, because the session then holds the value.
    settings: BTreeMap<String, String>,
    /// The value of the last `SET dialect`.
    dialect: String,
    /// Set by `SET implicit_select = 1`, with which an expression alone
    /// (`1 + 2`) is a query.
    implicit: bool,
}

impl<'a> Runner<'a> {
    pub(crate) fn new(opts: &'a Options, log: &'a mut InvocationLog) -> Self {
        let session_id = new_session_id();
        log.add("session_id", &session_id);
        let scheme = if opts.secure { "https" } else { "http" };
        let port = http_port(opts.port, opts.secure, |name| std::env::var(name).ok());
        let host = if opts.host.contains(':') {
            format!("[{}]", opts.host)
        } else {
            opts.host.clone()
        };
        let url = format!("{scheme}://{host}:{port}");
        log.add("url", &url);
        let client = Client::default()
            .with_url(url)
            .with_user(opts.user.clone())
            .with_password(opts.password.clone())
            .with_database(opts.database.clone())
            .with_setting("session_id", session_id);
        Runner {
            opts,
            client,
            log,
            ctx: Context::default(),
            settings: opts.settings.clone(),
            dialect: String::new(),
            implicit: false,
        }
    }

    /// Runs the statements in order. It stops at the first statement whose
    /// result does not match its test hint, as `clickhouse-client` does.
    pub(crate) async fn run(
        &mut self,
        stmts: &[Statement],
        out: &mut dyn Write,
    ) -> Result<(), Failure> {
        for st in stmts {
            self.run_one(st, out).await?;
        }
        Ok(())
    }

    async fn run_one(&mut self, st: &Statement, out: &mut dyn Write) -> Result<(), Failure> {
        if let Some(err) = &st.hint.parse_error {
            self.echo(st, out)?;
            return Err(Failure::Other(err.clone()));
        }
        self.log.add("query", &st.sql);
        let result = self.execute(&st.sql).await;
        // clickhouse-client parses a statement before it echoes it, so it does
        // not echo a statement with a syntax error.
        let unparsable = matches!(
            &result,
            Err(Failure::Server {
                code: SYNTAX_ERROR,
                message,
            }) if !is_syntax_error_of_function(message)
        );
        if !unparsable {
            self.echo(st, out)?;
        }
        let result = match result {
            Ok(text) => {
                out.write_all(&text)?;
                Ok(())
            }
            Err(e) => Err(e),
        };
        out.flush()?;

        match result {
            Ok(()) if st.hint.expects_error() => Err(Failure::Hint {
                message: format!(
                    "expected {}, but the query succeeded (query: {})",
                    st.hint.label(),
                    st.full
                ),
                code: None,
            }),
            Ok(()) => self.after_success(&st.sql),
            Err(Failure::Server { code, message }) if st.hint.expects_error() => {
                if st.hint.matches(code) {
                    self.log.add("expected_error_matched", &st.hint.label());
                    Ok(())
                } else {
                    Err(Failure::Hint {
                        message: format!(
                            "expected {}, but got {message} (query: {})",
                            st.hint.label(),
                            st.full
                        ),
                        code: Some(code),
                    })
                }
            }
            Err(e) => Err(e),
        }
    }

    /// Writes the statement for the `echo` test hints.
    fn echo(&self, st: &Statement, out: &mut dyn Write) -> std::io::Result<()> {
        if st.hint.echo_this {
            out.write_all(st.full.as_bytes())?;
            out.write_all(b"\n")?;
        }
        Ok(())
    }

    fn query(&self, sql: &str) -> clickhouse::query::Query {
        // `query_raw`: the test SQL has `?` in literals and in the ternary
        // operator, which `Client::query` reads as bind placeholders.
        let mut q = self.client.query_raw(sql);
        for (name, value) in &self.settings {
            q = q.with_setting(name.clone(), value.clone());
        }
        for (name, value) in &self.opts.params {
            // The raw value, as clickhouse-client sends it. `Query::param`
            // would escape a string value.
            q = q.with_setting(format!("param_{name}"), value.clone());
        }
        q
    }

    /// Runs one statement and returns its output.
    async fn execute(&mut self, sql: &str) -> Result<Vec<u8>, Failure> {
        let other_dialect = self.other_dialect(sql);
        let sql = if other_dialect {
            // Over HTTP the server parses the request body as one query, and
            // the parsers of other dialects do not accept a trailing
            // semicolon.
            let s = sql.trim_end_matches([' ', '\t', '\r', '\n']);
            s.strip_suffix(';').unwrap_or(s)
        } else {
            sql
        };
        if !returns_rows(sql) && !other_dialect && !self.implicit_select(sql) {
            self.query(sql).execute().await?;
            return Ok(Vec::new());
        }
        let (query, mut format) = if other_dialect {
            // The FORMAT clause is SQL; other dialects can use the word
            // differently (the `format` pipe of LogsQL).
            (sql.to_owned(), OutputFormat::tab_separated())
        } else {
            split_output_format(sql)
        };
        if !format.supported {
            return Err(Failure::Other(format!(
                "clickhouse-test-runner: output format {} is not supported: the shim reads results \
                 as Native with clickhouse-rs and writes only the TabSeparated formats and Null",
                format.name
            )));
        }
        let explain_null = format.discard && leading_keyword(sql) == "EXPLAIN";
        if explain_null {
            // With ignore_format_null_for_explain (on by default), the server
            // ignores `FORMAT Null` for EXPLAIN and sends the plan. The server
            // decides, so write the rows that it sends.
            format = OutputFormat::tab_separated();
        }
        // With `FORMAT Null`, send the text as it is: the server sends no
        // rows. Otherwise remove the clause, so that the server sends Native.
        let query = if format.discard || explain_null {
            sql.to_owned()
        } else {
            query
        };
        let mut cursor = self.query(&query).fetch_native()?;
        let mut out = Vec::new();
        let mut header = format.names || format.types;
        while let Some(block) = cursor.next().await? {
            let columns = block.columns();
            if header {
                header = false;
                if format.names {
                    write_header(
                        &mut out,
                        columns.iter().map(|c| c.name_bytes().to_vec()),
                        format.raw,
                    );
                }
                if format.types {
                    write_header(
                        &mut out,
                        columns
                            .iter()
                            .map(|c| c.data_type().to_string().into_bytes()),
                        format.raw,
                    );
                }
            }
            let mut values = Vec::with_capacity(columns.len());
            for c in columns {
                let column = decode_column(c).map_err(|e| {
                    Failure::Other(format!(
                        "clickhouse-rs: decode column {:?} of type {}: {}",
                        String::from_utf8_lossy(c.name_bytes()),
                        c.data_type(),
                        error_chain(&e)
                    ))
                })?;
                if column.len() != block.num_rows() {
                    return Err(Failure::Other(format!(
                        "clickhouse-rs: column {:?} has {} values in a block of {} rows",
                        String::from_utf8_lossy(c.name_bytes()),
                        column.len(),
                        block.num_rows()
                    )));
                }
                values.push(column);
            }
            if format.discard {
                // `FORMAT Null`: the values are decoded, but not written.
                continue;
            }
            for row in 0..block.num_rows() {
                for (i, (c, column)) in columns.iter().zip(&values).enumerate() {
                    if i > 0 {
                        out.push(b'\t');
                    }
                    render_cell(&mut out, c.data_type(), &column[row], format.raw, &self.ctx)
                        .map_err(|e| {
                            Failure::Other(format!(
                                "column {:?}: {e}",
                                String::from_utf8_lossy(c.name_bytes())
                            ))
                        })?;
                }
                out.push(b'\n');
            }
        }
        Ok(out)
    }

    /// Tells if the session uses another query language than SQL
    /// (`SET dialect = 'kusto'`, 'prql', ...). The shim cannot see if a
    /// statement in such a language returns rows, so it reads a result for
    /// each statement.
    fn other_dialect(&self, sql: &str) -> bool {
        !self.dialect.is_empty()
            && !self.dialect.eq_ignore_ascii_case("clickhouse")
            && leading_keyword(sql) != "SET"
    }

    /// Tells if the statement is an expression alone, which
    /// `SET implicit_select = 1` makes a query.
    fn implicit_select(&self, sql: &str) -> bool {
        self.implicit && !is_command(sql)
    }

    /// Updates the session state after a `USE` or `SET` statement.
    fn after_success(&mut self, sql: &str) -> Result<(), Failure> {
        match leading_keyword(sql).as_str() {
            "USE" => {
                let db = parse_use_database(sql, &self.opts.params).ok_or_else(|| {
                    Failure::Other(format!(
                        "clickhouse-test-runner: cannot read the database name of {sql:?}"
                    ))
                })?;
                // The client sends its database with each request, and that
                // database replaces the current database of the session.
                self.log.add("database", &db);
                self.client = self.client.clone().with_database(db);
            }
            "SET" => {
                for (name, value) in parse_set_assignments(sql) {
                    self.settings.remove(&name);
                    match name.as_str() {
                        "dialect" => self.dialect = value,
                        "implicit_select" => {
                            self.implicit = value != "0" && !value.eq_ignore_ascii_case("false")
                        }
                        _ => {}
                    }
                }
            }
            _ => {}
        }
        Ok(())
    }
}

/// Writes a header line (names or types). A Raw format writes it without
/// escapes, as its rows (`TabSeparatedRowOutputFormat::writeLine` in
/// ClickHouse).
fn write_header(out: &mut Vec<u8>, cells: impl Iterator<Item = Vec<u8>>, raw: bool) {
    for (i, cell) in cells.enumerate() {
        if i > 0 {
            out.push(b'\t');
        }
        if raw {
            out.extend_from_slice(&cell);
        } else {
            write_string(out, &cell, false);
        }
    }
    out.push(b'\n');
}

/// Tells if the statement starts with the keyword of a DDL, DML or other
/// command, which `SET implicit_select = 1` does not make a query. A first
/// word that a `(` follows is a function call (`replace('a', 'a', 'b')`), not
/// a keyword.
fn is_command(sql: &str) -> bool {
    !leading_word_is_call(sql)
        && matches!(
            leading_keyword(sql).as_str(),
            "INSERT"
                | "DELETE"
                | "UPDATE"
                | "REPLACE"
                | "CREATE"
                | "DROP"
                | "ALTER"
                | "SET"
                | "USE"
                | "TRUNCATE"
                | "RENAME"
                | "OPTIMIZE"
                | "SYSTEM"
                | "GRANT"
                | "REVOKE"
                | "ATTACH"
                | "DETACH"
                | "KILL"
                | "EXCHANGE"
                | "UNDROP"
                | "BACKUP"
                | "RESTORE"
        )
}

fn new_session_id() -> String {
    // Two hashers with random keys: 128 random bits without a dependency.
    let mut id = String::from("clickhouse-rs-test-runner-");
    for _ in 0..2 {
        let mut h = std::collections::hash_map::RandomState::new().build_hasher();
        h.write_u128(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or_default(),
        );
        h.write_u32(std::process::id());
        id.push_str(&format!("{:016x}", h.finish()));
    }
    id
}

/// Returns the database name of a `USE` statement. A `{name:Identifier}`
/// query parameter is replaced by its value.
pub(crate) fn parse_use_database(sql: &str, params: &BTreeMap<String, String>) -> Option<String> {
    let i = skip_whitespace_and_comments(sql.as_bytes(), 0) + "USE".len();
    let mut rest = sql.get(i..)?.trim();
    rest = rest.strip_suffix(';').unwrap_or(rest).trim();
    if let Some((word, after)) = rest.split_once(' ')
        && word.eq_ignore_ascii_case("DATABASE")
    {
        rest = after.trim();
    }
    match rest.as_bytes().first()? {
        b'`' | b'"' => unquote_identifier(rest),
        b'{' => {
            let (name, _) = rest.trim_matches(['{', '}']).split_once(':')?;
            params.get(name.trim()).cloned()
        }
        _ if rest.bytes().all(is_word_byte) => Some(rest.to_owned()),
        _ => None,
    }
}

/// Returns the settings that a `SET a = 1, b = 'x'` statement assigns, with
/// their values (string values without quotes).
pub(crate) fn parse_set_assignments(sql: &str) -> BTreeMap<String, String> {
    let i = skip_whitespace_and_comments(sql.as_bytes(), 0) + "SET".len();
    let body = sql.get(i..).unwrap_or("").trim();
    let body = body.strip_suffix(';').unwrap_or(body);
    let mut out = BTreeMap::new();
    let mut add = |part: &str| {
        if let Some((name, value)) = part.split_once('=') {
            let value = value.trim();
            let value = if value.len() >= 2 && value.starts_with('\'') && value.ends_with('\'') {
                &value[1..value.len() - 1]
            } else {
                value
            };
            out.insert(name.trim().to_owned(), value.to_owned());
        }
    };
    let b = body.as_bytes();
    let (mut depth, mut start, mut j) = (0i32, 0, 0);
    while j < b.len() {
        match b[j] {
            b'\'' | b'"' | b'`' => {
                j = skip_quoted(b, j);
                continue;
            }
            b'(' | b'[' => depth += 1,
            b')' | b']' => depth -= 1,
            b',' if depth == 0 => {
                add(&body[start..j]);
                start = j + 1;
            }
            _ => {}
        }
        j += 1;
    }
    add(&body[start..]);
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn use_database() {
        let params = BTreeMap::from([("CLICKHOUSE_DATABASE".to_owned(), "test_x".to_owned())]);
        for (input, want) in [
            ("USE db1;", "db1"),
            ("use `my db`", "my db"),
            ("-- c\nUSE DATABASE db2", "db2"),
            ("USE {CLICKHOUSE_DATABASE:Identifier};", "test_x"),
            ("USE \"db3\"", "db3"),
        ] {
            assert_eq!(
                parse_use_database(input, &params).as_deref(),
                Some(want),
                "{input}"
            );
        }
        assert_eq!(parse_use_database("USE db1, db2", &params), None);
    }

    #[test]
    fn syntax_error_of_function() {
        // Messages of server 26.10 (test 04700_backup_settings_default_round_trip).
        assert!(is_syntax_error_of_function(
            "Code: 62. DB::Exception: Syntax error: failed at position 87 (end of query): . \
             Expected one of: token, Comma, end of query: In scope SELECT \
             formatQuerySingleLine('BACKUP TABLE t'). (SYNTAX_ERROR)"
        ));
        assert!(!is_syntax_error_of_function(
            "Code: 62. DB::Exception: Syntax error: failed at position 11 (end of query): . \
             Expected one of: token, DoubleColon, OR, AND. (SYNTAX_ERROR)"
        ));
    }

    #[test]
    fn set_assignments() {
        assert_eq!(
            parse_set_assignments("SET dialect = 'kusto', max_threads=1, x = (1, 2);"),
            BTreeMap::from([
                ("dialect".to_owned(), "kusto".to_owned()),
                ("max_threads".to_owned(), "1".to_owned()),
                ("x".to_owned(), "(1, 2)".to_owned()),
            ])
        );
    }

    #[test]
    fn error_codes() {
        assert_eq!(
            server_error_code("Code: 60. DB::Exception: Unknown table. (UNKNOWN_TABLE)"),
            Some(60)
        );
        assert_eq!(server_error_code("Code: 395"), Some(395));
        assert_eq!(server_error_code("502 Bad Gateway"), None);
        // The forms of the exceptions that are not a DB::Exception.
        assert_eq!(
            server_error_code(
                "Poco::Exception. Code: 1000, e.code() = 0, Bad URI syntax: URI contains invalid \
                 characters (version 26.10.4.207 (official build))"
            ),
            Some(1000)
        );
        assert_eq!(
            server_error_code("std::exception. Code: 1001, type: std::bad_alloc, e.what() = x"),
            Some(1001)
        );
        assert_eq!(
            server_error_code("Unknown exception. Code: 1002, type: X"),
            Some(1002)
        );
        assert_eq!(server_error_code("Poco::Exception. no code"), None);
    }

    #[test]
    fn error_chains() {
        let e = ClientError::DataFormat("outer: inner".into());
        assert_eq!(error_chain(&e), "data format error: outer: inner");
        let io = std::io::Error::other(ClientError::NotEnoughData);
        assert_eq!(
            error_chain(&io),
            "not enough data, probably a row type mismatches a database schema"
        );
    }

    #[test]
    fn commands_are_not_implicit_selects() {
        for sql in [
            "INSERT INTO t VALUES (1)",
            "DELETE FROM t WHERE x = 1",
            "update t SET x = 2 WHERE x = 1",
            "REPLACE TABLE t AS SELECT 1",
            "CREATE TABLE t (x UInt8) ENGINE = Memory",
            "-- c\nALTER TABLE t DELETE WHERE 1",
            "SET implicit_select = 0",
        ] {
            assert!(is_command(sql), "{sql}");
        }
        for sql in [
            "1 + 1",
            "'a'",
            "SELECT 1",
            "now() > 0",
            "replace('abc', 'b', 'x')",
            "REPLACE ('a', 'a', 'b')",
            "(replace('a', 'a', 'b'))",
        ] {
            assert!(!is_command(sql), "{sql}");
        }
    }

    #[test]
    fn session_ids_differ() {
        assert_ne!(new_session_id(), new_session_id());
    }
}
