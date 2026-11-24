//! The native TCP protocol transport, selected by `tcp://` and `tcps://` URLs.
//!
//! The transport keeps the public API of the HTTP transport. The server returns columnar blocks,
//! which are converted to the format that the cursor reads (`RowBinary`,
//! `RowBinaryWithNamesAndTypes` or `Native`). For `INSERT`, the rows that [`Insert`] writes
//! are converted to blocks with the column types that the server sends.
//!
//! [`Insert`]: crate::insert::Insert

use crate::error::{Error, Result};
use crate::{Authentication, Client, Compression, settings};
use pool::{KeyAuth, PoolKey, SessionOptions};
use std::time::Duration;

mod connection;
mod error_codes;
mod insert;
mod params;
mod pool;
mod profile_events;
mod protocol;
mod query;
#[cfg(test)]
mod server_tests;
mod tls;

pub(crate) use insert::spawn_insert;
pub(crate) use pool::Pool;
pub(crate) use query::{BlocksFuture, query, query_blocks};

/// The default port of the native protocol.
const DEFAULT_PORT: u16 = 9000;
/// The default port of the native protocol with TLS.
const DEFAULT_SECURE_PORT: u16 = 9440;

/// The address of the server.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct Address {
    pub(crate) host: String,
    pub(crate) port: u16,
    pub(crate) secure: bool,
}

impl Address {
    pub(crate) fn parse(url: &str) -> Result<Self> {
        let parsed = url::Url::parse(url).map_err(|e| Error::InvalidParams(Box::new(e)))?;

        let secure = match parsed.scheme() {
            "tcp" => false,
            "tcps" => true,
            other => {
                return Err(Error::InvalidParams(
                    format!("unsupported URL scheme for the native TCP transport: {other:?}")
                        .into(),
                ));
            }
        };

        let host = match parsed.host() {
            Some(url::Host::Domain(domain)) => domain.to_owned(),
            Some(url::Host::Ipv4(ip)) => ip.to_string(),
            Some(url::Host::Ipv6(ip)) => ip.to_string(),
            None => {
                return Err(Error::InvalidParams(
                    format!("URL has no host: {url:?}").into(),
                ));
            }
        };

        let port = parsed.port().unwrap_or(if secure {
            DEFAULT_SECURE_PORT
        } else {
            DEFAULT_PORT
        });

        Ok(Self { host, port, secure })
    }
}

/// Parameters of the HTTP interface that are not settings.
///
/// The native protocol has fields or other mechanisms for them, or does not need them.
const HTTP_ONLY_PARAMETERS: &[&str] = &[
    settings::DATABASE,
    settings::DEFAULT_FORMAT,
    settings::QUERY,
    settings::QUERY_ID,
    settings::SESSION_ID,
    settings::ROLE,
    settings::DECOMPRESS,
    settings::CLIENT_PROTOCOL_VERSION,
    "session_timeout",
    "session_check",
    "close_session",
    "compress",
    "user",
    "password",
    "quota_key",
    "stacktrace",
    "buffer_size",
    "wait_end_of_query",
];

/// A query or an `INSERT` for the native transport.
pub(crate) struct Request {
    key: PoolKey,
    settings: Vec<(String, String)>,
    parameters: Vec<(String, String)>,
    query_id: String,
    /// The roles to set for the query, sorted; empty for the default roles.
    roles: Vec<String>,
    session: Option<SessionOptions>,
    compression: Compression,
    wait_end_of_query: bool,
    #[cfg(feature = "opentelemetry")]
    trace_context: Option<protocol::TraceContext>,
}

impl Request {
    pub(crate) fn new(client: &Client) -> Result<Self> {
        let address = Address::parse(&client.url)?;

        let auth = match &client.authentication {
            Authentication::Credentials { user, password } => KeyAuth::Password {
                user: user.clone().unwrap_or_else(|| "default".to_owned()),
                password: password.clone().unwrap_or_default(),
            },
            Authentication::Jwt { access_token } => KeyAuth::Jwt(access_token.clone()),
        };

        let get = |name: &str| client.settings.get(name).map(String::as_str);
        let is_true = |name: &str| {
            get(name).is_some_and(|value| value == "1" || value.eq_ignore_ascii_case("true"))
        };

        let mut settings = Vec::new();
        let mut parameters = Vec::new();

        for (name, value) in &client.settings {
            if let Some(parameter) = name.strip_prefix("param_") {
                parameters.push((parameter.to_owned(), value.clone()));
            } else if !HTTP_ONLY_PARAMETERS.contains(&name.as_str()) {
                settings.push((name.clone(), value.clone()));
            }
        }

        // Sort for a deterministic order of the packet.
        settings.sort();
        parameters.sort();

        let mut roles: Vec<String> = if client.roles.is_empty() {
            get(settings::ROLE).map(str::to_owned).into_iter().collect()
        } else {
            client.roles.iter().cloned().collect()
        };
        roles.sort();

        let session = get(settings::SESSION_ID)
            .filter(|id| !id.is_empty())
            .map(|id| -> Result<SessionOptions> {
                let timeout = match get("session_timeout") {
                    Some(timeout) => Duration::from_secs(timeout.parse().map_err(|_| {
                        Error::InvalidParams(format!("invalid session_timeout: {timeout:?}").into())
                    })?),
                    None => pool::DEFAULT_SESSION_TIMEOUT,
                };

                Ok(SessionOptions {
                    id: id.to_owned(),
                    timeout,
                    check: is_true("session_check"),
                    close: is_true("close_session"),
                })
            })
            .transpose()?;

        Ok(Self {
            key: PoolKey {
                address,
                database: client.database.clone().unwrap_or_default(),
                auth,
                quota_key: get("quota_key").unwrap_or_default().to_owned(),
                client_name: crate::headers::get_user_agent(
                    &client.app_product_info,
                    &client.stack_product_info,
                ),
            },
            settings,
            parameters,
            query_id: get(settings::QUERY_ID).unwrap_or_default().to_owned(),
            roles,
            session,
            compression: client.compression,
            wait_end_of_query: is_true("wait_end_of_query"),
            #[cfg(feature = "opentelemetry")]
            trace_context: protocol::current_trace_context(),
        })
    }

    /// Add a setting unless it is set already.
    pub(crate) fn add_setting(&mut self, name: &str, value: &str) {
        if !self.settings.iter().any(|(existing, _)| existing == name) {
            self.settings.push((name.to_owned(), value.to_owned()));
        }
    }

    pub(crate) fn setting(&self, name: &str) -> Option<&str> {
        self.settings
            .iter()
            .find(|(existing, _)| existing == name)
            .map(|(_, value)| value.as_str())
    }
}

/// Returns `true` if the statement changes the state of the session: `SET`, `USE`,
/// temporary tables and transactions.
///
/// A pooled connection is closed after such a statement, so that the change has no effect
/// on other queries, like with the HTTP interface without a session.
pub(crate) fn changes_session(sql: &[u8]) -> bool {
    let words = leading_words(sql, 4);
    let word = |index: usize| {
        words
            .get(index)
            .map(|word| String::from_utf8_lossy(word).to_ascii_uppercase())
    };

    match word(0).as_deref() {
        Some("SET" | "USE" | "BEGIN" | "COMMIT" | "ROLLBACK") => true,
        Some("CREATE") => match word(1).as_deref() {
            Some("TEMPORARY") => true,
            Some("OR") => word(3).as_deref() == Some("TEMPORARY"),
            _ => false,
        },
        _ => false,
    }
}

/// The first `count` words of the SQL, skipping whitespace and comments.
fn leading_words(mut sql: &[u8], count: usize) -> Vec<&[u8]> {
    let mut words = Vec::new();

    while words.len() < count {
        sql = skip_whitespace_and_comments(sql);

        let len = sql
            .iter()
            .position(|b| !(b.is_ascii_alphanumeric() || *b == b'_'))
            .unwrap_or(sql.len());

        if len == 0 {
            break;
        }

        let (word, rest) = sql.split_at(len);
        words.push(word);
        sql = rest;
    }

    words
}

fn skip_whitespace_and_comments(mut sql: &[u8]) -> &[u8] {
    loop {
        let trimmed = sql.trim_ascii_start();
        let comment = params::comment_len(trimmed);

        if comment == 0 {
            return trimmed;
        }

        sql = trimmed.get(comment..).unwrap_or_default();
    }
}

/// A format that the native transport converts to and from blocks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RowFormat {
    RowBinary { names: bool, types: bool },
    Native,
}

impl RowFormat {
    pub(crate) fn from_name(name: &str) -> Option<Self> {
        use crate::formats::{ROW_BINARY, ROW_BINARY_WITH_NAMES_AND_TYPES};

        Some(match name {
            ROW_BINARY => Self::RowBinary {
                names: false,
                types: false,
            },
            "RowBinaryWithNames" => Self::RowBinary {
                names: true,
                types: false,
            },
            ROW_BINARY_WITH_NAMES_AND_TYPES => Self::RowBinary {
                names: true,
                types: true,
            },
            "Native" => Self::Native,
            _ => return None,
        })
    }
}

/// The format named by a trailing `FORMAT <name>` clause of the SQL, if any.
pub(crate) fn trailing_format(sql: &str) -> Option<&str> {
    let sql = sql.trim_end().trim_end_matches(';').trim_end();
    let (rest, name) = sql.rsplit_once(|c: char| c.is_ascii_whitespace())?;

    let keyword_start = rest
        .trim_end()
        .rfind(|c: char| c.is_ascii_whitespace() || c == ')')
        .map_or(0, |index| index + 1);
    let keyword = &rest.trim_end()[keyword_start..];

    let is_identifier =
        !name.is_empty() && name.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_');

    (keyword.eq_ignore_ascii_case("FORMAT") && is_identifier).then_some(name)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_addresses() {
        let parse = |url: &str| Address::parse(url).map(|a| (a.host, a.port, a.secure)).ok();

        assert_eq!(
            parse("tcp://localhost"),
            Some(("localhost".into(), 9000, false))
        );
        assert_eq!(
            parse("tcps://example.com"),
            Some(("example.com".into(), 9440, true))
        );
        assert_eq!(
            parse("tcp://127.0.0.1:19000"),
            Some(("127.0.0.1".into(), 19000, false))
        );
        assert_eq!(
            parse("tcp://[::1]:9001?database=x"),
            Some(("::1".into(), 9001, false))
        );
        assert_eq!(parse("http://localhost:8123"), None);
        assert_eq!(parse("tcp://"), None);

        assert!(crate::is_tcp_url("TCP://localhost"));
        assert!(crate::is_tcp_url("tcps://localhost"));
        assert!(!crate::is_tcp_url("http://localhost"));
        assert!(!crate::is_tcp_url("localhost:9000"));
    }

    #[test]
    fn finds_trailing_format() {
        assert_eq!(
            trailing_format("INSERT INTO t(a, b) FORMAT RowBinary"),
            Some("RowBinary")
        );
        assert_eq!(
            trailing_format("insert into t format   CSV ;\n"),
            Some("CSV")
        );
        assert_eq!(
            trailing_format("INSERT INTO t (a)FORMAT Native"),
            Some("Native")
        );
        assert_eq!(trailing_format("SELECT 1"), None);
        assert_eq!(trailing_format("SELECT format FROM t"), None);
        assert_eq!(trailing_format("SELECT 'FORMAT x'"), None);
        assert_eq!(trailing_format("FORMAT"), None);
    }

    #[test]
    fn detects_session_changes() {
        for sql in [
            "SET max_threads = 1",
            "  use db",
            "-- comment\nSET x = 1",
            "/* a */ /* b */ USE x",
            "CREATE TEMPORARY TABLE t (x UInt8)",
            "create or replace temporary table t (x UInt8)",
            "BEGIN TRANSACTION",
        ] {
            assert!(changes_session(sql.as_bytes()), "{sql}");
        }

        for sql in [
            "SELECT 1",
            "SETTINGS",
            "CREATE TABLE t (x UInt8) ENGINE = Memory",
            "CREATE OR REPLACE TABLE t (x UInt8) ENGINE = Memory",
            "INSERT INTO t VALUES (1)",
            "-- SET x = 1\nSELECT 1",
            "",
        ] {
            assert!(!changes_session(sql.as_bytes()), "{sql}");
        }
    }
}
