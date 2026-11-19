//! The `clickhouse-client` command line, as far as `tests/clickhouse-test`
//! and the `.sql` tests use it.

use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Options {
    pub(crate) host: String,
    /// `None` when not given.
    pub(crate) port: Option<u16>,
    pub(crate) user: String,
    pub(crate) password: String,
    pub(crate) database: String,
    pub(crate) secure: bool,
    pub(crate) query: Option<String>,
    pub(crate) help: bool,
    /// The server settings to send with each query.
    pub(crate) settings: BTreeMap<String, String>,
    /// The query parameters from `--param_<name>=<value>`.
    pub(crate) params: BTreeMap<String, String>,
}

impl Default for Options {
    fn default() -> Self {
        Options {
            host: "localhost".into(),
            port: None,
            user: "default".into(),
            password: String::new(),
            database: "default".into(),
            secure: false,
            query: None,
            help: false,
            settings: BTreeMap::new(),
            params: BTreeMap::new(),
        }
    }
}

/// The command-line settings that the shim sends to the server.
/// `tests/clickhouse-test` also adds many randomized settings to the command
/// line. The shim drops them: they only change performance-related paths in
/// the server, and some of them are client-only.
const SERVER_SETTINGS: &[&str] = &[
    "log_comment",
    "max_insert_threads",
    "max_threads",
    "send_logs_level",
];

struct OptionSpec {
    long: &'static str,
    short: Option<&'static str>,
    has_arg: bool,
}

const fn opt(long: &'static str, short: Option<&'static str>, has_arg: bool) -> OptionSpec {
    OptionSpec {
        long,
        short,
        has_arg,
    }
}

const KNOWN_OPTIONS: &[OptionSpec] = &[
    opt("host", Some("h"), true),
    opt("port", None, true),
    opt("user", Some("u"), true),
    opt("password", None, true),
    opt("database", Some("d"), true),
    opt("query", Some("q"), true),
    opt("secure", Some("s"), false),
    opt("no-secure", None, false),
    // As in clickhouse-client: `-n` is `--multiquery`, `-m` is
    // `--multiline`. The shim always runs every statement of the input.
    opt("multiquery", Some("n"), false),
    opt("multi-query", None, false),
    opt("multiline", Some("m"), false),
    opt("help", None, false),
    // Accepted and ignored.
    opt("config-file", Some("C"), true),
    opt("client_logs_file", None, true),
    opt("allow_repeated_settings", None, false),
    opt("fake-drop", None, false),
];

fn find_long(name: &str) -> Option<&'static OptionSpec> {
    KNOWN_OPTIONS.iter().find(|o| o.long == name)
}

fn find_short(name: &str) -> Option<&'static OptionSpec> {
    KNOWN_OPTIONS.iter().find(|o| o.short == Some(name))
}

/// Matches `^-(?:\d+\.?\d*|\.\d+)(?:[eE][+-]?\d+)?$`: a negative number is a
/// value, not an option.
fn is_negative_number(s: &str) -> bool {
    let Some(rest) = s.strip_prefix('-') else {
        return false;
    };
    let b = rest.as_bytes();
    let digits = |mut i: usize| {
        let start = i;
        while i < b.len() && b[i].is_ascii_digit() {
            i += 1;
        }
        (i, i - start)
    };
    let mut i;
    if b.first() == Some(&b'.') {
        let (end, n) = digits(1);
        if n == 0 {
            return false;
        }
        i = end;
    } else {
        let (end, n) = digits(0);
        if n == 0 {
            return false;
        }
        i = end;
        if i < b.len() && b[i] == b'.' {
            i = digits(i + 1).0;
        }
    }
    if i < b.len() && (b[i] == b'e' || b[i] == b'E') {
        i += 1;
        if i < b.len() && (b[i] == b'+' || b[i] == b'-') {
            i += 1;
        }
        let (end, n) = digits(i);
        if n == 0 {
            return false;
        }
        i = end;
    }
    i == b.len()
}

fn set(opts: &mut Options, name: &str, value: String) -> Result<(), String> {
    match name {
        "host" => opts.host = value,
        "port" => {
            opts.port = Some(
                value
                    .parse()
                    .map_err(|e| format!("bad --port value {value:?}: {e}"))?,
            )
        }
        "user" => opts.user = value,
        "password" => opts.password = value,
        "database" => opts.database = value,
        "query" => opts.query = Some(value),
        "secure" => opts.secure = true,
        "no-secure" => opts.secure = false,
        "help" => opts.help = true,
        _ => {}
    }
    Ok(())
}

pub(crate) fn parse_args(argv: &[String]) -> Result<Options, String> {
    let mut opts = Options::default();
    let mut i = 0;
    while i < argv.len() {
        let arg = &argv[i];
        if arg == "--" {
            break;
        }
        if let Some(body) = arg.strip_prefix("--") {
            let (name, value) = match body.split_once('=') {
                Some((n, v)) => (n, Some(v.to_owned())),
                None => (body, None),
            };
            if name.is_empty() {
                i += 1;
                continue;
            }
            if let Some(o) = find_long(name) {
                if !o.has_arg {
                    set(&mut opts, o.long, String::new())?;
                    i += 1;
                    continue;
                }
                let value = match value {
                    Some(v) => v,
                    None => {
                        i += 1;
                        argv.get(i)
                            .cloned()
                            .ok_or_else(|| format!("option --{name} needs a value"))?
                    }
                };
                set(&mut opts, o.long, value)?;
                i += 1;
                continue;
            }
            // Any other long option is a setting (`--max_threads=1`) or a
            // query parameter (`--param_x=1`). The value is optional.
            let mut value = value;
            if value.is_none()
                && let Some(next) = argv.get(i + 1)
                && (!next.starts_with('-') || is_negative_number(next))
            {
                value = Some(next.clone());
                i += 1;
            }
            let value = value.unwrap_or_else(|| "1".to_owned());
            if let Some(p) = name.strip_prefix("param_") {
                opts.params.insert(p.to_owned(), value);
            } else {
                let setting = name.replace('-', "_");
                if SERVER_SETTINGS.contains(&setting.as_str()) {
                    opts.settings.insert(setting, value);
                }
            }
            i += 1;
            continue;
        }
        if let Some(short) = arg.strip_prefix('-')
            && !short.is_empty()
        {
            // Unknown short options are ignored, as in the other language
            // ports.
            if let Some(o) = find_short(short) {
                if o.has_arg {
                    i += 1;
                    let value = argv
                        .get(i)
                        .cloned()
                        .ok_or_else(|| format!("option {arg} needs a value"))?;
                    set(&mut opts, o.long, value)?;
                } else {
                    set(&mut opts, o.long, String::new())?;
                }
            }
        }
        // Positional arguments are ignored.
        i += 1;
    }
    Ok(opts)
}

pub(crate) const USAGE: &str = "\
usage: clickhouse-client [options]

A clickhouse-client imitation that runs queries through clickhouse-rs, so that
the ClickHouse tests/clickhouse-test runner can drive it. If --query is not
given, the queries are read from stdin. Every \";\"-separated statement runs.

Options:
  -h, --host HOST          Server host (default: localhost)
      --port PORT          Server port. The native TCP port of the test runner
                           (CLICKHOUSE_PORT_TCP) maps to CLICKHOUSE_PORT_HTTP:
                           clickhouse-rs speaks HTTP only.
  -u, --user USER          User name (default: default)
      --password PASSWORD  Password (default: empty)
  -d, --database DB        Database (default: default)
  -q, --query SQL          Queries to run
  -s, --secure             Use TLS
      --param_NAME=VALUE   Query parameter
      --SETTING=VALUE      log_comment, max_insert_threads, max_threads and
                           send_logs_level go to the server; other settings
                           are ignored
  -n, --multiquery         Ignored
  -m, --multiline          Ignored
      --help               Show this help

Environment variables:
  CLICKHOUSE_CLIENT_CLI_LOG  Log file for troubleshooting
";

/// Maps the port that `tests/clickhouse-test` gives (the native TCP port,
/// from `CLICKHOUSE_PORT_TCP`) to the HTTP port that the runner exports. Any
/// other port is used as given.
pub(crate) fn http_port(
    port: Option<u16>,
    secure: bool,
    env: impl Fn(&str) -> Option<String>,
) -> u16 {
    let (native_env, http_env, native_default, http_default) = if secure {
        (
            "CLICKHOUSE_PORT_TCP_SECURE",
            "CLICKHOUSE_PORT_HTTPS",
            9440,
            8443,
        )
    } else {
        ("CLICKHOUSE_PORT_TCP", "CLICKHOUSE_PORT_HTTP", 9000, 8123)
    };
    let env_port = |name: &str, default: u16| {
        env(name)
            .and_then(|v| v.parse::<u16>().ok())
            .unwrap_or(default)
    };
    let native = env_port(native_env, native_default);
    match port {
        Some(p) if p != native => p,
        _ => env_port(http_env, http_default),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(list: &[&str]) -> Vec<String> {
        list.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn sql_test_command_line() {
        // The command line that tests/clickhouse-test uses for a .sql test.
        let opts = parse_args(&args(&[
            "client",
            "--host=127.0.0.1",
            "--port=9000",
            "--no-secure",
            "--database=test_abc",
            "--send_logs_level=warning",
            "--multiquery",
            "--log_comment",
            "00001_select_1.sql-test_abc",
            "--max_block_size=1",
            "--allow_repeated_settings",
            "--client_logs_file=/tmp/x-fatal",
            "--param_CLICKHOUSE_DATABASE=test_abc",
        ]))
        .unwrap();
        assert_eq!(opts.host, "127.0.0.1");
        assert_eq!(opts.port, Some(9000));
        assert_eq!(opts.database, "test_abc");
        assert!(!opts.secure);
        assert_eq!(opts.query, None);
        assert_eq!(
            opts.settings,
            BTreeMap::from([
                ("send_logs_level".to_owned(), "warning".to_owned()),
                (
                    "log_comment".to_owned(),
                    "00001_select_1.sql-test_abc".to_owned()
                ),
            ])
        );
        assert_eq!(
            opts.params,
            BTreeMap::from([("CLICKHOUSE_DATABASE".to_owned(), "test_abc".to_owned())])
        );
    }

    #[test]
    fn short_options() {
        let opts = parse_args(&args(&[
            "-h", "db", "-u", "me", "-d", "x", "-q", "SELECT 1", "-n", "-s",
        ]))
        .unwrap();
        assert_eq!(opts.host, "db");
        assert_eq!(opts.user, "me");
        assert_eq!(opts.database, "x");
        assert_eq!(opts.query.as_deref(), Some("SELECT 1"));
        assert!(opts.secure);
    }

    #[test]
    fn setting_values() {
        let opts = parse_args(&args(&[
            "--max_threads",
            "-1",
            "--max-insert-threads",
            "2",
            "--query=SELECT 1",
        ]))
        .unwrap();
        assert_eq!(
            opts.settings,
            BTreeMap::from([
                ("max_threads".to_owned(), "-1".to_owned()),
                ("max_insert_threads".to_owned(), "2".to_owned()),
            ])
        );
    }

    #[test]
    fn errors() {
        assert!(parse_args(&args(&["--port=abc"])).is_err());
        assert!(parse_args(&args(&["--query"])).is_err());
    }

    #[test]
    fn negative_numbers() {
        for s in ["-1", "-1.5", "-.5", "-1e10", "-1.5E-3", "-2."] {
            assert!(is_negative_number(s), "{s}");
        }
        for s in ["-", "-x", "--max_threads", "-1e", "-.", "1", "-1-"] {
            assert!(!is_negative_number(s), "{s}");
        }
    }

    #[test]
    fn http_ports() {
        let none = |_: &str| None;
        assert_eq!(http_port(None, false, none), 8123);
        assert_eq!(http_port(Some(9000), false, none), 8123);
        assert_eq!(http_port(Some(18123), false, none), 18123);
        assert_eq!(http_port(Some(9440), true, none), 8443);

        let env = |name: &str| match name {
            "CLICKHOUSE_PORT_TCP" => Some("19000".to_owned()),
            "CLICKHOUSE_PORT_HTTP" => Some("18124".to_owned()),
            _ => None,
        };
        assert_eq!(http_port(Some(19000), false, env), 18124);
        assert_eq!(http_port(Some(9000), false, env), 9000);
    }
}
