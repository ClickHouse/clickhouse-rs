//! Test hints, as `clickhouse-client` parses them (`src/Client/TestHint.cpp`):
//!
//! ```text
//! SELECT throwIf(1); -- { serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }
//! SELECT 1 / 0;      -- { serverError 153, 6 }
//! -- { echoOn }
//! ```
//!
//! An error hint counts only when it is not in a leading comment. An `echo`
//! hint counts in any comment.

use crate::errorcodes::ERROR_CODES;
use crate::split::{
    heredoc_end, is_space, skip_block_comment, skip_line_comment, skip_quoted, starts_line_comment,
    utf8_space_len,
};

#[derive(Debug, Clone, Default)]
pub(crate) struct TestHint {
    pub(crate) server_errors: Vec<i32>,
    pub(crate) client_errors: Vec<i32>,
    /// Set by `echo`, `echoOn` (true) or `echoOff` (false).
    pub(crate) echo: Option<bool>,
    /// Set when a hint is malformed or names an unknown error.
    pub(crate) parse_error: Option<String>,
    /// Tells if this statement is printed before it runs.
    pub(crate) echo_this: bool,
}

impl TestHint {
    pub(crate) fn expects_error(&self) -> bool {
        !self.server_errors.is_empty() || !self.client_errors.is_empty()
    }

    /// Tells if the error code satisfies the hint. Through clickhouse-rs the
    /// server finds every error, also the syntax errors that
    /// `clickhouse-client` finds locally, so a server error can satisfy a
    /// `clientError` hint too.
    pub(crate) fn matches(&self, code: i32) -> bool {
        self.server_errors.contains(&code) || self.client_errors.contains(&code)
    }

    pub(crate) fn label(&self) -> String {
        let codes = |list: &[i32]| {
            list.iter()
                .map(|&c| error_name(c).map_or_else(|| c.to_string(), str::to_owned))
                .collect::<Vec<_>>()
                .join(", ")
        };
        match (self.server_errors.is_empty(), self.client_errors.is_empty()) {
            (false, false) => format!("error {}", codes(&self.server_errors)),
            (false, true) => format!("serverError {}", codes(&self.server_errors)),
            _ => format!("clientError {}", codes(&self.client_errors)),
        }
    }

    fn parse(&mut self, body: &str, leading: bool) {
        let tokens = tokenize_hint(body);
        for (i, tok) in tokens.iter().enumerate() {
            match *tok {
                "echo" | "echoOn" => self.echo = Some(true),
                "echoOff" => self.echo = Some(false),
                "retry" => {
                    // `retry N until|while serverError X` is used by a few tests
                    // that wait for a background process. It is not supported.
                    self.parse_error = Some("the \"retry\" test hint is not supported".into());
                    return;
                }
                "serverError" | "clientError" | "error" => {
                    if leading {
                        continue;
                    }
                    let mut codes = Vec::new();
                    for t in &tokens[i + 1..] {
                        if *t == "," {
                            continue;
                        }
                        if let Ok(n) = t.parse::<i32>() {
                            codes.push(n);
                            continue;
                        }
                        match error_code(t) {
                            Some(code) => codes.push(code),
                            None => {
                                self.parse_error =
                                    Some(format!("unknown error name {t:?} in test hint"));
                                return;
                            }
                        }
                    }
                    match *tok {
                        "serverError" => self.server_errors = codes,
                        "clientError" => self.client_errors = codes,
                        _ => {
                            self.server_errors = codes.clone();
                            self.client_errors = codes;
                        }
                    }
                    return;
                }
                _ => {}
            }
        }
    }
}

pub(crate) fn error_code(name: &str) -> Option<i32> {
    ERROR_CODES
        .iter()
        .find(|(n, _)| *n == name)
        .map(|&(_, code)| code)
}

pub(crate) fn error_name(code: i32) -> Option<&'static str> {
    ERROR_CODES
        .iter()
        .find(|&&(_, c)| c == code)
        .map(|&(name, _)| name)
}

/// Parses the hints in the comments of a statement (its `full` text).
pub(crate) fn parse_test_hint(full: &str) -> TestHint {
    let b = full.as_bytes();
    let mut h = TestHint::default();
    let mut leading = true;
    let mut i = 0;
    while i < b.len() {
        let end = if is_space(b[i]) {
            i += 1;
            continue;
        } else if utf8_space_len(b, i) > 0 {
            i += utf8_space_len(b, i);
            continue;
        } else if starts_line_comment(&b[i..]) {
            skip_line_comment(b, i)
        } else if b[i..].starts_with(b"/*") {
            skip_block_comment(b, i)
        } else if matches!(b[i], b'\'' | b'"' | b'`') {
            leading = false;
            i = skip_quoted(b, i);
            continue;
        } else if b[i] == b'$' {
            leading = false;
            i = heredoc_end(b, i).unwrap_or(i + 1);
            continue;
        } else {
            leading = false;
            i += 1;
            continue;
        };
        let comment = &full[i..end];
        if let Some(open) = comment.find('{')
            && let Some(close) = comment[open..].find('}')
        {
            h.parse(&comment[open + 1..open + close], leading);
        }
        i = end;
    }
    h
}

fn tokenize_hint(body: &str) -> Vec<&str> {
    let b = body.as_bytes();
    let mut out = Vec::new();
    let mut i = 0;
    while i < b.len() {
        let c = b[i];
        if is_space(c) {
            i += 1;
        } else if c == b',' {
            out.push(",");
            i += 1;
        } else {
            let mut j = i;
            while j < b.len() && !is_space(b[j]) && b[j] != b',' {
                j += 1;
            }
            out.push(&body[i..j]);
            i = j;
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse() {
        let cases: &[(&str, &str, &[i32], &[i32])] = &[
            (
                "server error by name",
                "SELECT 1; -- { serverError UNKNOWN_TABLE }",
                &[60],
                &[],
            ),
            (
                "server error by code",
                "SELECT 1; -- { serverError 60 }",
                &[60],
                &[],
            ),
            (
                "several codes",
                "SELECT 1; -- { serverError 153, ILLEGAL_DIVISION }",
                &[153, 153],
                &[],
            ),
            (
                "client error",
                "SELECT; -- { clientError SYNTAX_ERROR }",
                &[],
                &[62],
            ),
            ("error sets both", "SELECT 1; -- { error 62 }", &[62], &[62]),
            (
                "block comment",
                "SELECT 1 /* { serverError 60 } */;",
                &[60],
                &[],
            ),
            (
                "text after the hint",
                "SELECT 1; -- { serverError 60 } because there is no table",
                &[60],
                &[],
            ),
            (
                "leading hint is ignored",
                "-- { serverError 60 }\nSELECT 1;",
                &[],
                &[],
            ),
            (
                "braces in a string are not a hint",
                "SELECT '-- { serverError 60 }';",
                &[],
                &[],
            ),
            ("no hint", "SELECT 1; -- a comment", &[], &[]),
        ];
        for (name, input, server, client) in cases {
            let h = parse_test_hint(input);
            assert_eq!(h.parse_error, None, "{name}");
            assert_eq!(h.server_errors, *server, "{name}");
            assert_eq!(h.client_errors, *client, "{name}");
        }
    }

    #[test]
    fn parse_errors() {
        assert!(
            parse_test_hint("SELECT 1; -- { serverError NOT_A_REAL_ERROR_NAME }")
                .parse_error
                .is_some()
        );
        assert!(
            parse_test_hint("SELECT 1; -- { retry 3 until serverError 60 }")
                .parse_error
                .is_some()
        );
    }

    #[test]
    fn matches() {
        let h = parse_test_hint("SELECT; -- { clientError SYNTAX_ERROR }");
        // Through clickhouse-rs the server finds syntax errors.
        assert!(h.matches(62));
        assert!(!h.matches(60));
        assert_eq!(h.label(), "clientError SYNTAX_ERROR");
    }

    #[test]
    fn echo() {
        assert_eq!(parse_test_hint("-- { echoOn }\nSELECT 1;").echo, Some(true));
        assert_eq!(
            parse_test_hint("SELECT 1; -- { echoOff }").echo,
            Some(false)
        );
    }

    #[test]
    fn error_names() {
        assert_eq!(error_code("UNKNOWN_TABLE"), Some(60));
        assert_eq!(error_name(62), Some("SYNTAX_ERROR"));
        assert_eq!(error_code("NOT_A_REAL_ERROR_NAME"), None);
    }
}
