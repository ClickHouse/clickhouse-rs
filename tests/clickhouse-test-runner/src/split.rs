//! Splits a multi-statement script the way `clickhouse-client` does
//! (`ClientBase::executeMultiQuery` in ClickHouse).
//!
//! All offsets are byte offsets. The scanner stops only at ASCII bytes, so
//! every offset that it returns is a `char` boundary of the input.

use crate::hint::{TestHint, parse_test_hint};

/// One statement of a multi-statement script.
#[derive(Debug, Clone)]
pub(crate) struct Statement {
    /// The text to send to the server: from the first non-whitespace
    /// character (leading comments included) to the end of the statement,
    /// with the semicolon.
    pub(crate) sql: String,
    /// `sql` plus the same-line trailing comment. `clickhouse-client` reads
    /// the test hints from this text and prints it for `echo`.
    pub(crate) full: String,
    pub(crate) hint: TestHint,
}

/// Splits a script into statements.
///
/// It follows `clickhouse-client`:
/// - The `-- Tags:` header (the first line and the `--` lines after it) is
///   not part of any statement.
/// - A statement ends at a top-level `;`. A comment on the same line after
///   the `;` belongs to that statement; this is where test hints are.
/// - The inline data of `INSERT ... FORMAT <name>` (other than `Values`)
///   ends at an empty line, not at `;`.
/// - Comment-only text after the last statement is not a statement.
pub(crate) fn split_statements(text: &str) -> Vec<Statement> {
    let b = text.as_bytes();
    let mut out = Vec::new();
    let mut pos = test_tags_length(text);
    let mut echo = false;
    loop {
        pos = skip_whitespace(b, pos);
        if pos >= b.len() || !has_significant_token(&b[pos..]) {
            return out;
        }
        let begin = pos;
        let end = scan_statement_end(text, pos);
        let sql_end = end;
        // Consume the semicolons after the statement.
        pos = end;
        while pos < b.len() && b[pos] == b';' {
            pos += 1;
            let next = skip_whitespace_and_comments(b, pos);
            if next < b.len() && b[next] == b';' {
                pos = next;
            }
        }
        // Add the same-line trailing comment when the next statement starts
        // after the end of this line.
        let newline = b[pos..]
            .iter()
            .position(|&c| c == b'\n')
            .map_or(b.len(), |n| pos + n);
        if newline <= skip_whitespace_and_comments(b, pos) {
            pos = newline;
        }
        // clickhouse-client sends the statement with its semicolon. Tests
        // that read `system.query_log` see it.
        let sql = if sql_end < b.len() && b[sql_end] == b';' {
            &text[begin..=sql_end]
        } else {
            trim_end(&text[begin..sql_end])
        };
        let full = if sql_end == b.len() {
            // The last statement has no semicolon: clickhouse-client echoes
            // all the rest of the text, with its trailing newlines.
            &text[begin..]
        } else {
            trim_end(&text[begin..pos])
        };
        let mut hint = parse_test_hint(full);
        if let Some(e) = hint.echo {
            echo = e;
        }
        hint.echo_this = echo;
        out.push(Statement {
            sql: sql.to_owned(),
            full: full.to_owned(),
            hint,
        });
    }
}

fn trim_end(s: &str) -> &str {
    s.trim_end_matches([' ', '\t', '\r', '\n'])
}

/// Mirrors `getTestTagsLength`: the length of the leading `-- Tags: ...` line
/// and the `--` comment lines right after it.
fn test_tags_length(text: &str) -> usize {
    let b = text.as_bytes();
    let mut pos = 0;
    let mut first = true;
    loop {
        let line_start = pos;
        while pos < b.len() && (b[pos] == b' ' || b[pos] == b'\t') {
            pos += 1;
        }
        if !b[pos..].starts_with(b"--") {
            return line_start;
        }
        pos += 2;
        if first {
            while pos < b.len() && (b[pos] == b' ' || b[pos] == b'\t') {
                pos += 1;
            }
            if !b[pos..].starts_with(b"Tags:") {
                return 0;
            }
            pos += "Tags:".len();
            first = false;
        }
        let Some(eol) = b[pos..].iter().position(|&c| c == b'\r' || c == b'\n') else {
            return b.len();
        };
        let eol = pos + eol;
        let mut eol_len = 1;
        if eol + 1 < b.len() && (b[eol + 1] == b'\r' || b[eol + 1] == b'\n') && b[eol + 1] != b[eol]
        {
            eol_len = 2;
        }
        pos = eol + eol_len;
    }
}

/// Returns the offset of the `;` that ends the statement at `start`, or the
/// length of the text.
fn scan_statement_end(text: &str, start: usize) -> usize {
    let b = text.as_bytes();
    let mut parens = 0i32;
    let mut i = start;
    while i < b.len() {
        let c = b[i];
        match c {
            b'\'' | b'"' | b'`' => i = skip_quoted(b, i),
            b'-' | b'#' | b'/' if starts_line_comment(&b[i..]) => i = skip_line_comment(b, i),
            b'/' if b[i..].starts_with(b"/*") => i = skip_block_comment(b, i),
            b'$' => match heredoc_end(b, i) {
                Some(end) => i = end,
                None => i += 1,
            },
            b'(' | b'[' => {
                parens += 1;
                i += 1;
            }
            b')' | b']' => {
                parens -= 1;
                i += 1;
            }
            b';' if parens <= 0 => return i,
            _ if is_word_byte(c) => {
                let mut j = i;
                while j < b.len() && is_word_byte(b[j]) {
                    j += 1;
                }
                if parens <= 0
                    && b[i..j].eq_ignore_ascii_case(b"FORMAT")
                    && let Some(data_start) = insert_data_start(text, start, j)
                {
                    // The inline data ends at an empty line.
                    return match find(&b[data_start..], b"\n\n") {
                        Some(k) => data_start + k,
                        None => b.len(),
                    };
                }
                i = j;
            }
            _ => i += 1,
        }
    }
    b.len()
}

/// Tells whether the `FORMAT` keyword that ends at `format_end` starts the
/// inline data of an INSERT statement that begins at `start`, and returns the
/// offset of the data after the format name. The `Values` format is
/// excluded: its data ends at `;`, like a query.
fn insert_data_start(text: &str, start: usize, format_end: usize) -> Option<usize> {
    let b = text.as_bytes();
    if leading_keyword(&text[start..format_end]) != "INSERT" {
        return None;
    }
    let mut i = format_end;
    if i >= b.len() || !matches!(b[i], b' ' | b'\t' | b'\n' | b'\r') {
        return None;
    }
    while i < b.len() && matches!(b[i], b' ' | b'\t' | b'\n' | b'\r') {
        i += 1;
    }
    let mut j = i;
    while j < b.len() && is_word_byte(b[j]) {
        j += 1;
    }
    let word = &text[i..j];
    if j == i || word.eq_ignore_ascii_case("Values") || is_clause_keyword(word) {
        return None;
    }
    // After a SELECT, the FORMAT clause comes at the end of the SELECT, so
    // after its top-level FROM: `INSERT INTO t SELECT * FROM input('x UInt8')
    // FORMAT TSV`. Before that FROM, `format` is in the select list
    // (`SELECT format x FROM s`, `SELECT format DIV 2 FROM s`).
    if !format_after_select_from(&text[start..format_end - "FORMAT".len()]) {
        return None;
    }
    Some(j)
}

/// Tells whether a FORMAT that follows `head` can be the FORMAT clause of an
/// INSERT: `head` has no top-level SELECT, or a top-level FROM follows its
/// last top-level SELECT.
fn format_after_select_from(head: &str) -> bool {
    let mut after_select = false;
    let mut saw_from = false;
    for w in top_level_words(head) {
        if w.eq_ignore_ascii_case("SELECT") {
            after_select = true;
            saw_from = false;
        } else if w.eq_ignore_ascii_case("FROM") {
            saw_from = true;
        }
    }
    !after_select || saw_from
}

/// Returns the words of `sql` outside quotes, comments and parentheses.
fn top_level_words(sql: &str) -> Vec<&str> {
    let b = sql.as_bytes();
    let mut words = Vec::new();
    let mut depth = 0i32;
    let mut i = 0;
    while i < b.len() {
        let c = b[i];
        match c {
            b'\'' | b'"' | b'`' => i = skip_quoted(b, i),
            _ if starts_line_comment(&b[i..]) => i = skip_line_comment(b, i),
            _ if b[i..].starts_with(b"/*") => i = skip_block_comment(b, i),
            b'(' | b'[' => {
                depth += 1;
                i += 1;
            }
            b')' | b']' => {
                depth -= 1;
                i += 1;
            }
            _ if is_word_byte(c) => {
                let mut j = i;
                while j < b.len() && is_word_byte(b[j]) {
                    j += 1;
                }
                if depth == 0 {
                    words.push(&sql[i..j]);
                }
                i = j;
            }
            _ => i += 1,
        }
    }
    words
}

/// SQL words that can follow a column or an alias named `format`, as in
/// `INSERT INTO t SELECT format FROM s`. No input or output format has such a
/// name, so after FORMAT they show an identifier, not the FORMAT clause.
const CLAUSE_KEYWORDS: &[&str] = &[
    "FROM",
    "WHERE",
    "PREWHERE",
    "AS",
    "AND",
    "OR",
    "NOT",
    "IN",
    "IS",
    "LIKE",
    "ILIKE",
    "BETWEEN",
    "GROUP",
    "ORDER",
    "LIMIT",
    "OFFSET",
    "HAVING",
    "QUALIFY",
    "WINDOW",
    "UNION",
    "EXCEPT",
    "INTERSECT",
    "JOIN",
    "INNER",
    "LEFT",
    "RIGHT",
    "FULL",
    "CROSS",
    "ARRAY",
    "ON",
    "USING",
    "SETTINGS",
    "INTO",
    "ASC",
    "DESC",
    "NULLS",
    "COLLATE",
    "WITH",
    "THEN",
    "ELSE",
    "END",
    "WHEN",
    "OVER",
    "GLOBAL",
    "DIV",
    "MOD",
    "REGEXP",
    "FINAL",
    "SAMPLE",
    "FILL",
    "TIES",
    "BY",
];

pub(crate) fn is_clause_keyword(word: &str) -> bool {
    CLAUSE_KEYWORDS.iter().any(|k| k.eq_ignore_ascii_case(word))
}

/// Tells if a single-line comment starts here: `--`, `//`, `# ` or `#!`
/// (`Lexer::nextTokenImpl` in ClickHouse).
pub(crate) fn starts_line_comment(b: &[u8]) -> bool {
    b.starts_with(b"--") || b.starts_with(b"//") || b.starts_with(b"#!") || b.starts_with(b"# ")
}

/// Returns the offset after the quoted text that starts at `i`. A backslash
/// escapes the next byte, and a doubled quote is an escaped quote.
pub(crate) fn skip_quoted(b: &[u8], i: usize) -> usize {
    let q = b[i];
    let mut j = i + 1;
    while j < b.len() {
        if b[j] == b'\\' {
            j += 2;
            continue;
        }
        if b[j] == q {
            if j + 1 < b.len() && b[j + 1] == q {
                j += 2;
                continue;
            }
            return j + 1;
        }
        j += 1;
    }
    b.len()
}

/// Returns the offset of the newline that ends the line comment at `i`.
pub(crate) fn skip_line_comment(b: &[u8], i: usize) -> usize {
    b[i..]
        .iter()
        .position(|&c| c == b'\n')
        .map_or(b.len(), |n| i + n)
}

/// Returns the offset after the block comment at `i`. Block comments nest in
/// ClickHouse.
pub(crate) fn skip_block_comment(b: &[u8], i: usize) -> usize {
    let mut depth = 0;
    let mut j = i;
    while j + 1 < b.len() {
        if b[j] == b'/' && b[j + 1] == b'*' {
            depth += 1;
            j += 2;
        } else if b[j] == b'*' && b[j + 1] == b'/' {
            depth -= 1;
            j += 2;
            if depth == 0 {
                return j;
            }
        } else {
            j += 1;
        }
    }
    b.len()
}

/// Returns the end offset of the `$tag$ ... $tag$` heredoc that starts at
/// `i`, or `None` when the `$` at `i` does not start one.
pub(crate) fn heredoc_end(b: &[u8], i: usize) -> Option<usize> {
    let tag_end = i + 1 + b[i + 1..].iter().position(|&c| c == b'$')?;
    if !b[i + 1..tag_end].iter().all(|&c| is_word_byte(c)) {
        return None;
    }
    let delim = &b[i..=tag_end];
    let end = find(&b[tag_end + 1..], delim)?;
    Some(tag_end + 1 + end + delim.len())
}

pub(crate) fn find(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    if needle.is_empty() {
        return Some(0);
    }
    haystack.windows(needle.len()).position(|w| w == needle)
}

fn skip_whitespace(b: &[u8], mut i: usize) -> usize {
    while i < b.len() && is_space(b[i]) {
        i += 1;
    }
    i
}

pub(crate) fn is_space(c: u8) -> bool {
    matches!(c, b' ' | b'\t' | b'\n' | b'\r' | b'\x0c' | b'\x0b')
}

/// Returns the length of the Unicode whitespace character at `i`, or 0. The
/// ClickHouse lexer skips these (`skipWhitespacesUTF8`): U+0085, U+00A0,
/// U+180E, U+2000..U+200D, U+2028, U+2029, U+202F, U+205F, U+2060, U+3000
/// and U+FEFF.
pub(crate) fn utf8_space_len(b: &[u8], i: usize) -> usize {
    let c = &b[i..];
    if c.len() > 1 && c[0] == 0xC2 && (c[1] == 0x85 || c[1] == 0xA0) {
        return 2;
    }
    if c.len() > 2
        && matches!(
            (c[0], c[1], c[2]),
            (0xE1, 0xA0, 0x8E)
                | (0xE2, 0x80, 0x80..=0x8D | 0xA8 | 0xA9 | 0xAF)
                | (0xE2, 0x81, 0x9F | 0xA0)
                | (0xE3, 0x80, 0x80)
                | (0xEF, 0xBB, 0xBF)
        )
    {
        return 3;
    }
    0
}

pub(crate) fn skip_whitespace_and_comments(b: &[u8], mut i: usize) -> usize {
    while i < b.len() {
        let n = utf8_space_len(b, i);
        if is_space(b[i]) {
            i += 1;
        } else if n > 0 {
            i += n;
        } else if starts_line_comment(&b[i..]) {
            i = skip_line_comment(b, i);
        } else if b[i..].starts_with(b"/*") {
            i = skip_block_comment(b, i);
        } else {
            return i;
        }
    }
    i
}

fn has_significant_token(b: &[u8]) -> bool {
    skip_whitespace_and_comments(b, 0) < b.len()
}

/// Returns the first word of a statement in upper case, after comments,
/// whitespace and opening parentheses.
pub(crate) fn leading_keyword(sql: &str) -> String {
    let b = sql.as_bytes();
    let mut i = skip_whitespace_and_comments(b, 0);
    while i < b.len() && b[i] == b'(' {
        i = skip_whitespace_and_comments(b, i + 1);
    }
    let mut j = i;
    while j < b.len() && is_word_byte(b[j]) {
        j += 1;
    }
    sql[i..j].to_ascii_uppercase()
}

pub(crate) fn is_word_byte(c: u8) -> bool {
    c == b'_' || c.is_ascii_alphanumeric()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sqls(text: &str) -> Vec<String> {
        split_statements(text).into_iter().map(|s| s.sql).collect()
    }

    #[test]
    fn split() {
        let cases: &[(&str, &str, &[&str])] = &[
            ("single without semicolon", "SELECT 1", &["SELECT 1"]),
            (
                "several",
                "SELECT 1;\nSELECT 2;\n",
                &["SELECT 1;", "SELECT 2;"],
            ),
            (
                "semicolon in string",
                "SELECT 'a;b'; SELECT 2",
                &["SELECT 'a;b';", "SELECT 2"],
            ),
            (
                "escaped quote",
                r"SELECT 'a\';b'; SELECT 2",
                &[r"SELECT 'a\';b';", "SELECT 2"],
            ),
            (
                "doubled quote",
                "SELECT 'a'';b'; SELECT 2",
                &["SELECT 'a'';b';", "SELECT 2"],
            ),
            (
                "semicolon in identifier",
                "SELECT 1 AS `a;b`; SELECT 2",
                &["SELECT 1 AS `a;b`;", "SELECT 2"],
            ),
            (
                "semicolon in line comment",
                "SELECT 1 -- a;b\n; SELECT 2",
                &["SELECT 1 -- a;b\n;", "SELECT 2"],
            ),
            (
                "semicolon in block comment",
                "SELECT /* a;b */ 1; SELECT 2",
                &["SELECT /* a;b */ 1;", "SELECT 2"],
            ),
            (
                "nested block comment",
                "SELECT /* a /* b; */ c; */ 1; SELECT 2",
                &["SELECT /* a /* b; */ c; */ 1;", "SELECT 2"],
            ),
            (
                "heredoc",
                "SELECT $$a;'b$$; SELECT $x$;$x$",
                &["SELECT $$a;'b$$;", "SELECT $x$;$x$"],
            ),
            (
                "trailing comment only",
                "SELECT 1;\n-- the end\n",
                &["SELECT 1;"],
            ),
            ("empty", "  \n-- only a comment\n", &[]),
            (
                "tags header is skipped",
                "-- Tags: no-fasttest\n-- Tag no-fasttest: reason\n\nSELECT 1;",
                &["SELECT 1;"],
            ),
            (
                "leading comment stays with its statement",
                "SELECT 1;\n-- next\nSELECT 2;",
                &["SELECT 1;", "-- next\nSELECT 2;"],
            ),
            (
                "insert data ends at an empty line",
                "INSERT INTO t FORMAT CSV\n1,'a;b'\n2,c;\n\nSELECT 1;",
                &["INSERT INTO t FORMAT CSV\n1,'a;b'\n2,c;", "SELECT 1;"],
            ),
            (
                "insert values ends at a semicolon",
                "INSERT INTO t FORMAT Values (1);\nSELECT 1;",
                &["INSERT INTO t FORMAT Values (1);", "SELECT 1;"],
            ),
            (
                "format of a select is not insert data",
                "SELECT 1 FORMAT TSV;\nSELECT 2;",
                &["SELECT 1 FORMAT TSV;", "SELECT 2;"],
            ),
            (
                "insert select with a column named format",
                "INSERT INTO t SELECT format FROM s;\nSELECT 1;",
                &["INSERT INTO t SELECT format FROM s;", "SELECT 1;"],
            ),
            (
                "insert select from input has inline data",
                "INSERT INTO t SELECT x FROM input('x UInt8') FORMAT TSV\n1;\n2\n\nSELECT 1;",
                &[
                    "INSERT INTO t SELECT x FROM input('x UInt8') FORMAT TSV\n1;\n2",
                    "SELECT 1;",
                ],
            ),
            (
                "insert select with a format alias",
                "INSERT INTO t SELECT 1 AS format FROM numbers(1);\nSELECT 1;",
                &[
                    "INSERT INTO t SELECT 1 AS format FROM numbers(1);",
                    "SELECT 1;",
                ],
            ),
            (
                "insert select with an implicit alias after format",
                "INSERT INTO t SELECT format x FROM s;\nSELECT 1;",
                &["INSERT INTO t SELECT format x FROM s;", "SELECT 1;"],
            ),
            (
                "insert select with format DIV",
                "INSERT INTO t SELECT format DIV 2 FROM s;\nSELECT 1;",
                &["INSERT INTO t SELECT format DIV 2 FROM s;", "SELECT 1;"],
            ),
            (
                "insert select with format GLOBAL IN after FROM",
                "INSERT INTO t SELECT x FROM s WHERE format GLOBAL IN (1);\nSELECT 1;",
                &[
                    "INSERT INTO t SELECT x FROM s WHERE format GLOBAL IN (1);",
                    "SELECT 1;",
                ],
            ),
            (
                "insert with format before the data",
                "INSERT INTO t (a) SETTINGS async_insert = 0 FORMAT CSV\n1\n\nSELECT 1;",
                &[
                    "INSERT INTO t (a) SETTINGS async_insert = 0 FORMAT CSV\n1",
                    "SELECT 1;",
                ],
            ),
            (
                "double semicolon",
                "SELECT 1;;\nSELECT 2",
                &["SELECT 1;", "SELECT 2"],
            ),
            (
                "multibyte text",
                "SELECT 'ё;'; SELECT '\u{1F600}' -- é\n;",
                &["SELECT 'ё;';", "SELECT '\u{1F600}' -- é\n;"],
            ),
            (
                "semicolon in a // comment",
                "SELECT 1 // a;b\n; SELECT 2",
                &["SELECT 1 // a;b\n;", "SELECT 2"],
            ),
            (
                "same-line // comment stays with its statement",
                "SELECT 1; // μs\nSELECT 2;",
                &["SELECT 1;", "SELECT 2;"],
            ),
            (
                "unicode whitespace only",
                "SELECT 1;\n\u{00A0}\u{2028}\u{FEFF}",
                &["SELECT 1;"],
            ),
        ];
        for (name, input, want) in cases {
            assert_eq!(sqls(input), *want, "{name}");
        }
    }

    #[test]
    fn trailing_hint() {
        let stmts = split_statements(
            "SELECT throwIf(1); -- { serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }\nSELECT 2;",
        );
        assert_eq!(stmts.len(), 2);
        assert_eq!(
            stmts[0].full,
            "SELECT throwIf(1); -- { serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }"
        );
        assert_eq!(stmts[0].hint.server_errors, vec![395]);
        assert!(!stmts[1].hint.expects_error());
    }

    #[test]
    fn hint_on_next_line_is_leading() {
        // clickhouse-client takes only the same-line comment as the trailing
        // comment. A hint on the next line is a leading comment of the next
        // statement, and an error hint in a leading comment is ignored.
        let stmts = split_statements("SELECT 1;\n-- { serverError 60 }\nSELECT 2;");
        assert_eq!(stmts.len(), 2);
        assert!(!stmts[0].hint.expects_error());
        assert!(!stmts[1].hint.expects_error());
    }

    #[test]
    fn hint_inside_statement() {
        let stmts = split_statements("SELECT x -- { serverError UNKNOWN_IDENTIFIER }\n;");
        assert_eq!(stmts.len(), 1);
        assert_eq!(stmts[0].hint.server_errors, vec![47]);
    }

    #[test]
    fn echo() {
        let stmts = split_statements(
            "SELECT 1;\n-- { echoOn }\nSELECT 2; -- two\nSELECT 3;\n-- { echoOff }\nSELECT 4;\nSELECT 5; -- { echo }\n",
        );
        assert_eq!(stmts.len(), 5);
        let echo: Vec<bool> = stmts.iter().map(|s| s.hint.echo_this).collect();
        assert_eq!(echo, [false, true, true, false, true]);
        assert_eq!(stmts[1].full, "-- { echoOn }\nSELECT 2; -- two");
    }

    #[test]
    fn leading_keywords() {
        assert_eq!(leading_keyword("  -- c\n/* d */ (select 1)"), "SELECT");
        assert_eq!(leading_keyword("insert into t values (1)"), "INSERT");
        assert_eq!(leading_keyword("-- only a comment"), "");
        assert_eq!(leading_keyword("// c\nSELECT 1"), "SELECT");
        // The whitespace characters of ClickHouse `skipWhitespacesUTF8`
        // (test 01280_unicode_whitespaces_lexer).
        assert_eq!(
            leading_keyword("\u{0085}\u{00A0} \u{2000}\u{200A}\u{2028}SELECT\u{2029} 1"),
            "SELECT"
        );
        for c in [
            '\u{0085}', '\u{00A0}', '\u{180E}', '\u{2000}', '\u{2005}', '\u{200A}', '\u{200B}',
            '\u{200D}', '\u{2028}', '\u{2029}', '\u{202F}', '\u{205F}', '\u{2060}', '\u{3000}',
            '\u{FEFF}',
        ] {
            assert_eq!(
                leading_keyword(&format!("{c}(select 1)")),
                "SELECT",
                "{c:?}"
            );
        }
        // Other non-ASCII characters are not whitespace.
        for c in ['\u{00A1}', '\u{2010}', '\u{2030}', '\u{3001}', '\u{FEFE}'] {
            assert_eq!(leading_keyword(&format!("{c}SELECT 1")), "", "{c:?}");
        }
    }

    #[test]
    fn hint_after_unicode_whitespace_and_slash_comment() {
        // A `//` comment is a comment token, so it can hold a test hint.
        let stmts = split_statements("SELECT x; // { serverError UNKNOWN_IDENTIFIER }\n");
        assert_eq!(stmts[0].hint.server_errors, vec![47]);
        // Unicode whitespace does not end the leading comments: an error hint
        // there is ignored.
        let stmts = split_statements("\u{00A0}-- { serverError 60 }\nSELECT 1;");
        assert!(!stmts[0].hint.expects_error());
    }

    #[test]
    fn keeps_semicolon() {
        // clickhouse-client sends the semicolon; system.query_log shows it.
        let stmts = split_statements("SELECT 1; -- c\nSELECT 2\n");
        assert_eq!(stmts.len(), 2);
        assert_eq!(stmts[0].sql, "SELECT 1;");
        assert_eq!(stmts[1].sql, "SELECT 2");
    }

    #[test]
    fn echo_of_last_statement_without_semicolon() {
        // clickhouse-client echoes the rest of the text, with the newlines.
        let stmts = split_statements("SELECT 1;\nSELECT 2\n\n");
        assert_eq!(stmts.len(), 2);
        assert_eq!(stmts[0].full, "SELECT 1;");
        assert_eq!(stmts[1].full, "SELECT 2\n\n");
        assert_eq!(stmts[1].sql, "SELECT 2");
    }
}
