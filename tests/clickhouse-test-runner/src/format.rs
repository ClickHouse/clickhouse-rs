//! The output formats that the shim can write, and the `FORMAT` clause of a
//! query.

use crate::split::{
    heredoc_end, is_clause_keyword, is_word_byte, leading_keyword, skip_block_comment,
    skip_line_comment, skip_quoted, skip_whitespace_and_comments, starts_line_comment,
};

/// How the shim writes a result set.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct OutputFormat {
    pub(crate) name: String,
    /// A header line with the column names.
    pub(crate) names: bool,
    /// A header line with the column types.
    pub(crate) types: bool,
    /// Top-level strings are not escaped.
    pub(crate) raw: bool,
    /// The `Null` format: the values are read, but not written.
    pub(crate) discard: bool,
    pub(crate) supported: bool,
}

impl OutputFormat {
    pub(crate) fn tab_separated() -> Self {
        Self::new("TabSeparated")
    }

    fn new(name: &str) -> Self {
        let mut f = OutputFormat {
            name: name.to_owned(),
            names: false,
            types: false,
            raw: false,
            discard: false,
            supported: true,
        };
        // Format names are case-insensitive.
        match name.to_ascii_lowercase().as_str() {
            "tabseparated" | "tsv" => {}
            "tabseparatedwithnames" | "tsvwithnames" => f.names = true,
            "tabseparatedwithnamesandtypes" | "tsvwithnamesandtypes" => {
                f.names = true;
                f.types = true;
            }
            "tabseparatedraw" | "tsvraw" | "raw" => f.raw = true,
            "tabseparatedrawwithnames" | "tsvrawwithnames" | "rawwithnames" => {
                f.raw = true;
                f.names = true;
            }
            "tabseparatedrawwithnamesandtypes"
            | "tsvrawwithnamesandtypes"
            | "rawwithnamesandtypes" => {
                f.raw = true;
                f.names = true;
                f.types = true;
            }
            "null" => f.discard = true,
            _ => f.supported = false,
        }
        f
    }
}

/// The leading keywords of the statements that return a result set. The shim
/// runs these with `Query::fetch_native` and the other statements with
/// `Query::execute`. `KILL QUERY` and `KILL MUTATION` return the matched
/// queries or mutations. `FROM` starts the FROM-first syntax
/// (`FROM t SELECT x`) and the pipe syntax (`FROM t |> ...`).
const RESULT_KEYWORDS: &[&str] = &[
    "SELECT", "WITH", "SHOW", "DESC", "DESCRIBE", "EXISTS", "EXPLAIN", "CHECK", "KILL", "FROM",
];

pub(crate) fn returns_rows(sql: &str) -> bool {
    let k = leading_keyword(sql);
    RESULT_KEYWORDS.contains(&k.as_str())
}

/// Removes a top-level `FORMAT <name>` clause from a result-set statement and
/// returns the statement and the format.
///
/// clickhouse-rs reads the result as `Native` and the shim writes it as
/// `TabSeparated`, so only the `TabSeparated` family and `Null` can be
/// written. A statement with another format fails.
pub(crate) fn split_output_format(sql: &str) -> (String, OutputFormat) {
    match find_format_clause(sql) {
        None => (sql.to_owned(), OutputFormat::tab_separated()),
        Some((start, end, name)) => (
            format!("{}{}", &sql[..start], &sql[end..]),
            OutputFormat::new(&name),
        ),
    }
}

/// Returns the position of a top-level `FORMAT <name>` clause.
fn find_format_clause(sql: &str) -> Option<(usize, usize, String)> {
    let b = sql.as_bytes();
    let mut depth = 0i32;
    let mut i = 0;
    while i < b.len() {
        let c = b[i];
        match c {
            b'\'' | b'"' | b'`' => i = skip_quoted(b, i),
            _ if starts_line_comment(&b[i..]) => i = skip_line_comment(b, i),
            _ if b[i..].starts_with(b"/*") => i = skip_block_comment(b, i),
            b'$' => i = heredoc_end(b, i).unwrap_or(i + 1),
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
                if depth == 0
                    && b[i..j].eq_ignore_ascii_case(b"FORMAT")
                    && (i == 0 || b[i - 1] != b'.')
                    // `ORDER BY format DESC` ends the query too: a clause
                    // keyword after FORMAT shows an identifier named `format`.
                    && let Some((m, name)) = format_name(sql, j)
                    && !is_clause_keyword(&name)
                    && ends_query(b, m)
                {
                    return Some((i, m, name));
                }
                i = j;
            }
            _ => i += 1,
        }
    }
    None
}

/// Reads the format name after the FORMAT keyword that ends at `j`. Returns
/// the end of the name and the name.
fn format_name(sql: &str, j: usize) -> Option<(usize, String)> {
    let b = sql.as_bytes();
    let k = skip_whitespace_and_comments(b, j);
    if k == j || k >= b.len() {
        return None;
    }
    if b[k] == b'`' || b[k] == b'"' {
        // A quoted format name: FORMAT `Null`.
        let m = skip_quoted(b, k);
        return unquote_identifier(&sql[k..m]).map(|name| (m, name));
    }
    let mut m = k;
    while m < b.len() && is_word_byte(b[m]) {
        m += 1;
    }
    if m == k {
        return None;
    }
    Some((m, sql[k..m].to_owned()))
}

/// Tells if the text from `i` on can follow the FORMAT clause of a query:
/// nothing, a semicolon, or a SETTINGS clause. Otherwise FORMAT is an
/// identifier, as in `SELECT format FROM t`.
fn ends_query(b: &[u8], i: usize) -> bool {
    let k = skip_whitespace_and_comments(b, i);
    if k >= b.len() || b[k] == b';' {
        return true;
    }
    let mut m = k;
    while m < b.len() && is_word_byte(b[m]) {
        m += 1;
    }
    b[k..m].eq_ignore_ascii_case(b"SETTINGS")
}

/// Removes the quotes of a quoted identifier (`` `a b` `` or `"a b"`).
pub(crate) fn unquote_identifier(s: &str) -> Option<String> {
    let b = s.as_bytes();
    let q = *b.first()?;
    if b.len() < 2 || b[b.len() - 1] != q {
        return None;
    }
    let inner = &b[1..b.len() - 1];
    let mut out = Vec::with_capacity(inner.len());
    let mut i = 0;
    while i < inner.len() {
        let c = inner[i];
        if c == b'\\' && i + 1 < inner.len() {
            i += 1;
            out.push(inner[i]);
        } else if c == q && i + 1 < inner.len() && inner[i + 1] == q {
            i += 1;
            out.push(q);
        } else {
            out.push(c);
        }
        i += 1;
    }
    String::from_utf8(out).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn split_format() {
        let cases: &[(&str, &str, &str, bool)] = &[
            ("SELECT 1", "SELECT 1", "TabSeparated", true),
            ("SELECT 1 FORMAT TSV", "SELECT 1 ", "TSV", true),
            (
                "SELECT 1 FORMAT TSVWithNames SETTINGS max_threads = 1",
                "SELECT 1  SETTINGS max_threads = 1",
                "TSVWithNames",
                true,
            ),
            (
                "SELECT 1 FORMAT JSONEachRow",
                "SELECT 1 ",
                "JSONEachRow",
                false,
            ),
            (
                "SELECT 'FORMAT JSON'",
                "SELECT 'FORMAT JSON'",
                "TabSeparated",
                true,
            ),
            (
                "SELECT * FROM (SELECT 1 FORMAT JSON)",
                "SELECT * FROM (SELECT 1 FORMAT JSON)",
                "TabSeparated",
                true,
            ),
            (
                "SELECT t.format FROM t",
                "SELECT t.format FROM t",
                "TabSeparated",
                true,
            ),
            ("SELECT 1 FORMAT null", "SELECT 1 ", "null", true),
            ("SELECT 1 FORMAT TSVRaw", "SELECT 1 ", "TSVRaw", true),
            ("SELECT 1 FORMAT `Null`;", "SELECT 1 ;", "Null", true),
            // FORMAT as an identifier, not the clause.
            (
                "SELECT format FROM t",
                "SELECT format FROM t",
                "TabSeparated",
                true,
            ),
            (
                "SELECT x AS format FROM t WHERE format = 1",
                "SELECT x AS format FROM t WHERE format = 1",
                "TabSeparated",
                true,
            ),
            (
                "SELECT 1 AS format FROM numbers(1)",
                "SELECT 1 AS format FROM numbers(1)",
                "TabSeparated",
                true,
            ),
            (
                "SELECT format FROM (SELECT 1 AS format)",
                "SELECT format FROM (SELECT 1 AS format)",
                "TabSeparated",
                true,
            ),
            (
                "SELECT 1 AS format",
                "SELECT 1 AS format",
                "TabSeparated",
                true,
            ),
            (
                "SELECT 1 AS format ORDER BY format DESC",
                "SELECT 1 AS format ORDER BY format DESC",
                "TabSeparated",
                true,
            ),
            (
                "SELECT 1 AS format ORDER BY format ASC",
                "SELECT 1 AS format ORDER BY format ASC",
                "TabSeparated",
                true,
            ),
            (
                "SELECT 1 AS format ORDER BY format DESC FORMAT TSV",
                "SELECT 1 AS format ORDER BY format DESC ",
                "TSV",
                true,
            ),
            (
                "SELECT 1 FORMAT TSV -- c\n;",
                "SELECT 1  -- c\n;",
                "TSV",
                true,
            ),
        ];
        for (input, sql, name, supported) in cases {
            let (got_sql, f) = split_output_format(input);
            assert_eq!(got_sql, *sql, "{input}");
            assert_eq!(f.name, *name, "{input}");
            assert_eq!(f.supported, *supported, "{input}");
        }
    }

    #[test]
    fn format_flags() {
        let f = OutputFormat::new("TSVRawWithNamesAndTypes");
        assert!(f.supported && f.raw && f.names && f.types && !f.discard);
        assert!(OutputFormat::new("Null").discard);
        assert!(!OutputFormat::new("CSV").supported);
    }

    #[test]
    fn result_statements() {
        assert!(returns_rows("SELECT 1"));
        assert!(returns_rows("(SELECT 1) UNION ALL (SELECT 2)"));
        assert!(returns_rows("with 1 as x select x"));
        assert!(returns_rows("KILL MUTATION WHERE table = 't'"));
        assert!(returns_rows("kill query where query_id = 'x' sync"));
        assert!(returns_rows("// c\nSELECT 1"));
        assert!(!returns_rows("INSERT INTO t VALUES (1)"));
        assert!(!returns_rows("CREATE TABLE t (x UInt8) ENGINE = Memory"));
    }
}
