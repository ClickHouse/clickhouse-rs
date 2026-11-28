//! Renaming of query parameters for servers older than 26.7.
//!
//! These servers read query parameters as settings. If the name of a parameter is also the name
//! of a setting (for example `limit`), the server converts the value to the type of the setting
//! and fails to read it. The parameters are renamed in the query to names that no setting has.

/// The prefix of renamed parameters. Names of settings never start with an underscore.
pub(crate) const PREFIX: &str = "_chrs_";

/// Rename the substitutions `{name: Type}` of the given parameters to `{_chrs_name: Type}`.
///
/// String literals, quoted identifiers, comments and heredocs are not changed.
pub(crate) fn rename_parameters(sql: &[u8], names: &[&str]) -> Vec<u8> {
    let mut output = Vec::with_capacity(sql.len() + 16);
    let mut index = 0;

    while let Some(&byte) = sql.get(index) {
        let rest = sql.get(index..).unwrap_or_default();

        let skip = match byte {
            b'\'' | b'"' | b'`' => quoted_len(rest, byte),
            b'-' | b'#' | b'/' if comment_len(rest) > 0 => comment_len(rest),
            b'$' => heredoc_len(rest),
            b'{' => {
                if let Some((name_start, name_end)) = substitution_name(rest)
                    && rest
                        .get(name_start..name_end)
                        .is_some_and(|name| names.iter().any(|&p| p.as_bytes() == name))
                {
                    output.extend_from_slice(rest.get(..name_start).unwrap_or_default());
                    output.extend_from_slice(PREFIX.as_bytes());
                    index += name_start;
                }

                1
            }
            _ => 1,
        };

        let end = (index + skip.max(1)).min(sql.len());
        output.extend_from_slice(sql.get(index..end).unwrap_or_default());
        index = end;
    }

    output
}

/// The length of a quoted string or identifier, including the quotes.
fn quoted_len(input: &[u8], quote: u8) -> usize {
    let mut index = 1;

    while let Some(&byte) = input.get(index) {
        index += 1;

        if byte == b'\\' {
            index += 1;
        } else if byte == quote {
            // A doubled quote is an escaped quote.
            if input.get(index) == Some(&quote) {
                index += 1;
            } else {
                return index;
            }
        }
    }

    input.len()
}

/// The length of the comment at the start of `input`, or `0` if `input` does not start with one.
pub(crate) fn comment_len(input: &[u8]) -> usize {
    if input.starts_with(b"--") || input.starts_with(b"# ") || input.starts_with(b"#!") {
        line_comment_len(input)
    } else if input.starts_with(b"/*") {
        block_comment_len(input)
    } else {
        0
    }
}

fn line_comment_len(input: &[u8]) -> usize {
    input
        .iter()
        .position(|&byte| byte == b'\n')
        .map_or(input.len(), |end| end + 1)
}

/// Multiline comments can be nested.
fn block_comment_len(input: &[u8]) -> usize {
    let mut depth = 0usize;
    let mut index = 0;

    while index < input.len() {
        let rest = input.get(index..).unwrap_or_default();

        if rest.starts_with(b"/*") {
            depth += 1;
            index += 2;
        } else if rest.starts_with(b"*/") {
            depth -= 1;
            index += 2;

            if depth == 0 {
                return index;
            }
        } else {
            index += 1;
        }
    }

    input.len()
}

/// The length of a heredoc `$tag$ ... $tag$`, or `1` if `input` does not start with one.
fn heredoc_len(input: &[u8]) -> usize {
    let tag_len = input
        .iter()
        .skip(1)
        .position(|&byte| !(byte.is_ascii_alphanumeric() || byte == b'_'))
        .map_or(input.len(), |len| len + 1);

    if input.get(tag_len) != Some(&b'$') {
        return 1;
    }

    let tag = input.get(..=tag_len).unwrap_or_default();
    let body = input.get(tag.len()..).unwrap_or_default();

    body.windows(tag.len())
        .position(|window| window == tag)
        .map_or(input.len(), |end| tag.len() + end + tag.len())
}

/// The range of the name in a substitution `{ name : Type }` at the start of `input`.
fn substitution_name(input: &[u8]) -> Option<(usize, usize)> {
    let skip_whitespace = |mut index: usize| {
        while input.get(index).is_some_and(u8::is_ascii_whitespace) {
            index += 1;
        }
        index
    };

    let start = skip_whitespace(1);

    if !input
        .get(start)
        .is_some_and(|&byte| byte.is_ascii_alphabetic() || byte == b'_')
    {
        return None;
    }

    let end = start
        + input
            .get(start..)?
            .iter()
            .position(|&byte| !(byte.is_ascii_alphanumeric() || byte == b'_'))?;

    (input.get(skip_whitespace(end)) == Some(&b':')).then_some((start, end))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rename(sql: &str, names: &[&str]) -> String {
        String::from_utf8(rename_parameters(sql.as_bytes(), names)).unwrap()
    }

    #[test]
    fn renames_substitutions() {
        assert_eq!(
            rename(
                "SELECT {limit:UInt64}, { page : String }, {x:Array(UInt8)}",
                &["limit", "page", "x"]
            ),
            "SELECT {_chrs_limit:UInt64}, { _chrs_page : String }, {_chrs_x:Array(UInt8)}"
        );
        assert_eq!(
            rename(
                "SELECT * FROM {table:Identifier} LIMIT {limit:UInt64}",
                &["limit"]
            ),
            "SELECT * FROM {table:Identifier} LIMIT {_chrs_limit:UInt64}"
        );
    }

    #[test]
    fn keeps_strings_identifiers_and_comments() {
        let names = &["a"];

        for sql in [
            "SELECT '{a:UInt8}'",
            "SELECT 'it''s {a:UInt8}'",
            "SELECT 'x\\' {a:UInt8}'",
            "SELECT \"{a:UInt8}\"",
            "SELECT `{a:UInt8}`",
            "SELECT 1 -- {a:UInt8}",
            "SELECT 1 # {a:UInt8}",
            "SELECT /* /* {a:UInt8} */ {a:UInt8} */ 1",
            "SELECT $$ {a:UInt8} $$",
            "SELECT $tag$ {a:UInt8} $tag$",
            "SELECT {ab:UInt8}, {a}, map('a', 1)",
        ] {
            assert_eq!(rename(sql, names), sql);
        }

        assert_eq!(
            rename("SELECT '{a:U}', {a:U} -- {a:U}\n, {a:U}", names),
            "SELECT '{a:U}', {_chrs_a:U} -- {a:U}\n, {_chrs_a:U}"
        );
    }

    #[test]
    fn handles_incomplete_input() {
        for sql in [
            "SELECT '",
            "SELECT {",
            "SELECT { a",
            "/* unterminated",
            "$tag$ x",
            "SELECT $",
        ] {
            assert_eq!(rename(sql, &["a"]), sql);
        }
    }
}
