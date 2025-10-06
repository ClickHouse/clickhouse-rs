use std::fmt::{self, Display, Write};

use crate::{
    error::{Error, Result},
    row::{self, Row},
};

pub use bind::{Bind, Identifier};

mod bind;
pub(crate) mod escape;
pub(crate) mod ser;

#[derive(Debug, Clone)]
pub(crate) enum SqlBuilder {
    InProgress(Vec<Part>, Option<String>),
    Failed(String),
}

#[derive(Debug, Clone)]
pub(crate) enum Part {
    Arg,
    Fields,
    Text(String),
}

/// Display SQL query as string.
impl fmt::Display for SqlBuilder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SqlBuilder::InProgress(parts, output_format_opt) => {
                let mut sql = String::new();

                for part in parts {
                    match part {
                        Part::Arg => sql.write_char('?')?,
                        Part::Fields => sql.write_str("?fields")?,
                        Part::Text(text) => sql.write_str(text)?,
                    }
                }

                if let Some(output_format) = output_format_opt {
                    let insert_pos = Self::find_format_position(&sql);
                    sql.insert_str(insert_pos, &format!(" FORMAT {output_format}"));
                }

                f.write_str(&sql)
            }
            SqlBuilder::Failed(err) => f.write_str(err),
        }
    }
}

impl SqlBuilder {
    pub(crate) fn new(template: &str) -> Self {
        let mut parts = Vec::new();
        let mut rest = template;
        while let Some(idx) = rest.find('?') {
            if rest[idx + 1..].starts_with('?') {
                parts.push(Part::Text(rest[..idx + 1].to_string()));
                rest = &rest[idx + 2..];
                continue;
            } else if idx != 0 {
                parts.push(Part::Text(rest[..idx].to_string()));
            }

            rest = &rest[idx + 1..];
            if let Some(restfields) = rest.strip_prefix("fields") {
                parts.push(Part::Fields);
                rest = restfields;
            } else {
                parts.push(Part::Arg);
            }
        }

        if !rest.is_empty() {
            parts.push(Part::Text(rest.to_string()));
        }

        SqlBuilder::InProgress(parts, None)
    }

    pub(crate) fn set_output_format(&mut self, format: impl Into<String>) {
        if let Self::InProgress(_, format_opt) = self {
            *format_opt = Some(format.into());
        }
    }

    pub(crate) fn bind_arg(&mut self, value: impl Bind) {
        let Self::InProgress(parts, _) = self else {
            return;
        };

        if let Some(part) = parts.iter_mut().find(|p| matches!(p, Part::Arg)) {
            let mut s = String::new();

            if let Err(err) = value.write(&mut s) {
                return self.error(format_args!("invalid argument: {err}"));
            }

            *part = Part::Text(s);
        } else {
            self.error("unexpected bind(), all arguments are already bound");
        }
    }

    pub(crate) fn bind_fields<T: Row>(&mut self) {
        let Self::InProgress(parts, _) = self else {
            return;
        };

        if let Some(fields) = row::join_column_names::<T>() {
            for part in parts.iter_mut().filter(|p| matches!(p, Part::Fields)) {
                *part = Part::Text(fields.clone());
            }
        } else if parts.iter().any(|p| matches!(p, Part::Fields)) {
            self.error("argument ?fields cannot be used with non-struct row types");
        }
    }

    pub(crate) fn finish(mut self) -> Result<String> {
        let mut sql = String::new();

        if let Self::InProgress(parts, _) = &self {
            for part in parts {
                match part {
                    Part::Text(text) => sql.push_str(text),
                    Part::Arg => {
                        self.error("unbound query argument");
                        break;
                    }
                    Part::Fields => {
                        self.error("unbound query argument ?fields");
                        break;
                    }
                }
            }
        }

        match self {
            Self::InProgress(_, output_format_opt) => {
                if let Some(output_format) = output_format_opt {
                    let insert_pos = Self::find_format_position(&sql);
                    sql.insert_str(insert_pos, &format!(" FORMAT {output_format}"));
                }
                Ok(sql)
            }
            Self::Failed(err) => Err(Error::InvalidParams(err.into())),
        }
    }

    fn find_format_position(sql: &str) -> usize {
        enum State {
            Normal,
            InString(char),
            InLineComment,
            InBlockComment,
        }

        let mut state = State::Normal;
        let mut last_meaningful_byte_pos = 0;
        let mut chars = sql.char_indices().peekable();

        while let Some((byte_pos, ch)) = chars.next() {
            match state {
                State::Normal => match ch {
                    '\'' | '`' | '"' => {
                        state = State::InString(ch);
                        last_meaningful_byte_pos = byte_pos + ch.len_utf8();
                    }
                    '-' if chars.peek().is_some_and(|(_, c)| *c == '-') => {
                        state = State::InLineComment;
                        chars.next();
                    }
                    '/' if chars.peek().is_some_and(|(_, c)| *c == '*') => {
                        state = State::InBlockComment;
                        chars.next();
                    }
                    ';' | ' ' | '\t' | '\n' | '\r' => {
                        // trailing characters
                    }
                    _ => {
                        last_meaningful_byte_pos = byte_pos + ch.len_utf8();
                    }
                },
                State::InString(quote_char) => {
                    last_meaningful_byte_pos = byte_pos + ch.len_utf8();
                    if ch == quote_char {
                        if chars.peek().is_some_and(|(_, c)| *c == quote_char) {
                            if let Some((next_pos, next_ch)) = chars.next() {
                                last_meaningful_byte_pos = next_pos + next_ch.len_utf8();
                            }
                        } else {
                            state = State::Normal;
                        }
                    }
                }
                State::InLineComment => {
                    if ch == '\n' {
                        state = State::Normal;
                    }
                }
                State::InBlockComment => {
                    if ch == '*' && chars.peek().is_some_and(|(_, c)| *c == '/') {
                        state = State::Normal;
                        chars.next();
                    }
                }
            }
        }

        last_meaningful_byte_pos
    }

    fn error(&mut self, err: impl Display) {
        *self = Self::Failed(format!("invalid SQL: {err}"));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use clickhouse_derive::Row;

    #[allow(unused)]
    #[derive(Row)]
    #[clickhouse(crate = "crate")]
    struct Row {
        a: u32,
        b: u32,
    }

    #[allow(unused)]
    #[derive(Row)]
    #[clickhouse(crate = "crate")]
    struct Unnamed(u32, u32);

    #[test]
    fn bound_args() {
        let mut sql = SqlBuilder::new("SELECT ?fields FROM test WHERE a = ? AND b < ?");
        assert_eq!(
            sql.to_string(),
            "SELECT ?fields FROM test WHERE a = ? AND b < ?"
        );

        sql.bind_arg("foo");
        assert_eq!(
            sql.to_string(),
            "SELECT ?fields FROM test WHERE a = 'foo' AND b < ?"
        );

        sql.bind_arg(42);
        assert_eq!(
            sql.to_string(),
            "SELECT ?fields FROM test WHERE a = 'foo' AND b < 42"
        );

        sql.bind_fields::<Row>();
        assert_eq!(
            sql.to_string(),
            "SELECT `a`,`b` FROM test WHERE a = 'foo' AND b < 42"
        );

        assert_eq!(
            sql.finish().unwrap(),
            r"SELECT `a`,`b` FROM test WHERE a = 'foo' AND b < 42"
        );
    }

    #[test]
    fn in_clause() {
        fn t(arg: &[&str], expected: &str) {
            let mut sql = SqlBuilder::new("SELECT ?fields FROM test WHERE a IN ?");
            sql.bind_arg(arg);
            sql.bind_fields::<Row>();
            assert_eq!(sql.finish().unwrap(), expected);
        }

        const ARGS: &[&str] = &["bar", "baz", "foobar"];
        t(&ARGS[..0], r"SELECT `a`,`b` FROM test WHERE a IN []");
        t(&ARGS[..1], r"SELECT `a`,`b` FROM test WHERE a IN ['bar']");
        t(
            &ARGS[..2],
            r"SELECT `a`,`b` FROM test WHERE a IN ['bar','baz']",
        );
        t(
            ARGS,
            r"SELECT `a`,`b` FROM test WHERE a IN ['bar','baz','foobar']",
        );
    }

    // See #18.
    #[test]
    fn question_marks_inside() {
        let mut sql = SqlBuilder::new("SELECT 1 FROM test WHERE a IN ? AND b = ?");
        sql.bind_arg(&["a?b", "c?"][..]);
        sql.bind_arg("a?");
        assert_eq!(
            sql.finish().unwrap(),
            r"SELECT 1 FROM test WHERE a IN ['a?b','c?'] AND b = 'a?'"
        );
    }

    #[test]
    fn question_escape() {
        let sql = SqlBuilder::new("SELECT 1 FROM test WHERE a IN 'a??b'");
        assert_eq!(
            sql.finish().unwrap(),
            r"SELECT 1 FROM test WHERE a IN 'a?b'"
        );
    }

    #[test]
    fn option_as_null() {
        let mut sql = SqlBuilder::new("SELECT 1 FROM test WHERE a = ?");
        sql.bind_arg(None::<u32>);
        assert_eq!(sql.finish().unwrap(), r"SELECT 1 FROM test WHERE a = NULL");
    }

    #[test]
    fn option_as_value() {
        let mut sql = SqlBuilder::new("SELECT 1 FROM test WHERE a = ?");
        sql.bind_arg(Some(1u32));
        assert_eq!(sql.finish().unwrap(), r"SELECT 1 FROM test WHERE a = 1");
    }

    #[test]
    fn failures() {
        let mut sql = SqlBuilder::new("SELECT 1");
        sql.bind_arg(42);
        let err = sql.finish().unwrap_err();
        assert!(err.to_string().contains("all arguments are already bound"));

        let mut sql = SqlBuilder::new("SELECT ?fields");
        sql.bind_fields::<Unnamed>();
        let err = sql.finish().unwrap_err();
        assert!(
            err.to_string()
                .contains("argument ?fields cannot be used with non-struct row types")
        );

        let mut sql = SqlBuilder::new("SELECT a FROM test WHERE b = ? AND c = ?");
        sql.bind_arg(42);
        let err = sql.finish().unwrap_err();
        assert!(err.to_string().contains("unbound query argument"));

        let mut sql = SqlBuilder::new("SELECT ?fields FROM test WHERE b = ?");
        sql.bind_arg(42);
        let err = sql.finish().unwrap_err();
        assert!(err.to_string().contains("unbound query argument ?fields"));
    }

    #[cfg(test)]
    mod find_format_position {
        use super::*;

        #[test]
        fn test_multiline_query_with_format() {
            let mut sql =
                SqlBuilder::new("SELECT\nid,\nname\nFROM users\nWHERE active = true   ;\n  ");
            sql.set_output_format("JSONEachRow");
            assert_eq!(
                sql.finish().unwrap(),
                "SELECT\nid,\nname\nFROM users\nWHERE active = true FORMAT JSONEachRow   ;\n  "
            );
        }

        #[test]
        fn test_with_placeholder_and_format() {
            let mut sql = SqlBuilder::new("SELECT * FROM `table`.`name` WHERE id = ?   ;\n  ");
            sql.bind_arg(42);
            sql.set_output_format("JSONEachRow");
            assert_eq!(
                sql.finish().unwrap(),
                "SELECT * FROM `table`.`name` WHERE id = 42 FORMAT JSONEachRow   ;\n  "
            );
        }

        #[test]
        fn test_mixed_identifier() {
            let mut sql =
                SqlBuilder::new(r#"SELECT `style`, "ansi_style" FROM `t1` JOIN "table2""#);
            sql.set_output_format("JSONEachRow");
            assert_eq!(
                sql.finish().unwrap(),
                "SELECT `style`, \"ansi_style\" FROM `t1` JOIN \"table2\" FORMAT JSONEachRow"
            );
        }

        #[test]
        fn test_mixed_identifier_multiline() {
            let mut sql =
                SqlBuilder::new("SELECT `number`\nFROM `system`.\"numbers\"\nLIMIT 1\r\n");
            sql.set_output_format("JSONEachRow");
            assert_eq!(
                sql.finish().unwrap(),
                "SELECT `number`\nFROM `system`.\"numbers\"\nLIMIT 1 FORMAT JSONEachRow\r\n"
            );
        }
    }
}
