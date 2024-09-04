use std::fmt::Display;

use crate::{
    error::{Error, Result},
    row::{self, Row},
};

pub use bind::{Bind, Identifier};

mod bind;
pub(crate) mod escape;
mod ser;

#[derive(Clone)]
pub(crate) enum SqlBuilder {
    InProgress(Vec<Part>),
    Failed(String),
}

#[derive(Clone)]
pub(crate) enum Part {
    Arg,
    Fields,
    Text(String),
}

impl SqlBuilder {
    pub(crate) fn new(template: &str) -> Self {
        let mut iter = template.split('?');
        let prefix = String::from(iter.next().unwrap());
        let mut parts = vec![Part::Text(prefix)];

        for s in iter {
            let text = if let Some(text) = s.strip_prefix("fields") {
                parts.push(Part::Fields);
                text
            } else {
                parts.push(Part::Arg);
                s
            };

            parts.push(Part::Text(text.into()));
        }

        SqlBuilder::InProgress(parts)
    }

    pub(crate) fn bind_arg(&mut self, value: impl Bind) {
        let Self::InProgress(parts) = self else {
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
        let Self::InProgress(parts) = self else {
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

    pub(crate) fn append(&mut self, suffix: &str) {
        let Self::InProgress(parts) = self else {
            return;
        };

        if let Some(Part::Text(text)) = parts.last_mut() {
            text.push_str(suffix);
        } else {
            // Do nothing, it will fail in `finish()`.
        }
    }

    pub(crate) fn finish(mut self) -> Result<String> {
        let mut sql = String::new();

        if let Self::InProgress(parts) = &self {
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
            Self::InProgress(_) => Ok(sql),
            Self::Failed(err) => Err(Error::InvalidParams(err.into())),
        }
    }

    fn error(&mut self, err: impl Display) {
        *self = Self::Failed(format!("invalid SQL: {err}"));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // XXX: need for `derive(Row)`. Provide `row(crate = ..)` instead.
    use crate as clickhouse;
    use clickhouse_derive::Row;

    #[allow(unused)]
    #[derive(Row)]
    struct Row {
        a: u32,
        b: u32,
    }

    #[allow(unused)]
    #[derive(Row)]
    struct Unnamed(u32, u32);

    #[test]
    fn bound_args() {
        let mut sql = SqlBuilder::new("SELECT ?fields FROM test WHERE a = ? AND b < ?");
        sql.bind_arg("foo");
        sql.bind_arg(42);
        sql.bind_fields::<Row>();
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
        assert!(err
            .to_string()
            .contains("argument ?fields cannot be used with non-struct row types"));

        let mut sql = SqlBuilder::new("SELECT a FROM test WHERE b = ? AND c = ?");
        sql.bind_arg(42);
        let err = sql.finish().unwrap_err();
        assert!(err.to_string().contains("unbound query argument"));

        let mut sql = SqlBuilder::new("SELECT ?fields FROM test WHERE b = ?");
        sql.bind_arg(42);
        let err = sql.finish().unwrap_err();
        assert!(err.to_string().contains("unbound query argument ?fields"));
    }
}
