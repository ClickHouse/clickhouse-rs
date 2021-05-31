use std::fmt;

use crate::{
    error::{Error, Result},
    row::{self, Row},
};

pub use bind::{Bind, Identifier};

mod bind;
pub(crate) mod escape;

#[derive(Clone)]
pub(crate) enum SqlBuilder {
    InProgress { result: String },
    Failed(fmt::Error),
}

// We replace "?fields" with this in order to avoid look-ahead parsing.
const FIELDS_PLACEHOLDER: &str = "_#fields#_";

impl SqlBuilder {
    pub(crate) fn new(template: &str) -> Self {
        SqlBuilder::InProgress {
            result: template.trim().replace("?fields", FIELDS_PLACEHOLDER),
        }
    }

    pub(crate) fn bind_arg(&mut self, value: impl Bind) {
        if let Self::InProgress { result } = self {
            let mut iter = result.splitn(2, '?');

            let (prefix, suffix) = match (iter.next(), iter.next()) {
                (Some(prefix), Some(suffix)) => (prefix, suffix),
                _ => panic!("all query arguments are already bound"),
            };

            let mut next = String::with_capacity(result.len() + value.reserve() - 1);
            next.push_str(prefix);

            if let Err(err) = value.write(&mut next) {
                *self = SqlBuilder::Failed(err);
                return;
            }

            next.push_str(suffix);
            *result = next;
        }
    }

    pub(crate) fn bind_fields<T: Row>(&mut self) {
        if let Self::InProgress { result } = self {
            if let Some(fields) = row::join_column_names::<T>() {
                *result = result.replace(FIELDS_PLACEHOLDER, &fields);
            }
        }
    }

    pub(crate) fn append(&mut self, suffix: &str) {
        if let Self::InProgress { result } = self {
            result.push_str(suffix);
        }
    }

    pub(crate) fn finish(self) -> Result<String> {
        match self {
            Self::InProgress { result } => {
                if result.contains('?') {
                    panic!("unbound query argument: ?");
                }

                if result.contains(FIELDS_PLACEHOLDER) {
                    panic!("unbound query argument: ?fields");
                }

                Ok(result)
            }
            Self::Failed(err) => Err(Error::InvalidParams(Box::new(err))),
        }
    }
}

#[cfg(test)]
mod test {
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

    #[test]
    fn it_builds_sql_with_bound_args() {
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
    fn it_builds_sql_with_in_clause() {
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
}
