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
    InProgress { parts: Vec<Part>, size: usize },
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
        let mut size = prefix.len();
        let mut parts = vec![Part::Text(prefix)];

        for s in iter {
            let text = if let Some(text) = s.strip_prefix("fields") {
                parts.push(Part::Fields);
                text
            } else {
                parts.push(Part::Arg);
                s
            };

            size += text.len();
            parts.push(Part::Text(text.into()));
        }

        SqlBuilder::InProgress { parts, size }
    }

    pub(crate) fn bind_arg(&mut self, value: impl Bind) {
        if let Self::InProgress { parts, size } = self {
            if let Some(part) = parts.iter_mut().find(|p| matches!(p, Part::Arg)) {
                let mut s = String::new();

                if let Err(err) = value.write(&mut s) {
                    *self = SqlBuilder::Failed(err);
                    return;
                }

                *size += s.len();
                *part = Part::Text(s);
            } else {
                panic!("all query arguments are already bound");
            }
        }
    }

    pub(crate) fn bind_fields<T: Row>(&mut self) {
        if let Self::InProgress { parts, size } = self {
            if let Some(fields) = row::join_column_names::<T>() {
                for part in parts.iter_mut().filter(|p| matches!(p, Part::Fields)) {
                    *size += fields.len();
                    *part = Part::Text(fields.clone());
                }
            }
        }
    }

    pub(crate) fn append(&mut self, suffix: &str) {
        if let Self::InProgress { parts, size } = self {
            if let Some(Part::Text(text)) = parts.last_mut() {
                *size += suffix.len();
                text.push_str(suffix);
            } else {
                unreachable!();
            }
        }
    }

    pub(crate) fn finish(self) -> Result<String> {
        match self {
            Self::InProgress { parts, size } => {
                Ok(parts
                    .into_iter()
                    .fold(String::with_capacity(size), |mut res, part| {
                        if let Part::Text(text) = part {
                            debug_assert!(res.len() + text.len() <= res.capacity());
                            res.push_str(&text);
                            res
                        } else {
                            panic!("unbound query argument ? or ?fields");
                        }
                    }))
            }
            Self::Failed(err) => Err(Error::InvalidParams(err.into())),
        }
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

    // See #18.
    #[test]
    fn it_builds_sql_with_question_marks_inside() {
        let mut sql = SqlBuilder::new("SELECT 1 FROM test WHERE a IN ? AND b = ?");
        sql.bind_arg(&["a?b", "c?"][..]);
        sql.bind_arg("a?");
        assert_eq!(
            sql.finish().unwrap(),
            r"SELECT 1 FROM test WHERE a IN ['a?b','c?'] AND b = 'a?'"
        );
    }
}
