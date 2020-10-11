use std::fmt;

use crate::{
    error::{Error, Result},
    introspection::{self, Reflection},
    sealed::Sealed,
};

mod escape;

#[derive(Clone)]
pub enum SqlBuilder {
    InProgress { result: String },
    Failed(fmt::Error),
}

// We replace "?fields" with this in order to avoid look-ahead parsing.
const FIELDS_PLACEHOLDER: &str = "_#fields#_";

impl SqlBuilder {
    pub fn new(template: &str) -> Self {
        SqlBuilder::InProgress {
            result: template.trim().replace("?fields", FIELDS_PLACEHOLDER),
        }
    }

    pub fn bind_arg(&mut self, value: impl Bind) {
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

    pub fn bind_fields<T: Reflection>(&mut self) {
        if let Self::InProgress { result } = self {
            if let Some(fields) = introspection::join_field_names::<T>() {
                *result = result.replace(FIELDS_PLACEHOLDER, &fields);
            }
        }
    }

    pub fn append(&mut self, suffix: &str) {
        if let Self::InProgress { result } = self {
            result.push_str(suffix);
        }
    }

    pub fn finish(self) -> Result<String> {
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

pub trait Bind: Sealed {
    #[doc(hidden)]
    fn reserve(&self) -> usize;

    #[doc(hidden)]
    fn write(&self, dst: impl fmt::Write) -> fmt::Result;
}

macro_rules! impl_num {
    ($ty:ty, $reserve:literal) => {
        impl Sealed for $ty {}

        impl Bind for $ty {
            #[inline]
            fn reserve(&self) -> usize {
                $reserve
            }

            #[inline]
            fn write(&self, mut dst: impl fmt::Write) -> fmt::Result {
                write!(dst, "{}", self)
            }
        }
    };
}

impl_num!(i8, 4);
impl_num!(u8, 3);
impl_num!(i16, 6);
impl_num!(u16, 5);
impl_num!(i32, 11);
impl_num!(u32, 10);
impl_num!(i64, 20);
impl_num!(u64, 20);
impl_num!(i128, 40);
impl_num!(u128, 39);

impl Sealed for &str {}

impl Bind for &str {
    #[inline]
    fn reserve(&self) -> usize {
        self.len()
    }

    #[inline]
    fn write(&self, dst: impl fmt::Write) -> fmt::Result {
        escape::string(self, dst)
    }
}

#[test]
fn it_builds_sql() {
    #[allow(dead_code)]
    #[derive(Reflection)]
    struct Row {
        a: u32,
        b: u32,
    }

    let mut sql = SqlBuilder::new("SELECT ?fields FROM test WHERE a = ? AND b < ?");
    sql.bind_arg("foo");
    sql.bind_arg(42);
    sql.bind_fields::<Row>();

    assert_eq!(
        sql.finish().unwrap(),
        r"SELECT a,b FROM test WHERE a = 'foo' AND b < 42"
    );
}
