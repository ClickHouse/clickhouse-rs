use std::{fmt, marker::PhantomData};

use bytes::Bytes;
use hyper::{Body, Request};
use serde::Deserialize;
use tokio::stream::StreamExt;
use url::Url;

use crate::{
    buflist::BufList,
    error::{Error, Result},
    escape,
    introspection::{self, Reflection},
    response::Response,
    rowbinary, Client,
};

const BUFFER_SIZE: usize = 8 * 1024;

#[derive(Clone)]
pub struct Query {
    client: Client,
    sql: SqlBuilder,
}

impl Query {
    pub(crate) fn new(client: &Client, template: &str) -> Self {
        Self {
            client: client.clone(),
            sql: SqlBuilder::new(template),
        }
    }

    pub fn bind(mut self, value: impl Bind) -> Self {
        self.sql.bind_arg(value);
        self
    }

    pub async fn execute(self) -> Result<()> {
        // TODO: should we read the body?
        let _ = self.do_execute::<()>()?.resolve().await?;
        Ok(())
    }

    pub fn fetch<T: Reflection>(self) -> Result<Cursor<T>> {
        Ok(Cursor {
            response: self.do_execute::<T>()?,
            buffer: vec![0; BUFFER_SIZE],
            pending: BufList::default(),
            _marker: PhantomData,
        })
    }

    fn do_execute<T: Reflection>(mut self) -> Result<Response> {
        let mut url = Url::parse(&self.client.url).expect("TODO");
        let mut pairs = url.query_pairs_mut();
        pairs.clear();

        if let Some(database) = &self.client.database {
            pairs.append_pair("database", database);
        }

        self.sql.bind_fields::<T>();
        let query = self.sql.finish()?;
        pairs.append_pair("query", &query);
        for (name, value) in &self.client.options {
            pairs.append_pair(name, value);
        }
        drop(pairs);

        let mut builder = Request::get(url.as_str());

        if let Some(user) = &self.client.user {
            builder = builder.header("X-ClickHouse-User", user);
        }

        if let Some(password) = &self.client.password {
            builder = builder.header("X-ClickHouse-Key", password);
        }

        let request = builder
            .body(Body::empty())
            .map_err(|err| Error::InvalidParams(Box::new(err)))?;

        Ok(self.client.client.request(request).into())
    }
}

pub struct Cursor<T> {
    response: Response,
    buffer: Vec<u8>,
    pending: BufList<Bytes>,
    _marker: PhantomData<T>,
}

impl<T> Cursor<T> {
    #[allow(clippy::should_implement_trait)]
    pub async fn next<'a, 'b: 'a>(&'a mut self) -> Result<Option<T>>
    where
        T: Deserialize<'b>,
    {
        // XXX: a workaround for https://github.com/rust-lang/rust/issues/51132.
        fn fuck_mut<'a, T>(ptr: &mut T) -> &'a mut T {
            unsafe { &mut *(ptr as *mut T) }
        }

        let body = self.response.resolve().await?;

        loop {
            let buffer = fuck_mut(&mut self.buffer);

            match rowbinary::deserialize_from(&mut self.pending, buffer) {
                Ok(value) => {
                    self.pending.commit();
                    return Ok(Some(value));
                }
                Err(Error::NotEnoughData) => {
                    self.pending.rollback();
                }
                Err(err) => return Err(err),
            }

            match body.try_next().await? {
                Some(chunk) => self.pending.push(chunk),
                None if self.pending.bufs_cnt() > 0 => return Err(Error::NotEnoughData),
                None => return Ok(None),
            }
        }
    }
}

#[derive(Clone)]
enum SqlBuilder {
    InProgress { result: String },
    Failed(fmt::Error),
}

// We replace "?fields" with this in order to avoid look-ahead parsing.
const FIELDS_PLACEHOLDER: &str = "_#fields#_";

impl SqlBuilder {
    fn new(template: &str) -> Self {
        SqlBuilder::InProgress {
            result: template.trim().replace("?fields", FIELDS_PLACEHOLDER),
        }
    }

    fn bind_arg(&mut self, value: impl Bind) {
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

    fn bind_fields<T: Reflection>(&mut self) {
        if let Self::InProgress { result } = self {
            if let Some(fields) = introspection::join_field_names::<T>() {
                *result = result.replace(FIELDS_PLACEHOLDER, &fields);
            }
        }
    }

    fn finish(self) -> Result<String> {
        match self {
            Self::InProgress { mut result } => {
                if result.contains('?') {
                    panic!("unbound query argument: ?");
                }

                if result.contains(FIELDS_PLACEHOLDER) {
                    panic!("unbound query argument: ?fields");
                }

                result.push_str(" FORMAT RowBinary");
                Ok(result)
            }
            Self::Failed(err) => Err(Error::InvalidParams(Box::new(err))),
        }
    }
}

pub trait Bind: private::Sealed {
    #[doc(hidden)]
    fn reserve(&self) -> usize;

    #[doc(hidden)]
    fn write(&self, dst: impl fmt::Write) -> fmt::Result;
}

macro_rules! impl_num {
    ($ty:ty, $reserve:literal) => {
        impl private::Sealed for $ty {}

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

impl private::Sealed for &str {}

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

mod private {
    pub trait Sealed {}
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
        r"SELECT a,b FROM test WHERE a = 'foo' AND b < 42 FORMAT RowBinary"
    );
}
