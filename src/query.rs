use std::{fmt, marker::PhantomData};

use bytes::Bytes;
use either::Either;
use hyper::{client::ResponseFuture, Body, Request};
use serde::Deserialize;
use tokio::stream::StreamExt;
use url::Url;

use crate::{
    buflist::BufList,
    error::{Error, Result},
    introspection::{self, Reflection},
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

    pub fn fetch<T: Reflection>(mut self) -> Result<Cursor<T>> {
        let mut url = Url::parse(&self.client.url).expect("TODO");
        let mut pairs = url.query_pairs_mut();
        pairs.clear();

        if let Some(database) = &self.client.database {
            pairs.append_pair("database", database);
        }

        // TODO: what about escaping a table name?
        // https://clickhouse.yandex/docs/en/query_language/syntax/#syntax-identifiers

        self.sql.bind_fields::<T>();
        let query = self.sql.finish()?;
        pairs.append_pair("query", &query);
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

        let sending = self.client.client.request(request);

        Ok(Cursor {
            buffer: vec![0; BUFFER_SIZE],
            inner: Either::Left(sending),
            pending: BufList::default(),
            _marker: PhantomData,
        })
    }
}

pub struct Cursor<T> {
    buffer: Vec<u8>,
    inner: Either<ResponseFuture, Body>,
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

        let body = match &mut self.inner {
            Either::Left(fut) => {
                let body = fut.await?.into_body();
                self.inner = Either::Right(body);
                self.inner.as_mut().right().unwrap()
            }
            Either::Right(body) => body,
        };

        loop {
            let buffer = fuck_mut(&mut self.buffer);

            match rowbinary::deserialize_from(&mut self.pending, buffer) {
                Ok(value) => return Ok(Some(value)),
                Err(Error::NotEnoughData) => {}
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

impl SqlBuilder {
    fn new(template: &str) -> Self {
        SqlBuilder::InProgress {
            result: template.trim().replace("?fields", "_#fields#_"),
        }
    }

    fn bind_arg(&mut self, value: impl Bind) {
        if let Self::InProgress { result } = self {
            let mut iter = result.splitn(2, '?');

            let (prefix, suffix) = match (iter.next(), iter.next()) {
                (Some(prefix), Some(suffix)) => (prefix, suffix),
                _ => panic!("invalid number of query arguments"),
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
            let fields = introspection::collect_field_names::<T>().join(",");
            *result = result.replace("_#fields#_", &fields);
        }
    }

    fn finish(self) -> Result<String> {
        match self {
            Self::InProgress { mut result } => {
                // TODO: check the number of arguments.
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
    fn write(&self, mut dst: impl fmt::Write) -> fmt::Result {
        // TODO: escape.
        write!(dst, "{:?}", self)
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
    sql.bind_arg("kek");
    sql.bind_arg(42);
    sql.bind_fields::<Row>();

    assert_eq!(
        sql.finish().unwrap(),
        r#"SELECT a,b FROM test WHERE a = "kek" AND b < 42 FORMAT RowBinary"#
    );
}
