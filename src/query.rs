use std::marker::PhantomData;

use bytes::Bytes;
use hyper::{header::CONTENT_LENGTH, Body, Method, Request};
use serde::Deserialize;
use tokio::stream::StreamExt;
use url::Url;

use crate::{
    buflist::BufList,
    error::{Error, Result},
    introspection::Reflection,
    response::Response,
    rowbinary,
    sql_builder::{Bind, SqlBuilder},
    Client,
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
        let _ = self.do_execute::<()>(Method::POST)?.resolve().await?;
        Ok(())
    }

    pub fn fetch<T: Reflection>(mut self) -> Result<RowCursor<T>> {
        self.sql.append(" FORMAT RowBinary");

        Ok(RowCursor {
            response: self.do_execute::<T>(Method::GET)?,
            buffer: vec![0; BUFFER_SIZE],
            pending: BufList::default(),
            _marker: PhantomData,
        })
    }

    pub async fn fetch_one<T>(self) -> Result<T>
    where
        T: Reflection + for<'b> Deserialize<'b>,
    {
        match self.fetch()?.next().await {
            Ok(Some(row)) => Ok(row),
            Ok(None) => Err(Error::RowNotFound),
            Err(err) => Err(err),
        }
    }

    #[deprecated(since = "0.4.0", note = "use `Query::fetch()` instead")]
    pub fn rows<T: Reflection>(self) -> Result<RowCursor<T>> {
        self.fetch()
    }

    fn do_execute<T: Reflection>(mut self, method: Method) -> Result<Response> {
        let mut url = Url::parse(&self.client.url).expect("TODO");
        let mut pairs = url.query_pairs_mut();
        pairs.clear();

        if let Some(database) = &self.client.database {
            pairs.append_pair("database", database);
        }

        self.sql.bind_fields::<T>();
        let query = self.sql.finish()?;
        pairs.append_pair("allow_experimental_live_view", "1"); // TODO: send only if it's required.
        pairs.append_pair("query", &query);
        for (name, value) in &self.client.options {
            pairs.append_pair(name, value);
        }
        drop(pairs);

        let mut builder = Request::builder()
            .method(method)
            .uri(url.as_str())
            .header(CONTENT_LENGTH, "0");

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

pub struct RowCursor<T> {
    response: Response,
    buffer: Vec<u8>,
    pending: BufList<Bytes>,
    _marker: PhantomData<T>,
}

impl<T> RowCursor<T> {
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
