use std::marker::PhantomData;

use bytes::Bytes;
use futures::TryStreamExt;
use hyper::{
    header::{ACCEPT_ENCODING, CONTENT_LENGTH},
    Body, Method, Request,
};
use serde::Deserialize;
use url::Url;

use crate::{
    buflist::BufList,
    error::{Error, Result},
    response::Response,
    row::Row,
    rowbinary,
    sql::{Bind, SqlBuilder},
    Client,
};

const BUFFER_SIZE: usize = 8 * 1024;
const MAX_QUERY_LEN_TO_USE_GET: usize = 8192;

#[must_use]
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
        let _ = self.do_execute::<()>(false)?.resolve().await?;
        Ok(())
    }

    pub fn fetch<T: Row>(mut self) -> Result<RowCursor<T>> {
        self.sql.append(" FORMAT RowBinary");

        Ok(RowCursor {
            response: self.do_execute::<T>(true)?,
            buffer: vec![0; BUFFER_SIZE],
            pending: BufList::default(),
            _marker: PhantomData,
        })
    }

    pub async fn fetch_one<T>(self) -> Result<T>
    where
        T: Row + for<'b> Deserialize<'b>,
    {
        match self.fetch()?.next().await {
            Ok(Some(row)) => Ok(row),
            Ok(None) => Err(Error::RowNotFound),
            Err(err) => Err(err),
        }
    }

    /// Executes the query and returns all the generated results, collected into a Vec.
    /// Note that `T` must be owned.
    pub async fn fetch_all<T>(self) -> Result<Vec<T>>
    where
        T: Row + for<'b> Deserialize<'b>,
    {
        let mut result = Vec::new();
        let mut cursor = self.fetch::<T>()?;

        while let Some(row) = cursor.next().await? {
            result.push(row);
        }

        Ok(result)
    }

    #[deprecated(since = "0.4.0", note = "use `Query::fetch()` instead")]
    pub fn rows<T: Row>(self) -> Result<RowCursor<T>> {
        self.fetch()
    }

    fn do_execute<T: Row>(mut self, read_only: bool) -> Result<Response> {
        let mut url = Url::parse(&self.client.url).expect("TODO");
        let mut pairs = url.query_pairs_mut();
        pairs.clear();

        if let Some(database) = &self.client.database {
            pairs.append_pair("database", database);
        }

        self.sql.bind_fields::<T>();
        let query = self.sql.finish()?;

        let use_post = !read_only || query.len() > MAX_QUERY_LEN_TO_USE_GET;
        let method = if use_post { Method::POST } else { Method::GET };

        let (body, content_length) = if use_post {
            if read_only {
                pairs.append_pair("readonly", "1");
            }
            let len = query.len();
            (Body::from(query), len)
        } else {
            pairs.append_pair("query", &query);
            (Body::empty(), 0)
        };

        #[cfg(feature = "lz4")]
        if self.client.compression == crate::Compression::Lz4 {
            pairs.append_pair("compress", "1");
        }

        #[allow(deprecated)]
        if self.client.compression.encoding().is_some() {
            pairs.append_pair("enable_http_compression", "1");
        }

        for (name, value) in &self.client.options {
            pairs.append_pair(name, value);
        }
        drop(pairs);

        let mut builder = Request::builder().method(method).uri(url.as_str());

        if content_length == 0 {
            builder = builder.header(CONTENT_LENGTH, "0");
        } else {
            builder = builder.header(CONTENT_LENGTH, content_length.to_string());
        }

        if let Some(user) = &self.client.user {
            builder = builder.header("X-ClickHouse-User", user);
        }

        if let Some(password) = &self.client.password {
            builder = builder.header("X-ClickHouse-Key", password);
        }

        #[allow(deprecated)]
        if let Some(encoding) = self.client.compression.encoding() {
            builder = builder.header(ACCEPT_ENCODING, encoding);
        }

        let request = builder
            .body(body)
            .map_err(|err| Error::InvalidParams(Box::new(err)))?;

        let future = self.client.client.request(request);
        Ok(Response::new(future, self.client.compression))
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

        let chunks = self.response.resolve().await?;

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

            match chunks.try_next().await? {
                Some(chunk) => self.pending.push(chunk),
                None if self.pending.bufs_cnt() > 0 => return Err(Error::NotEnoughData),
                None => return Ok(None),
            }
        }
    }
}
