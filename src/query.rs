use hyper::{
    header::{ACCEPT_ENCODING, CONTENT_LENGTH},
    Body, Method, Request,
};
use serde::Deserialize;
use url::Url;

use crate::{
    cursor::RowBinaryCursor,
    error::{Error, Result},
    response::Response,
    row::Row,
    sql::{Bind, SqlBuilder},
    Client,
};

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
        self.do_execute(false)?.finish().await
    }

    /// TODO: docs, panics
    pub fn fetch<T: Row>(mut self) -> Result<RowCursor<T>> {
        self.sql.bind_fields::<T>();
        self.sql.append(" FORMAT RowBinary");

        let response = self.do_execute(true)?;
        Ok(RowCursor(RowBinaryCursor::new(response)))
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

    pub(crate) fn do_execute(self, read_only: bool) -> Result<Response> {
        let query = self.sql.finish()?;

        let mut url =
            Url::parse(&self.client.url).map_err(|err| Error::InvalidParams(Box::new(err)))?;
        let mut pairs = url.query_pairs_mut();
        pairs.clear();

        if let Some(database) = &self.client.database {
            pairs.append_pair("database", database);
        }

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

/// A cursor that emits rows.
pub struct RowCursor<T>(RowBinaryCursor<T>);

impl<T> RowCursor<T> {
    /// Emits the next row.
    pub async fn next<'a, 'b: 'a>(&'a mut self) -> Result<Option<T>>
    where
        T: Deserialize<'b>,
    {
        self.0.next().await
    }
}
