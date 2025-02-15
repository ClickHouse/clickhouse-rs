use hyper::{header::CONTENT_LENGTH, Method, Request};
use serde::{Deserialize, Serialize};
use std::fmt::Display;
use url::Url;

use crate::{
    error::{Error, Result},
    headers::with_request_headers,
    request_body::RequestBody,
    response::Response,
    row::Row,
    sql::{ser, Bind, SqlBuilder},
    Client,
};

pub use crate::cursor::RowCursor;

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

    /// Display SQL query as string.
    pub fn sql_display(&self) -> &impl Display {
        &self.sql
    }

    /// Binds `value` to the next `?` in the query.
    ///
    /// The `value`, which must either implement [`Serialize`] or be an
    /// [`Identifier`], will be appropriately escaped.
    ///
    /// All possible errors will be returned as [`Error::InvalidParams`]
    /// during query execution (`execute()`, `fetch()` etc.)
    ///
    /// WARNING: This means that the query must not have any extra `?`, even if
    /// they are in a string literal! Use `??` to have plain `?` in query.
    ///
    /// [`Serialize`]: serde::Serialize
    /// [`Identifier`]: crate::sql::Identifier
    #[track_caller]
    pub fn bind(mut self, value: impl Bind) -> Self {
        self.sql.bind_arg(value);
        self
    }

    /// Executes the query.
    pub async fn execute(self) -> Result<()> {
        self.do_execute()?.finish().await
    }

    /// Executes the query, returning a [`RowCursor`] to obtain results.
    ///
    /// # Example
    ///
    /// ```
    /// # async fn example() -> clickhouse::error::Result<()> {
    /// #[derive(clickhouse::Row, serde::Deserialize)]
    /// struct MyRow<'a> {
    ///     no: u32,
    ///     name: &'a str,
    /// }
    ///
    /// let mut cursor = clickhouse::Client::default()
    ///     .query("SELECT ?fields FROM some WHERE no BETWEEN 0 AND 1")
    ///     .fetch::<MyRow<'_>>()?;
    ///
    /// while let Some(MyRow { name, no }) = cursor.next().await? {
    ///     println!("{name}: {no}");
    /// }
    /// # Ok(()) }
    /// ```
    pub fn fetch<T: Row>(mut self) -> Result<RowCursor<T>> {
        self.sql.bind_fields::<T>();
        self.sql.append(" FORMAT RowBinary");

        let response = self.do_execute()?;
        Ok(RowCursor::new(response))
    }

    /// Executes the query and returns just a single row.
    ///
    /// Note that `T` must be owned.
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

    /// Executes the query and returns at most one row.
    ///
    /// Note that `T` must be owned.
    pub async fn fetch_optional<T>(self) -> Result<Option<T>>
    where
        T: Row + for<'b> Deserialize<'b>,
    {
        self.fetch()?.next().await
    }

    /// Executes the query and returns all the generated results,
    /// collected into a Vec.
    ///
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

    pub(crate) fn do_execute(self) -> Result<Response> {
        let query = self.sql.finish()?;

        let mut url =
            Url::parse(&self.client.url).map_err(|err| Error::InvalidParams(Box::new(err)))?;
        let mut pairs = url.query_pairs_mut();
        pairs.clear();

        if let Some(database) = &self.client.database {
            pairs.append_pair("database", database);
        }

        if self.client.compression.is_lz4() {
            pairs.append_pair("compress", "1");
        }

        for (name, value) in &self.client.options {
            pairs.append_pair(name, value);
        }
        drop(pairs);

        let mut builder = Request::builder().method(Method::POST).uri(url.as_str());
        builder = with_request_headers(builder, &self.client.headers, &self.client.products_info);
        builder = builder.header(CONTENT_LENGTH, query.len().to_string());

        if let Some(user) = &self.client.user {
            builder = builder.header("X-ClickHouse-User", user);
        }

        if let Some(password) = &self.client.password {
            builder = builder.header("X-ClickHouse-Key", password);
        }

        let request = builder
            .body(RequestBody::full(query))
            .map_err(|err| Error::InvalidParams(Box::new(err)))?;

        let future = self.client.http.request(request);
        Ok(Response::new(future, self.client.compression))
    }

    /// Similar to [`Client::with_option`], but for this particular query only.
    pub fn with_option(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.client.add_option(name, value);
        self
    }

    /// Specify server side parameter for query.
    ///
    /// In queries you can reference params as {name: type} e.g. {val: Int32}.
    pub fn param(mut self, name: &str, value: impl Serialize) -> Self {
        let mut param = String::from("");
        if let Err(err) = ser::write_param(&mut param, &value) {
            self.sql = SqlBuilder::Failed(format!("invalid param: {err}"));
            self
        } else {
            self.with_option(format!("param_{name}"), param)
        }
    }
}
