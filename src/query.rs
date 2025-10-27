use hyper::{Method, Request, header::CONTENT_LENGTH};
use serde::Serialize;
use std::fmt::Display;
use url::Url;

use crate::{
    Client,
    error::{Error, Result},
    formats,
    headers::with_request_headers,
    request_body::RequestBody,
    response::Response,
    row::{Row, RowOwned, RowRead},
    sql::{Bind, SqlBuilder, ser},
};

const MAX_QUERY_LEN_TO_USE_GET: usize = 8192;

pub use crate::cursors::{BytesCursor, RowCursor};
use crate::headers::with_authentication;

#[must_use]
#[derive(Clone)]
pub struct Query {
    client: Client,
    sql: SqlBuilder,
    wait_end_of_query: bool,
}

impl Query {
    pub(crate) fn new(client: &Client, template: &str) -> Self {
        Self {
            client: client.clone(),
            sql: SqlBuilder::new(template),
            wait_end_of_query: false,
        }
    }

    pub(crate) fn new_buffered(client: &Client, template: &str) -> Self {
        Self {
            client: client.clone(),
            sql: SqlBuilder::new(template),
            wait_end_of_query: true,
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
    /// during query execution (`execute()`, `fetch()`, etc.).
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
        self.do_execute(false, None)?.finish().await
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

        let validation = self.client.get_validation();
        let format = if validation {
            formats::ROW_BINARY_WITH_NAMES_AND_TYPES
        } else {
            formats::ROW_BINARY
        };

        let response = self.do_execute(true, Some(format))?;
        Ok(RowCursor::new(response, validation))
    }

    /// Executes the query and returns just a single row.
    ///
    /// Note that `T` must be owned.
    pub async fn fetch_one<T>(self) -> Result<T>
    where
        T: RowOwned + RowRead,
    {
        match self.fetch::<T>()?.next().await {
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
        T: RowOwned + RowRead,
    {
        self.fetch::<T>()?.next().await
    }

    /// Executes the query and returns all the generated results,
    /// collected into a Vec.
    ///
    /// Note that `T` must be owned.
    pub async fn fetch_all<T>(self) -> Result<Vec<T>>
    where
        T: RowOwned + RowRead,
    {
        let mut result = Vec::new();
        let mut cursor = self.fetch::<T>()?;

        while let Some(row) = cursor.next().await? {
            result.push(row);
        }

        Ok(result)
    }

    /// Executes the query, returning a [`BytesCursor`] to obtain results as raw
    /// bytes containing data in the [provided format].
    ///
    /// [provided format]: https://clickhouse.com/docs/en/interfaces/formats
    pub fn fetch_bytes(self, format: impl AsRef<str>) -> Result<BytesCursor> {
        let response = self.do_execute(true, Some(format.as_ref()))?;
        Ok(BytesCursor::new(response))
    }

    pub(crate) fn do_execute(
        self,
        read_only: bool,
        default_format: Option<&str>,
    ) -> Result<Response> {
        let query_formatted = format!("{}", self.sql_display());
        let query = self.sql.finish()?;

        let execution_span = tracing::info_span!(
            "clickhouse.query",
            status = tracing::field::Empty,
            otel.status_code = tracing::field::Empty,
            otel.kind = "CLIENT",
            db.system.name = "clickhouse",
            db.query.text = query_formatted,
            db.response.returned_rows = tracing::field::Empty,
            db.response.read_bytes = tracing::field::Empty,
            db.response.read_rows = tracing::field::Empty,
            db.response.written_bytes = tracing::field::Empty,
            db.response.written_rows = tracing::field::Empty,
            clickhouse.wait_end_of_query = self.wait_end_of_query,
        )
        .entered();

        let mut url =
            Url::parse(&self.client.url).map_err(|err| Error::InvalidParams(Box::new(err)))?;
        let mut pairs = url.query_pairs_mut();
        pairs.clear();

        if let Some(format) = default_format {
            pairs.append_pair("default_format", format);
        }

        if let Some(database) = &self.client.database {
            pairs.append_pair("database", database);
        }

        let use_post = !read_only || query.len() > MAX_QUERY_LEN_TO_USE_GET;

        let (method, body, content_length) = if use_post {
            if read_only {
                pairs.append_pair("readonly", "1");
            }
            let len = query.len();
            (Method::POST, RequestBody::full(query), len)
        } else {
            pairs.append_pair("query", &query);
            (Method::GET, RequestBody::empty(), 0)
        };

        if self.client.compression.is_lz4() {
            pairs.append_pair("compress", "1");
        }

        if self.wait_end_of_query {
            pairs.append_pair("wait_end_of_query", "1");
        }

        for (name, value) in &self.client.options {
            pairs.append_pair(name, value);
        }

        pairs.extend_pairs(self.client.roles.iter().map(|role| ("role", role)));

        drop(pairs);

        let mut builder = Request::builder().method(method).uri(url.as_str());
        builder = with_request_headers(builder, &self.client.headers, &self.client.products_info);
        builder = with_authentication(builder, &self.client.authentication);

        if content_length == 0 {
            builder = builder.header(CONTENT_LENGTH, "0");
        } else {
            builder = builder.header(CONTENT_LENGTH, content_length.to_string());
        }

        let request = builder
            .body(body)
            .map_err(|err| Error::InvalidParams(Box::new(err)))?;

        let future = self.client.http.request(request);
        Ok(Response::new(
            future,
            self.client.compression,
            execution_span.exit(),
        ))
    }

    /// Configure the [roles] to use when executing this query.
    ///
    /// Overrides any roles previously set by this method, [`Query::with_option`],
    /// [`Client::with_roles`] or [`Client::with_option`].
    ///
    /// An empty iterator may be passed to clear the set roles.
    ///
    /// [roles]: https://clickhouse.com/docs/operations/access-rights#role-management
    pub fn with_roles(self, roles: impl IntoIterator<Item = impl Into<String>>) -> Self {
        Self {
            client: self.client.with_roles(roles),
            ..self
        }
    }

    /// Clear any explicit [roles] previously set on this `Query` or inherited from [`Client`].
    ///
    /// Overrides any roles previously set by [`Query::with_roles`], [`Query::with_option`],
    /// [`Client::with_roles`] or [`Client::with_option`].
    ///
    /// [roles]: https://clickhouse.com/docs/operations/access-rights#role-management
    pub fn with_default_roles(self) -> Self {
        Self {
            client: self.client.with_default_roles(),
            ..self
        }
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
