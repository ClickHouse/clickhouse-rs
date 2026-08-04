use bytes::Bytes;
use hyper::{
    Method, Request,
    header::{CONTENT_LENGTH, CONTENT_TYPE, HeaderValue, TRANSFER_ENCODING},
};
use serde::Serialize;
use std::fmt::Display;
use tracing::Instrument;
use url::Url;

use crate::{
    Client, Compression,
    error::{Error, Result},
    formats,
    headers::with_request_headers,
    request_body::RequestBody,
    response::Response,
    row::{Row, RowOwned, RowRead},
    sql::{Bind, SqlBuilder, ser},
};

pub use crate::cursors::{BytesCursor, NativeCursor, RowCursor};
use crate::headers::with_authentication;
use crate::settings;
use crate::settings::CLIENT_PROTOCOL_VERSION;

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

    pub(crate) fn raw(client: &Client, query: &str) -> Self {
        Self {
            client: client.clone(),
            sql: SqlBuilder::raw(query),
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
        // Enter the span for the `self.do_execute()` call
        let span = self.make_span(None);

        async {
            let mut response = self
                .do_execute(None)
                .inspect_err(|e| e.record_in_current_span("error executing query"))?;

            response
                .finish()
                .await
                .inspect_err(|e| e.record_in_current_span("response error"))
        }
        .instrument(span)
        .await
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
        let validation = self.client.get_validation();
        let format = if validation {
            formats::ROW_BINARY_WITH_NAMES_AND_TYPES
        } else {
            formats::ROW_BINARY
        };

        let span = self.make_span(Some(format)).entered();

        self.sql.bind_fields::<T>();

        let response = self
            .do_execute(Some(format))
            .inspect_err(|e| e.record_in_current_span("error executing fetch"))?;

        Ok(RowCursor::new(response, validation, span.exit()))
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
        let format = format.as_ref();

        let span = self.make_span(Some(format)).entered();

        let response = self.do_execute(Some(format))?;
        Ok(BytesCursor::new(response, span.exit()))
    }

    pub fn fetch_native(mut self) -> Result<NativeCursor> {
        let span = self.make_span(Some("Native")).entered();

        let client_protocol_version = self.client.settings.get(CLIENT_PROTOCOL_VERSION);

        // Setting `client_protocol_version` to a nonzero value changes the Native response format
        // in ways we are unable to handle, and we would have no way to detect a nonzero version
        // in the response. Return an error if the user is messing around with this setting
        // (which they should not be).
        //
        // https://clickhouse.com/docs/reference/interfaces/specs/NativeFormat#revision-output
        // Original report: https://github.com/ClickHouse/clickhouse-rs/pull/464
        if let Some(version) = client_protocol_version
            && version != "0"
        {
            return Err(Error::Other(format!(
                "Client does not support a nonzero `{CLIENT_PROTOCOL_VERSION}` setting ({version:?}), \
                 please set this setting to \"0\" or leave it unset"
            ).into()));
        }

        // FIXME: use HTTP body compression instead of block-level compression
        self.client = self.client.with_compression(Compression::None);

        let response = self.do_execute(Some("Native"))?;

        Ok(NativeCursor::new(response, span.exit()))
    }

    pub(crate) fn make_span(&self, response_format: Option<&str>) -> tracing::Span {
        // https://opentelemetry.io/docs/specs/semconv/db/sql/
        // TODO: write our own Semantic Conventions for ClickHouse
        tracing::info_span!(
            "clickhouse.query",
            // OTel conventional fields
            // Note that `Empty` or `Option::None` fields are not reported,
            // so we can avoid adding noise to logs when the `opentelemetry` feature is disabled.
            otel.status_code = tracing::field::Empty,
            otel.kind = cfg!(feature = "opentelemetry").then_some("client"),
            error.type = tracing::field::Empty,
            db.system.name = cfg!(feature = "opentelemetry").then_some("clickhouse"),
            // Only log full query text at TRACE level
            // Important that this is taken before client-side parameters are populated
            // FIXME: we can't use `enabled!` due to https://github.com/tokio-rs/tracing/issues/2448
            // but we don't want to log the full query at all verbosity levels.
            // db.query.text = tracing::enabled!(tracing::Level::TRACE).then(|| self.sql.to_string()),
            // TODO: generate summary
            db.query.summary = tracing::field::Empty,
            db.response.status_code = tracing::field::Empty,
            db.response.returned_rows = tracing::field::Empty,
            // ClickHouse-specific extension fields
            clickhouse.request.session_id = self.client.get_setting(settings::SESSION_ID),
            clickhouse.request.query_id = self.client.get_setting(settings::QUERY_ID),
            clickhouse.response.received_bytes = tracing::field::Empty,
            clickhouse.response.decoded_bytes = tracing::field::Empty,
            clickhouse.response.format = response_format,
        )
    }

    pub(crate) fn do_execute(self, default_format: Option<&str>) -> Result<Response> {
        let query = self.sql.finish()?;

        let mut url =
            Url::parse(&self.client.url).map_err(|err| Error::InvalidParams(Box::new(err)))?;
        let mut pairs = url.query_pairs_mut();
        pairs.clear();

        if let Some(format) = default_format {
            pairs.append_pair(settings::DEFAULT_FORMAT, format);
        }

        if let Some(database) = &self.client.database {
            pairs.append_pair(settings::DATABASE, database);
        }

        if self.client.compression.is_enabled() {
            #[cfg(feature = "zstd")]
            if matches!(self.client.compression, crate::Compression::Zstd(_)) {
                pairs.append_pair(settings::ENABLE_HTTP_COMPRESSION, "1");
            } else {
                pairs.append_pair(settings::COMPRESS, "1");
            }

            #[cfg(not(feature = "zstd"))]
            pairs.append_pair(settings::COMPRESS, "1");
        }

        let mut parameters = Vec::new();
        for (name, value) in &self.client.settings {
            if name.starts_with("param_") {
                parameters.push((name.as_str(), value.as_str()));
            } else {
                pairs.append_pair(name, value);
            }
        }

        pairs.extend_pairs(self.client.roles.iter().map(|role| (settings::ROLE, role)));

        drop(pairs);

        let multipart = (!parameters.is_empty())
            .then(|| multipart_query_body(&query, &mut parameters))
            .transpose()
            .inspect_err(|err| err.record_in_current_span("invalid params in query"))?;

        let mut builder = Request::builder().method(Method::POST).uri(url.as_str());
        builder = with_request_headers(builder, &self.client.headers, &self.client.products_info);
        builder = with_authentication(builder, &self.client.authentication);

        #[cfg(feature = "zstd")]
        if matches!(self.client.compression, crate::Compression::Zstd(_)) {
            builder = builder.header("Accept-Encoding", "zstd");
        }

        let body = if let Some(multipart) = multipart {
            // The client controls headers that frame this body. `headers_mut()` is
            // `None` if a caller-provided header has already invalidated the builder;
            // leave that error intact for `builder.body()` below.
            if let Some(headers) = builder.headers_mut() {
                headers.remove(CONTENT_LENGTH);
                headers.remove(TRANSFER_ENCODING);
                headers.insert(
                    CONTENT_TYPE,
                    HeaderValue::from_str(&multipart.content_type).map_err(|err| {
                        let err = Error::InvalidParams(Box::new(err));
                        err.record_in_current_span("invalid params in query");
                        err
                    })?,
                );
            }

            RequestBody::full(multipart.body)
        } else {
            // Retain the raw SQL request format for parameter-free queries.
            builder = builder.header(CONTENT_LENGTH, query.len().to_string());
            RequestBody::full(query)
        };

        let request = builder.body(body).map_err(|err| {
            let err = Error::InvalidParams(Box::new(err));
            err.record_in_current_span("invalid params in query");
            err
        })?;

        let future = self.client.http.request(request);
        Ok(Response::new(future, self.client.compression))
    }

    /// Configure the [roles] to use when executing this query.
    ///
    /// Overrides any roles previously set by this method, [`Query::with_setting`],
    /// [`Client::with_roles`] or [`Client::with_setting`].
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
    /// Overrides any roles previously set by [`Query::with_roles`], [`Query::with_setting`],
    /// [`Client::with_roles`] or [`Client::with_setting`].
    ///
    /// [roles]: https://clickhouse.com/docs/operations/access-rights#role-management
    pub fn with_default_roles(self) -> Self {
        Self {
            client: self.client.with_default_roles(),
            ..self
        }
    }

    /// Similar to [`Client::with_option`], but for this particular query only.
    #[deprecated(since = "0.14.3", note = "please use `with_setting` instead")]
    pub fn with_option(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.client.set_setting(name, value);
        self
    }

    /// Similar to [`Client::with_setting`], but for this particular query only.
    pub fn with_setting(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.client.set_setting(name, value);
        self
    }

    // Used in `clickhouse-ext-arrow` to track Arrow adoption.
    /// Similar to [`Client::with_product_info()`], but for this query only.
    pub fn with_product_info(
        self,
        product_name: impl Into<String>,
        product_version: impl Into<String>,
    ) -> Self {
        Self {
            client: self.client.with_product_info(product_name, product_version),
            ..self
        }
    }

    /// Specify a ClickHouse server-side query parameter.
    ///
    /// This creates a ClickHouse `param_<name>` request parameter. In queries,
    /// you can reference parameters as `{name: Type}`, for example
    /// `{val: Int32}`. `name` must match the supported bare identifier grammar:
    /// `[A-Za-z_][A-Za-z0-9_]*`. An invalid name returns [`Error::InvalidParams`]
    /// during execution.
    pub fn param(mut self, name: &str, value: impl Serialize) -> Self {
        let mut param = String::from("");
        if let Err(err) = ser::write_param(&mut param, &value) {
            self.sql = SqlBuilder::Failed(format!("invalid param: {err}"));
            self
        } else {
            self.with_setting(format!("param_{name}"), param)
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::error::Error;
    use crate::{Client, settings};

    #[test]
    fn fetch_native_errors_nonzero_client_protocol_version() {
        let client = Client::default().with_url("http://localhost:8123");

        // Assert that it does not error by default.
        let _cursor = client.query("SELECT * FROM foo").fetch_native().unwrap();

        let err = client
            .query("SELECT * FROM foo")
            .with_setting(settings::CLIENT_PROTOCOL_VERSION, "54492")
            .fetch_native()
            .err()
            .unwrap_or_else(|| panic!("expected error"));

        let Error::Other(e) = err else {
            panic!("unexpected error kind: {err:?}");
        };

        let err_str = e.to_string();

        assert!(
            err_str.contains("client_protocol_version"),
            "unexpected error: {err_str:?}"
        );
    }
}

const MULTIPART_BOUNDARY_PREFIX: &str = "clickhouse-rs-boundary-";

struct MultipartQueryBody {
    body: Bytes,
    content_type: String,
}

fn multipart_query_body(
    query: &str,
    parameters: &mut [(&str, &str)],
) -> Result<MultipartQueryBody> {
    for (field_name, _) in parameters.iter() {
        if !is_valid_parameter_field_name(field_name) {
            return Err(invalid_params(format!(
                "invalid ClickHouse query parameter name: {field_name:?}"
            )));
        }
    }

    parameters.sort_unstable_by(|(left, _), (right, _)| left.cmp(right));

    let boundary = multipart_boundary(query, parameters)?;
    let mut body = Vec::new();

    append_multipart_field(&mut body, &boundary, "query", query);
    for (field_name, value) in parameters {
        append_multipart_field(&mut body, &boundary, field_name, value);
    }
    body.extend_from_slice(b"--");
    body.extend_from_slice(boundary.as_bytes());
    body.extend_from_slice(b"--\r\n");

    Ok(MultipartQueryBody {
        body: Bytes::from(body),
        content_type: format!("multipart/form-data; boundary={boundary}"),
    })
}

fn is_valid_parameter_field_name(field_name: &str) -> bool {
    let Some(name) = field_name.strip_prefix("param_") else {
        return false;
    };
    let mut chars = name.bytes();
    matches!(chars.next(), Some(b'a'..=b'z' | b'A'..=b'Z' | b'_'))
        && chars.all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
}

fn multipart_boundary(query: &str, parameters: &[(&str, &str)]) -> Result<String> {
    let mut suffix = 0_usize;
    loop {
        let boundary = format!("{MULTIPART_BOUNDARY_PREFIX}{suffix}");
        if !payload_contains_boundary(query, &boundary)
            && parameters
                .iter()
                .all(|(_, value)| !payload_contains_boundary(value, &boundary))
        {
            return Ok(boundary);
        }

        suffix = suffix
            .checked_add(1)
            .ok_or_else(|| invalid_params("unable to construct multipart query body"))?;
    }
}

fn payload_contains_boundary(payload: &str, boundary: &str) -> bool {
    payload.starts_with(&format!("--{boundary}")) || payload.contains(&format!("\r\n--{boundary}"))
}

fn append_multipart_field(body: &mut Vec<u8>, boundary: &str, name: &str, value: &str) {
    body.extend_from_slice(b"--");
    body.extend_from_slice(boundary.as_bytes());
    body.extend_from_slice(b"\r\nContent-Disposition: form-data; name=\"");
    body.extend_from_slice(name.as_bytes());
    body.extend_from_slice(b"\"\r\n\r\n");
    body.extend_from_slice(value.as_bytes());
    body.extend_from_slice(b"\r\n");
}

fn invalid_params(message: impl Into<String>) -> Error {
    Error::InvalidParams(Box::new(std::io::Error::new(
        std::io::ErrorKind::InvalidInput,
        message.into(),
    )))
}

#[cfg(test)]
mod multipart_tests {
    use super::*;

    #[test]
    fn post_query_params_multipart_body() {
        let query = "SELECT {a: String}";
        let mut parameters = [("param_z", "last"), ("param_a", "first")];

        let multipart = multipart_query_body(query, &mut parameters).unwrap();
        let boundary = multipart
            .content_type
            .strip_prefix("multipart/form-data; boundary=")
            .unwrap();
        let expected = format!(
            "--{boundary}\r\nContent-Disposition: form-data; name=\"query\"\r\n\r\n{query}\r\n\
             --{boundary}\r\nContent-Disposition: form-data; name=\"param_a\"\r\n\r\nfirst\r\n\
             --{boundary}\r\nContent-Disposition: form-data; name=\"param_z\"\r\n\r\nlast\r\n\
             --{boundary}--\r\n"
        );

        assert_eq!(multipart.body, expected);
    }

    #[test]
    fn post_query_params_boundary_collisions() {
        let boundary = format!("{MULTIPART_BOUNDARY_PREFIX}0");
        let value = format!("\r\n--{boundary}inside");
        let mut parameters = [("param_value", value.as_str())];
        let multipart =
            multipart_query_body(&format!("--{boundary}at-the-start"), &mut parameters).unwrap();

        assert_eq!(
            multipart.content_type,
            format!("multipart/form-data; boundary={MULTIPART_BOUNDARY_PREFIX}1")
        );
    }

    #[test]
    fn post_query_params_identifier_validation() {
        for name in ["param_a", "param_A9", "param__"] {
            assert!(is_valid_parameter_field_name(name), "{name}");
        }
        for name in [
            "param_",
            "param_9a",
            "param_a-b",
            "param_a\nb",
            "param_a\rb",
            "param_a\0b",
            "param_a\"b",
            "param_a\\b",
        ] {
            assert!(!is_valid_parameter_field_name(name), "{name:?}");
            assert!(multipart_query_body("SELECT 1", &mut [(name, "value")]).is_err());
        }
    }
}

#[cfg(all(test, feature = "test-util"))]
mod transport_tests {
    use super::*;
    use crate::test;
    use hyper::Request;

    fn parse_multipart_fields(request: &Request<Bytes>) -> Vec<(String, Vec<u8>)> {
        let content_type = request
            .headers()
            .get_all(CONTENT_TYPE)
            .iter()
            .map(|value| value.to_str().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(content_type.len(), 1);
        let boundary = content_type[0]
            .strip_prefix("multipart/form-data; boundary=")
            .expect("multipart content type");
        let opening = format!("--{boundary}\r\n");
        let marker = format!("\r\n--{boundary}");
        let closing = b"--\r\n";
        let mut input = request
            .body()
            .strip_prefix(opening.as_bytes())
            .expect("opening boundary");
        let mut fields = Vec::new();

        loop {
            let header_end = find_bytes(input, b"\r\n\r\n").expect("field headers");
            let headers = std::str::from_utf8(&input[..header_end]).expect("UTF-8 headers");
            let name = headers
                .strip_prefix("Content-Disposition: form-data; name=\"")
                .and_then(|header| header.strip_suffix('"'))
                .expect("only a Content-Disposition header")
                .to_owned();
            input = &input[header_end + 4..];

            let value_end = find_bytes(input, marker.as_bytes()).expect("next boundary");
            fields.push((name, input[..value_end].to_vec()));
            input = &input[value_end + marker.len()..];

            if input.starts_with(closing) {
                assert_eq!(&input[closing.len()..], b"");
                return fields;
            }
            input = input
                .strip_prefix(b"\r\n")
                .expect("field boundary terminator");
        }
    }

    fn find_bytes(haystack: &[u8], needle: &[u8]) -> Option<usize> {
        haystack
            .windows(needle.len())
            .position(|window| window == needle)
    }

    fn assert_single_content_length(request: &Request<Bytes>) {
        let content_length = request
            .headers()
            .get_all(CONTENT_LENGTH)
            .iter()
            .collect::<Vec<_>>();
        assert_eq!(content_length.len(), 1);
        assert_eq!(
            content_length[0]
                .to_str()
                .unwrap()
                .parse::<usize>()
                .unwrap(),
            request.body().len()
        );
    }

    #[tokio::test]
    async fn post_query_params_transport() {
        let mock = test::Mock::new();
        let record = mock.add(test::handlers::record_request());
        let client = Client::default()
            .with_mock(&mock)
            .with_database("test_db")
            .with_roles(["reader", "writer"])
            .with_setting("max_block_size", "123")
            .with_setting("param_z", "last");
        #[cfg(feature = "lz4")]
        let client = client.with_compression(Compression::Lz4);

        let mut cursor = client
            .query("SELECT {a: UInt8}, {z: String}")
            .param("a", 42)
            .fetch::<String>()
            .unwrap();
        assert!(cursor.next().await.unwrap().is_none());

        let request = record.request().await;
        assert_eq!(request.method(), Method::POST);
        assert_single_content_length(&request);

        let pairs = url::form_urlencoded::parse(request.uri().query().unwrap().as_bytes())
            .map(|(name, value)| (name.into_owned(), value.into_owned()))
            .collect::<Vec<_>>();
        for expected in [
            ("database", "test_db"),
            ("default_format", "RowBinary"),
            ("max_block_size", "123"),
            ("role", "reader"),
            ("role", "writer"),
        ] {
            assert!(
                pairs
                    .iter()
                    .any(|(name, value)| name == expected.0 && value == expected.1),
                "missing {expected:?} in {pairs:?}"
            );
        }
        assert!(!pairs.iter().any(|(name, _)| name.starts_with("param_")));
        #[cfg(feature = "lz4")]
        assert!(
            pairs
                .iter()
                .any(|(name, value)| name == "compress" && value == "1"),
            "missing compression setting in {pairs:?}"
        );

        assert_eq!(
            parse_multipart_fields(&request),
            vec![
                (
                    "query".to_owned(),
                    b"SELECT {a: UInt8}, {z: String}".to_vec()
                ),
                ("param_a".to_owned(), b"42".to_vec()),
                ("param_z".to_owned(), b"last".to_vec()),
            ]
        );
    }

    #[tokio::test]
    async fn post_query_params_transport_controls_framing_headers() {
        let mock = test::Mock::new();
        let record = mock.add(test::handlers::record_request());
        let client = Client::default()
            .with_mock(&mock)
            .with_header("Content-Type", "text/plain")
            .with_header("content-type", "application/json")
            .with_header("Content-Length", "1")
            .with_header("content-length", "2")
            .with_header("Transfer-Encoding", "chunked");

        client
            .query("SELECT {value: UInt8}")
            .param("value", 1)
            .execute()
            .await
            .unwrap();

        let request = record.request().await;
        let content_types = request
            .headers()
            .get_all(CONTENT_TYPE)
            .iter()
            .collect::<Vec<_>>();
        assert_eq!(content_types.len(), 1);
        assert!(
            content_types[0]
                .to_str()
                .unwrap()
                .starts_with("multipart/form-data; boundary=")
        );
        assert_single_content_length(&request);
        assert!(request.headers().get(TRANSFER_ENCODING).is_none());
    }

    #[cfg(feature = "zstd")]
    #[tokio::test]
    async fn post_query_params_transport_keeps_zstd_negotiation() {
        let mock = test::Mock::new();
        let record = mock.add(test::handlers::record_request());

        Client::default()
            .with_mock(&mock)
            .with_compression(Compression::zstd())
            .query("SELECT {value: UInt8}")
            .param("value", 1)
            .execute()
            .await
            .unwrap();

        let request = record.request().await;
        let pairs = url::form_urlencoded::parse(request.uri().query().unwrap().as_bytes())
            .collect::<Vec<_>>();
        assert!(
            pairs
                .iter()
                .any(|(name, value)| { name == settings::ENABLE_HTTP_COMPRESSION && value == "1" })
        );
        assert_eq!(
            request.headers().get("Accept-Encoding"),
            Some(&HeaderValue::from_static("zstd"))
        );
        assert_eq!(
            parse_multipart_fields(&request),
            vec![
                ("query".to_owned(), b"SELECT {value: UInt8}".to_vec()),
                ("param_value".to_owned(), b"1".to_vec()),
            ]
        );
    }

    #[tokio::test]
    async fn post_query_params_transport_rejects_invalid_names_and_headers() {
        for suffix in ["", "a\nb", "a\rb", "a\0b", "a\"b", "a\\b"] {
            let mut mock = test::Mock::new();
            mock.add(test::handlers::failure(test::status::INTERNAL_SERVER_ERROR));
            mock.non_exhaustive();
            let error = Client::default()
                .with_mock(&mock)
                .query("SELECT {value: String}")
                .with_setting(format!("param_{suffix}"), "value")
                .execute()
                .await
                .unwrap_err();
            assert!(
                matches!(error, Error::InvalidParams(_)),
                "{suffix:?}: {error:?}"
            );
        }

        for (name, value) in [("invalid header", "value"), ("X-Test", "bad\nvalue")] {
            let mut mock = test::Mock::new();
            mock.add(test::handlers::failure(test::status::INTERNAL_SERVER_ERROR));
            mock.non_exhaustive();
            let error = Client::default()
                .with_mock(&mock)
                .with_header(name, value)
                .query("SELECT {value: UInt8}")
                .param("value", 1)
                .execute()
                .await
                .unwrap_err();
            assert!(
                matches!(error, Error::InvalidParams(_)),
                "{name:?}: {error:?}"
            );
        }
    }

    #[tokio::test]
    async fn post_query_params_transport_keeps_raw_queries_unchanged() {
        let mock = test::Mock::new();
        let record = mock.add(test::handlers::record_request());

        Client::default()
            .with_mock(&mock)
            .query("SELECT 1")
            .execute()
            .await
            .unwrap();

        let request = record.request().await;
        assert_eq!(request.body(), &Bytes::from_static(b"SELECT 1"));
        assert!(request.headers().get(CONTENT_TYPE).is_none());
        assert_single_content_length(&request);
    }

    #[tokio::test]
    async fn post_query_params_transport_preserves_last_write_wins() {
        let mock = test::Mock::new();
        let record = mock.add(test::handlers::record_request());
        let client = Client::default()
            .with_mock(&mock)
            .with_setting("param_value", "client");

        client
            .query("SELECT {value: String}")
            .param("value", "query-param")
            .with_setting("param_value", "query-setting")
            .execute()
            .await
            .unwrap();

        let request = record.request().await;
        assert_eq!(
            parse_multipart_fields(&request),
            vec![
                ("query".to_owned(), b"SELECT {value: String}".to_vec()),
                ("param_value".to_owned(), b"query-setting".to_vec()),
            ]
        );
    }
}
