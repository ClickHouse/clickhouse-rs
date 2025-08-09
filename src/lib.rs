#![doc = include_str!("../README.md")]
#![cfg_attr(docsrs, feature(doc_cfg))]
#![cfg_attr(docsrs, feature(doc_auto_cfg))]

#[macro_use]
extern crate static_assertions;

pub use self::{
    compression::Compression,
    row::{Row, RowOwned, RowRead, RowWrite},
};
use self::{error::Result, http_client::HttpClient};
pub use clickhouse_derive::Row;
use std::{collections::HashMap, fmt::Display, sync::Arc};

pub mod error;
pub mod insert;
#[cfg(feature = "inserter")]
pub mod inserter;
pub mod query;
pub mod serde;
pub mod sql;
#[cfg(feature = "test-util")]
pub mod test;

mod bytes_ext;
mod compression;
mod cursors;
mod headers;
mod http_client;
mod request_body;
mod response;
mod row;
mod row_metadata;
mod rowbinary;
#[cfg(feature = "inserter")]
mod ticks;

/// A client containing HTTP pool.
///
/// ### Cloning behavior
/// Clones share the same HTTP transport but store their own configurations.
/// Any `with_*` configuration method (e.g., [`Client::with_option`]) applies
/// only to future clones, because [`Client::clone`] creates a deep copy
/// of the [`Client`] configuration, except the transport.
#[derive(Clone)]
pub struct Client {
    http: Arc<dyn HttpClient>,

    url: String,
    database: Option<String>,
    authentication: Authentication,
    compression: Compression,
    options: HashMap<String, String>,
    headers: HashMap<String, String>,
    products_info: Vec<ProductInfo>,
    validation: bool,

    #[cfg(feature = "test-util")]
    mocked: bool,
}

#[derive(Clone)]
struct ProductInfo {
    name: String,
    version: String,
}

impl Display for ProductInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}", self.name, self.version)
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum Authentication {
    Credentials {
        user: Option<String>,
        password: Option<String>,
    },
    Jwt {
        access_token: String,
    },
}

impl Default for Authentication {
    fn default() -> Self {
        Self::Credentials {
            user: None,
            password: None,
        }
    }
}

impl Default for Client {
    fn default() -> Self {
        Self::with_http_client(http_client::default())
    }
}

impl Client {
    /// Creates a new client with a specified underlying HTTP client.
    ///
    /// See `HttpClient` for details.
    pub fn with_http_client(client: impl HttpClient) -> Self {
        Self {
            http: Arc::new(client),
            url: String::new(),
            database: None,
            authentication: Authentication::default(),
            compression: Compression::default(),
            options: HashMap::new(),
            headers: HashMap::new(),
            products_info: Vec::default(),
            validation: true,
            #[cfg(feature = "test-util")]
            mocked: false,
        }
    }

    /// Specifies ClickHouse's url. Should point to HTTP endpoint.
    ///
    /// # Examples
    /// ```
    /// # use clickhouse::Client;
    /// let client = Client::default().with_url("http://localhost:8123");
    /// ```
    pub fn with_url(mut self, url: impl Into<String>) -> Self {
        self.url = url.into();
        self
    }

    /// Specifies a database name.
    ///
    /// # Examples
    /// ```
    /// # use clickhouse::Client;
    /// let client = Client::default().with_database("test");
    /// ```
    pub fn with_database(mut self, database: impl Into<String>) -> Self {
        self.database = Some(database.into());
        self
    }

    /// Specifies a user.
    ///
    /// # Panics
    /// If called after [`Client::with_access_token`].
    ///
    /// # Examples
    /// ```
    /// # use clickhouse::Client;
    /// let client = Client::default().with_user("test");
    /// ```
    pub fn with_user(mut self, user: impl Into<String>) -> Self {
        match self.authentication {
            Authentication::Jwt { .. } => {
                panic!("`user` cannot be set together with `access_token`");
            }
            Authentication::Credentials { password, .. } => {
                self.authentication = Authentication::Credentials {
                    user: Some(user.into()),
                    password,
                };
            }
        }
        self
    }

    /// Specifies a password.
    ///
    /// # Panics
    /// If called after [`Client::with_access_token`].
    ///
    /// # Examples
    /// ```
    /// # use clickhouse::Client;
    /// let client = Client::default().with_password("secret");
    /// ```
    pub fn with_password(mut self, password: impl Into<String>) -> Self {
        match self.authentication {
            Authentication::Jwt { .. } => {
                panic!("`password` cannot be set together with `access_token`");
            }
            Authentication::Credentials { user, .. } => {
                self.authentication = Authentication::Credentials {
                    user,
                    password: Some(password.into()),
                };
            }
        }
        self
    }

    /// A JWT access token to authenticate with ClickHouse.
    /// JWT token authentication is supported in ClickHouse Cloud only.
    /// Should not be called after [`Client::with_user`] or
    /// [`Client::with_password`].
    ///
    /// # Panics
    /// If called after [`Client::with_user`] or [`Client::with_password`].
    ///
    /// # Examples
    /// ```
    /// # use clickhouse::Client;
    /// let client = Client::default().with_access_token("jwt");
    /// ```
    pub fn with_access_token(mut self, access_token: impl Into<String>) -> Self {
        match self.authentication {
            Authentication::Credentials { user, password }
                if user.is_some() || password.is_some() =>
            {
                panic!("`access_token` cannot be set together with `user` or `password`");
            }
            _ => {
                self.authentication = Authentication::Jwt {
                    access_token: access_token.into(),
                }
            }
        }
        self
    }

    /// Specifies a compression mode. See [`Compression`] for details.
    /// By default, `Lz4` is used.
    ///
    /// # Examples
    /// ```
    /// # use clickhouse::{Client, Compression};
    /// # #[cfg(feature = "lz4")]
    /// let client = Client::default().with_compression(Compression::Lz4);
    /// ```
    pub fn with_compression(mut self, compression: Compression) -> Self {
        self.compression = compression;
        self
    }

    /// Used to specify options that will be passed to all queries.
    ///
    /// # Example
    /// ```
    /// # use clickhouse::Client;
    /// Client::default().with_option("allow_nondeterministic_mutations", "1");
    /// ```
    pub fn with_option(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.options.insert(name.into(), value.into());
        self
    }

    /// Used to specify a header that will be passed to all queries.
    ///
    /// # Example
    /// ```
    /// # use clickhouse::Client;
    /// Client::default().with_header("Cookie", "A=1");
    /// ```
    pub fn with_header(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.headers.insert(name.into(), value.into());
        self
    }

    /// Specifies the product name and version that will be included
    /// in the default User-Agent header. Multiple products are supported.
    /// This could be useful for the applications built on top of this client.
    ///
    /// # Examples
    ///
    /// Sample default User-Agent header:
    ///
    /// ```plaintext
    /// clickhouse-rs/0.12.2 (lv:rust/1.67.0, os:macos)
    /// ```
    ///
    /// Sample User-Agent with a single product information:
    ///
    /// ```
    /// # use clickhouse::Client;
    /// let client = Client::default().with_product_info("MyDataSource", "v1.0.0");
    /// ```
    ///
    /// ```plaintext
    /// MyDataSource/v1.0.0 clickhouse-rs/0.12.2 (lv:rust/1.67.0, os:macos)
    /// ```
    ///
    /// Sample User-Agent with multiple products information
    /// (NB: the products are added in the reverse order of
    /// [`Client::with_product_info`] calls, which could be useful to add
    /// higher abstraction layers first):
    ///
    /// ```
    /// # use clickhouse::Client;
    /// let client = Client::default()
    ///     .with_product_info("MyDataSource", "v1.0.0")
    ///     .with_product_info("MyApp", "0.0.1");
    /// ```
    ///
    /// ```plaintext
    /// MyApp/0.0.1 MyDataSource/v1.0.0 clickhouse-rs/0.12.2 (lv:rust/1.67.0, os:macos)
    /// ```
    pub fn with_product_info(
        mut self,
        product_name: impl Into<String>,
        product_version: impl Into<String>,
    ) -> Self {
        self.products_info.push(ProductInfo {
            name: product_name.into(),
            version: product_version.into(),
        });
        self
    }

    /// Starts a new INSERT statement.
    ///
    /// # Panics
    /// If `T` has unnamed fields, e.g. tuples.
    pub fn insert<T: Row>(&self, table: &str) -> Result<insert::Insert<T>> {
        insert::Insert::new(self, table)
    }

    /// Creates an inserter to perform multiple INSERTs.
    #[cfg(feature = "inserter")]
    pub fn inserter<T: Row>(&self, table: &str) -> Result<inserter::Inserter<T>> {
        inserter::Inserter::new(self, table)
    }

    /// Starts a new SELECT/DDL query.
    pub fn query(&self, query: &str) -> query::Query {
        query::Query::new(self, query)
    }

    /// Enables or disables [`Row`] data types validation against the database schema
    /// at the cost of performance. Validation is enabled by default, and in this mode,
    /// the client will use `RowBinaryWithNamesAndTypes` format.
    ///
    /// If you are looking to maximize performance, you could disable validation using this method.
    /// When validation is disabled, the client switches to `RowBinary` format usage instead.
    ///
    /// The downside with plain `RowBinary` is that instead of clearer error messages,
    /// a mismatch between [`Row`] and database schema will result
    /// in a [`error::Error::NotEnoughData`] error without specific details.
    ///
    /// However, depending on the dataset, there might be x1.1 to x3 performance improvement,
    /// but that highly depends on the shape and volume of the dataset.
    ///
    /// It is always recommended to measure the performance impact of validation
    /// in your specific use case. Additionally, writing smoke tests to ensure that
    /// the row types match the ClickHouse schema is highly recommended,
    /// if you plan to disable validation in your application.
    pub fn with_validation(mut self, enabled: bool) -> Self {
        self.validation = enabled;
        self
    }

    /// Used internally to check if the validation mode is enabled,
    /// as it takes into account the `test-util` feature flag.
    #[inline]
    pub(crate) fn get_validation(&self) -> bool {
        #[cfg(feature = "test-util")]
        if self.mocked {
            return false;
        }
        self.validation
    }

    /// Used internally to modify the options map of an _already cloned_
    /// [`Client`] instance.
    pub(crate) fn add_option(&mut self, name: impl Into<String>, value: impl Into<String>) {
        self.options.insert(name.into(), value.into());
    }

    /// Use a mock server for testing purposes.
    ///
    /// # Note
    ///
    /// The client will always use `RowBinary` format instead of `RowBinaryWithNamesAndTypes`,
    /// as otherwise it'd be required to provide RBWNAT header in the mocks,
    /// which is pointless in that kind of tests.
    #[cfg(feature = "test-util")]
    pub fn with_mock(mut self, mock: &test::Mock) -> Self {
        self.url = mock.url().to_string();
        self.mocked = true;
        self
    }
}

/// This is a private API exported only for internal purposes.
/// Do not use it in your code directly, it doesn't follow semver.
#[doc(hidden)]
pub mod _priv {
    pub use crate::row::RowKind;

    #[cfg(feature = "lz4")]
    pub fn lz4_compress(uncompressed: &[u8]) -> super::Result<bytes::Bytes> {
        crate::compression::lz4::compress(uncompressed)
    }
}

#[cfg(test)]
mod client_tests {
    use crate::{Authentication, Client};

    #[test]
    fn it_can_use_credentials_auth() {
        assert_eq!(
            Client::default()
                .with_user("bob")
                .with_password("secret")
                .authentication,
            Authentication::Credentials {
                user: Some("bob".into()),
                password: Some("secret".into()),
            }
        );
    }

    #[test]
    fn it_can_use_credentials_auth_user_only() {
        assert_eq!(
            Client::default().with_user("alice").authentication,
            Authentication::Credentials {
                user: Some("alice".into()),
                password: None,
            }
        );
    }

    #[test]
    fn it_can_use_credentials_auth_password_only() {
        assert_eq!(
            Client::default().with_password("secret").authentication,
            Authentication::Credentials {
                user: None,
                password: Some("secret".into()),
            }
        );
    }

    #[test]
    fn it_can_override_credentials_auth() {
        assert_eq!(
            Client::default()
                .with_user("bob")
                .with_password("secret")
                .with_user("alice")
                .with_password("something_else")
                .authentication,
            Authentication::Credentials {
                user: Some("alice".into()),
                password: Some("something_else".into()),
            }
        );
    }

    #[test]
    fn it_can_use_jwt_auth() {
        assert_eq!(
            Client::default().with_access_token("my_jwt").authentication,
            Authentication::Jwt {
                access_token: "my_jwt".into(),
            }
        );
    }

    #[test]
    fn it_can_override_jwt_auth() {
        assert_eq!(
            Client::default()
                .with_access_token("my_jwt")
                .with_access_token("my_jwt_2")
                .authentication,
            Authentication::Jwt {
                access_token: "my_jwt_2".into(),
            }
        );
    }

    #[test]
    #[should_panic(expected = "`access_token` cannot be set together with `user` or `password`")]
    fn it_cannot_use_jwt_after_with_user() {
        let _ = Client::default()
            .with_user("bob")
            .with_access_token("my_jwt");
    }

    #[test]
    #[should_panic(expected = "`access_token` cannot be set together with `user` or `password`")]
    fn it_cannot_use_jwt_after_with_password() {
        let _ = Client::default()
            .with_password("secret")
            .with_access_token("my_jwt");
    }

    #[test]
    #[should_panic(expected = "`access_token` cannot be set together with `user` or `password`")]
    fn it_cannot_use_jwt_after_both_with_user_and_with_password() {
        let _ = Client::default()
            .with_user("alice")
            .with_password("secret")
            .with_access_token("my_jwt");
    }

    #[test]
    #[should_panic(expected = "`user` cannot be set together with `access_token`")]
    fn it_cannot_use_with_user_after_jwt() {
        let _ = Client::default()
            .with_access_token("my_jwt")
            .with_user("alice");
    }

    #[test]
    #[should_panic(expected = "`password` cannot be set together with `access_token`")]
    fn it_cannot_use_with_password_after_jwt() {
        let _ = Client::default()
            .with_access_token("my_jwt")
            .with_password("secret");
    }

    #[test]
    fn it_sets_validation_mode() {
        let client = Client::default();
        assert!(client.validation);
        let client = client.with_validation(false);
        assert!(!client.validation);
        let client = client.with_validation(true);
        assert!(client.validation);
    }

	#[test]
    fn it_does_follow_previous_configuration() {
        let client = Client::default()
            .with_option("async_insert", "1");
        assert_eq!(
            client.options,
            client.clone().options,
        );
    }

    #[test]
    fn it_does_not_follow_future_configuration() {
        let client = Client::default();
        let client_clone = client.clone();
        let client = client.with_option("async_insert", "1");
        assert_ne!(
            client.options,
            client_clone.options,
        );
    }
}
