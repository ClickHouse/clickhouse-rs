#![doc = include_str!("../README.md")]
#![cfg_attr(docsrs, feature(doc_cfg))]
#![cfg_attr(docsrs, feature(doc_auto_cfg))]

#[macro_use]
extern crate static_assertions;

use std::{collections::HashMap, sync::Arc, time::Duration};

#[cfg(feature = "tls")]
use hyper_tls::HttpsConnector;
use hyper_util::{
    client::legacy::{connect::HttpConnector, Client as HyperClient},
    rt::TokioExecutor,
};

use self::{error::Result, http_client::HttpClient};

pub use self::{compression::Compression, row::Row};
pub use clickhouse_derive::Row;

pub mod error;
pub mod insert;
#[cfg(feature = "inserter")]
pub mod inserter;
pub mod query;
pub mod serde;
pub mod sql;
#[cfg(feature = "test-util")]
pub mod test;
#[cfg(feature = "watch")]
pub mod watch;

mod buflist;
mod compression;
mod cursor;
mod http_client;
mod request_body;
mod response;
mod row;
mod rowbinary;
#[cfg(feature = "inserter")]
mod ticks;

const TCP_KEEPALIVE: Duration = Duration::from_secs(60);

// ClickHouse uses 3s by default.
// See https://github.com/ClickHouse/ClickHouse/blob/368cb74b4d222dc5472a7f2177f6bb154ebae07a/programs/server/config.xml#L201
const POOL_IDLE_TIMEOUT: Duration = Duration::from_secs(2);

/// A client containing HTTP pool.
#[derive(Clone)]
pub struct Client {
    http: Arc<dyn HttpClient>,

    url: String,
    database: Option<String>,
    user: Option<String>,
    password: Option<String>,
    compression: Compression,
    options: HashMap<String, String>,
    headers: HashMap<String, String>,
}

impl Default for Client {
    fn default() -> Self {
        #[allow(unused_mut)]
        let mut connector = HttpConnector::new();

        // TODO: make configurable in `Client::builder()`.
        connector.set_keepalive(Some(TCP_KEEPALIVE));

        #[cfg(any(feature = "native-tls", feature = "rustls-tls"))]
        connector.enforce_http(false);

        #[cfg(all(feature = "native-tls", not(feature = "rustls-tls")))]
        let connector = hyper_tls::HttpsConnector::new_with_connector(connector);

        #[cfg(feature = "rustls-tls")]
        let connector = hyper_rustls::HttpsConnectorBuilder::new()
            .with_webpki_roots()
            .https_or_http()
            .enable_http1()
            .wrap_connector(connector);

        let client = HyperClient::builder(TokioExecutor::new())
            .pool_idle_timeout(POOL_IDLE_TIMEOUT)
            .build(connector);

        Self::with_http_client(client)
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
            user: None,
            password: None,
            compression: Compression::default(),
            options: HashMap::new(),
            headers: HashMap::new(),
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
    /// # Examples
    /// ```
    /// # use clickhouse::Client;
    /// let client = Client::default().with_user("test");
    /// ```
    pub fn with_user(mut self, user: impl Into<String>) -> Self {
        self.user = Some(user.into());
        self
    }

    /// Specifies a password.
    ///
    /// # Examples
    /// ```
    /// # use clickhouse::Client;
    /// let client = Client::default().with_password("secret");
    /// ```
    pub fn with_password(mut self, password: impl Into<String>) -> Self {
        self.password = Some(password.into());
        self
    }

    /// Specifies a compression mode. See [`Compression`] for details.
    /// By default, `Lz4` is used.
    ///
    /// # Examples
    /// ```
    /// # use clickhouse::{Client, Compression};
    /// # #[cfg(feature = "lz4")]
    /// let client = Client::default().with_compression(Compression::Lz4Hc(4));
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

    /// Starts a new WATCH query.
    ///
    /// The `query` can be either the table name or a SELECT query.
    /// In the second case, a new LV table is created.
    #[cfg(feature = "watch")]
    pub fn watch(&self, query: &str) -> watch::Watch {
        watch::Watch::new(self, query)
    }

    /// Used internally to modify the options map of an _already cloned_
    /// [`Client`] instance.
    pub(crate) fn add_option(&mut self, name: impl Into<String>, value: impl Into<String>) {
        self.options.insert(name.into(), value.into());
    }
}

/// This is a private API exported only for internal purposes.
/// Do not use it in your code directly, it doesn't follow semver.
#[doc(hidden)]
pub mod _priv {
    #[cfg(feature = "lz4")]
    pub fn lz4_compress(
        uncompressed: &[u8],
        mode: super::Compression,
    ) -> super::Result<bytes::Bytes> {
        crate::compression::lz4::compress(uncompressed, mode)
    }
}
