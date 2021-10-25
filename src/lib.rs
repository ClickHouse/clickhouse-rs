#![cfg_attr(docsrs, feature(doc_cfg))]

#[macro_use]
extern crate static_assertions;

use std::{collections::HashMap, time::Duration};

use hyper::client::connect::HttpConnector;

pub use clickhouse_derive::Row;

use self::error::Result;
pub use self::{compression::Compression, row::Row};

pub mod error;
pub mod insert;
pub mod inserter;
pub mod query;
pub mod sql;
#[cfg(feature = "test-util")]
#[cfg_attr(docsrs, doc(cfg(feature = "test-util")))]
pub mod test;
#[cfg(feature = "watch")]
#[cfg_attr(docsrs, doc(cfg(feature = "watch")))]
pub mod watch;

mod buflist;
mod compression;
mod cursor;
mod response;
mod row;
mod rowbinary;

mod sealed {
    pub trait Sealed {}
}

const TCP_KEEPALIVE: Duration = Duration::from_secs(60);

#[derive(Clone)]
pub struct Client {
    client: hyper::Client<HttpConnector>,

    url: String,
    database: Option<String>,
    user: Option<String>,
    password: Option<String>,
    compression: Compression,
    options: HashMap<String, String>,
}

impl Default for Client {
    fn default() -> Self {
        let mut connector = HttpConnector::new();

        // TODO: make configurable in `Client::builder()`.
        connector.set_keepalive(Some(TCP_KEEPALIVE));

        Self {
            client: hyper::Client::builder().build(connector),
            url: String::new(),
            database: None,
            user: None,
            password: None,
            compression: Compression::default(),
            options: HashMap::new(),
        }
    }
}

impl Client {
    // TODO: use `url` crate?
    pub fn with_url(mut self, url: impl Into<String>) -> Self {
        self.url = url.into();
        self
    }

    pub fn with_database(mut self, database: impl Into<String>) -> Self {
        self.database = Some(database.into());
        self
    }

    pub fn with_user(mut self, user: impl Into<String>) -> Self {
        self.user = Some(user.into());
        self
    }

    pub fn with_password(mut self, password: impl Into<String>) -> Self {
        self.password = Some(password.into());
        self
    }

    pub fn with_compression(mut self, compression: Compression) -> Self {
        // TODO: remove when compression will be implemented.
        self.compression = if cfg!(feature = "test-util") {
            Compression::None
        } else {
            compression
        };
        self
    }

    pub fn with_option(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.options.insert(name.into(), value.into());
        self
    }

    /// Starts a new INSERT statement.
    ///
    /// # Panics
    /// If `T` has unnamed fields, e.g. tuples.
    pub fn insert<T: Row>(&self, table: &str) -> Result<insert::Insert<T>> {
        insert::Insert::new(self, table)
    }

    pub fn inserter<T: Row>(&self, table: &str) -> Result<inserter::Inserter<T>> {
        inserter::Inserter::new(self, table)
    }

    pub fn query(&self, query: &str) -> query::Query {
        query::Query::new(self, query)
    }

    #[cfg(feature = "watch")]
    pub fn watch(&self, query: &str) -> watch::Watch {
        watch::Watch::new(self, query)
    }
}
