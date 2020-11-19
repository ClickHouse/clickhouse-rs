use std::collections::HashMap;

use hyper::{self, client::HttpConnector};

use self::error::Result;
pub use self::introspection::Reflection;

mod buflist;
pub mod error;
pub mod insert;
pub mod inserter;
mod introspection;
pub mod query;
mod response;
mod rowbinary;
mod sql_builder;
pub mod watch;

mod sealed {
    pub trait Sealed {}
}

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
        Self {
            client: hyper::Client::new(),
            url: String::new(),
            database: None,
            user: None,
            password: None,
            compression: Compression::None,
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
        self.compression = compression;
        self
    }

    pub fn with_option(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.options.insert(name.into(), value.into());
        self
    }

    pub fn insert<T: Reflection>(&self, table: &str) -> Result<insert::Insert<T>> {
        insert::Insert::new(self, table)
    }

    pub fn inserter<T: Reflection>(&self, table: &str) -> Result<inserter::Inserter<T>> {
        inserter::Inserter::new(self, table)
    }

    pub fn query(&self, query: &str) -> query::Query {
        query::Query::new(self, query)
    }

    pub fn watch(&self, query: &str) -> watch::Watch {
        watch::Watch::new(self, query)
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
#[non_exhaustive]
pub enum Compression {
    None,
    Gzip,
    Zlib,
    Brotli,
}
