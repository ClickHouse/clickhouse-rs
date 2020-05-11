use std::{marker::PhantomData, panic};

use bytes::BytesMut;
use hyper::{self, body, client::HttpConnector, Body, Request, Response};
use serde::Serialize;
use tokio::task::JoinHandle;
use url::Url;

use self::error::{Error, Result};
pub use self::introspection::Reflection;

pub mod error;
mod introspection;
mod rowbinary;

#[derive(Clone)]
pub struct Client {
    client: hyper::Client<HttpConnector>,

    url: String,
    database: Option<String>,
    user: Option<String>,
    password: Option<String>,
}

impl Default for Client {
    fn default() -> Self {
        Self {
            client: hyper::Client::new(),
            url: String::new(),
            database: None,
            user: None,
            password: None,
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

    pub fn insert<T: Reflection>(&self, table: &str) -> Result<Insert<T>> {
        let mut url = Url::parse(&self.url).expect("TODO");
        let mut pairs = url.query_pairs_mut();
        pairs.clear();

        if let Some(database) = &self.database {
            pairs.append_pair("database", database);
        }

        // TODO: cache field names.
        let fields = introspection::collect_field_names::<T>().join(",");

        // TODO: what about escaping a table name?
        // https://clickhouse.yandex/docs/en/query_language/syntax/#syntax-identifiers
        let query = format!("INSERT INTO {}({}) FORMAT RowBinary", table, fields);
        pairs.append_pair("query", &query);
        drop(pairs);

        let mut builder = Request::post(url.as_str());

        if let Some(user) = &self.user {
            builder = builder.header("X-ClickHouse-User", user);
        }

        if let Some(password) = &self.password {
            builder = builder.header("X-ClickHouse-Key", password);
        }

        let (sender, body) = Body::channel();

        let request = builder
            .body(body)
            .map_err(|err| Error::InvalidParams(Box::new(err)))?;

        let sending = self.client.request(request);
        let handle = tokio::spawn(sending);

        Ok(Insert {
            buffer: BytesMut::with_capacity(128 * 1024), // TODO
            sender,
            handle,
            _marker: PhantomData,
        })
    }
}

pub struct Insert<T> {
    buffer: BytesMut,
    sender: body::Sender,
    handle: JoinHandle<hyper::Result<Response<Body>>>,
    _marker: PhantomData<T>,
}

impl<T> Insert<T>
where
    T: Serialize,
{
    pub async fn write(&mut self, row: &T) -> Result<()> {
        rowbinary::serialize_into(&mut self.buffer, row)?;
        let chunk = self.buffer.split().freeze();
        self.sender.send_data(chunk).await?;
        Ok(())
    }

    pub async fn end(mut self) -> Result<()> {
        drop(self.sender);

        let response = match (&mut self.handle).await {
            Ok(res) => res?,
            Err(err) if err.is_panic() => panic::resume_unwind(err.into_panic()),
            Err(err) => {
                // TODO
                return Err(Error::Custom(format!("unexpected error: {}", err)));
            }
        };

        if !response.status().is_success() {
            let bytes = body::to_bytes(response.into_body()).await?;
            let reason = String::from_utf8_lossy(&bytes).into_owned();

            return Err(Error::BadResponse(reason));
        }

        Ok(())
    }
}
