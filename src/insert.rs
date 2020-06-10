use std::{marker::PhantomData, mem, panic};

use bytes::BytesMut;
use hyper::{self, body, Body, Request};
use serde::Serialize;
use tokio::task::JoinHandle;
use url::Url;

use crate::{
    error::{Error, Result},
    introspection::{self, Reflection},
    response::Response,
    rowbinary, Client,
};

const BUFFER_SIZE: usize = 128 * 1024;
const MIN_CHUNK_SIZE: usize = BUFFER_SIZE - 1024;

pub struct Insert<T> {
    buffer: BytesMut,
    sender: Option<body::Sender>,
    handle: JoinHandle<Result<()>>,
    _marker: PhantomData<T>,
}

impl<T> Insert<T> {
    pub(crate) fn new(client: &Client, table: &str) -> Result<Self>
    where
        T: Reflection,
    {
        let mut url = Url::parse(&client.url).expect("TODO");
        let mut pairs = url.query_pairs_mut();
        pairs.clear();

        if let Some(database) = &client.database {
            pairs.append_pair("database", database);
        }

        let fields = introspection::join_field_names::<T>()
            .expect("the row type must be a struct or a wrapper around it");

        // TODO: what about escaping a table name?
        // https://clickhouse.yandex/docs/en/query_language/syntax/#syntax-identifiers
        let query = format!("INSERT INTO {}({}) FORMAT RowBinary", table, fields);
        pairs.append_pair("query", &query);
        drop(pairs);

        let mut builder = Request::post(url.as_str());

        if let Some(user) = &client.user {
            builder = builder.header("X-ClickHouse-User", user);
        }

        if let Some(password) = &client.password {
            builder = builder.header("X-ClickHouse-Key", password);
        }

        let (sender, body) = Body::channel();

        let request = builder
            .body(body)
            .map_err(|err| Error::InvalidParams(Box::new(err)))?;

        let future = client.client.request(request);
        let handle = tokio::spawn(async move {
            // TODO: should we read the body?
            let _ = Response::from(future).resolve().await?;
            Ok(())
        });

        Ok(Insert {
            buffer: BytesMut::with_capacity(BUFFER_SIZE),
            sender: Some(sender),
            handle,
            _marker: PhantomData,
        })
    }

    pub async fn write(&mut self, row: &T) -> Result<()>
    where
        T: Serialize,
    {
        rowbinary::serialize_into(&mut self.buffer, row)?;
        self.send_chunk_if_exceeds(MIN_CHUNK_SIZE).await?;
        Ok(())
    }

    pub async fn end(mut self) -> Result<()> {
        self.send_chunk_if_exceeds(1).await?;
        drop(self.sender.take());

        match (&mut self.handle).await {
            Ok(res) => res,
            Err(err) if err.is_panic() => panic::resume_unwind(err.into_panic()),
            Err(err) => {
                // TODO
                Err(Error::Custom(format!("unexpected error: {}", err)))
            }
        }
    }

    async fn send_chunk_if_exceeds(&mut self, threshold: usize) -> Result<()> {
        if self.buffer.len() >= threshold {
            // Hyper uses non-trivial and inefficient (see benches) schema of buffering chunks.
            // It's difficult to determine when allocations occur.
            // So, instead we control it manually here and rely on the system allocator.
            let chunk = mem::replace(&mut self.buffer, BytesMut::with_capacity(BUFFER_SIZE));

            if let Some(sender) = &mut self.sender {
                sender.send_data(chunk.freeze()).await?;
            }
        }

        Ok(())
    }
}

impl<T> Drop for Insert<T> {
    fn drop(&mut self) {
        if let Some(sender) = self.sender.take() {
            sender.abort();
        }
    }
}
