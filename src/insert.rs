use std::{future::Future, marker::PhantomData, mem, panic};

use bytes::{Bytes, BytesMut};
use hyper::{self, body, Body, Request};
use serde::Serialize;
use tokio::task::JoinHandle;
use url::Url;

use crate::{
    error::{Error, Result},
    response::Response,
    row::{self, Row},
    rowbinary, Client, Compression,
};

const BUFFER_SIZE: usize = 128 * 1024;
const MIN_CHUNK_SIZE: usize = BUFFER_SIZE - 1024;

#[must_use]
pub struct Insert<T> {
    buffer: BytesMut,
    sender: Option<body::Sender>,
    #[cfg(feature = "lz4")]
    compression: Compression,
    handle: JoinHandle<Result<()>>,
    _marker: PhantomData<fn() -> T>, // TODO: test contravariance.
}

impl<T> Insert<T> {
    pub(crate) fn new(client: &Client, table: &str) -> Result<Self>
    where
        T: Row,
    {
        let mut url = Url::parse(&client.url).expect("TODO");
        let mut pairs = url.query_pairs_mut();
        pairs.clear();

        if let Some(database) = &client.database {
            pairs.append_pair("database", database);
        }

        let fields = row::join_column_names::<T>()
            .expect("the row type must be a struct or a wrapper around it");

        // TODO: what about escaping a table name?
        // https://clickhouse.yandex/docs/en/query_language/syntax/#syntax-identifiers
        let query = format!("INSERT INTO {}({}) FORMAT RowBinary", table, fields);
        pairs.append_pair("query", &query);

        if client.compression.is_lz4() {
            pairs.append_pair("decompress", "1");
        }

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

        let future = client.client._request(request);
        let handle =
            tokio::spawn(async move { Response::new(future, Compression::None).finish().await });

        Ok(Self {
            buffer: BytesMut::with_capacity(BUFFER_SIZE),
            sender: Some(sender),
            #[cfg(feature = "lz4")]
            compression: client.compression,
            handle,
            _marker: PhantomData,
        })
    }

    pub fn write<'a>(&'a mut self, row: &T) -> impl Future<Output = Result<()>> + 'a + Send
    where
        T: Serialize,
    {
        assert!(self.sender.is_some(), "write() after error");

        let result = rowbinary::serialize_into(&mut self.buffer, row);
        if result.is_err() {
            self.abort();
        }

        async move {
            result?;
            self.send_chunk_if_exceeds(MIN_CHUNK_SIZE).await?;
            Ok(())
        }
    }

    pub async fn end(mut self) -> Result<()> {
        self.send_chunk_if_exceeds(1).await?;
        self.wait_handle().await
    }

    async fn send_chunk_if_exceeds(&mut self, threshold: usize) -> Result<()> {
        if self.buffer.len() >= threshold {
            // Hyper uses non-trivial and inefficient (see benches) schema of buffering chunks.
            // It's difficult to determine when allocations occur.
            // So, instead we control it manually here and rely on the system allocator.
            let chunk = self.take_and_prepare_chunk()?;

            if let Some(sender) = &mut self.sender {
                if sender.send_data(chunk).await.is_err() {
                    self.abort();
                    self.wait_handle().await?; // real error should be here.
                    return Err(Error::Network("channel closed".into()));
                }
            }
        }

        Ok(())
    }

    async fn wait_handle(&mut self) -> Result<()> {
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

    #[cfg(feature = "lz4")]
    fn take_and_prepare_chunk(&mut self) -> Result<Bytes> {
        Ok(if self.compression.is_lz4() {
            let compressed = crate::compression::lz4::compress(&self.buffer, self.compression)?;
            self.buffer.clear();
            compressed
        } else {
            mem::replace(&mut self.buffer, BytesMut::with_capacity(BUFFER_SIZE)).freeze()
        })
    }

    #[cfg(not(feature = "lz4"))]
    fn take_and_prepare_chunk(&mut self) -> Result<Bytes> {
        Ok(mem::replace(&mut self.buffer, BytesMut::with_capacity(BUFFER_SIZE)).freeze())
    }

    fn abort(&mut self) {
        if let Some(sender) = self.sender.take() {
            sender.abort();
        }
    }
}

impl<T> Drop for Insert<T> {
    fn drop(&mut self) {
        self.abort();
    }
}
