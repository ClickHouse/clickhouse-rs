use std::{future::Future, marker::PhantomData, mem, panic, pin::Pin, time::Duration};

use bytes::{Bytes, BytesMut};
use hyper::{self, body, Body, Request};
use serde::Serialize;
use tokio::{
    task::JoinHandle,
    time::{Instant, Sleep},
};
use url::Url;

use crate::{
    error::{Error, Result},
    response::Response,
    row::{self, Row},
    rowbinary, Client, Compression,
};

const BUFFER_SIZE: usize = 128 * 1024;
const MIN_CHUNK_SIZE: usize = BUFFER_SIZE - 1024; // slightly less to avoid extra reallocations

/// Performs only one `INSERT`.
///
/// Rows are being sent progressively to spread network load.
#[must_use]
pub struct Insert<T> {
    buffer: BytesMut,
    #[cfg(feature = "wa-37420")]
    chunk_count: usize,
    sender: Option<body::Sender>,
    #[cfg(feature = "lz4")]
    compression: Compression,
    send_timeout: Option<Duration>,
    end_timeout: Option<Duration>,
    // Use boxed `Sleep` to reuse a timer entry, it improves performance.
    // Also, `tokio::time::timeout()` significantly increases a future's size.
    sleep: Pin<Box<Sleep>>,
    handle: JoinHandle<Result<()>>,
    _marker: PhantomData<fn() -> T>, // TODO: test contravariance.
}

// It should be a regular function, but it decreases performance.
macro_rules! timeout {
    ($self:expr, $timeout:ident, $fut:expr) => {{
        if let Some(timeout) = $self.$timeout {
            $self.sleep.as_mut().reset(Instant::now() + timeout);
        }

        tokio::select! {
            res = $fut => Some(res),
            _ = &mut $self.sleep, if $self.$timeout.is_some() => None,
        }
    }};
}

impl<T> Insert<T> {
    pub(crate) fn new(
        client: &Client,
        table: &str,
        async_insert_options: AsyncInsertOptions,
    ) -> Result<Self>
    where
        T: Row,
    {
        let mut url = Url::parse(&client.url).map_err(|err| Error::InvalidParams(err.into()))?;
        let mut pairs = url.query_pairs_mut();
        pairs.clear();

        if let Some(database) = &client.database {
            pairs.append_pair("database", database);
        }

        let fields = row::join_column_names::<T>()
            .expect("the row type must be a struct or a wrapper around it");

        let settings_clause = async_insert_options.into_query();

        // TODO: what about escaping a table name?
        // https://clickhouse.yandex/docs/en/query_language/syntax/#syntax-identifiers
        let query = format!("INSERT INTO {table} ({fields}) FORMAT RowBinary {settings_clause}");
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
            #[cfg(feature = "wa-37420")]
            chunk_count: 0,
            sender: Some(sender),
            #[cfg(feature = "lz4")]
            compression: client.compression,
            send_timeout: None,
            end_timeout: None,
            sleep: Box::pin(tokio::time::sleep(Duration::new(0, 0))),
            handle,
            _marker: PhantomData,
        })
    }

    /// Sets timeouts for different operations.
    ///
    /// `send_timeout` restricts time on sending a data chunk to a socket.
    /// `None` disables the timeout, it's a default.
    /// It's roughly equivalent to `tokio::time::timeout(insert.write(..))`.
    ///
    /// `end_timeout` restricts time on waiting for a response from the CH server.
    /// Thus, it includes all work needed to handle `INSERT` by the CH server,
    /// e.g. handling all materialized views and so on.
    /// `None` disables the timeout, it's a default.
    /// It's roughly equivalent to `tokio::time::timeout(insert.end(..))`.
    ///
    /// These timeouts are much more performant (~x10) than wrapping `write()` and `end()` calls
    /// into `tokio::time::timeout()`.
    pub fn with_timeouts(
        mut self,
        send_timeout: Option<Duration>,
        end_timeout: Option<Duration>,
    ) -> Self {
        self.set_timeouts(send_timeout, end_timeout);
        self
    }

    pub(crate) fn set_timeouts(
        &mut self,
        send_timeout: Option<Duration>,
        end_timeout: Option<Duration>,
    ) {
        self.send_timeout = send_timeout;
        self.end_timeout = end_timeout;
    }

    /// Serializes and writes to the socket a provided row.
    ///
    /// # Panics
    /// If called after previous call returned an error.
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
            if self.buffer.len() >= MIN_CHUNK_SIZE {
                self.send_chunk().await?;
            }
            Ok(())
        }
    }

    /// Ends `INSERT`.
    /// Succeeds if the server returns 200.
    ///
    /// If it isn't called, the whole `INSERT` is aborted.
    pub async fn end(mut self) -> Result<()> {
        if !self.buffer.is_empty() {
            self.send_chunk().await?;
        }

        self.sender = None; // terminate the sender successfully
        self.wait_handle().await
    }

    async fn send_chunk(&mut self) -> Result<()> {
        if self.sender.is_none() {
            return Ok(());
        }

        // A temporary workaround for https://github.com/ClickHouse/ClickHouse/issues/37420.
        #[cfg(feature = "wa-37420")]
        self.prepend_bom();

        // Hyper uses non-trivial and inefficient schema of buffering chunks.
        // It's difficult to determine when allocations occur.
        // So, instead we control it manually here and rely on the system allocator.
        let chunk = self.take_and_prepare_chunk()?;

        let sender = self.sender.as_mut().unwrap(); // checked above

        let is_timed_out = match timeout!(self, send_timeout, sender.send_data(chunk)) {
            Some(Ok(())) => return Ok(()),
            Some(Err(_)) => false, // an actual error will be returned from `wait_handle`
            None => true,
        };

        // Error handling.

        self.abort();

        // TODO: is it required to wait the handle in the case of timeout?
        let res = self.wait_handle().await;

        if is_timed_out {
            Err(Error::TimedOut)
        } else {
            res?; // a real error should be here.
            Err(Error::Network("channel closed".into()))
        }
    }

    async fn wait_handle(&mut self) -> Result<()> {
        match timeout!(self, end_timeout, &mut self.handle) {
            Some(Ok(res)) => res,
            Some(Err(err)) if err.is_panic() => panic::resume_unwind(err.into_panic()),
            Some(Err(err)) => Err(Error::Custom(format!("unexpected error: {err}"))),
            None => {
                // We can do nothing useful here, so just shut down the background task.
                self.handle.abort();
                Err(Error::TimedOut)
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

    #[cfg(feature = "wa-37420")]
    fn prepend_bom(&mut self) {
        if self.chunk_count == 0 && self.buffer.starts_with(&[0xef, 0xbb, 0xbf]) {
            let mut new_chunk = BytesMut::with_capacity(self.buffer.len() + 3);
            new_chunk.extend_from_slice(&[0xef, 0xbb, 0xbf]);
            new_chunk.extend_from_slice(&self.buffer);
            self.buffer = new_chunk;
        }

        self.chunk_count += 1;
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

#[derive(Debug, Default)]
pub struct AsyncInsertOptions {
    async_insert: OptionValue<bool>,
    async_insert_threads: OptionValue<usize>,
    wait_for_async_insert: OptionValue<bool>,
    wait_for_async_insert_timeout: OptionValue<usize>,
    async_insert_max_data_size: OptionValue<usize>,
    async_insert_max_query_number: OptionValue<usize>,
    async_insert_busy_timeout_ms: OptionValue<usize>,
    async_insert_stale_timeout_ms: OptionValue<usize>,
    async_insert_deduplicate: OptionValue<bool>,
}

impl AsyncInsertOptions {
    fn into_query(self) -> String {
        let mut options = vec![];

        if let OptionValue::Specified(async_insert) = self.async_insert {
            let value = if async_insert { 1 } else { 0 };
            options.push(format!("async_insert={value}"));
        }

        if let OptionValue::Specified(async_insert_threads) = self.async_insert_threads {
            options.push(format!("async_insert_threads={async_insert_threads}"));
        }

        if let OptionValue::Specified(wait_for_async_insert) = self.wait_for_async_insert {
            let value = if wait_for_async_insert { 1 } else { 0 };
            options.push(format!("wait_for_async_insert={value}"));
        }

        if let OptionValue::Specified(wait_for_async_insert_timeout) =
            self.wait_for_async_insert_timeout
        {
            options.push(format!(
                "wait_for_async_insert_timeout={wait_for_async_insert_timeout}"
            ));
        }

        if let OptionValue::Specified(async_insert_max_data_size) = self.async_insert_max_data_size
        {
            options.push(format!(
                "async_insert_max_data_size={async_insert_max_data_size}"
            ));
        }

        if let OptionValue::Specified(async_insert_max_query_number) =
            self.async_insert_max_query_number
        {
            options.push(format!(
                "async_insert_max_query_number={async_insert_max_query_number}"
            ));
        }

        if let OptionValue::Specified(async_insert_busy_timeout_ms) =
            self.async_insert_busy_timeout_ms
        {
            options.push(format!(
                "async_insert_busy_timeout_ms={async_insert_busy_timeout_ms}"
            ));
        }

        if let OptionValue::Specified(async_insert_stale_timeout_ms) =
            self.async_insert_stale_timeout_ms
        {
            options.push(format!(
                "async_insert_stale_timeout_ms={async_insert_stale_timeout_ms}"
            ));
        }

        if let OptionValue::Specified(async_insert_deduplicate) = self.async_insert_deduplicate {
            let value = if async_insert_deduplicate { 1 } else { 0 };
            options.push(format!("async_insert_deduplicate={value}"));
        }

        if options.is_empty() {
            return "".to_string();
        }

        format!("SETTINGS {}", options.join(", "))
    }
}

#[derive(Debug)]
enum OptionValue<T> {
    Unspecified,
    Specified(T),
}

impl<T> Default for OptionValue<T> {
    fn default() -> Self {
        Self::Unspecified
    }
}

#[derive(Debug, Default)]
pub struct AsyncInsertOptionsBuilder {
    async_insert: OptionValue<bool>,
    async_insert_threads: OptionValue<usize>,
    wait_for_async_insert: OptionValue<bool>,
    wait_for_async_insert_timeout: OptionValue<usize>,
    async_insert_max_data_size: OptionValue<usize>,
    async_insert_max_query_number: OptionValue<usize>,
    async_insert_busy_timeout_ms: OptionValue<usize>,
    async_insert_stale_timeout_ms: OptionValue<usize>,
    async_insert_deduplicate: OptionValue<bool>,
}

impl AsyncInsertOptionsBuilder {
    pub fn build(self) -> AsyncInsertOptions {
        AsyncInsertOptions {
            async_insert: self.async_insert,
            async_insert_threads: self.async_insert_threads,
            wait_for_async_insert: self.wait_for_async_insert,
            wait_for_async_insert_timeout: self.wait_for_async_insert_timeout,
            async_insert_max_data_size: self.async_insert_max_data_size,
            async_insert_max_query_number: self.async_insert_max_query_number,
            async_insert_busy_timeout_ms: self.async_insert_busy_timeout_ms,
            async_insert_stale_timeout_ms: self.async_insert_stale_timeout_ms,
            async_insert_deduplicate: self.async_insert_deduplicate,
        }
    }

    pub fn async_insert(mut self, async_insert: bool) -> Self {
        self.async_insert = OptionValue::Specified(async_insert);
        self
    }

    pub fn async_insert_threads(mut self, async_insert_threads: usize) -> Self {
        self.async_insert_threads = OptionValue::Specified(async_insert_threads);
        self
    }

    pub fn wait_for_async_insert(mut self, wait_for_async_insert: bool) -> Self {
        self.wait_for_async_insert = OptionValue::Specified(wait_for_async_insert);
        self
    }

    pub fn wait_for_async_insert_timeout(mut self, wait_for_async_insert_timeout: usize) -> Self {
        self.wait_for_async_insert_timeout = OptionValue::Specified(wait_for_async_insert_timeout);
        self
    }

    pub fn async_insert_max_data_size(mut self, async_insert_max_data_size: usize) -> Self {
        self.async_insert_max_data_size = OptionValue::Specified(async_insert_max_data_size);
        self
    }

    pub fn async_insert_max_query_number(mut self, async_insert_max_query_number: usize) -> Self {
        self.async_insert_max_query_number = OptionValue::Specified(async_insert_max_query_number);
        self
    }

    pub fn async_insert_busy_timeout_ms(mut self, async_insert_busy_timeout_ms: usize) -> Self {
        self.async_insert_busy_timeout_ms = OptionValue::Specified(async_insert_busy_timeout_ms);
        self
    }

    pub fn async_insert_stale_timeout_ms(mut self, async_insert_stale_timeout_ms: usize) -> Self {
        self.async_insert_stale_timeout_ms = OptionValue::Specified(async_insert_stale_timeout_ms);
        self
    }

    pub fn async_insert_deduplicate(mut self, async_insert_deduplicate: bool) -> Self {
        self.async_insert_deduplicate = OptionValue::Specified(async_insert_deduplicate);
        self
    }
}

impl AsyncInsertOptions {
    pub fn builder() -> AsyncInsertOptionsBuilder {
        AsyncInsertOptionsBuilder::default()
    }
}
