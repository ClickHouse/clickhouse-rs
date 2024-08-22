use std::{future::Future, marker::PhantomData, mem, panic, pin::Pin, time::Duration};

use bytes::{Bytes, BytesMut};
use hyper::{self, Request};
use replace_with::replace_with_or_abort;
use serde::Serialize;
use tokio::{
    task::JoinHandle,
    time::{Instant, Sleep},
};
use url::Url;

use crate::headers::with_request_headers;
use crate::{
    error::{Error, Result},
    request_body::{ChunkSender, RequestBody},
    response::Response,
    row::{self, Row},
    rowbinary, Client, Compression,
};

// The desired max frame size.
const BUFFER_SIZE: usize = 256 * 1024;
// Threshold to send a chunk. Should be slightly less than `BUFFER_SIZE`
// to avoid extra reallocations in case of a big last row.
const MIN_CHUNK_SIZE: usize = BUFFER_SIZE - 2048;

const_assert!(BUFFER_SIZE.is_power_of_two()); // to use the whole buffer's capacity

/// Performs one `INSERT`.
///
/// The [`Insert::end`] must be called to finalize the `INSERT`.
/// Otherwise, the whole `INSERT` will be aborted.
///
/// Rows are being sent progressively to spread network load.
#[must_use]
pub struct Insert<T> {
    state: InsertState,
    buffer: BytesMut,
    #[cfg(feature = "lz4")]
    compression: Compression,
    send_timeout: Option<Duration>,
    end_timeout: Option<Duration>,
    // Use boxed `Sleep` to reuse a timer entry, it improves performance.
    // Also, `tokio::time::timeout()` significantly increases a future's size.
    sleep: Pin<Box<Sleep>>,
    _marker: PhantomData<fn() -> T>, // TODO: test contravariance.
}

enum InsertState {
    NotStarted {
        client: Box<Client>,
        sql: String,
    },
    Active {
        sender: ChunkSender,
        handle: JoinHandle<Result<()>>,
    },
    Terminated {
        handle: JoinHandle<Result<()>>,
    },
    Completed,
}

impl InsertState {
    fn sender(&mut self) -> Option<&mut ChunkSender> {
        match self {
            InsertState::Active { sender, .. } => Some(sender),
            _ => None,
        }
    }

    fn handle(&mut self) -> Option<&mut JoinHandle<Result<()>>> {
        match self {
            InsertState::Active { handle, .. } | InsertState::Terminated { handle } => Some(handle),
            _ => None,
        }
    }

    fn client_with_sql(&self) -> Option<(&Client, &str)> {
        match self {
            InsertState::NotStarted { client, sql } => Some((client, sql)),
            _ => None,
        }
    }

    fn terminated(&mut self) {
        debug_assert!(matches!(self, InsertState::Active { .. }));
        replace_with_or_abort(self, |_self| match _self {
            InsertState::Active { handle, .. } => InsertState::Terminated { handle },
            _ => unreachable!(),
        });
    }

    fn with_option(&mut self, name: impl Into<String>, value: impl Into<String>) {
        assert!(matches!(self, InsertState::NotStarted { .. }));
        replace_with_or_abort(self, |_self| match _self {
            InsertState::NotStarted { mut client, sql } => {
                client.add_option(name, value);
                InsertState::NotStarted { client, sql }
            }
            _ => unreachable!(),
        });
    }
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
    // TODO: remove Result
    pub(crate) fn new(client: &Client, table: &str) -> Result<Self>
    where
        T: Row,
    {
        let fields = row::join_column_names::<T>()
            .expect("the row type must be a struct or a wrapper around it");

        // TODO: what about escaping a table name?
        // https://clickhouse.com/docs/en/sql-reference/syntax#identifiers
        let sql = format!("INSERT INTO {}({}) FORMAT RowBinary", table, fields);

        Ok(Self {
            state: InsertState::NotStarted {
                client: Box::new(client.clone()),
                sql,
            },
            buffer: BytesMut::with_capacity(BUFFER_SIZE),
            #[cfg(feature = "lz4")]
            compression: client.compression,
            send_timeout: None,
            end_timeout: None,
            sleep: Box::pin(tokio::time::sleep(Duration::new(0, 0))),
            _marker: PhantomData,
        })
    }

    /// Sets timeouts for different operations.
    ///
    /// `send_timeout` restricts time on sending a data chunk to a socket.
    /// `None` disables the timeout, it's a default.
    /// It's roughly equivalent to `tokio::time::timeout(insert.write(...))`.
    ///
    /// `end_timeout` restricts time on waiting for a response from the CH
    /// server. Thus, it includes all work needed to handle `INSERT` by the
    /// CH server, e.g. handling all materialized views and so on.
    /// `None` disables the timeout, it's a default.
    /// It's roughly equivalent to `tokio::time::timeout(insert.end(...))`.
    ///
    /// These timeouts are much more performant (~x10) than wrapping `write()`
    /// and `end()` calls into `tokio::time::timeout()`.
    pub fn with_timeouts(
        mut self,
        send_timeout: Option<Duration>,
        end_timeout: Option<Duration>,
    ) -> Self {
        self.set_timeouts(send_timeout, end_timeout);
        self
    }

    /// Similar to [`Client::with_option`], but for this particular INSERT
    /// statement only.
    ///
    /// # Panics
    /// If called after the request is started, e.g., after [`Insert::write`].
    #[track_caller]
    pub fn with_option(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.state.with_option(name, value);
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

    /// Serializes the provided row into an internal buffer.
    /// Once the buffer is full, it's sent to a background task writing to the
    /// socket.
    ///
    /// Close to:
    /// ```ignore
    /// async fn write<T>(&self, row: &T) -> Result<usize>;
    /// ```
    ///
    /// A returned future doesn't depend on the row's lifetime.
    ///
    /// Returns an error if the row cannot be serialized or the background task
    /// failed. Once failed, the whole `INSERT` is aborted and cannot be
    /// used anymore.
    ///
    /// # Panics
    /// If called after the previous call that returned an error.
    pub fn write<'a>(&'a mut self, row: &T) -> impl Future<Output = Result<()>> + 'a + Send
    where
        T: Serialize,
    {
        let result = self.do_write(row);

        async move {
            result?;
            if self.buffer.len() >= MIN_CHUNK_SIZE {
                self.send_chunk().await?;
            }
            Ok(())
        }
    }

    #[inline(always)]
    pub(crate) fn do_write(&mut self, row: &T) -> Result<usize>
    where
        T: Serialize,
    {
        match self.state {
            InsertState::NotStarted { .. } => self.init_request(),
            InsertState::Active { .. } => Ok(()),
            _ => panic!("write() after error"),
        }?;

        let old_buf_size = self.buffer.len();
        let result = rowbinary::serialize_into(&mut self.buffer, row);
        let written = self.buffer.len() - old_buf_size;

        if result.is_err() {
            self.abort();
        }

        result.and(Ok(written))
    }

    /// Ends `INSERT`, the server starts processing the data.
    ///
    /// Succeeds if the server returns 200, that means the `INSERT` was handled
    /// successfully, including all materialized views and quorum writes.
    ///
    /// NOTE: If it isn't called, the whole `INSERT` is aborted.
    pub async fn end(mut self) -> Result<()> {
        if !self.buffer.is_empty() {
            self.send_chunk().await?;
        }
        self.state.terminated();
        self.wait_handle().await
    }

    async fn send_chunk(&mut self) -> Result<()> {
        debug_assert!(matches!(self.state, InsertState::Active { .. }));

        // Hyper uses non-trivial and inefficient schema of buffering chunks.
        // It's difficult to determine when allocations occur.
        // So, instead we control it manually here and rely on the system allocator.
        let chunk = self.take_and_prepare_chunk()?;

        let sender = self.state.sender().unwrap(); // checked above

        let is_timed_out = match timeout!(self, send_timeout, sender.send(chunk)) {
            Some(true) => return Ok(()),
            Some(false) => false, // an actual error will be returned from `wait_handle`
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
        match self.state.handle() {
            Some(handle) => {
                let result = match timeout!(self, end_timeout, &mut *handle) {
                    Some(Ok(res)) => res,
                    Some(Err(err)) if err.is_panic() => panic::resume_unwind(err.into_panic()),
                    Some(Err(err)) => Err(Error::Custom(format!("unexpected error: {err}"))),
                    None => {
                        // We can do nothing useful here, so just shut down the background task.
                        handle.abort();
                        Err(Error::TimedOut)
                    }
                };
                self.state = InsertState::Completed;
                result
            }
            _ => Ok(()),
        }
    }

    #[cfg(feature = "lz4")]
    fn take_and_prepare_chunk(&mut self) -> Result<Bytes> {
        Ok(if self.compression.is_lz4() {
            let compressed = crate::compression::lz4::compress(&self.buffer)?;
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

    #[cold]
    #[track_caller]
    #[inline(never)]
    fn init_request(&mut self) -> Result<()> {
        debug_assert!(matches!(self.state, InsertState::NotStarted { .. }));
        let (client, sql) = self.state.client_with_sql().unwrap(); // checked above

        let mut url = Url::parse(&client.url).map_err(|err| Error::InvalidParams(err.into()))?;
        let mut pairs = url.query_pairs_mut();
        pairs.clear();

        if let Some(database) = &client.database {
            pairs.append_pair("database", database);
        }

        pairs.append_pair("query", sql);

        if client.compression.is_lz4() {
            pairs.append_pair("decompress", "1");
        }

        for (name, value) in &client.options {
            pairs.append_pair(name, value);
        }

        drop(pairs);

        let mut builder = Request::post(url.as_str());
        builder = with_request_headers(builder, &client.headers, client.app_name.as_deref());

        if let Some(user) = &client.user {
            builder = builder.header("X-ClickHouse-User", user);
        }

        if let Some(password) = &client.password {
            builder = builder.header("X-ClickHouse-Key", password);
        }

        let (sender, body) = RequestBody::chunked();

        let request = builder
            .body(body)
            .map_err(|err| Error::InvalidParams(Box::new(err)))?;

        let future = client.http.request(request);
        // TODO: introduce `Executor` to allow bookkeeping of spawned tasks.
        let handle =
            tokio::spawn(async move { Response::new(future, Compression::None).finish().await });

        self.state = InsertState::Active { handle, sender };
        Ok(())
    }

    fn abort(&mut self) {
        if let Some(sender) = self.state.sender() {
            sender.abort();
        }
    }
}

impl<T> Drop for Insert<T> {
    fn drop(&mut self) {
        self.abort();
    }
}
