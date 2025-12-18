use crate::headers::{with_authentication, with_request_headers};
use crate::{
    Client, Compression,
    error::{Error, Result},
    request_body::{ChunkSender, RequestBody},
    response::Response,
    settings,
};
use bytes::{Bytes, BytesMut};
use hyper::{self, Request};
use std::ops::ControlFlow;
use std::task::{Context, Poll, ready};
use std::{cmp, future::Future, io, mem, panic, pin::Pin, time::Duration};
use tokio::io::AsyncWrite;
use tokio::{
    task::JoinHandle,
    time::{Instant, Sleep},
};
use url::Url;

#[cfg(doc)]
use tokio::io::AsyncWriteExt;

#[cfg(feature = "lz4")]
pub use compression::CompressedData;

// The desired max frame size.
const BUFFER_SIZE: usize = 256 * 1024;

/// Performs one `INSERT`, sending pre-formatted data.
///
/// The [`InsertFormatted::end`] method must be called to finalize the `INSERT`.
/// Otherwise, the whole `INSERT` will be aborted.
///
/// Rows are sent progressively to spread network load.
///
/// # Note: Not Validated
/// Unlike [`Insert`][crate::insert::Insert] and [`Inserter`][crate::insert::Inserter],
/// this does not perform any validation on the submitted data.
///
/// Only the use of self-describing formats (e.g. CSV, TabSeparated, JSON) is recommended.
///
/// See the [list of supported formats](https://clickhouse.com/docs/interfaces/formats)
/// for details.
#[must_use]
pub struct InsertFormatted {
    state: InsertState,
    #[cfg(feature = "lz4")]
    compression: Compression,
    send_timeout: Option<Timeout>,
    end_timeout: Option<Timeout>,
    // Use boxed `Sleep` to reuse a timer entry, it improves performance.
    // Also, `tokio::time::timeout()` significantly increases a future's size.
    sleep: Pin<Box<Sleep>>,
}

struct Timeout {
    duration: Duration,
    is_set: bool,
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
    #[inline(always)]
    fn is_not_started(&self) -> bool {
        matches!(self, Self::NotStarted { .. })
    }

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

    #[inline]
    fn expect_client_mut(&mut self) -> &mut Client {
        let Self::NotStarted { client, .. } = self else {
            panic!("cannot modify client options while an insert is in-progress")
        };

        client
    }

    fn terminated(&mut self) {
        match mem::replace(self, InsertState::Completed) {
            InsertState::NotStarted { .. } | InsertState::Completed => (),
            InsertState::Active { handle, .. } => {
                *self = InsertState::Terminated { handle };
            }
            InsertState::Terminated { handle } => {
                *self = InsertState::Terminated { handle };
            }
        }
    }
}

impl InsertFormatted {
    pub(crate) fn new(client: &Client, sql: String) -> Self {
        Self {
            state: InsertState::NotStarted {
                client: Box::new(client.clone()),
                sql,
            },
            #[cfg(feature = "lz4")]
            compression: client.compression,
            send_timeout: None,
            end_timeout: None,
            sleep: Box::pin(tokio::time::sleep(Duration::new(0, 0))),
        }
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

    /// Configure the [roles] to use when executing `INSERT` statements.
    ///
    /// Overrides any roles previously set by this method, [`InsertFormatted::with_option`],
    /// [`Client::with_roles`] or [`Client::with_option`].
    ///
    /// An empty iterator may be passed to clear the set roles.
    ///
    /// [roles]: https://clickhouse.com/docs/operations/access-rights#role-management
    ///
    /// # Panics
    /// If called after the request is started, e.g., after [`InsertFormatted::write`].
    pub fn with_roles(mut self, roles: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.state.expect_client_mut().set_roles(roles);
        self
    }

    /// Clear any explicit [roles] previously set on this `Insert` or inherited from [`Client`].
    ///
    /// Overrides any roles previously set by [`InsertFormatted::with_roles`], [`InsertFormatted::with_option`],
    /// [`Client::with_roles`] or [`Client::with_option`].
    ///
    /// [roles]: https://clickhouse.com/docs/operations/access-rights#role-management
    ///
    /// # Panics
    /// If called after the request is started, e.g., after [`InsertFormatted::write`].
    pub fn with_default_roles(mut self) -> Self {
        self.state.expect_client_mut().clear_roles();
        self
    }

    /// Similar to [`Client::with_option`], but for this particular INSERT
    /// statement only.
    ///
    /// # Panics
    /// If called after the request is started, e.g., after [`InsertFormatted::write`].
    #[track_caller]
    pub fn with_option(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.state.expect_client_mut().add_option(name, value);
        self
    }

    pub(crate) fn set_timeouts(
        &mut self,
        send_timeout: Option<Duration>,
        end_timeout: Option<Duration>,
    ) {
        self.send_timeout = Timeout::new_opt(send_timeout);
        self.end_timeout = Timeout::new_opt(end_timeout);
    }

    /// Wrap this `InsertFormatted` with a buffer of a default size.
    ///
    /// The returned type also implements [`AsyncWrite`].
    ///
    /// To set the capacity, use [`Self::buffered_with_capacity()`].
    pub fn buffered(self) -> BufInsertFormatted {
        self.buffered_with_capacity(BUFFER_SIZE)
    }

    /// Wrap this `InsertFormatted` with a buffer of a given size.
    ///
    /// The returned type also implements [`AsyncWrite`].
    pub fn buffered_with_capacity(self, capacity: usize) -> BufInsertFormatted {
        BufInsertFormatted::new(self, capacity)
    }

    /// Send a chunk of data.
    ///
    /// If compression is enabled, the data is compressed first.
    ///
    /// To pre-compress the data, use [`Self::send_compressed()`] instead.
    ///
    /// # Note: Unbuffered
    /// This immediately compresses and queues the data to be sent on the connection
    /// without waiting for more chunks. For best performance, chunks should not be too small.
    ///
    /// Use [`Self::buffered()`] for a buffered implementation which also implements [`AsyncWrite`].
    pub async fn send(&mut self, data: Bytes) -> Result<()> {
        #[cfg(feature = "lz4")]
        let data = if self.compression.is_lz4() {
            CompressedData::from_slice(&data).0
        } else {
            data
        };

        self.send_inner(data).await
    }

    async fn send_inner(&mut self, mut data: Bytes) -> Result<()> {
        std::future::poll_fn(move |cx| {
            loop {
                ready!(self.poll_ready(cx))?;

                // Potentially cheaper than cloning `data` which touches the refcount
                match self.try_send(mem::take(&mut data)) {
                    ControlFlow::Break(res) => return Poll::Ready(res),
                    ControlFlow::Continue(unsent) => {
                        data = unsent;
                    }
                }
            }
        })
        .await
    }

    #[inline]
    pub(crate) fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<()>> {
        if self.state.is_not_started() {
            self.init_request()?;
        }

        let Some(sender) = self.state.sender() else {
            return Poll::Ready(Err(Error::Network("channel closed".into())));
        };

        match sender.poll_ready(cx) {
            Poll::Ready(true) => {
                Timeout::reset_opt(self.send_timeout.as_mut());
                Poll::Ready(Ok(()))
            }
            Poll::Ready(false) => Poll::Ready(Err(Error::Network("channel closed".into()))),
            Poll::Pending => {
                ready!(Timeout::poll_opt(
                    self.send_timeout.as_mut(),
                    self.sleep.as_mut(),
                    cx
                ));
                self.abort();
                Poll::Ready(Err(Error::TimedOut))
            }
        }
    }

    #[inline(always)]
    pub(crate) fn try_send(&mut self, bytes: Bytes) -> ControlFlow<Result<()>, Bytes> {
        let Some(sender) = self.state.sender() else {
            return ControlFlow::Break(Err(Error::Network("channel closed".into())));
        };

        sender
            .try_send(bytes)
            .map_break(|res| res.map_err(|e| Error::Network(e.into())))
    }

    /// Ends `INSERT`, the server starts processing the data.
    ///
    /// Succeeds if the server returns 200, that means the `INSERT` was handled
    /// successfully, including all materialized views and quorum writes.
    ///
    /// NOTE: If this isn't called, the whole `INSERT` is aborted.
    pub async fn end(mut self) -> Result<()> {
        std::future::poll_fn(|cx| self.poll_end(cx)).await
    }

    pub(crate) fn poll_end(&mut self, cx: &mut Context<'_>) -> Poll<Result<()>> {
        self.state.terminated();
        self.poll_wait_handle(cx)
    }

    fn poll_wait_handle(&mut self, cx: &mut Context<'_>) -> Poll<Result<()>> {
        let Some(handle) = self.state.handle() else {
            return Poll::Ready(Ok(()));
        };

        let Poll::Ready(res) = Pin::new(&mut *handle).poll(cx) else {
            ready!(Timeout::poll_opt(
                self.end_timeout.as_mut(),
                self.sleep.as_mut(),
                cx
            ));

            // We can do nothing useful here, so just shut down the background task.
            handle.abort();
            return Poll::Ready(Err(Error::TimedOut));
        };

        let res = match res {
            Ok(res) => res,
            Err(err) if err.is_panic() => panic::resume_unwind(err.into_panic()),
            Err(err) => Err(Error::Custom(format!("unexpected error: {err}"))),
        };

        self.state = InsertState::Completed;

        Poll::Ready(res)
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
            pairs.append_pair(settings::DATABASE, database);
        }

        pairs.append_pair(settings::QUERY, sql);

        if client.compression.is_lz4() {
            pairs.append_pair(settings::DECOMPRESS, "1");
        }

        for (name, value) in &client.options {
            pairs.append_pair(name, value);
        }

        drop(pairs);

        let mut builder = Request::post(url.as_str());
        builder = with_request_headers(builder, &client.headers, &client.products_info);
        builder = with_authentication(builder, &client.authentication);

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

impl Drop for InsertFormatted {
    fn drop(&mut self) {
        self.abort();
    }
}

/// A wrapper around [`InsertFormatted`] which buffers writes.
pub struct BufInsertFormatted {
    insert: InsertFormatted,
    buffer: BytesMut,
    capacity: usize,
}

impl BufInsertFormatted {
    fn new(insert: InsertFormatted, capacity: usize) -> Self {
        Self {
            insert,
            buffer: BytesMut::with_capacity(capacity),
            capacity,
        }
    }

    /// Return the number of buffered bytes.
    #[inline(always)]
    pub fn buf_len(&self) -> usize {
        self.buffer.len()
    }

    /// Return the current capacity of the buffer.
    ///
    /// Note: Size is Not Constant
    /// This may be smaller than the original capacity if part of the buffer
    /// is still being used by the connection.
    ///
    /// This may be larger if a call to [`Self::write_buffered()`] caused the buffer to expand.
    #[inline(always)]
    pub fn capacity(&self) -> usize {
        self.buffer.capacity()
    }

    /// Write data to the buffer without waiting for it to be flushed.
    ///
    /// May cause the buffer to resize to fit the data.
    #[inline(always)]
    pub fn write_buffered(&mut self, data: &[u8]) {
        self.buffer.extend_from_slice(data);
    }

    // `#[inline]` is *supposed* to work
    // https://doc.rust-lang.org/reference/attributes/codegen.html#r-attributes.codegen.inline.async
    // but it's apparently not implemented yet: https://github.com/rust-lang/rust/pull/149245
    #[inline(always)]
    pub async fn write_all(&mut self, mut data: &[u8]) -> Result<()> {
        std::future::poll_fn(|cx| {
            while !data.is_empty() {
                let written = ready!(self.poll_write_inner(data, cx))?;
                data = &data[written..];
            }

            Poll::Ready(Ok(()))
        })
        .await
    }

    // `poll_write` but it returns `crate::Result` instead of `io::Result`
    #[inline(always)]
    fn poll_write_inner(&mut self, data: &[u8], cx: &mut Context<'_>) -> Poll<Result<usize>> {
        if self.insert.state.is_not_started() {
            // We don't want to wait for the buffer to be full before we start the request,
            // in the event of an error.
            self.insert.init_request()?;
        }

        if self.buffer.len() >= self.capacity {
            ready!(self.poll_flush_inner(cx))?;
            debug_assert!(self.buffer.is_empty());
        }

        if self.buffer.capacity() == 0 {
            self.buffer.reserve(self.capacity);
        }

        let write_len = cmp::min(self.buffer.capacity(), data.len());

        self.buffer.extend_from_slice(&data[..write_len]);
        Poll::Ready(Ok(write_len))
    }

    /// Flush the buffer to the server as a single chunk.
    ///
    /// If [compression is enabled][Client::compression], the full buffer will be compressed.
    ///
    ///
    #[inline(always)]
    pub async fn flush(&mut self) -> Result<()> {
        std::future::poll_fn(|cx| self.poll_flush_inner(cx)).await
    }

    #[inline(always)]
    fn poll_flush_inner(&mut self, cx: &mut Context<'_>) -> Poll<Result<()>> {
        if self.buffer.is_empty() {
            return Poll::Ready(Ok(()));
        }

        ready!(self.insert.poll_ready(cx))?;

        let data = self.buffer.split().freeze();

        #[cfg(feature = "lz4")]
        let data = if self.insert.compression.is_lz4() {
            CompressedData::from(data).0
        } else {
            data
        };

        let ControlFlow::Break(res) = self.insert.try_send(data) else {
            unreachable!("BUG: we just checked that `ChunkSender` was ready")
        };

        Poll::Ready(res)
    }

    /// Flushes the buffer, then calls [`InsertFormatted::end()`].
    ///
    /// Cancel-safe.
    #[inline(always)]
    pub async fn end(&mut self) -> Result<()> {
        std::future::poll_fn(|cx| self.poll_end(cx)).await
    }

    #[inline(always)]
    fn poll_end(&mut self, cx: &mut Context<'_>) -> Poll<Result<()>> {
        if !self.buffer.is_empty() {
            ready!(self.poll_flush_inner(cx))?;
            debug_assert!(self.buffer.is_empty());
        }

        self.insert.poll_end(cx)
    }
}

impl AsyncWrite for BufInsertFormatted {
    #[inline(always)]
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::result::Result<usize, io::Error>> {
        self.poll_write_inner(buf, cx).map_err(Into::into)
    }

    #[inline(always)]
    fn poll_flush(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<std::result::Result<(), io::Error>> {
        self.poll_flush_inner(cx).map_err(Into::into)
    }

    #[inline(always)]
    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<std::result::Result<(), io::Error>> {
        self.poll_end(cx).map_err(Into::into)
    }
}

impl Timeout {
    fn new_opt(duration: Option<Duration>) -> Option<Self> {
        duration.map(|duration| Self {
            duration,
            is_set: false,
        })
    }

    /// Returns `Poll::Pending` if `None`.
    #[inline(always)]
    fn poll_opt(this: Option<&mut Self>, sleep: Pin<&mut Sleep>, cx: &mut Context<'_>) -> Poll<()> {
        if let Some(this) = this {
            this.poll(sleep, cx)
        } else {
            Poll::Pending
        }
    }

    #[inline]
    fn poll(&mut self, mut sleep: Pin<&mut Sleep>, cx: &mut Context<'_>) -> Poll<()> {
        if !self.is_set
            && let Some(deadline) = Instant::now().checked_add(self.duration)
        {
            sleep.as_mut().reset(deadline);
            self.is_set = true;
        }

        ready!(sleep.as_mut().poll(cx));
        self.is_set = false;

        Poll::Ready(())
    }

    #[inline(always)]
    fn reset_opt(this: Option<&mut Self>) {
        if let Some(this) = this {
            this.is_set = false;
        }
    }
}

// Just so I don't have to repeat this feature flag a hundred times.
#[cfg(feature = "lz4")]
mod compression {
    use crate::error::{Error, Result};
    use crate::insert_formatted::InsertFormatted;
    use bytes::Bytes;

    /// A chunk of pre-compressed data.
    #[cfg_attr(docsrs, doc(cfg(feature = "lz4")))]
    pub struct CompressedData(pub(crate) Bytes);

    impl CompressedData {
        /// Compress a slice of bytes.
        #[inline(always)]
        pub fn from_slice(slice: &[u8]) -> Self {
            Self(
                crate::compression::lz4::compress(slice)
                    .expect("BUG: `lz4::compress()` should not error"),
            )
        }
    }

    impl<T> From<T> for CompressedData
    where
        T: AsRef<[u8]>,
    {
        #[inline(always)]
        fn from(value: T) -> Self {
            Self::from_slice(value.as_ref())
        }
    }

    impl InsertFormatted {
        /// Send a chunk of pre-compressed data.
        ///
        /// # Errors
        /// In addition to network errors, this will return [`Error::Compression`] if the
        /// [`Client`][crate::Client] does not have compression enabled.
        pub async fn send_compressed(&mut self, data: CompressedData) -> Result<()> {
            if !self.compression.is_lz4() {
                return Err(Error::Compression(
                    "attempting to send compressed data, but compression is not enabled".into(),
                ));
            }

            self.send_inner(data.0).await
        }
    }
}
