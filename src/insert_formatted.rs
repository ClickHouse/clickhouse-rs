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
use std::task::{Context, Poll, ready};
use std::{future::Future, io, mem, panic, pin::Pin, time::Duration};
use tokio::io::AsyncWrite;
use tokio::{
    task::JoinHandle,
    time::{Instant, Sleep},
};
use url::Url;

#[cfg(doc)]
use tokio::io::AsyncWriteExt;

// The desired max frame size.
const BUFFER_SIZE: usize = 256 * 1024;
// Threshold to send a chunk. Should be slightly less than `BUFFER_SIZE`
// to avoid extra reallocations in case of a big last row.
const MIN_CHUNK_SIZE: usize = const {
    // to use the whole buffer's capacity
    assert!(BUFFER_SIZE.is_power_of_two());
    BUFFER_SIZE - 2048
};

/// Performs one `INSERT`, sending pre-formatted data.
///
/// The [`InsertFormatted::end`] method must be called to finalize the `INSERT`.
/// Otherwise, the whole `INSERT` will be aborted.
///
/// Rows are sent progressively to spread network load.
#[must_use]
pub struct InsertFormatted {
    state: InsertState,
    buffer: Buffer,
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

struct Buffer {
    buffer: BytesMut,
    #[cfg(feature = "lz4")]
    compression: Compression,
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
            buffer: Buffer::new(client),
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

    /// Ends `INSERT`, the server starts processing the data.
    ///
    /// Succeeds if the server returns 200, that means the `INSERT` was handled
    /// successfully, including all materialized views and quorum writes.
    ///
    /// NOTE: If it isn't called, the whole `INSERT` is aborted.
    ///
    /// This is the equivalent of calling [`AsyncWrite::poll_shutdown()`]
    /// or [`AsyncWriteExt::shutdown()`].
    pub async fn end(mut self) -> Result<()> {
        std::future::poll_fn(|cx| self.poll_end(cx)).await
    }

    fn poll_end(&mut self, cx: &mut Context<'_>) -> Poll<Result<()>> {
        if !self.buffer.is_empty() {
            ready!(self.poll_send_chunk(cx))?;
        }

        self.state.terminated();
        self.poll_wait_handle(cx)
    }

    fn poll_send_chunk(&mut self, cx: &mut Context<'_>) -> Poll<Result<()>> {
        if self.buffer.is_empty() {
            return Poll::Ready(Ok(()));
        }

        let Some(sender) = self.state.sender() else {
            return Poll::Ready(Err(Error::Network("channel closed".into())));
        };

        match sender.poll_ready(cx) {
            Poll::Ready(true) => (),
            Poll::Ready(false) => {
                return Poll::Ready(Err(Error::Network("channel closed".into())));
            }
            Poll::Pending => {
                ready!(Timeout::poll_opt(
                    self.send_timeout.as_mut(),
                    self.sleep.as_mut(),
                    cx
                ));
                self.abort();
                return Err(Error::TimedOut).into();
            }
        }

        Timeout::reset_opt(self.send_timeout.as_mut());

        let chunk = self
            .buffer
            .take_and_prepare_chunk()
            .map_err(io::Error::other)?;

        if sender.try_send(chunk) {
            Poll::Ready(Ok(()))
        } else {
            Poll::Ready(Err(Error::Network("channel closed".into())))
        }
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

impl AsyncWrite for InsertFormatted {
    #[inline]
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::result::Result<usize, io::Error>> {
        if matches!(self.state, InsertState::NotStarted { .. }) {
            self.init_request()?;
        }

        if self.buffer.len() >= MIN_CHUNK_SIZE {
            ready!(self.as_mut().poll_flush(cx))?;
        }

        self.buffer.extend_from_slice(buf);

        Poll::Ready(Ok(buf.len()))
    }

    #[inline]
    fn poll_flush(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<std::result::Result<(), io::Error>> {
        self.poll_send_chunk(cx).map_err(Into::into)
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<std::result::Result<(), io::Error>> {
        self.poll_end(cx).map_err(Into::into)
    }
}

impl Drop for InsertFormatted {
    fn drop(&mut self) {
        self.abort();
    }
}

impl Buffer {
    #[cfg_attr(not(feature = "lz4"), allow(unused_variables))]
    fn new(client: &Client) -> Self {
        Self {
            buffer: BytesMut::with_capacity(BUFFER_SIZE),
            #[cfg(feature = "lz4")]
            compression: client.compression,
        }
    }

    #[inline(always)]
    fn is_empty(&self) -> bool {
        self.buffer.is_empty()
    }

    #[inline(always)]
    fn len(&self) -> usize {
        self.buffer.len()
    }

    #[inline]
    fn extend_from_slice(&mut self, slice: &[u8]) {
        self.buffer.extend_from_slice(slice);
    }

    #[inline]
    fn take_and_prepare_chunk(&mut self) -> Result<Bytes> {
        #[cfg(feature = "lz4")]
        if self.compression.is_lz4() {
            let compressed = crate::compression::lz4::compress(&self.buffer)?;
            self.buffer.clear();
            return Ok(compressed);
        }

        Ok(mem::replace(&mut self.buffer, BytesMut::with_capacity(BUFFER_SIZE)).freeze())
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
