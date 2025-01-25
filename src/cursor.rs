use crate::{
    bytes_ext::BytesExt,
    error::{Error, Result},
    response::{Chunks, Response, ResponseFuture},
    rowbinary,
};
use bytes::Bytes;
use futures::TryStreamExt;
use serde::Deserialize;
use std::marker::PhantomData;

#[cfg(feature = "futures03")]
const INITIAL_BYTES_CURSOR_BUFFER_SIZE: usize = 1024;

// === RawCursor ===

struct RawCursor(RawCursorState);

enum RawCursorState {
    Waiting(ResponseFuture),
    Loading(RawCursorLoading),
}

struct RawCursorLoading {
    chunks: Chunks,
    net_size: u64,
    data_size: u64,
}

impl RawCursor {
    pub(crate) fn new(response: Response) -> Self {
        Self(RawCursorState::Waiting(response.into_future()))
    }

    async fn next(&mut self) -> Result<Option<Bytes>> {
        if matches!(self.0, RawCursorState::Waiting(_)) {
            self.resolve().await?;
        }

        let state = match &mut self.0 {
            RawCursorState::Loading(state) => state,
            RawCursorState::Waiting(_) => unreachable!(),
        };

        match state.chunks.try_next().await {
            Ok(Some(chunk)) => {
                state.net_size += chunk.net_size as u64;
                state.data_size += chunk.data.len() as u64;
                Ok(Some(chunk.data))
            }
            Ok(None) => Ok(None),
            Err(err) => Err(err),
        }
    }

    async fn resolve(&mut self) -> Result<()> {
        if let RawCursorState::Waiting(future) = &mut self.0 {
            let chunks = future.await;
            self.0 = RawCursorState::Loading(RawCursorLoading {
                chunks: chunks?,
                net_size: 0,
                data_size: 0,
            });
        }
        Ok(())
    }

    fn received_bytes(&self) -> u64 {
        match &self.0 {
            RawCursorState::Waiting(_) => 0,
            RawCursorState::Loading(state) => state.net_size,
        }
    }

    fn decoded_bytes(&self) -> u64 {
        match &self.0 {
            RawCursorState::Waiting(_) => 0,
            RawCursorState::Loading(state) => state.data_size,
        }
    }
}

// === BytesCursor ===

/// Unlike [`RowCursor`] which emits rows deserialized as structures from RowBinary,
/// this cursor emits raw bytes without deserialization.
/// It can be iterated over using [`futures::StreamExt::next`].
/// Additionally, if the requested format emits each row on a newline
/// (e.g. `JSONEachRow`, `CSV`, `TSV`, etc.), the cursor can be converted into a stream of lines
/// using [`futures::AsyncBufReadExt::lines`]`, which will allow for more convenient processing.
#[cfg(feature = "futures03")]
pub struct BytesCursor {
    raw: RawCursor,
    buffer: Vec<u8>,
    filled: usize,
    consumed: usize,
}

#[cfg(feature = "futures03")]
impl BytesCursor {
    pub(crate) fn new(response: Response) -> Self {
        Self {
            raw: RawCursor::new(response),
            buffer: Vec::with_capacity(INITIAL_BYTES_CURSOR_BUFFER_SIZE),
            filled: 0,
            consumed: 0,
        }
    }
}

#[cfg(feature = "futures03")]
impl futures::stream::Stream for BytesCursor {
    type Item = Result<Bytes>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        use futures::FutureExt;
        match self.raw.next().boxed().poll_unpin(cx) {
            std::task::Poll::Ready(result) => match result {
                Ok(Some(bytes)) => std::task::Poll::Ready(Some(Ok(bytes))),
                Ok(None) => std::task::Poll::Ready(None),
                Err(err) => std::task::Poll::Ready(Some(Err(err))),
            },
            std::task::Poll::Pending => std::task::Poll::Pending,
        }
    }
}

#[cfg(feature = "futures03")]
impl futures::AsyncRead for BytesCursor {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut [u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        use futures::FutureExt;
        let buf = match buf.len() {
            0 => return std::task::Poll::Ready(Ok(0)),
            len => &mut buf[..len],
        };

        match self.raw.next().boxed().poll_unpin(cx) {
            std::task::Poll::Ready(Ok(Some(bytes))) => {
                let len = std::cmp::min(bytes.len(), buf.len());
                buf[..len].copy_from_slice(&bytes[..len]);
                std::task::Poll::Ready(Ok(len))
            }
            std::task::Poll::Ready(Ok(None)) => std::task::Poll::Ready(Ok(0)),
            std::task::Poll::Ready(Err(err)) => std::task::Poll::Ready(Err(err.into_io())),
            std::task::Poll::Pending => std::task::Poll::Pending,
        }
    }
}

#[cfg(feature = "futures03")]
impl futures::AsyncBufRead for BytesCursor {
    fn poll_fill_buf(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<&[u8]>> {
        use futures::FutureExt;
        let this = self.get_mut();
        if this.filled == this.consumed {
            let poll_result = this.raw.next().boxed().poll_unpin(cx);
            match poll_result {
                std::task::Poll::Ready(Ok(Some(bytes))) => {
                    this.filled = bytes.len();
                    this.buffer.extend(bytes);
                }
                std::task::Poll::Ready(Ok(None)) => return std::task::Poll::Ready(Ok(&[])),
                std::task::Poll::Ready(Err(err)) => {
                    return std::task::Poll::Ready(Err(err.into_io()))
                }
                std::task::Poll::Pending => return std::task::Poll::Pending,
            }
        }
        std::task::Poll::Ready(Ok(&this.buffer[this.consumed..]))
    }

    fn consume(mut self: std::pin::Pin<&mut Self>, amt: usize) {
        self.consumed += amt;
        if self.consumed == self.filled {
            self.filled = 0;
            self.consumed = 0;
            self.buffer.clear();
        }
    }
}

// XXX: it was a workaround for https://github.com/rust-lang/rust/issues/51132,
//      but introduced #24 and must be fixed.
fn workaround_51132<'a, T: ?Sized>(ptr: &T) -> &'a T {
    // SAFETY: actually, it leads to unsoundness, see #24
    unsafe { &*(ptr as *const T) }
}

// === RowCursor ===

/// A cursor that emits rows deserialized as structures from RowBinary.
#[must_use]
pub struct RowCursor<T> {
    raw: RawCursor,
    bytes: BytesExt,
    _marker: PhantomData<T>,
}

impl<T> RowCursor<T> {
    pub(crate) fn new(response: Response) -> Self {
        Self {
            raw: RawCursor::new(response),
            bytes: BytesExt::default(),
            _marker: PhantomData,
        }
    }

    /// Emits the next row.
    ///
    /// The result is unspecified if it's called after `Err` is returned.
    pub async fn next<'a, 'b: 'a>(&'a mut self) -> Result<Option<T>>
    where
        T: Deserialize<'b>,
    {
        loop {
            let mut slice = workaround_51132(self.bytes.slice());

            match rowbinary::deserialize_from(&mut slice) {
                Ok(value) => {
                    self.bytes.set_remaining(slice.len());
                    return Ok(Some(value));
                }
                Err(Error::NotEnoughData) => {}
                Err(err) => return Err(err),
            }

            match self.raw.next().await? {
                Some(chunk) => self.bytes.extend(chunk),
                None if self.bytes.remaining() > 0 => {
                    // If some data is left, we have an incomplete row in the buffer.
                    // This is usually a schema mismatch on the client side.
                    return Err(Error::NotEnoughData);
                }
                None => return Ok(None),
            }
        }
    }

    /// Returns the total size in bytes received from the CH server since
    /// the cursor was created.
    ///
    /// This method counts only size without HTTP headers for now.
    /// It can be changed in the future without notice.
    #[inline]
    pub fn received_bytes(&self) -> u64 {
        self.raw.received_bytes()
    }

    /// Returns the total size in bytes decompressed since the cursor was
    /// created.
    #[inline]
    pub fn decoded_bytes(&self) -> u64 {
        self.raw.decoded_bytes()
    }
}

// === JsonCursor ===

#[cfg(feature = "watch")]
pub(crate) struct JsonCursor<T> {
    raw: RawCursor,
    bytes: BytesExt,
    line: String,
    _marker: PhantomData<T>,
}

// We use `JSONEachRowWithProgress` to avoid infinite HTTP connections.
// See https://github.com/ClickHouse/ClickHouse/issues/22996 for details.
#[cfg(feature = "watch")]
#[derive(Deserialize)]
#[serde(rename_all = "lowercase")]
enum JsonRow<T> {
    Row(T),
    Progress {},
}

#[cfg(feature = "watch")]
impl<T> JsonCursor<T> {
    const INITIAL_BUFFER_SIZE: usize = 1024;

    pub(crate) fn new(response: Response) -> Self {
        Self {
            raw: RawCursor::new(response),
            bytes: BytesExt::default(),
            line: String::with_capacity(Self::INITIAL_BUFFER_SIZE),
            _marker: PhantomData,
        }
    }

    pub(crate) async fn next<'a, 'b: 'a>(&'a mut self) -> Result<Option<T>>
    where
        T: Deserialize<'b>,
    {
        use bytes::Buf;
        use std::io::BufRead;

        loop {
            self.line.clear();

            let read = match self.bytes.slice().reader().read_line(&mut self.line) {
                Ok(read) => read,
                Err(err) => return Err(Error::Custom(err.to_string())),
            };

            if let Some(line) = self.line.strip_suffix('\n') {
                self.bytes.advance(read);

                match serde_json::from_str(workaround_51132(line)) {
                    Ok(JsonRow::Row(value)) => return Ok(Some(value)),
                    Ok(JsonRow::Progress { .. }) => continue,
                    Err(err) => return Err(Error::BadResponse(err.to_string())),
                }
            }

            match self.raw.next().await? {
                Some(chunk) => self.bytes.extend(chunk),
                None => return Ok(None),
            }
        }
    }
}
