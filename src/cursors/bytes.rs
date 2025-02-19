use crate::{
    bytes_ext::BytesExt,
    cursors::RawCursor,
    error::{Error, Result},
    response::Response,
};
use bytes::{Bytes, BytesMut};
use std::{
    io::Result as IoResult,
    pin::Pin,
    task::{ready, Context, Poll},
};
use tokio::io::{AsyncBufRead, AsyncRead, ReadBuf};

/// A cursor over raw bytes of the response returned by [`Query::fetch_bytes`].
///
/// Unlike [`RowCursor`] which emits rows deserialized as structures from
/// RowBinary, this cursor emits raw bytes without deserialization.
///
/// # Integration
///
/// Additionally to [`BytesCursor::next`] and [`BytesCursor::collect`],
/// this cursor implements:
/// * [`AsyncRead`] and [`AsyncBufRead`] for `tokio`-based ecosystem.
/// * [`futures::Stream`], [`futures::AsyncRead`] and [`futures::AsyncBufRead`]
///   for `futures`-based ecosystem. (requires the `futures03` feature)
///
/// For instance, if the requested format emits each row on a newline
/// (e.g. `JSONEachRow`, `CSV`, `TSV`, etc.), the cursor can be read line by
/// line using [`AsyncBufReadExt::lines`]. Note that this method
/// produces a new `String` for each line, so it's not the most performant way
/// to iterate.
///
/// Note: methods of these traits use [`std::io::Error`] for errors.
/// To get an original error from this crate, see TODO.
///
/// [`RowCursor`]: crate::query::RowCursor
/// [`Query::fetch_bytes`]: crate::query::Query::fetch_bytes
/// [`AsyncBufReadExt::lines`]: tokio::io::AsyncBufReadExt::lines
pub struct BytesCursor {
    raw: RawCursor,
    bytes: BytesExt,
}

// TODO: what if any next/poll_* called AFTER error returned?
// TODO: describe io::Error <-> Error conversion.

impl BytesCursor {
    pub(crate) fn new(response: Response) -> Self {
        Self {
            raw: RawCursor::new(response),
            bytes: BytesExt::default(),
        }
    }

    /// Emits the next bytes chunk.
    pub async fn next(&mut self) -> Result<Option<Bytes>> {
        assert!(
            self.bytes.is_empty(),
            "mixing `BytesCursor::next()` and `AsyncRead` API methods is not allowed"
        );

        self.raw.next().await
    }

    /// Collects the whole response into a single [`Bytes`].
    pub async fn collect(&mut self) -> Result<Bytes> {
        let mut chunks = Vec::new();
        let mut total_len = 0;

        while let Some(chunk) = self.next().await? {
            total_len += chunk.len();
            chunks.push(chunk);
        }

        // The whole response is in a single chunk.
        if chunks.len() == 1 {
            return Ok(chunks.pop().unwrap());
        }

        let mut collected = BytesMut::with_capacity(total_len);
        for chunk in chunks {
            collected.extend_from_slice(&chunk);
        }
        debug_assert_eq!(collected.capacity(), total_len);

        Ok(collected.freeze())
    }

    #[cold]
    fn poll_refill(&mut self, cx: &mut Context<'_>) -> Poll<IoResult<bool>> {
        debug_assert_eq!(self.bytes.remaining(), 0);

        // TODO: should we repeat if `poll_next` returns an empty buffer?

        match ready!(self.raw.poll_next(cx).map_err(Error::into_io)?) {
            Some(chunk) => {
                self.bytes.extend(chunk);
                Poll::Ready(Ok(true))
            }
            None => Poll::Ready(Ok(false)),
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

impl AsyncRead for BytesCursor {
    #[inline]
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<IoResult<()>> {
        while buf.remaining() > 0 {
            if self.bytes.is_empty() && !ready!(self.poll_refill(cx)?) {
                break;
            }

            let bytes = self.bytes.slice();
            let len = bytes.len().min(buf.remaining());
            buf.put_slice(&bytes[..len]);
            self.bytes.advance(len);
        }

        Poll::Ready(Ok(()))
    }
}

impl AsyncBufRead for BytesCursor {
    #[inline]
    fn poll_fill_buf(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<IoResult<&[u8]>> {
        if self.bytes.is_empty() && !ready!(self.poll_refill(cx)?) {
            return Poll::Ready(Ok(&[]));
        }

        Poll::Ready(Ok(self.get_mut().bytes.slice()))
    }

    #[inline]
    fn consume(mut self: Pin<&mut Self>, amt: usize) {
        assert!(
            amt <= self.bytes.remaining(),
            "invalid `AsyncBufRead::consume` usage"
        );
        self.bytes.advance(amt);
    }
}

#[cfg(feature = "futures03")]
impl futures::AsyncRead for BytesCursor {
    #[inline]
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<IoResult<usize>> {
        let mut buf = ReadBuf::new(buf);
        ready!(AsyncRead::poll_read(self, cx, &mut buf)?);
        Poll::Ready(Ok(buf.filled().len()))
    }
}

#[cfg(feature = "futures03")]
impl futures::AsyncBufRead for BytesCursor {
    #[inline]
    fn poll_fill_buf(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<IoResult<&[u8]>> {
        AsyncBufRead::poll_fill_buf(self, cx)
    }

    #[inline]
    fn consume(self: Pin<&mut Self>, amt: usize) {
        AsyncBufRead::consume(self, amt);
    }
}

#[cfg(feature = "futures03")]
impl futures::stream::Stream for BytesCursor {
    type Item = crate::error::Result<bytes::Bytes>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        assert!(
            self.bytes.is_empty(),
            "mixing `Stream` and `AsyncRead` API methods is not allowed"
        );

        self.raw.poll_next(cx).map(Result::transpose)
    }
}
