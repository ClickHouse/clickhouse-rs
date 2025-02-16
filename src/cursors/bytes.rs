use crate::{bytes_ext::BytesExt, cursors::RawCursor, error::Error, response::Response};
use std::{
    io::Result as IoResult,
    pin::Pin,
    task::{ready, Context, Poll},
};
use tokio::io::{AsyncBufRead, AsyncRead, ReadBuf};

/// Unlike [`RowCursor`] which emits rows deserialized as structures from
/// RowBinary, this cursor emits raw bytes without deserialization.
/// It can be iterated over using [`futures::StreamExt::next`].
/// Additionally, if the requested format emits each row on a newline
/// (e.g. `JSONEachRow`, `CSV`, `TSV`, etc.), the cursor can be converted into a
/// stream of lines using [`futures::AsyncBufReadExt::lines`]`, which will allow
/// for more convenient processing.
// TODO: note about errors
pub struct BytesCursor {
    raw: RawCursor,
    bytes: BytesExt,
}

// TODO: what if any poll_* called AFTER error returned?

impl BytesCursor {
    pub(crate) fn new(response: Response) -> Self {
        Self {
            raw: RawCursor::new(response),
            bytes: BytesExt::default(),
        }
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
