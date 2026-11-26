use crate::{cursors::RawCursor, error::Result, query_summary::QuerySummary, response::Response};
use bytes::{Buf, Bytes, BytesMut};
use futures_util::TryFutureExt;
use std::{
    io::Result as IoResult,
    pin::Pin,
    task::{Context, Poll, ready},
};
use tokio::io::{AsyncBufRead, AsyncRead, ReadBuf};
use tracing::Instrument;

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
/// * [`futures_util::Stream`], [`futures_util::AsyncRead`] and
///   [`futures_util::AsyncBufRead`] for `futures`-based ecosystem.
///   (requires the `futures03` feature)
///
/// For instance, if the requested format emits each row on a newline
/// (e.g. `JSONEachRow`, `CSV`, `TSV`, etc.), the cursor can be read line by
/// line using `AsyncBufReadExt::lines`. Note that this method
/// produces a new `String` for each line, so it's not the most performant way
/// to iterate.
///
/// Note: methods of these traits use [`std::io::Error`] for errors.
/// To get an original error from this crate, use `From` conversion.
///
/// [`RowCursor`]: crate::query::RowCursor
/// [`Query::fetch_bytes`]: crate::query::Query::fetch_bytes
pub struct BytesCursor {
    raw: RawCursor,
    bytes: Bytes,
    span: tracing::Span,
}

// TODO: what if any next/poll_* called AFTER error returned?

impl BytesCursor {
    pub(crate) fn new(response: Response, span: tracing::Span) -> Self {
        Self {
            raw: RawCursor::new(response),
            bytes: Bytes::default(),
            span,
        }
    }

    /// Emits the next bytes chunk.
    ///
    /// # Cancel safety
    ///
    /// This method is cancellation safe.
    pub async fn next(&mut self) -> Result<Option<Bytes>> {
        assert!(
            self.bytes.is_empty(),
            "mixing `BytesCursor::next()` and `AsyncRead` API methods is not allowed"
        );

        self.raw
            .next()
            .inspect_err(|e| tracing::debug!(error=?e, "error from BytesCursor::next()"))
            .instrument(self.span.clone())
            .await
    }

    /// Poll for the next chunk of bytes.
    #[inline(always)]
    pub fn poll_next(&mut self, cx: &mut Context<'_>) -> Poll<Result<Option<Bytes>>> {
        assert!(
            self.bytes.is_empty(),
            "mixing `BytesCursor::poll_next()` and `AsyncRead` API methods is not allowed"
        );

        let _guard = self.span.enter();

        self.raw.poll_next(cx)
    }

    /// Collects the whole response into a single [`Bytes`].
    ///
    /// # Cancel safety
    ///
    /// This method is NOT cancellation safe.
    /// If cancelled, already collected bytes are lost.
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
        debug_assert_eq!(self.bytes.len(), 0);

        let _guard = self.span.enter();

        // Theoretically, `self.raw.poll_next(cx)` can return empty chunks.
        // In this case, we should continue polling until we get a non-empty chunk or
        // end of stream in order to avoid false positive `Ok(0)` in I/O traits.
        while self.bytes.is_empty() {
            match ready!(self.raw.poll_next(cx)) {
                Ok(Some(chunk)) => self.bytes = chunk,
                Ok(None) => return Poll::Ready(Ok(false)),
                Err(e) => {
                    tracing::debug!(error=?e, "error reading from cursor");
                    return Poll::Ready(Err(e.into()));
                }
            }
        }

        Poll::Ready(Ok(true))
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

    /// Returns the parsed `X-ClickHouse-Summary` response header, if
    /// present. Available once the response headers have been received.
    ///
    /// Note: the summary values may be incomplete unless the query was
    /// executed with `wait_end_of_query=1`.
    #[inline]
    pub fn summary(&self) -> Option<&QuerySummary> {
        self.raw.summary()
    }

    #[inline]
    #[doc(hidden)]
    pub fn _priv_span(&self) -> &tracing::Span {
        &self.span
    }
}

impl AsyncRead for BytesCursor {
    #[inline]
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<IoResult<()>> {
        if buf.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }

        if self.bytes.is_empty() && !ready!(self.poll_refill(cx)?) {
            return Poll::Ready(Ok(()));
        }

        let len = self.bytes.len().min(buf.remaining());
        buf.put_slice(&self.bytes[..len]);
        self.bytes.advance(len);

        Poll::Ready(Ok(()))
    }
}

impl AsyncBufRead for BytesCursor {
    #[inline]
    fn poll_fill_buf(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<IoResult<&[u8]>> {
        if self.bytes.is_empty() {
            ready!(self.poll_refill(cx)?);
        }

        Poll::Ready(Ok(&self.get_mut().bytes))
    }

    #[inline]
    fn consume(mut self: Pin<&mut Self>, amt: usize) {
        assert!(
            amt <= self.bytes.len(),
            "invalid `AsyncBufRead::consume` usage"
        );
        self.bytes.advance(amt);
    }
}

impl Drop for BytesCursor {
    fn drop(&mut self) {
        let _span = self.span.enter();

        tracing::record_all!(
            self.span,
            clickhouse.response.received_bytes = self.received_bytes(),
            clickhouse.response.decoded_bytes = self.decoded_bytes(),
        );

        tracing::debug!("finished raw query");
    }
}

#[cfg(feature = "futures03")]
impl futures_util::AsyncRead for BytesCursor {
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
impl futures_util::AsyncBufRead for BytesCursor {
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
impl futures_util::stream::Stream for BytesCursor {
    type Item = crate::error::Result<bytes::Bytes>;

    #[inline]
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        assert!(
            self.bytes.is_empty(),
            "mixing `Stream` and `AsyncRead` API methods is not allowed"
        );

        let this = &mut *self;

        let _guard = this.span.enter();

        this.raw.poll_next(cx).map(Result::transpose)
    }
}

#[cfg(feature = "futures03")]
impl futures_util::stream::FusedStream for BytesCursor {
    #[inline]
    fn is_terminated(&self) -> bool {
        self.bytes.is_empty() && self.raw.is_terminated()
    }
}

#[cfg(test)]
mod tests {
    use crate::{Client, Compression};
    use std::time::Duration;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
    };

    async fn spawn_chunked_server(chunks: &'static [&'static [u8]]) -> std::net::SocketAddr {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            while let Ok((mut socket, _)) = listener.accept().await {
                tokio::spawn(async move {
                    let mut req_buf = [0_u8; 1024];
                    let _ = socket.read(&mut req_buf).await;
                    let header =
                        b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n";
                    if socket.write_all(header).await.is_err() {
                        return;
                    }
                    for chunk in chunks {
                        let frame = format!("{:x}\r\n", chunk.len());
                        if socket.write_all(frame.as_bytes()).await.is_err()
                            || socket.write_all(chunk).await.is_err()
                            || socket.write_all(b"\r\n").await.is_err()
                            || socket.flush().await.is_err()
                        {
                            return;
                        }
                        tokio::time::sleep(Duration::from_millis(10)).await;
                    }
                    let _ = socket.write_all(b"0\r\n\r\n").await;
                    let _ = socket.flush().await;
                });
            }
        });
        addr
    }

    #[tokio::test]
    async fn async_read_preserves_chunks_across_pending_refills() {
        let chunks: &'static [&'static [u8]] = &[b"hello ", b"world", b"!\n"];
        let addr = spawn_chunked_server(chunks).await;
        let client = Client::default()
            .with_url(format!("http://{addr}"))
            .with_compression(Compression::None);

        let mut copy_cursor = client.query("SELECT 1").fetch_bytes("CSV").unwrap();
        let mut copied = Vec::new();
        let n = tokio::io::copy(&mut copy_cursor, &mut copied)
            .await
            .unwrap();
        assert_eq!(n, 13);
        assert_eq!(copied, b"hello world!\n");

        let mut loop_cursor = client.query("SELECT 1").fetch_bytes("CSV").unwrap();
        assert_eq!(loop_cursor.read(&mut []).await.unwrap(), 0);
        let mut read_out = Vec::new();
        let mut buf = [0_u8; 64];
        loop {
            let read_len = loop_cursor.read(&mut buf).await.unwrap();
            if read_len == 0 {
                break;
            }
            read_out.extend_from_slice(&buf[..read_len]);
        }
        assert_eq!(read_out, b"hello world!\n");

        let mut end_cursor = client.query("SELECT 1").fetch_bytes("CSV").unwrap();
        let mut end_out = Vec::new();
        let end_len = end_cursor.read_to_end(&mut end_out).await.unwrap();
        assert_eq!(end_len, 13);
        assert_eq!(end_out, b"hello world!\n");

        #[cfg(feature = "futures03")]
        {
            let mut fut_cursor = client.query("SELECT 1").fetch_bytes("CSV").unwrap();
            let mut fut_out = Vec::new();
            let fut_len = futures_util::io::copy(&mut fut_cursor, &mut fut_out)
                .await
                .unwrap();
            assert_eq!(fut_len, 13);
            assert_eq!(fut_out, b"hello world!\n");
        }
    }
}
