#[cfg(feature = "lz4")]
use crate::compression::lz4::Lz4HttpDecoder;
#[cfg(feature = "zstd")]
use crate::compression::zstd::ZstdHttpDecoder;
use crate::{
    error::{Error, Result},
    query_summary::QuerySummary,
};
use bstr::ByteSlice;
use bytes::{BufMut, Bytes, BytesMut};
use futures_util::stream::{self, Stream, TryStreamExt};
use http_body_util::BodyExt as _;
use hyper::header::CONTENT_ENCODING;
use hyper::{
    HeaderMap, StatusCode,
    body::{Body as _, Incoming},
};
use hyper_util::client::legacy::ResponseFuture as HyperResponseFuture;
use std::{
    future::{self, Future},
    pin::{Pin, pin},
    task::{Context, Poll},
};
use tracing::Instrument;

// === Response ===

pub(crate) enum Response {
    // Headers haven't been received yet.
    // `Box<_>` improves performance by reducing the size of the whole future.
    Waiting(ResponseFuture),
    // Headers have been received, streaming the body.
    Loading(Chunks),
}

pub(crate) type ResponseFuture =
    Pin<Box<dyn Future<Output = Result<(Chunks, Option<Box<QuerySummary>>)>> + Send>>;

impl Response {
    pub(crate) fn new(response: HyperResponseFuture) -> Self {
        let span = tracing::info_span!(
            "response",
            otel.status_code = tracing::field::Empty,
            otel.status_description = tracing::field::Empty,
            error.type = tracing::field::Empty,
            db.response_code = tracing::field::Empty,
        );

        Self::Waiting(Box::pin(collect_response(response).instrument(span)))
    }

    pub(crate) fn into_future(self) -> ResponseFuture {
        match self {
            Self::Waiting(future) => future,
            Self::Loading(_) => panic!("response is already streaming"),
        }
    }

    pub(crate) async fn finish(&mut self) -> Result<()> {
        let chunks = loop {
            match self {
                Self::Waiting(future) => {
                    let (chunks, _summary) = future.await?;
                    *self = Self::Loading(chunks);
                }
                Self::Loading(chunks) => break chunks,
            }
        };

        while chunks.try_next().await?.is_some() {}
        Ok(())
    }
}

async fn collect_response(
    response: HyperResponseFuture,
) -> Result<(Chunks, Option<Box<QuerySummary>>)> {
    let (parts, body) = response.await?.into_parts();

    let status = parts.status;
    let exception_code = parts.headers.get("X-ClickHouse-Exception-Code");

    tracing::record_all!(
        tracing::Span::current(),
        // Note: not supposed to set `otel.status_code` unless an error occurs
        db.response.status_code = status.as_u16(),
    );

    if status == StatusCode::OK && exception_code.is_none() {
        let tag = parts
            .headers
            .get("X-ClickHouse-Exception-Tag")
            .map(|value| value.as_bytes().into());

        let summary = parts
            .headers
            .get("X-ClickHouse-Summary")
            .and_then(|v| v.to_str().ok())
            .and_then(QuerySummary::from_header)
            .map(Box::new); // More likely to be successful, start streaming.
        // It still can fail, but we'll handle it in `DetectDbException`.
        Ok((Chunks::new(body, &parts.headers, tag)?, summary))
    } else {
        // An instantly failed request.
        let error = collect_bad_response(
            status,
            exception_code
                .and_then(|value| value.to_str().ok())
                .map(|code| format!("Code: {code}")),
            body,
            parts.headers,
        )
        .await;

        error.record_in_current_span("response error");

        Err(error)
    }
}

#[cold]
#[inline(never)]
async fn collect_bad_response(
    status: StatusCode,
    exception_code: Option<String>,
    body: Incoming,
    headers: HeaderMap,
) -> Error {
    // Collect the whole body into one contiguous buffer to simplify handling.
    // Only network errors can occur here and we return them instead of status code
    // because it means the request can be repeated to get a more detailed error.
    //
    // TODO: we don't implement any length checks and a malicious peer (e.g. MITM)
    //       might make us consume arbitrary amounts of memory.
    let raw_bytes = match body.collect().await {
        Ok(collected) => collected.to_bytes(),
        // If we can't collect the body, return standardised reason for the status code.
        Err(_) => return Error::BadResponse(reason(status, exception_code)),
    };
    if raw_bytes.is_empty() {
        return Error::BadResponse(reason(status, exception_code));
    }

    // Try to decompress the body, because CH uses compression even for errors.
    let stream = stream::once(future::ready(Result::<_>::Ok(raw_bytes.slice(..))));
    let bytes = match Decompress::new(stream, &headers) {
        // We're collecting already fetched chunks, thus only decompression errors can
        // be here. If decompression is failed, we should try the raw body because
        // it can be sent without any compression if some proxy is used, which
        // typically know nothing about CH params.
        Ok(stream) => collect_bytes(stream.map_ok(|chunk| chunk.data))
            .await
            .unwrap_or(raw_bytes),
        Err(e) => {
            tracing::warn!("decompression error in collecting error-response: {e}");
            raw_bytes
        }
    };

    let reason = String::from_utf8(bytes.into())
        .map(|reason| reason.trim().into())
        // If we have a unreadable response, return standardised reason for the status code.
        .unwrap_or_else(|_| reason(status, exception_code));

    Error::BadResponse(reason)
}

async fn collect_bytes(stream: impl Stream<Item = Result<Bytes>>) -> Result<Bytes> {
    let mut stream = pin!(stream);

    let mut bytes = Vec::new();

    // TODO: avoid extra copying if there is only one chunk in the stream.
    while let Some(chunk) = stream.try_next().await? {
        bytes.put(chunk);
    }

    Ok(bytes.into())
}

fn reason(status: StatusCode, exception_code: Option<String>) -> String {
    exception_code.unwrap_or_else(|| {
        format!(
            "{} {}",
            status.as_str(),
            status.canonical_reason().unwrap_or("<unknown>"),
        )
    })
}

// === Chunks ===

pub(crate) struct Chunk {
    pub(crate) data: Bytes,
    pub(crate) net_size: usize,
}

// * Uses `Option<_>` to make this stream fused.
// * Uses `Box<_>` in order to reduce the size of cursors.
pub(crate) struct Chunks {
    inner: Option<Box<DetectDbException<Decompress<IncomingStream>>>>,
}

impl Chunks {
    fn new(
        stream: Incoming,
        headers: &HeaderMap,
        exception_tag: Option<Box<[u8]>>,
    ) -> Result<Self> {
        let stream = IncomingStream(stream);
        let stream = Decompress::new(stream, headers)?;
        let stream = DetectDbException::new(stream, exception_tag);
        Ok(Self {
            inner: Some(Box::new(stream)),
        })
    }

    pub(crate) fn empty() -> Self {
        Self { inner: None }
    }

    #[cfg(feature = "futures03")]
    pub(crate) fn is_terminated(&self) -> bool {
        self.inner.is_none()
    }
}

impl Stream for Chunks {
    type Item = Result<Chunk>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // We use `take()` to make the stream fused, including the case of panics.
        if let Some(mut stream) = self.inner.take() {
            let res = Pin::new(&mut stream).poll_next(cx);

            if matches!(res, Poll::Pending | Poll::Ready(Some(Ok(_)))) {
                self.inner = Some(stream);
            }

            res
        } else {
            Poll::Ready(None)
        }
    }

    // `size_hint()` is unimplemented because unused.
}

// === IncomingStream ===

// * Produces bytes from incoming data frames.
// * Skips trailer frames (CH doesn't use them for now).
// * Converts hyper errors to our own.
struct IncomingStream(Incoming);

impl Stream for IncomingStream {
    type Item = Result<Bytes>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut incoming = Pin::new(&mut self.get_mut().0);

        loop {
            break match incoming.as_mut().poll_frame(cx) {
                Poll::Ready(Some(Ok(frame))) => match frame.into_data() {
                    Ok(bytes) => Poll::Ready(Some(Ok(bytes))),
                    Err(_frame) => continue,
                },
                Poll::Ready(Some(Err(err))) => Poll::Ready(Some(Err(err.into()))),
                Poll::Ready(None) => Poll::Ready(None),
                Poll::Pending => Poll::Pending,
            };
        }
    }
}

// === Decompress ===
#[allow(clippy::large_enum_variant)] // the structure ultimately gets boxed in `Chunks`
enum Decompress<S> {
    Plain(S),
    #[cfg(feature = "lz4")]
    Lz4(Lz4HttpDecoder<S>),
    #[cfg(feature = "zstd")]
    Zstd(ZstdHttpDecoder<S>),
}

impl<S> Decompress<S> {
    fn new(stream: S, headers: &HeaderMap) -> Result<Self> {
        let Some(content_encoding) = headers.get(CONTENT_ENCODING) else {
            return Ok(Self::Plain(stream));
        };

        if content_encoding
            .as_bytes()
            .trim_ascii()
            .eq_ignore_ascii_case(b"zstd")
        {
            #[cfg(feature = "zstd")]
            return Ok(Self::Zstd(ZstdHttpDecoder::new(stream)));

            #[cfg(not(feature = "zstd"))]
            return Err(Error::decompression(
                "ClickHouse server or proxy returned `Content-Encoding: zstd` but the `zstd` feature of the `clickhouse` crate is not enabled",
            ));
        }

        if content_encoding
            .as_bytes()
            .trim_ascii()
            .eq_ignore_ascii_case(b"lz4")
        {
            #[cfg(feature = "lz4")]
            return Ok(Self::Lz4(Lz4HttpDecoder::new(stream)));

            #[cfg(not(feature = "lz4"))]
            return Err(Error::decompression(
                "ClickHouse server or proxy returned `Content-Encoding: lz4` but the `lz4` feature of the `clickhouse` crate is not enabled",
            ));
        }

        Err(Error::decompression(format!(
            "ClickHouse server or proxy returned unknown `Content-Encoding`: {content_encoding:?}"
        )))
    }
}

impl<S> Stream for Decompress<S>
where
    S: Stream<Item = Result<Bytes>> + Unpin,
{
    type Item = Result<Chunk>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match &mut *self {
            Self::Plain(stream) => Pin::new(stream)
                .poll_next(cx)
                .map_ok(|bytes| Chunk {
                    net_size: bytes.len(),
                    data: bytes,
                })
                .map_err(Into::into),
            #[cfg(feature = "lz4")]
            Self::Lz4(stream) => Pin::new(stream).poll_next(cx),
            #[cfg(feature = "zstd")]
            Self::Zstd(stream) => Pin::new(stream).poll_next(cx),
        }
    }
}

// === DetectDbException ===

struct DetectDbException<S> {
    stream: S,
    /// Only used when the server sent an exception tag (25.11+).
    tagged: Option<Box<TaggedState>>,
}

/// State used to find a tagged exception block that can be split across several chunks.
///
/// Format of the block, see `WriteBufferFromHTTPServerResponse::cancelWithException`:
/// ```text
///   \r\n__exception__\r\n<tag>\r\n<message>\n<message length> <tag>\r\n__exception__\r\n
/// ```
struct TaggedState {
    tag: Box<[u8]>,
    /// `\r\n__exception__\r\n<tag>\r\n`
    opening: Box<[u8]>,
    /// `<tag>\r\n__exception__\r\n`
    closing: Box<[u8]>,
    /// Bytes held back from previously received chunks: either a tail that can be
    /// the beginning of the opening marker or, if `in_exception`, the exception block so far.
    held: BytesMut,
    /// Network size of the held bytes, reported with the next chunk.
    held_net_size: usize,
    /// Whether the opening marker has been found and the rest of the block is being collected.
    in_exception: bool,
    /// Whether the inner stream has ended, but the held bytes have been returned.
    finished: bool,
}

impl<S> DetectDbException<S> {
    fn new(stream: S, exception_tag: Option<Box<[u8]>>) -> Self {
        let tagged = exception_tag.as_deref().map(|tag| {
            Box::new(TaggedState {
                tag: tag.into(),
                opening: [b"\r\n__exception__\r\n", tag, b"\r\n"].concat().into(),
                closing: [tag, b"\r\n__exception__\r\n"].concat().into(),
                held: BytesMut::new(),
                held_net_size: 0,
                in_exception: false,
                finished: false,
            })
        });

        Self { stream, tagged }
    }
}

impl<S> Stream for DetectDbException<S>
where
    S: Stream<Item = Result<Chunk>> + Unpin,
{
    type Item = Result<Chunk>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = &mut *self;

        let Some(state) = this.tagged.as_deref_mut() else {
            // An old server, which writes exceptions without a tag.
            let res = Pin::new(&mut this.stream).poll_next(cx);

            if let Poll::Ready(Some(Ok(chunk))) = &res
                && let Some(err) = extract_exception(&chunk.data, None)
            {
                err.record_in_current_span("response error");
                return Poll::Ready(Some(Err(err)));
            }

            return res;
        };

        loop {
            if state.finished {
                return Poll::Ready(None);
            }

            let res = match Pin::new(&mut this.stream).poll_next(cx) {
                Poll::Ready(res) => res,
                Poll::Pending => return Poll::Pending,
            };

            let res = match res {
                Some(Ok(chunk)) => match state.push(chunk) {
                    Some(res) => res,
                    None => continue,
                },
                // The server doesn't finish the response properly after writing an exception,
                // so the transport may fail before the closing marker has been received.
                Some(Err(err)) if !state.in_exception => return Poll::Ready(Some(Err(err))),
                Some(Err(_)) | None => match state.finish() {
                    Some(res) => res,
                    None => return Poll::Ready(None),
                },
            };

            if let Err(err) = &res {
                err.record_in_current_span("response error");
            }

            return Poll::Ready(Some(res));
        }
    }
}

impl TaggedState {
    /// Returns `None` if more data is needed.
    fn push(&mut self, chunk: Chunk) -> Option<Result<Chunk>> {
        self.held_net_size += chunk.net_size;

        if self.in_exception {
            self.held.extend_from_slice(&chunk.data);
            return self.take_complete_exception().map(Err);
        }

        let data = if self.held.is_empty() {
            chunk.data
        } else {
            self.held.extend_from_slice(&chunk.data);
            self.held.split().freeze()
        };

        if let Some(start) = data.find(&self.opening) {
            self.in_exception = true;
            self.held.extend_from_slice(&data[start..]);

            if let Some(err) = self.take_complete_exception() {
                // Like before, data in the same chunk as the exception is discarded.
                return Some(Err(err));
            }

            return (start > 0).then(|| Ok(self.make_chunk(data.slice(..start))));
        }

        // Hold back a tail that can be the beginning of the opening marker.
        let keep = partial_prefix_len(&data, &self.opening);
        let rows = data.slice(..data.len() - keep);
        self.held.extend_from_slice(&data[rows.len()..]);

        (!rows.is_empty()).then(|| Ok(self.make_chunk(rows)))
    }

    /// Called when the stream ended or failed.
    fn finish(&mut self) -> Option<Result<Chunk>> {
        self.finished = true;

        if self.in_exception {
            self.in_exception = false;
            let block = self.held.split();
            // The closing marker is missing, return the message we have got.
            let msg = &block[self.opening.len()..];
            return Some(Err(Error::BadResponse(
                String::from_utf8_lossy(msg).trim().into(),
            )));
        }

        if self.held.is_empty() {
            return None;
        }

        let rest = self.held.split().freeze();
        Some(Ok(self.make_chunk(rest)))
    }

    fn take_complete_exception(&mut self) -> Option<Error> {
        let pos = self.held[self.opening.len()..].find(&self.closing)?;
        let end = self.opening.len() + pos + self.closing.len();
        let err = extract_exception_new(&self.held[..end], &self.tag).unwrap_or_else(|| {
            Error::BadResponse(
                String::from_utf8_lossy(&self.held[self.opening.len()..end - self.closing.len()])
                    .trim()
                    .into(),
            )
        });

        self.in_exception = false;
        self.held.clear();
        Some(err)
    }

    fn make_chunk(&mut self, data: Bytes) -> Chunk {
        Chunk {
            data,
            net_size: std::mem::take(&mut self.held_net_size),
        }
    }
}

/// Returns the length of the longest suffix of `data` that is a proper prefix of `marker`.
fn partial_prefix_len(data: &[u8], marker: &[u8]) -> usize {
    let tail_start = data.len().saturating_sub(marker.len() - 1);

    (tail_start..data.len())
        .find(|&i| marker.starts_with(&data[i..]))
        .map_or(0, |i| data.len() - i)
}

fn extract_exception(chunk: &[u8], tag: Option<&[u8]>) -> Option<Error> {
    // 25.11 introduced a new exception tagging format that's incompatible with the previous
    // https://github.com/ClickHouse/clickhouse-rs/issues/359
    if let Some(tag) = tag {
        // A server that sent a tag writes exceptions only in the new format, so text in the data
        // that merely looks like an old exception must not end the stream.
        chunk
            .ends_with(b"__exception__\r\n")
            .then(|| extract_exception_new(chunk, tag))
            .flatten()
    } else if chunk.ends_with(b"))\n") {
        // `))\n` is very rare in real data, so it's fast dirty check.
        // In random data, it occurs with a probability of ~6*10^-8 only.
        extract_exception_old(chunk)
    } else {
        None
    }
}

// Format:
// ```
//   <data>Code: <code>. DB::Exception: <desc> (version <version> (official build))\n
// ```
#[cold]
#[inline(never)]
fn extract_exception_old(chunk: &[u8]) -> Option<Error> {
    let index = chunk.rfind(b"Code:")?;

    if !(chunk[index..].contains_str(b"DB::") && chunk[index..].contains_str(b"Exception:")) {
        return None;
    }

    let exception = String::from_utf8_lossy(&chunk[index..chunk.len() - 1]);
    Some(Error::BadResponse(exception.into()))
}

// https://github.com/ClickHouse/ClickHouse/blob/4eaa92852bac117e95f28abe61237b0257d939d6/src/Server/HTTP/WriteBufferFromHTTPServerResponse.cpp#L347-L357
#[cold]
#[inline(never)]
fn extract_exception_new(chunk: &[u8], tag: &[u8]) -> Option<Error> {
    // Strip the chunk backwards until we get to the `<message length>`
    let rem = chunk
        .strip_suffix(b"\r\n__exception__\r\n")?
        .strip_suffix(tag)?
        .strip_suffix(b" ")?;

    // `<message length>` is *NOT* 8 bytes, because it's actually an integer formatted as text:
    // https://github.com/ClickHouse/ClickHouse/blob/4eaa92852bac117e95f28abe61237b0257d939d6/src/Server/HTTP/WriteBufferFromHTTPServerResponse.cpp#L376
    //
    // This means we actually need to search for the `\n` that's added to terminate the message:
    // https://github.com/ClickHouse/ClickHouse/blob/4eaa92852bac117e95f28abe61237b0257d939d6/src/Server/HTTP/WriteBufferFromHTTPServerResponse.cpp#L373-L374
    let msg_len_start = rem.rfind(b"\n")? + 1;

    // `msg_len_start` should always be either in-bounds or just past the end
    let msg_len = match parse_msg_len(&rem[msg_len_start..]) {
        Ok(msg_len) => msg_len,
        // At this point we can be fairly certain we've found the exception tag,
        // so it's better to fail with an error than continue.
        Err(e) => return Some(e),
    };

    // Note: checked operations in case `msg_len` is incorrect
    let Some(msg) = msg_len_start
        .checked_sub(msg_len)
        .and_then(|msg_start| rem.get(msg_start..msg_len_start))
    else {
        return Some(Error::Other(
            format!("found exception tag in response but message length was invalid: {msg_len} (chunk len: {})", chunk.len())
                .into(),
        ));
    };

    // We shouldn't discard the exception message if it fails to validate as UTF-8
    Some(Error::BadResponse(
        String::from_utf8_lossy(msg).trim().into(),
    ))
}

// FIXME: this can be replaced with `usize::from_ascii()` when stable
// https://github.com/rust-lang/rust/issues/134821
fn parse_msg_len(len_bytes: &[u8]) -> Result<usize, Error> {
    let len_utf8 = str::from_utf8(len_bytes).map_err(|e| {
        Error::Other(
            format!("found exception tag in response but failed to parse message length: {e}")
                .into(),
        )
    })?;

    len_utf8.parse().map_err(|e| {
        Error::Other(
            format!("found exception tag in response but failed to parse message length {len_utf8:?}: {e}")
                .into(),
        )
    })
}

#[test]
fn it_extracts_exception_old() {
    let errors = [
        "Code: 159. DB::Exception: Timeout exceeded: elapsed 1.2 seconds, maximum: 0.1. (TIMEOUT_EXCEEDED) (version 24.10.1.2812 (official build))",
        "Code: 210. DB::NetException: I/O error: Broken pipe, while writing to socket (127.0.0.1:9000 -> 127.0.0.1:54646). (NETWORK_ERROR) (version 23.8.8.20 (official build))",
    ];

    for error in errors {
        let chunk = format!("{error}\n");
        let err = extract_exception(chunk.as_bytes(), None).expect("failed to extract exception");
        assert_eq!(err.to_string(), format!("bad response: {error}"));
    }
}

#[test]
fn it_extracts_exception_new() {
    let tag = b"rnywyenlaeqynhmu";
    let chunk = b"\r\n__exception__\r\nrnywyenlaeqynhmu\r\nCode: 159. DB::Exception: Timeout exceeded: elapsed 126.147987 ms, maximum: 100 ms. (TIMEOUT_EXCEEDED) (version 25.12.1.649 (official build))\n142 rnywyenlaeqynhmu\r\n__exception__\r\n";
    let error = "Code: 159. DB::Exception: Timeout exceeded: elapsed 126.147987 ms, maximum: 100 ms. (TIMEOUT_EXCEEDED) (version 25.12.1.649 (official build))";

    let err = extract_exception(chunk, Some(tag)).expect("failed to extract exception");
    assert_eq!(err.to_string(), format!("bad response: {error}"));
}

#[test]
fn it_ignores_old_exception_text_when_tagged() {
    // A string column can end a chunk with text shaped like an old-format exception.
    let chunk = b"\x05helloCode: 202. DB::Exception: Too many simultaneous queries. (TOO_MANY_SIMULTANEOUS_QUERIES) (version 25.12.1.649 (official build))\n";

    assert!(extract_exception(chunk, Some(b"rnywyenlaeqynhmu")).is_none());
    assert!(extract_exception(chunk, None).is_some());
}

#[cfg(test)]
fn detect_exception_in_chunks(chunks: &[&[u8]], tag: &[u8]) -> (Vec<u8>, Option<Error>) {
    let chunks = chunks
        .iter()
        .map(|data| {
            Ok(Chunk {
                data: Bytes::copy_from_slice(data),
                net_size: data.len(),
            })
        })
        .collect::<Vec<_>>();

    let mut stream = DetectDbException::new(stream::iter(chunks), Some(tag.into()));
    let mut cx = Context::from_waker(std::task::Waker::noop());
    let mut data = Vec::new();

    loop {
        match Pin::new(&mut stream).poll_next(&mut cx) {
            Poll::Ready(Some(Ok(chunk))) => {
                assert!(!chunk.data.is_empty());
                data.extend_from_slice(&chunk.data);
            }
            Poll::Ready(Some(Err(err))) => return (data, Some(err)),
            Poll::Ready(None) => return (data, None),
            Poll::Pending => unreachable!("`stream::iter` is always ready"),
        }
    }
}

#[test]
fn it_extracts_exception_new_split_across_chunks() {
    // https://github.com/ClickHouse/clickhouse-rs/issues/498
    let tag = b"rnywyenlaeqynhmu";
    let rows = b"{\"number\":\"0\"}\n{\"number\":\"1\"}\n";
    let error = "Code: 395. DB::Exception: v 1. (FUNCTION_THROW_IF_VALUE_IS_NON_ZERO) (version 25.12.1.649 (official build))";
    let block = format!(
        "\r\n__exception__\r\nrnywyenlaeqynhmu\r\n{error}\n{} rnywyenlaeqynhmu\r\n__exception__\r\n",
        error.len() + 1
    );
    let body = [&rows[..], block.as_bytes()].concat();

    for cut in 0..=body.len() {
        let (data, err) = detect_exception_in_chunks(&[&body[..cut], &body[cut..]], tag);
        let err = err.unwrap_or_else(|| panic!("no exception detected, cut at {cut}"));
        assert_eq!(
            err.to_string(),
            format!("bad response: {error}"),
            "cut at {cut}"
        );
        assert!(rows.starts_with(&data), "cut at {cut}");
    }

    // Byte by byte.
    let chunks = body.chunks(1).collect::<Vec<_>>();
    let (data, err) = detect_exception_in_chunks(&chunks, tag);
    assert_eq!(err.unwrap().to_string(), format!("bad response: {error}"));
    assert_eq!(data, rows);
}

#[test]
fn it_extracts_truncated_exception_new() {
    // The stream ends before the closing marker.
    let tag = b"rnywyenlaeqynhmu";
    let body = b"\x01\x02\r\n__exception__\r\nrnywyenlaeqynhmu\r\nCode: 395. DB::Exception: v 1.\n32 rnywye";

    let (data, err) = detect_exception_in_chunks(&[&body[..10], &body[10..]], tag);
    assert_eq!(data, b"\x01\x02");
    assert_eq!(
        err.unwrap().to_string(),
        "bad response: Code: 395. DB::Exception: v 1.\n32 rnywye"
    );
}

#[test]
fn it_keeps_data_resembling_exception_marker() {
    let tag = b"rnywyenlaeqynhmu";
    // Ends with a prefix of the opening marker, but it's just data.
    let body = b"\x05hello\r\n__exception__\r\nrnywye";

    for cut in 0..=body.len() {
        let (data, err) = detect_exception_in_chunks(&[&body[..cut], &body[cut..]], tag);
        assert!(err.is_none(), "cut at {cut}");
        assert_eq!(data, body, "cut at {cut}");
    }

    // Another tag.
    let body =
        b"\r\n__exception__\r\nqwertyuiopasdfgh\r\nmsg\n4 qwertyuiopasdfgh\r\n__exception__\r\n";
    let (data, err) = detect_exception_in_chunks(&[body], tag);
    assert!(err.is_none());
    assert_eq!(data, body);
}
