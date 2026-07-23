use bstr::ByteSlice;
use bytes::{BufMut, Bytes};
use futures_util::stream::{self, Stream, TryStreamExt};
use http_body_util::BodyExt as _;
use hyper::{
    StatusCode,
    body::{Body as _, Incoming},
};
use hyper_util::client::legacy::ResponseFuture as HyperResponseFuture;
use std::{
    future::{self, Future},
    pin::{Pin, pin},
    task::{Context, Poll},
};

#[cfg(feature = "lz4")]
use crate::compression::lz4::Lz4Decoder;
#[cfg(feature = "zstd")]
use crate::compression::zstd::ZstdHttpDecoder;
use crate::{
    compression::Compression,
    error::{Error, Result},
    query_summary::QuerySummary,
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
    pub(crate) fn new(response: HyperResponseFuture, compression: Compression) -> Self {
        let span = tracing::info_span!(
            "response",
            otel.status_code = tracing::field::Empty,
            otel.status_description = tracing::field::Empty,
            error.type = tracing::field::Empty,
            db.response_code = tracing::field::Empty,
        );

        Self::Waiting(Box::pin(
            collect_response(response, compression).instrument(span),
        ))
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
    compression: Compression,
) -> Result<(Chunks, Option<Box<QuerySummary>>)> {
    let response = response.await?;

    let status = response.status();
    let exception_code = response.headers().get("X-ClickHouse-Exception-Code");

    tracing::record_all!(
        tracing::Span::current(),
        // Note: not supposed to set `otel.status_code` unless an error occurs
        db.response.status_code = status.as_u16(),
    );

    if status == StatusCode::OK && exception_code.is_none() {
        let tag = response
            .headers()
            .get("X-ClickHouse-Exception-Tag")
            .map(|value| value.as_bytes().into());

        let summary = response
            .headers()
            .get("X-ClickHouse-Summary")
            .and_then(|v| v.to_str().ok())
            .and_then(QuerySummary::from_header)
            .map(Box::new); // More likely to be successful, start streaming.
        // It still can fail, but we'll handle it in `DetectDbException`.
        Ok((Chunks::new(response.into_body(), compression, tag), summary))
    } else {
        // An instantly failed request.
        let error = collect_bad_response(
            status,
            exception_code
                .and_then(|value| value.to_str().ok())
                .map(|code| format!("Code: {code}")),
            response.into_body(),
            compression,
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
    compression: Compression,
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
        Err(_) => return bad_response_or_typed(reason(status, exception_code)),
    };
    if raw_bytes.is_empty() {
        return bad_response_or_typed(reason(status, exception_code));
    }

    // Try to decompress the body, because CH uses compression even for errors.
    let stream = stream::once(future::ready(Result::<_>::Ok(raw_bytes.slice(..))));
    let stream = Decompress::new(stream, compression).map_ok(|chunk| chunk.data);

    // We're collecting already fetched chunks, thus only decompression errors can
    // be here. If decompression is failed, we should try the raw body because
    // it can be sent without any compression if some proxy is used, which
    // typically know nothing about CH params.
    let bytes = collect_bytes(stream).await.unwrap_or(raw_bytes);

    let reason = String::from_utf8(bytes.into())
        .map(|reason| reason.trim().into())
        // If we have a unreadable response, return standardised reason for the status code.
        .unwrap_or_else(|_| reason(status, exception_code));

    bad_response_or_typed(reason)
}

/// Returns [`Error::ServerError`] if `s` parses as a typed CH exception,
/// otherwise [`Error::BadResponse(s)`].
fn bad_response_or_typed(s: String) -> Error {
    parse_server_exception(&s).unwrap_or(Error::BadResponse(s))
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
    fn new(stream: Incoming, compression: Compression, exception_tag: Option<Box<[u8]>>) -> Self {
        let stream = IncomingStream(stream);
        let stream = Decompress::new(stream, compression);
        let stream = DetectDbException {
            stream,
            exception_tag,
        };
        Self {
            inner: Some(Box::new(stream)),
        }
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

enum Decompress<S> {
    Plain(S),
    #[cfg(feature = "lz4")]
    Lz4(Lz4Decoder<S>),
    #[cfg(feature = "zstd")]
    Zstd(ZstdHttpDecoder<S>),
}

impl<S> Decompress<S> {
    fn new(stream: S, compression: Compression) -> Self {
        match compression {
            Compression::None => Self::Plain(stream),
            #[cfg(feature = "lz4")]
            #[allow(deprecated)]
            Compression::Lz4 | Compression::Lz4Hc(_) => Self::Lz4(Lz4Decoder::new(stream)),
            #[cfg(feature = "zstd")]
            Compression::Zstd(_) => Self::Zstd(ZstdHttpDecoder::new(stream)),
        }
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
    exception_tag: Option<Box<[u8]>>,
}

impl<S> Stream for DetectDbException<S>
where
    S: Stream<Item = Result<Chunk>> + Unpin,
{
    type Item = Result<Chunk>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let res = Pin::new(&mut self.stream).poll_next(cx);

        if let Poll::Ready(Some(Ok(chunk))) = &res
            && let Some(err) = extract_exception(&chunk.data, self.exception_tag.as_deref())
        {
            err.record_in_current_span("response error");
            return Poll::Ready(Some(Err(err)));
        }

        res
    }
}

fn extract_exception(chunk: &[u8], tag: Option<&[u8]>) -> Option<Error> {
    // 25.11 introduced a new exception tagging format that's incompatible with the previous
    // https://github.com/ClickHouse/clickhouse-rs/issues/359
    if let Some(tag) = tag
        && chunk.ends_with(b"__exception__\r\n")
    {
        extract_exception_new(chunk, tag)
    } else if chunk.ends_with(b"))\n") {
        // `))\n` is very rare in real data, so it's fast dirty check.
        // In random data, it occurs with a probability of ~6*10^-8 only.
        extract_exception_old(chunk)
    } else {
        None
    }
}

/// Try to parse a ClickHouse server exception string into a typed
/// `Error::ServerError`. Returns `None` for unrecognised formats so the
/// caller can fall back to `Error::BadResponse` and preserve the raw text.
///
/// Recognised shapes:
/// - `"Code: NNN. DB::Exception: <message> (NAME) (version <v> (official build))"`
/// - `"Code: NNN. DB::NetException: <message> (NAME)"`
/// - `"Code: NNN"` (header-only fast-fail; `name = None`, `message = ""`)
pub(crate) fn parse_server_exception(s: &str) -> Option<Error> {
    let s = s.trim();
    let after_kw = s.strip_prefix("Code: ")?;

    // Numeric code.
    let digit_count = after_kw.bytes().take_while(u8::is_ascii_digit).count();
    if digit_count == 0 {
        return None;
    }
    let code: u32 = after_kw[..digit_count].parse().ok()?;
    let after_code = &after_kw[digit_count..];

    // Header-only "Code: NNN".
    if after_code.is_empty() {
        return Some(Error::ServerError {
            code,
            name: None,
            message: String::new(),
        });
    }

    // Expect ". DB::<Class>: <message> [(NAME)] [(version ...)]"
    //
    // We look for the first ':' after the class name (e.g. "Exception:" or
    // "NetException:") rather than the first ": " (colon + space). Some
    // CH versions and some proxies elide the space between the class name
    // colon and the message. Locking onto ": " causes the parser to walk
    // past the boundary and lose leading message text on those shapes.
    // Splitting on the first ':' and `trim_start()`ing handles both forms
    // identically.
    let body = after_code
        .strip_prefix('.')?
        .trim_start()
        .strip_prefix("DB::")?;
    let body = body.find(':').map(|i| body[i + 1..].trim_start())?;

    let (message, name) = strip_trailing_metadata(body);
    Some(Error::ServerError {
        code,
        name,
        message: message.trim().trim_end_matches('.').trim_end().to_string(),
    })
}

/// Walk parenthesised groups from the right of a server-exception body and
/// peel off `(version ...)` / `(... official build)` metadata, capturing the
/// first uppercase-only group as the exception NAME.
fn strip_trailing_metadata(body: &str) -> (&str, Option<String>) {
    let mut s = body.trim_end();
    let mut name: Option<String> = None;

    while s.ends_with(')') {
        // Find the matching '(' for the trailing ')' by counting nesting.
        let bytes = s.as_bytes();
        let mut depth: i32 = 0;
        let mut open_idx: Option<usize> = None;
        for i in (0..bytes.len()).rev() {
            match bytes[i] {
                b')' => depth += 1,
                b'(' => {
                    depth -= 1;
                    if depth == 0 {
                        open_idx = Some(i);
                        break;
                    }
                }
                _ => {}
            }
        }
        let Some(open) = open_idx else {
            return (s.trim_end(), name);
        };
        let inner = &s[open + 1..s.len() - 1];

        // CH exception NAME tags are SCREAMING_SNAKE_CASE: ASCII uppercase
        // letters and underscores, optionally with digits. A pure-digit
        // group like "(1)" or "(123)" is NOT a name — it's user data
        // (e.g. "Too many parts (300)") that happens to share the
        // `(...)` shape. Require at least one uppercase letter or
        // underscore so user data cannot be misread as the NAME tag.
        let is_name = !inner.is_empty()
            && inner
                .chars()
                .all(|c| c.is_ascii_uppercase() || c.is_ascii_digit() || c == '_')
            && inner
                .chars()
                .any(|c| c.is_ascii_uppercase() || c == '_');
        let is_version = inner.starts_with("version ") || inner.contains("official build");

        if name.is_none() && is_name {
            name = Some(inner.to_string());
            s = s[..open].trim_end();
        } else if is_version {
            s = s[..open].trim_end();
        } else {
            // Unrecognised parenthesised group — assume it's part of the message.
            return (s.trim_end(), name);
        }
    }
    (s.trim_end(), name)
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
    Some(parse_server_exception(&exception).unwrap_or_else(|| Error::BadResponse(exception.into())))
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
    let text = String::from_utf8_lossy(msg);
    let trimmed = text.trim();
    Some(parse_server_exception(trimmed).unwrap_or_else(|| Error::BadResponse(trimmed.to_string())))
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
    let cases = [
        (
            "Code: 159. DB::Exception: Timeout exceeded: elapsed 1.2 seconds, maximum: 0.1. (TIMEOUT_EXCEEDED) (version 24.10.1.2812 (official build))",
            159u32,
            Some("TIMEOUT_EXCEEDED"),
            "Timeout exceeded: elapsed 1.2 seconds, maximum: 0.1",
        ),
        (
            "Code: 210. DB::NetException: I/O error: Broken pipe, while writing to socket (127.0.0.1:9000 -> 127.0.0.1:54646). (NETWORK_ERROR) (version 23.8.8.20 (official build))",
            210,
            Some("NETWORK_ERROR"),
            "I/O error: Broken pipe, while writing to socket (127.0.0.1:9000 -> 127.0.0.1:54646)",
        ),
    ];

    for (raw, expected_code, expected_name, expected_msg) in cases {
        let chunk = format!("{raw}\n");
        let err = extract_exception(chunk.as_bytes(), None).expect("failed to extract exception");
        match err {
            Error::ServerError {
                code,
                name,
                message,
            } => {
                assert_eq!(code, expected_code, "raw: {raw}");
                assert_eq!(name.as_deref(), expected_name, "raw: {raw}");
                assert_eq!(message, expected_msg, "raw: {raw}");
            }
            other => panic!("expected ServerError, got: {other:?} (raw: {raw})"),
        }
    }
}

#[test]
fn it_extracts_exception_new() {
    let tag = b"rnywyenlaeqynhmu";
    let chunk = b"\r\n__exception__\r\nrnywyenlaeqynhmu\r\nCode: 159. DB::Exception: Timeout exceeded: elapsed 126.147987 ms, maximum: 100 ms. (TIMEOUT_EXCEEDED) (version 25.12.1.649 (official build))\n142 rnywyenlaeqynhmu\r\n__exception__\r\n";

    let err = extract_exception(chunk, Some(tag)).expect("failed to extract exception");
    match err {
        Error::ServerError {
            code,
            name,
            message,
        } => {
            assert_eq!(code, 159);
            assert_eq!(name.as_deref(), Some("TIMEOUT_EXCEEDED"));
            assert_eq!(
                message,
                "Timeout exceeded: elapsed 126.147987 ms, maximum: 100 ms"
            );
        }
        other => panic!("expected ServerError, got: {other:?}"),
    }
}

#[test]
fn parse_server_exception_handles_header_only_form() {
    // X-ClickHouse-Exception-Code only — no body decoded.
    match parse_server_exception("Code: 252") {
        Some(Error::ServerError {
            code,
            name,
            message,
        }) => {
            assert_eq!(code, 252);
            assert_eq!(name, None);
            assert_eq!(message, "");
        }
        other => panic!("expected ServerError, got: {other:?}"),
    }
}

#[test]
fn parse_server_exception_handles_too_many_parts() {
    // The motivating ClickFlow case (`is_too_many_parts` substring matcher).
    let raw = "Code: 252. DB::Exception: Too many parts (300). \
               (TOO_MANY_PARTS) (version 24.1.1.234 (official build))";
    match parse_server_exception(raw) {
        Some(Error::ServerError {
            code,
            name,
            message,
        }) => {
            assert_eq!(code, 252);
            assert_eq!(name.as_deref(), Some("TOO_MANY_PARTS"));
            assert_eq!(message, "Too many parts (300)");
        }
        other => panic!("expected ServerError, got: {other:?}"),
    }
}

#[test]
fn parse_server_exception_returns_none_for_non_clickhouse_responses() {
    // Proxy / load-balancer error pages should fall back to BadResponse.
    assert!(parse_server_exception("502 Bad Gateway").is_none());
    assert!(parse_server_exception("<html>nginx error</html>").is_none());
    assert!(parse_server_exception("Code: NaN. malformed").is_none());
}

#[test]
fn parse_server_exception_no_name_tag() {
    // Some CH versions / proxies trim the (NAME) tag.
    let raw = "Code: 47. DB::Exception: Unknown identifier: foo.";
    match parse_server_exception(raw) {
        Some(Error::ServerError {
            code,
            name,
            message,
        }) => {
            assert_eq!(code, 47);
            assert_eq!(name, None);
            assert_eq!(message, "Unknown identifier: foo");
        }
        other => panic!("expected ServerError, got: {other:?}"),
    }
}

#[test]
fn parse_server_exception_compressed_body_falls_back() {
    // Edge: compressed (zstd / lz4) error body — bytes-as-utf8 is gibberish
    // that does NOT start with "Code: ". Caller must fall back to
    // Error::BadResponse with the raw text rather than producing a typed
    // variant from garbage. We assert at the parser level that gibberish
    // returns None; the wire-path callers (collect_bad_response,
    // extract_exception_old, extract_exception_new) all use
    // `.unwrap_or(Error::BadResponse(...))`, so None → BadResponse.
    // zstd frame magic + a few following bytes; convert via utf8_lossy
    // because raw \x in str literals must be ASCII.
    let zstd_bytes = b"\x28\xb5\x2f\xfd\x00\x58\x99\x07\x00";
    let zstd_magic_prefix = String::from_utf8_lossy(zstd_bytes);
    assert!(parse_server_exception(&zstd_magic_prefix).is_none());
    // lz4 frame magic likewise.
    let lz4_bytes = b"\x04\x22\x4d\x18\x40\x40\xc0";
    let lz4_magic_prefix = String::from_utf8_lossy(lz4_bytes);
    assert!(parse_server_exception(&lz4_magic_prefix).is_none());
}

#[test]
fn parse_server_exception_tolerates_missing_space_after_inner_colon() {
    // Edge: some CH versions (and certain proxies' re-formatted bodies)
    // emit "DB::Exception:<message>" with no space after the inner colon.
    // The parser splits on the first ':' after the class prefix and
    // `trim_start`s the remainder, so both `Exception: msg` and
    // `Exception:msg` produce identical typed output: same code, same
    // (NAME) tag, full message preserved.
    let raw_no_space = "Code: 159. DB::Exception:Timeout exceeded: elapsed 100ms, maximum: 50ms (TIMEOUT_EXCEEDED)";
    let raw_with_space = "Code: 159. DB::Exception: Timeout exceeded: elapsed 100ms, maximum: 50ms (TIMEOUT_EXCEEDED)";

    fn unpack(e: Error) -> (u32, Option<String>, String) {
        match e {
            Error::ServerError {
                code,
                name,
                message,
            } => (code, name, message),
            other => panic!("expected ServerError, got: {other:?}"),
        }
    }

    let no_space = unpack(parse_server_exception(raw_no_space).expect("no-space form must parse"));
    let with_space = unpack(
        parse_server_exception(raw_with_space).expect("canonical form must parse"),
    );
    assert_eq!(
        no_space, with_space,
        "no-space and canonical forms must produce identical (code, name, message)",
    );

    let (code, name, message) = no_space;
    assert_eq!(code, 159);
    assert_eq!(name.as_deref(), Some("TIMEOUT_EXCEEDED"));
    assert!(
        message.starts_with("Timeout exceeded"),
        "leading message text must not be truncated; got {message:?}",
    );
    assert!(
        message.contains("maximum: 50ms"),
        "trailing colon-space inside the message must not be re-truncated; got {message:?}",
    );
}

#[test]
fn parse_server_exception_does_not_strip_user_data_parens() {
    // Edge: user-supplied data containing parentheses should NOT be
    // stripped as the (NAME) tag. The name-detection rule is
    // "ASCII uppercase + digits + underscore, with at least one
    // uppercase letter or underscore"; pure-digit groups, mixed-case
    // content, and arbitrary text inside the trailing paren group must
    // all stay in the message.

    // Pure-digit trailing paren — must NOT be misread as NAME.
    let raw_digit_only = "Code: 252. DB::Exception: Too many parts (300).";
    match parse_server_exception(raw_digit_only) {
        Some(Error::ServerError {
            code,
            name,
            message,
        }) => {
            assert_eq!(code, 252);
            assert_eq!(
                name, None,
                "pure-digit paren group must not be stripped as NAME; got name={name:?}",
            );
            assert!(
                message.contains("(300)"),
                "user-data paren must remain in message; got {message:?}",
            );
        }
        other => panic!("expected ServerError, got: {other:?}"),
    }

    let raw = "Code: 36. DB::Exception: Failed to parse path '/etc/data/file (1).csv'. (BAD_ARGUMENTS) (version 25.1.1.111 (official build))";
    match parse_server_exception(raw) {
        Some(Error::ServerError {
            code,
            name,
            message,
        }) => {
            assert_eq!(code, 36);
            assert_eq!(name.as_deref(), Some("BAD_ARGUMENTS"));
            assert!(
                message.contains("(1).csv"),
                "user-data parens must remain in message; got {message:?}",
            );
            // Ensure we did NOT accidentally strip "(1)" as a name tag —
            // that group is digits-only and not uppercase, so it must be
            // left alone.
            assert!(
                !message.ends_with(".csv'."),
                "trailing period of message should be trimmed by parser; got {message:?}",
            );
        }
        other => panic!("expected ServerError, got: {other:?}"),
    }

    // Variant: NAME-shaped tag mixed with arbitrary message-side parens.
    let raw_mixed = "Code: 36. DB::Exception: Bad path '/srv/x (y).csv' (mode: O_RDWR). (BAD_ARGUMENTS)";
    match parse_server_exception(raw_mixed) {
        Some(Error::ServerError {
            code,
            name,
            message,
        }) => {
            assert_eq!(code, 36);
            assert_eq!(name.as_deref(), Some("BAD_ARGUMENTS"));
            assert!(message.contains("(y).csv"), "got {message:?}");
            assert!(message.contains("(mode: O_RDWR)"), "got {message:?}");
        }
        other => panic!("expected ServerError, got: {other:?}"),
    }
}

#[test]
fn parse_server_exception_header_code_with_proxy_body_falls_back() {
    // Edge: the header-only fast-fail path can collide with a proxy that
    // also emits text alongside the X-ClickHouse-Exception-Code header.
    // If the post-code text doesn't match ". DB::<Class>: ...", we must
    // return None so the caller emits BadResponse with the full raw body
    // — preserving the proxy's text for the operator.
    let raw_with_proxy_tail = "Code: 252 [nginx upstream timeout]";
    assert!(parse_server_exception(raw_with_proxy_tail).is_none());

    let raw_with_html = "Code: 502\n<html><body>Bad Gateway</body></html>";
    assert!(parse_server_exception(raw_with_html).is_none());

    // Sanity: bare "Code: 252" still produces the typed header-only form.
    assert!(matches!(
        parse_server_exception("Code: 252"),
        Some(Error::ServerError { code: 252, name: None, .. })
    ));
}
