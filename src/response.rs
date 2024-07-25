use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use bstr::ByteSlice;
use bytes::{BufMut, Bytes};
use futures::{
    future,
    stream::{self, Stream, TryStreamExt},
};
use http_body_util::BodyExt as _;
use hyper::{
    body::{Body as _, Incoming},
    StatusCode,
};
use hyper_util::client::legacy::ResponseFuture;

#[cfg(feature = "lz4")]
use crate::compression::lz4::Lz4Decoder;
use crate::{
    compression::Compression,
    error::{Error, Result},
};

// === Response ===

pub(crate) enum Response {
    // Headers haven't been received yet.
    // `Box<_>` improves performance by reducing the size of the whole future.
    Waiting(Pin<Box<dyn Future<Output = Result<Chunks>> + Send>>),
    // Headers have been received, streaming the body.
    Loading(Chunks),
}

impl Response {
    pub(crate) fn new(response: ResponseFuture, compression: Compression) -> Self {
        Self::Waiting(Box::pin(async move {
            let response = response.await?;
            let status = response.status();
            let body = response.into_body();

            if status == StatusCode::OK {
                // More likely to be successful, start streaming.
                // It still can fail, but we'll handle it in `DetectDbException`.
                Ok(Chunks::new(body, compression))
            } else {
                // An instantly failed request.
                Err(collect_bad_response(status, body, compression).await)
            }
        }))
    }

    #[inline]
    pub(crate) fn chunks(&mut self) -> Option<&mut Chunks> {
        match self {
            Self::Waiting(_) => None,
            Self::Loading(chunks) => Some(chunks),
        }
    }

    #[cold]
    #[inline(never)]
    pub(crate) async fn chunks_slow(&mut self) -> Result<&mut Chunks> {
        loop {
            match self {
                Self::Waiting(future) => *self = Self::Loading(future.await?),
                Self::Loading(chunks) => break Ok(chunks),
            }
        }
    }

    pub(crate) async fn finish(&mut self) -> Result<()> {
        let chunks = self.chunks_slow().await?;
        while chunks.try_next().await?.is_some() {}
        Ok(())
    }
}

#[cold]
#[inline(never)]
async fn collect_bad_response(
    status: StatusCode,
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
        Err(_) => return Error::BadResponse(stringify_status(status)),
    };

    // Try to decompress the body, because CH uses compression even for errors.
    let stream = stream::once(future::ready(Result::<_>::Ok(raw_bytes.slice(..))));
    let stream = Decompress::new(stream, compression);

    // We're collecting already fetched chunks, thus only decompression errors can
    // be here. If decompression is failed, we should try the raw body because
    // it can be sent without any compression if some proxy is used, which
    // typically know nothing about CH params.
    let bytes = collect_bytes(stream).await.unwrap_or(raw_bytes);

    let reason = String::from_utf8(bytes.into())
        .map(|reason| reason.trim().into())
        // If we have a unreadable response, return standardised reason for the status code.
        .unwrap_or_else(|_| stringify_status(status));

    Error::BadResponse(reason)
}

async fn collect_bytes(stream: impl Stream<Item = Result<Bytes>>) -> Result<Bytes> {
    futures::pin_mut!(stream);

    let mut bytes = Vec::new();

    // TODO: avoid extra copying if there is only one chunk in the stream.
    while let Some(chunk) = stream.try_next().await? {
        bytes.put(chunk);
    }

    Ok(bytes.into())
}

fn stringify_status(status: StatusCode) -> String {
    format!(
        "{} {}",
        status.as_str(),
        status.canonical_reason().unwrap_or("<unknown>"),
    )
}

// === Chunks ===

// * Uses `Option<_>` to make this stream fused.
// * Uses `Box<_>` in order to reduce the size of cursors.
pub(crate) struct Chunks(Option<Box<DetectDbException<Decompress<IncomingStream>>>>);

impl Chunks {
    fn new(stream: Incoming, compression: Compression) -> Self {
        let stream = IncomingStream(stream);
        let stream = Decompress::new(stream, compression);
        let stream = DetectDbException::new(stream);
        Self(Some(Box::new(stream)))
    }
}

impl Stream for Chunks {
    type Item = Result<Bytes>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // `take()` prevents from use after caught panic.
        if let Some(mut stream) = self.0.take() {
            let res = Pin::new(&mut stream).poll_next(cx);

            if matches!(res, Poll::Pending | Poll::Ready(Some(Ok(_)))) {
                self.0 = Some(stream);
            } else {
                assert!(self.0.is_none());
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
}

impl<S> Decompress<S> {
    fn new(stream: S, compression: Compression) -> Self {
        match compression {
            Compression::None => Self::Plain(stream),
            #[cfg(feature = "lz4")]
            Compression::Lz4 | Compression::Lz4Hc(_) => Self::Lz4(Lz4Decoder::new(stream)),
        }
    }
}

impl<S> Stream for Decompress<S>
where
    S: Stream<Item = Result<Bytes>> + Unpin,
{
    type Item = Result<Bytes>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match &mut *self {
            Self::Plain(stream) => Pin::new(stream).poll_next(cx).map_err(Into::into),
            #[cfg(feature = "lz4")]
            Self::Lz4(stream) => Pin::new(stream).poll_next(cx),
        }
    }
}

// === DetectDbException ===

enum DetectDbException<S> {
    Stream(S),
    Exception(Option<Error>),
}

impl<S> DetectDbException<S> {
    fn new(stream: S) -> Self {
        Self::Stream(stream)
    }
}

impl<S> Stream for DetectDbException<S>
where
    S: Stream<Item = Result<Bytes>> + Unpin,
{
    type Item = Result<Bytes>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match &mut *self {
            Self::Stream(stream) => {
                let mut res = Pin::new(stream).poll_next(cx);

                if let Poll::Ready(Some(Ok(chunk))) = &mut res {
                    if let Some(err) = extract_exception(chunk) {
                        *self = Self::Exception(Some(err));

                        // NOTE: now `chunk` can be empty, but it's ok for
                        // callers.
                    }
                }

                res
            }
            Self::Exception(err) => Poll::Ready(err.take().map(Err)),
        }
    }
}

// Format:
// ```
//   <data>Code: <code>. DB::Exception: <desc> (version <version> (official build))\n
// ```
fn extract_exception(chunk: &mut Bytes) -> Option<Error> {
    // `))\n` is very rare in real data, so it's fast dirty check.
    // In random data, it occurs with a probability of ~6*10^-8 only.
    if chunk.ends_with(b"))\n") {
        extract_exception_slow(chunk)
    } else {
        None
    }
}

#[cold]
#[inline(never)]
fn extract_exception_slow(chunk: &mut Bytes) -> Option<Error> {
    let index = chunk.rfind(b"Code:")?;

    if !chunk[index..].contains_str(b"DB::Exception:") {
        return None;
    }

    let exception = chunk.split_off(index);
    let exception = String::from_utf8_lossy(&exception[..exception.len() - 1]);
    Some(Error::BadResponse(exception.into()))
}
