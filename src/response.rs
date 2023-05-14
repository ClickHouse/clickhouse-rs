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
use hyper::{body, client::ResponseFuture, Body, StatusCode};

#[cfg(feature = "lz4")]
use crate::compression::lz4::Lz4Decoder;
use crate::{
    compression::Compression,
    error::{Error, Result},
};

pub(crate) enum Response {
    Waiting(Pin<Box<dyn Future<Output = Result<Chunks<Body>>> + Send>>),
    Loading(Chunks<Body>),
}

impl Response {
    pub(crate) fn new(response: ResponseFuture, compression: Compression) -> Self {
        // Boxing here significantly improves performance by reducing the size of `chunks()`.
        Self::Waiting(Box::pin(async move {
            let response = response.await?;
            let status = response.status();
            let body = response.into_body();

            if status == StatusCode::OK {
                Ok(Chunks::new(body, compression))
            } else {
                Err(collect_bad_response(status, body, compression).await)
            }
        }))
    }

    #[inline]
    pub(crate) fn chunks(&mut self) -> Option<&mut Chunks<Body>> {
        match self {
            Self::Waiting(_) => None,
            Self::Loading(chunks) => Some(chunks),
        }
    }

    #[cold]
    #[inline(never)]
    pub(crate) async fn chunks_slow(&mut self) -> Result<&mut Chunks<Body>> {
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
async fn collect_bad_response(status: StatusCode, body: Body, compression: Compression) -> Error {
    // Collect the whole body into one contiguous buffer to simplify handling.
    // Only network errors can occur here and we return them instead of status code
    // because it means the request can be repeated to get a more detailed error.
    //
    // TODO: we don't implement any length checks and a malicious peer (e.g. MITM)
    //       might make us consume arbitrary amounts of memory.
    let raw_bytes = match body::to_bytes(body).await {
        Ok(bytes) => bytes,
        Err(err) => return err.into(),
    };

    // Try to decompress the body, because CH compresses any responses, even with errors.
    let stream = stream::once(future::ready(Result::<_>::Ok(raw_bytes.slice(..))));
    let stream = Decompress::new(stream, compression);

    // We're collecting already fetched chunks, thus only decompression errors can be here.
    // If decompression is failed, we should try the raw body because it can be sent without
    // any compression if some proxy is used, which typically know nothing about CH params.
    let bytes = collect_bytes(stream).await.unwrap_or(raw_bytes);

    let reason = String::from_utf8(bytes.into())
        .map(|reason| reason.trim().into())
        .unwrap_or_else(|_| {
            // If we have a unreadable response, return standardised reason for the status code.
            format!(
                "{} {}",
                status.as_str(),
                status.canonical_reason().unwrap_or("<unknown>"),
            )
        });

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

// * Uses `Option<_>` to make this stream fused.
// * Uses `Box<_>` in order to reduce the size of cursors.
pub(crate) struct Chunks<S>(Option<Box<DetectDbException<Decompress<ConvertError<S>>>>>);

impl<S, E> Chunks<S>
where
    S: Stream<Item = Result<Bytes, E>> + Unpin,
    Error: From<E>,
{
    fn new(stream: S, compression: Compression) -> Self {
        let stream = DetectDbException::Stream(Decompress::new(ConvertError(stream), compression));
        Self(Some(Box::new(stream)))
    }
}

impl<S, E> Stream for Chunks<S>
where
    S: Stream<Item = Result<Bytes, E>> + Unpin,
    Error: From<E>,
{
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

struct ConvertError<S>(S);

impl<S, E> Stream for ConvertError<S>
where
    S: Stream<Item = Result<Bytes, E>> + Unpin,
    Error: From<E>,
{
    type Item = Result<Bytes>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.0)
            .poll_next(cx)
            .map_err(|err| err.into())
    }
}

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

enum DetectDbException<S> {
    Stream(S),
    Exception(Option<Error>),
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

                        // NOTE: now `chunk` can be empty, but it's ok for callers.
                    }
                }

                res
            }
            Self::Exception(err) => Poll::Ready(err.take().map(Err)),
        }
    }
}

// Format:
//   <data>Code: <code>. DB::Exception: <desc> (version <version> (official build))\n
fn extract_exception(chunk: &mut Bytes) -> Option<Error> {
    // `))\n` is very rare in real data and occurs with a probability of ~6*10^-8 in random ones.
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
