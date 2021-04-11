use std::{
    pin::Pin,
    task::{Context, Poll},
};

#[cfg(feature = "brotli")]
use async_compression::tokio::bufread::BrotliDecoder;
#[cfg(feature = "gzip")]
use async_compression::tokio::bufread::GzipDecoder;
#[cfg(feature = "zlib")]
use async_compression::tokio::bufread::ZlibDecoder;

#[cfg(any(feature = "brotli", feature = "gzip", feature = "zlib"))]
use tokio_util::codec::{BytesCodec, FramedRead};
#[cfg(any(feature = "brotli", feature = "gzip", feature = "zlib"))]
use tokio_util::io::StreamReader;

use bytes::Bytes;
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

pub enum Response {
    Waiting(ResponseFuture, Compression),
    Loading(Chunks<Body>),
}

impl Response {
    pub fn new(future: ResponseFuture, compression: Compression) -> Self {
        Self::Waiting(future, compression)
    }

    pub async fn resolve(&mut self) -> Result<&mut Chunks<Body>> {
        if let Self::Waiting(response, compression) = self {
            let response = response.await?;
            let status = response.status();
            let body = response.into_body();

            if status != StatusCode::OK {
                // TODO(loyd): test decompression of error response
                let bytes = body::to_bytes(body).await?;

                let mut chunks = decompress_stream(
                    stream::once(future::ready(Result::<_>::Ok(bytes.clone()))),
                    *compression,
                );
                let bytes = match chunks.try_next().await {
                    Ok(chunk) => chunk.unwrap_or_default(),
                    // Original response is more useful than decompression error
                    Err(_) => bytes,
                };

                let reason = String::from_utf8_lossy(&bytes).trim().into();
                return Err(Error::BadResponse(reason));
            }

            let chunks = decompress_stream(body, *compression);
            *self = Self::Loading(chunks);
        }

        match self {
            Self::Waiting(..) => unreachable!(),
            Self::Loading(chunks) => Ok(chunks),
        }
    }
}

fn decompress_stream<S, E>(stream: S, compression: Compression) -> Chunks<S>
where
    S: Stream<Item = Result<Bytes, E>> + Unpin,
    Error: From<E>,
{
    Chunks(match compression {
        Compression::None => Inner::Plain(stream),
        #[cfg(feature = "lz4")]
        Compression::Lz4 => Inner::Lz4(Lz4Decoder::new(stream)),
        #[cfg(feature = "gzip")]
        Compression::Gzip => Inner::Gzip(FramedRead::new(
            GzipDecoder::new(StreamReader::new(stream)),
            BytesCodec::new(),
        )),
        #[cfg(feature = "zlib")]
        Compression::Zlib => Inner::Zlib(FramedRead::new(
            ZlibDecoder::new(StreamReader::new(stream)),
            BytesCodec::new(),
        )),
        #[cfg(feature = "brotli")]
        Compression::Brotli => Inner::Brotli(FramedRead::new(
            BrotliDecoder::new(StreamReader::new(stream)),
            BytesCodec::new(),
        )),
    })
}

pub struct Chunks<S>(Inner<S>);

enum Inner<S> {
    Plain(S),
    #[cfg(feature = "lz4")]
    Lz4(Lz4Decoder<S>),
    #[cfg(feature = "gzip")]
    Gzip(FramedRead<GzipDecoder<StreamReader<S, Bytes>>, BytesCodec>),
    #[cfg(feature = "zlib")]
    Zlib(FramedRead<ZlibDecoder<StreamReader<S, Bytes>>, BytesCodec>),
    #[cfg(feature = "brotli")]
    Brotli(FramedRead<BrotliDecoder<StreamReader<S, Bytes>>, BytesCodec>),
    Empty,
}

impl<S, E> Stream for Chunks<S>
where
    S: Stream<Item = Result<Bytes, E>> + Unpin,
    Error: From<E>,
{
    type Item = Result<Bytes>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        use Inner::*;
        let res = match self.0 {
            Plain(ref mut inner) => map_poll_err(Pin::new(inner).poll_next(cx), Into::into),
            #[cfg(feature = "lz4")]
            Lz4(ref mut inner) => Pin::new(inner).poll_next(cx),
            #[cfg(feature = "gzip")]
            Gzip(ref mut inner) => Pin::new(inner).poll_next(cx),
            #[cfg(feature = "zlib")]
            Zlib(ref mut inner) => Pin::new(inner).poll_next(cx),
            #[cfg(feature = "brotli")]
            Brotli(ref mut inner) => Pin::new(inner).poll_next(cx),
            Empty => Poll::Ready(None),
        };

        if let Poll::Ready(None) = res {
            self.0 = Inner::Empty;
        }

        res
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        use Inner::*;
        match &self.0 {
            Plain(inner) => inner.size_hint(),
            #[cfg(feature = "lz4")]
            Lz4(inner) => inner.size_hint(),
            #[cfg(feature = "gzip")]
            Gzip(inner) => inner.size_hint(),
            #[cfg(feature = "zlib")]
            Zlib(inner) => inner.size_hint(),
            #[cfg(feature = "brotli")]
            Brotli(inner) => inner.size_hint(),
            Empty => (0, Some(0)),
        }
    }
}

// XXX: https://github.com/rust-lang/rust/issues/63514
fn map_poll_err<T, E, E2>(
    poll: Poll<Option<Result<T, E>>>,
    f: impl FnOnce(E) -> E2,
) -> Poll<Option<Result<T, E2>>> {
    match poll {
        Poll::Ready(Some(Ok(val))) => Poll::Ready(Some(Ok(val))),
        Poll::Ready(Some(Err(err))) => Poll::Ready(Some(Err(f(err)))),
        Poll::Ready(None) => Poll::Ready(None),
        Poll::Pending => Poll::Pending,
    }
}
