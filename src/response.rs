use std::{
    pin::Pin,
    task::{Context, Poll},
};

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

pub(crate) enum Response {
    Waiting(ResponseFuture, Compression),
    Loading(Chunks<Body>),
}

impl Response {
    pub(crate) fn new(future: ResponseFuture, compression: Compression) -> Self {
        Self::Waiting(future, compression)
    }

    pub(crate) async fn chunks(&mut self) -> Result<&mut Chunks<Body>> {
        if let Self::Waiting(response, compression) = self {
            let response = response.await?;
            let status = response.status();
            let body = response.into_body();

            if status != StatusCode::OK {
                let reason = collect_bad_response(body, *compression).await?;
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

    pub(crate) async fn finish(&mut self) -> Result<()> {
        let chunks = self.chunks().await?;
        while chunks.try_next().await?.is_some() {}
        Ok(())
    }
}

async fn collect_bad_response(body: Body, compression: Compression) -> Result<String> {
    let bytes = body::to_bytes(body).await?;

    let mut chunks = decompress_stream(
        stream::once(future::ready(Result::<_>::Ok(bytes.clone()))),
        compression,
    );
    let bytes = match chunks.try_next().await {
        Ok(chunk) => chunk.unwrap_or_default(),
        // Original response is more useful than a decompression error.
        Err(_) => bytes,
    };

    Ok(String::from_utf8_lossy(&bytes).trim().into())
}

fn decompress_stream<S, E>(stream: S, compression: Compression) -> Chunks<S>
where
    S: Stream<Item = Result<Bytes, E>> + Unpin,
    Error: From<E>,
{
    let inner = match compression {
        Compression::None => Inner::Plain(stream),
        #[cfg(feature = "lz4")]
        Compression::Lz4 | Compression::Lz4Hc(_) | Compression::Lz4Fast(_) => {
            Inner::Lz4(Lz4Decoder::new(stream))
        }
    };

    Chunks(Box::new(inner))
}

// We use `Box<_>` here to reduce the size of cursors.
pub(crate) struct Chunks<S>(Box<Inner<S>>);

enum Inner<S> {
    Plain(S),
    #[cfg(feature = "lz4")]
    Lz4(Lz4Decoder<S>),
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
        let res = match &mut *self.0 {
            Plain(inner) => Pin::new(inner).poll_next(cx).map_err(Into::into),
            #[cfg(feature = "lz4")]
            Lz4(inner) => Pin::new(inner).poll_next(cx),
            Empty => Poll::Ready(None),
        };

        if let Poll::Ready(None) = res {
            *self.0 = Inner::Empty;
        }

        res
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        use Inner::*;
        match &*self.0 {
            Plain(inner) => inner.size_hint(),
            #[cfg(feature = "lz4")]
            Lz4(inner) => inner.size_hint(),
            Empty => (0, Some(0)),
        }
    }
}
