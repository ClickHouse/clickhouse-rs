use crate::{
    error::Result,
    response::{Chunks, Response, ResponseFuture},
};
use bytes::Bytes;
use futures::{FutureExt, TryStreamExt};
use std::task::{Context, Poll};

/// A cursor over raw bytes of a query response.
/// All other cursors are built on top of this one.
pub(crate) struct RawCursor(RawCursorState);

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

    pub(crate) async fn next(&mut self) -> Result<Option<Bytes>> {
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

    pub(crate) fn poll_next(&mut self, cx: &mut Context<'_>) -> Poll<Result<Option<Bytes>>> {
        // TODO: implement `next()` based on `poll_next()` and avoid `boxed()`.
        self.next().boxed().poll_unpin(cx)
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

    pub(crate) fn received_bytes(&self) -> u64 {
        match &self.0 {
            RawCursorState::Waiting(_) => 0,
            RawCursorState::Loading(state) => state.net_size,
        }
    }

    pub(crate) fn decoded_bytes(&self) -> u64 {
        match &self.0 {
            RawCursorState::Waiting(_) => 0,
            RawCursorState::Loading(state) => state.data_size,
        }
    }
}
