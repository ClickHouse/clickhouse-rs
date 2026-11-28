use crate::error::Error;
use crate::native::Block;
use crate::native::reader::BlockReader;
use crate::response::{Response, ResponseFuture};
use futures_util::future::BoxFuture;
use std::pin::Pin;
use std::task::{Context, Poll, ready};

pub struct NativeCursor {
    state: CursorState,
    span: tracing::Span,
}

enum CursorState {
    Loading(ResponseFuture),
    Reading(BoxFuture<'static, Result<Option<(Block, BlockReader)>, Error>>),
    /// The native TCP transport delivers blocks without encoding them.
    #[cfg(feature = "tcp")]
    TcpLoading(crate::tcp::BlocksFuture),
    #[cfg(feature = "tcp")]
    TcpReading(tokio::sync::mpsc::Receiver<Result<Block, Error>>),
    Done,
}

impl NativeCursor {
    pub(crate) fn new(response: Response, span: tracing::Span) -> Self {
        NativeCursor {
            state: CursorState::Loading(response.into_future()),
            span,
        }
    }

    #[cfg(feature = "tcp")]
    pub(crate) fn from_blocks(blocks: crate::tcp::BlocksFuture, span: tracing::Span) -> Self {
        NativeCursor {
            state: CursorState::TcpLoading(blocks),
            span,
        }
    }

    pub async fn next(&mut self) -> Result<Option<Block>, Error> {
        std::future::poll_fn(|cx| self.poll_next(cx)).await
    }

    fn poll_next(&mut self, cx: &mut Context<'_>) -> Poll<Result<Option<Block>, Error>> {
        let _span = self.span.enter();

        loop {
            match &mut self.state {
                CursorState::Loading(fut) => {
                    match ready!(Pin::new(fut).poll(cx)) {
                        // TODO: expose summary
                        Ok((chunks, _summary)) => {
                            self.state = CursorState::Reading(Box::pin(read_block(
                                BlockReader::new(chunks),
                            )));
                        }
                        Err(e) => {
                            self.state = CursorState::Done;
                            return Poll::Ready(Err(e));
                        }
                    }
                }
                CursorState::Reading(fut) => {
                    return match ready!(Pin::new(fut).poll(cx)) {
                        Ok(Some((block, reader))) => {
                            self.state = CursorState::Reading(Box::pin(read_block(reader)));
                            Poll::Ready(Ok(Some(block)))
                        }
                        Ok(None) => {
                            self.state = CursorState::Done;
                            Poll::Ready(Ok(None))
                        }
                        Err(e) => {
                            self.state = CursorState::Done;
                            Poll::Ready(Err(e))
                        }
                    };
                }
                #[cfg(feature = "tcp")]
                CursorState::TcpLoading(fut) => match ready!(fut.as_mut().poll(cx)) {
                    Ok(receiver) => self.state = CursorState::TcpReading(receiver),
                    Err(e) => {
                        self.state = CursorState::Done;
                        return Poll::Ready(Err(e));
                    }
                },
                #[cfg(feature = "tcp")]
                CursorState::TcpReading(receiver) => {
                    return match ready!(receiver.poll_recv(cx)) {
                        Some(Ok(block)) => Poll::Ready(Ok(Some(block))),
                        Some(Err(e)) => {
                            self.state = CursorState::Done;
                            Poll::Ready(Err(e))
                        }
                        None => {
                            self.state = CursorState::Done;
                            Poll::Ready(Ok(None))
                        }
                    };
                }
                CursorState::Done => return Poll::Ready(Ok(None)),
            }
        }
    }
}

async fn read_block(mut reader: BlockReader) -> Result<Option<(Block, BlockReader)>, Error> {
    Ok(reader.read_block().await?.map(|block| (block, reader)))
}
