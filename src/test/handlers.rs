use std::{convert::Infallible, marker::PhantomData};

use bytes::{Buf, Bytes, BytesMut};
use futures::{
    channel::{self, mpsc::UnboundedReceiver},
    stream, Stream, StreamExt,
};
use hyper::{body, Body, Request, Response, StatusCode};
use serde::{Deserialize, Serialize};

use super::{Handler, HandlerFn};
use crate::{error::Result, rowbinary, sealed::Sealed};

// raw

struct RawHandler<F>(Option<F>);

impl<F> Handler for RawHandler<F>
where
    F: FnOnce(Request<Body>) -> Response<Body> + Send + 'static,
{
    type Control = ();

    fn make(&mut self) -> (HandlerFn, Self::Control) {
        let h = Box::new(self.0.take().expect("raw handler must be called only once"));
        (h, ())
    }
}

impl<F> Sealed for RawHandler<F> {}

fn raw(f: impl FnOnce(Request<Body>) -> Response<Body> + Send + 'static) -> impl Handler {
    RawHandler(Some(f))
}

// failure

pub fn failure(status: StatusCode) -> impl Handler {
    let reason = status.canonical_reason().unwrap_or("<unknown status code>");
    raw(move |_req| {
        Response::builder()
            .status(status)
            .body(Body::from(reason))
            .expect("invalid builder")
    })
}

// provide

pub fn provide<T>(rows: impl Stream<Item = T> + Send + 'static) -> impl Handler
where
    T: Serialize,
{
    let s = rows.map(|row| -> Result<Bytes> {
        let mut buffer = BytesMut::with_capacity(128);
        rowbinary::serialize_into(&mut buffer, &row)?;
        Ok(buffer.freeze())
    });
    raw(move |_req| Response::new(Body::wrap_stream(s)))
}

// record

struct RecordHandler<T>(PhantomData<T>);

impl<T> Handler for RecordHandler<T>
where
    T: for<'a> Deserialize<'a> + Send + 'static,
{
    type Control = RecordControl<T>;

    #[doc(hidden)]
    fn make(&mut self) -> (HandlerFn, Self::Control) {
        let (tx, rx) = channel::mpsc::unbounded();
        let control = RecordControl(rx);

        let h = Box::new(move |req: Request<Body>| -> Response<Body> {
            let fut = async move {
                let body = req.into_body();
                let mut buf = body::aggregate(body).await.expect("invalid request");

                while buf.has_remaining() {
                    let row = rowbinary::deserialize_from(&mut buf, &mut [])
                        .expect("failed to deserialize");
                    tx.unbounded_send(row).expect("failed to send, test ended?");
                }

                Ok::<_, Infallible>("")
            };

            Response::new(Body::wrap_stream(stream::once(fut)))
        });

        (h, control)
    }
}

impl<T> Sealed for RecordHandler<T> {}

pub struct RecordControl<T>(UnboundedReceiver<T>);

impl<T> RecordControl<T> {
    pub async fn collect<C>(self) -> C
    where
        C: Default + Extend<T>,
    {
        self.0.collect().await
    }
}

pub fn record<T>() -> impl Handler<Control = RecordControl<T>>
where
    T: for<'a> Deserialize<'a> + Send + 'static,
{
    RecordHandler(PhantomData)
}
