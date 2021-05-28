use bytes::{Bytes, BytesMut};
use futures::{Stream, StreamExt};
use hyper::{Body, Request, Response, StatusCode};
use serde::Serialize;

use super::Handler;
use crate::{error::Result, rowbinary};

// raw

struct RawHandler<F>(Option<F>);

impl<F> Handler for RawHandler<F>
where
    F: FnOnce(Request<Body>) -> Response<Body> + Send + 'static,
{
    fn handle(&mut self, req: Request<Body>) -> Response<Body> {
        self.0.take().expect("handler must be called only once")(req)
    }
}

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
