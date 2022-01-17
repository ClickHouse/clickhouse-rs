use std::{convert::Infallible, marker::PhantomData};

use bytes::{Buf, Bytes, BytesMut};
use futures::{
    channel::{mpsc, oneshot},
    stream, Stream, StreamExt,
};
use hyper::{body, Body, Request, Response, StatusCode};
use sealed::sealed;
use serde::{Deserialize, Serialize};

use super::{Handler, HandlerFn};
use crate::{error::Result, rowbinary};

// === raw ===

struct RawHandler<F>(Option<F>);

#[sealed]
impl<F> super::Handler for RawHandler<F>
where
    F: FnOnce(Request<Body>) -> Response<Body> + Send + 'static,
{
    type Control = ();

    fn make(&mut self) -> (HandlerFn, Self::Control) {
        let h = Box::new(self.0.take().expect("raw handler must be called only once"));
        (h, ())
    }
}

fn raw(f: impl FnOnce(Request<Body>) -> Response<Body> + Send + 'static) -> impl Handler {
    RawHandler(Some(f))
}

// === failure ===

pub fn failure(status: StatusCode) -> impl Handler {
    let reason = status.canonical_reason().unwrap_or("<unknown status code>");
    raw(move |_req| {
        Response::builder()
            .status(status)
            .body(Body::from(reason))
            .expect("invalid builder")
    })
}

// === provide ===

pub fn provide<T>(rows: impl Stream<Item = T> + Send + 'static) -> impl Handler
where
    T: Serialize,
{
    let s = rows.map(|row| -> Result<Bytes> {
        let mut buffer = BytesMut::with_capacity(256);
        rowbinary::serialize_into(&mut buffer, &row)?;
        Ok(buffer.freeze())
    });
    raw(move |_req| Response::new(Body::wrap_stream(s)))
}

// === record ===

struct RecordHandler<T>(PhantomData<T>);

#[sealed]
impl<T> super::Handler for RecordHandler<T>
where
    T: for<'a> Deserialize<'a> + Send + 'static,
{
    type Control = RecordControl<T>;

    #[doc(hidden)]
    fn make(&mut self) -> (HandlerFn, Self::Control) {
        let (tx, rx) = mpsc::unbounded();
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

pub struct RecordControl<T>(mpsc::UnboundedReceiver<T>);

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

// === record_ddl ===

struct RecordDdlHandler;

#[sealed]
impl super::Handler for RecordDdlHandler {
    type Control = RecordDdlControl;

    #[doc(hidden)]
    fn make(&mut self) -> (HandlerFn, Self::Control) {
        let (tx, rx) = oneshot::channel();
        let control = RecordDdlControl(rx);

        let h = Box::new(move |req: Request<Body>| -> Response<Body> {
            let fut = async move {
                let body = req.into_body();
                let buf = body::to_bytes(body).await.expect("invalid request");
                let _ = tx.send(buf);
                Ok::<_, Infallible>("")
            };

            Response::new(Body::wrap_stream(stream::once(fut)))
        });

        (h, control)
    }
}

pub struct RecordDdlControl(oneshot::Receiver<Bytes>);

impl RecordDdlControl {
    pub async fn query(self) -> String {
        let buf = self.0.await.expect("query canceled");
        String::from_utf8(buf.to_vec()).expect("query is not DDL")
    }
}

pub fn record_ddl() -> impl Handler<Control = RecordDdlControl> {
    RecordDdlHandler
}

// === watch ===

#[cfg(feature = "watch")]
#[derive(Serialize)]
#[serde(rename_all = "lowercase")]
enum JsonRow<T> {
    Row(T),
}

#[cfg(feature = "watch")]
pub fn watch<T>(rows: impl Stream<Item = (u64, T)> + Send + 'static) -> impl Handler
where
    T: Serialize,
{
    #[derive(Serialize)]
    struct RowPayload<T> {
        _version: u64,
        #[serde(flatten)]
        data: T,
    }

    let s = rows.map(|(_version, data)| -> Result<Bytes> {
        let payload = RowPayload { _version, data };
        let row = JsonRow::Row(payload);
        let mut json = serde_json::to_string(&row).expect("invalid json");
        json.push('\n');
        Ok(json.into())
    });
    raw(move |_req| Response::new(Body::wrap_stream(s)))
}

#[cfg(feature = "watch")]
pub fn watch_only_events(rows: impl Stream<Item = u64> + Send + 'static) -> impl Handler {
    #[derive(Serialize)]
    struct EventPayload {
        version: u64,
    }

    let s = rows.map(|version| -> Result<Bytes> {
        let payload = EventPayload { version };
        let row = JsonRow::Row(payload);
        let mut json = serde_json::to_string(&row).expect("invalid json");
        json.push('\n');
        Ok(json.into())
    });
    raw(move |_req| Response::new(Body::wrap_stream(s)))
}
