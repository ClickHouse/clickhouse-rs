use std::marker::PhantomData;

use bytes::{Buf, Bytes};
use futures::channel::oneshot;
use hyper::{Request, Response, StatusCode};
use sealed::sealed;
use serde::{Deserialize, Serialize};

use super::{Handler, HandlerFn};
use crate::rowbinary;

const BUFFER_INITIAL_CAPACITY: usize = 1024;

// === Thunk ===

struct Thunk(Response<Bytes>);

#[sealed]
impl super::Handler for Thunk {
    type Control = ();

    fn make(self) -> (HandlerFn, Self::Control) {
        (Box::new(|_| self.0), ())
    }
}

// === failure ===

#[track_caller]
pub fn failure(status: StatusCode) -> impl Handler {
    let reason = status.canonical_reason().unwrap_or("<unknown status code>");

    Response::builder()
        .status(status)
        .body(Bytes::from(reason))
        .map(Thunk)
        .expect("invalid builder")
}

// === provide ===

#[track_caller]
pub fn provide<T>(rows: impl IntoIterator<Item = T>) -> impl Handler
where
    T: Serialize,
{
    let mut buffer = Vec::with_capacity(BUFFER_INITIAL_CAPACITY);
    for row in rows {
        rowbinary::serialize_into(&mut buffer, &row).expect("failed to serialize");
    }
    Thunk(Response::new(buffer.into()))
}

// === record ===

struct RecordHandler<T>(PhantomData<T>);

#[sealed]
impl<T> super::Handler for RecordHandler<T> {
    type Control = RecordControl<T>;

    #[doc(hidden)]
    fn make(self) -> (HandlerFn, Self::Control) {
        let (tx, rx) = oneshot::channel();
        let marker = PhantomData;
        let control = RecordControl { rx, marker };

        let h = Box::new(move |request: Request<Bytes>| -> Response<Bytes> {
            let body = request.into_body();
            let _ = tx.send(body);
            Response::new(<_>::default())
        });

        (h, control)
    }
}

pub struct RecordControl<T> {
    rx: oneshot::Receiver<Bytes>,
    marker: PhantomData<T>,
}

impl<T> RecordControl<T>
where
    T: for<'a> Deserialize<'a>,
{
    pub async fn collect<C>(self) -> C
    where
        C: Default + Extend<T>,
    {
        let mut buffer = self.rx.await.expect("query canceled");
        let mut result = C::default();

        while buffer.has_remaining() {
            let row: T =
                rowbinary::deserialize_from(&mut buffer, &mut []).expect("failed to deserialize");
            result.extend(std::iter::once(row));
        }

        result
    }
}

#[track_caller]
pub fn record<T>() -> impl Handler<Control = RecordControl<T>> {
    RecordHandler(PhantomData)
}

// === record_ddl ===

struct RecordDdlHandler;

#[sealed]
impl super::Handler for RecordDdlHandler {
    type Control = RecordDdlControl;

    #[doc(hidden)]
    fn make(self) -> (HandlerFn, Self::Control) {
        let (tx, rx) = oneshot::channel();
        let control = RecordDdlControl(rx);

        let h = Box::new(move |request: Request<Bytes>| -> Response<Bytes> {
            let body = request.into_body();
            let _ = tx.send(body);
            Response::new(<_>::default())
        });

        (h, control)
    }
}

pub struct RecordDdlControl(oneshot::Receiver<Bytes>);

impl RecordDdlControl {
    pub async fn query(self) -> String {
        let buffer = self.0.await.expect("query canceled");
        String::from_utf8(buffer.to_vec()).expect("query is not DDL")
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
#[track_caller]
pub fn watch<T>(rows: impl IntoIterator<Item = (u64, T)>) -> impl Handler
where
    T: Serialize,
{
    #[derive(Serialize)]
    struct RowPayload<T> {
        _version: u64,
        #[serde(flatten)]
        data: T,
    }

    let mut buffer = Vec::with_capacity(BUFFER_INITIAL_CAPACITY);
    for (_version, data) in rows {
        let payload = RowPayload { _version, data };
        let row = JsonRow::Row(payload);
        serde_json::to_writer(&mut buffer, &row).expect("failed to serialize");
        buffer.push(b'\n');
    }

    Thunk(Response::new(Bytes::from(buffer)))
}

#[cfg(feature = "watch")]
#[track_caller]
pub fn watch_only_events(rows: impl IntoIterator<Item = u64>) -> impl Handler {
    #[derive(Serialize)]
    struct EventPayload {
        version: u64,
    }

    let mut buffer = Vec::with_capacity(BUFFER_INITIAL_CAPACITY);
    for version in rows {
        let payload = EventPayload { version };
        let row = JsonRow::Row(payload);
        serde_json::to_writer(&mut buffer, &row).expect("failed to serialize");
        buffer.push(b'\n');
    }

    Thunk(Response::new(Bytes::from(buffer)))
}
