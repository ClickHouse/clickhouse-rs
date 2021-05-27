#![allow(clippy::new_without_default)]

use std::{
    convert::Infallible,
    net::SocketAddr,
    sync::{
        atomic::{AtomicU16, Ordering},
        Arc,
    },
};

use bytes::{Bytes, BytesMut};
use futures::{
    channel::{self, mpsc::UnboundedSender},
    lock::Mutex,
    Stream, StreamExt,
};
use hyper::service::{make_service_fn, service_fn};
use hyper::{Body, Request, Response, Server, StatusCode};
use serde::Serialize;

use crate::{error::Result, rowbinary};

pub struct Mock {
    url: String,
    tx: UnboundedSender<Box<dyn Handler + Send>>,
    non_exhaustive: bool,
}

static NEXT_PORT: AtomicU16 = AtomicU16::new(15420);

impl Mock {
    pub fn new() -> Self {
        // TODO: need to reassign if the port has already been taken.
        let port = NEXT_PORT.fetch_add(1, Ordering::Relaxed);
        let addr = SocketAddr::from(([127, 0, 0, 1], port));

        let (tx, rx) = channel::mpsc::unbounded::<Box<dyn Handler + Send>>();
        let rx = Arc::new(Mutex::new(rx));

        // Hm, here is one of the ugliest code that I've written ever.
        let make_service = make_service_fn(move |_conn| {
            let rx1 = rx.clone();
            async move {
                let rx2 = rx1.clone();
                Ok::<_, Infallible>(service_fn(move |req| {
                    let rx3 = rx2.clone();
                    async move {
                        let mut handler = {
                            let mut rx = rx3.lock().await;
                            rx.next().await.expect("mock is dropped")
                        };
                        Ok::<_, Infallible>(handler.handle(req))
                    }
                }))
            }
        });

        let server = Server::bind(&addr).serve(make_service);

        // TODO: handle error
        tokio::spawn(server);

        Self {
            tx,
            url: format!("http://{}", addr),
            non_exhaustive: false,
        }
    }

    pub fn url(&self) -> &str {
        &self.url
    }

    pub fn add(&self, handler: impl Handler + Send + 'static) {
        self.tx
            .unbounded_send(Box::new(handler))
            .expect("the test server is down");
    }

    pub fn non_exhaustive(&mut self) {
        self.non_exhaustive = true;
    }
}

pub trait Handler {
    fn handle(&mut self, req: Request<Body>) -> Response<Body>;
}

// List: https://github.com/ClickHouse/ClickHouse/blob/495c6e03aa9437dac3cd7a44ab3923390bef9982/src/Server/HTTPHandler.cpp#L132
pub mod status {
    use super::*;

    pub const UNAUTHORIZED: StatusCode = StatusCode::UNAUTHORIZED;
    pub const FORBIDDEN: StatusCode = StatusCode::FORBIDDEN;
    pub const BAD_REQUEST: StatusCode = StatusCode::BAD_REQUEST;
    pub const NOT_FOUND: StatusCode = StatusCode::NOT_FOUND;
    pub const PAYLOAD_TOO_LARGE: StatusCode = StatusCode::PAYLOAD_TOO_LARGE;
    pub const NOT_IMPLEMENTED: StatusCode = StatusCode::NOT_IMPLEMENTED;
    pub const SERVICE_UNAVAILABLE: StatusCode = StatusCode::SERVICE_UNAVAILABLE;
    pub const LENGTH_REQUIRED: StatusCode = StatusCode::LENGTH_REQUIRED;
    pub const INTERNAL_SERVER_ERROR: StatusCode = StatusCode::INTERNAL_SERVER_ERROR;
}

pub struct OnSelect {
    response: Option<Response<Body>>,
}

impl OnSelect {
    pub fn new() -> Self {
        Self { response: None }
    }

    pub fn success<T>(mut self, rows: impl Stream<Item = T> + Send + 'static) -> Self
    where
        T: Serialize,
    {
        let s = rows.map(|row| -> Result<Bytes> {
            let mut buffer = BytesMut::with_capacity(128);
            rowbinary::serialize_into(&mut buffer, &row)?;
            Ok(buffer.freeze())
        });
        self.response = Some(Response::new(Body::wrap_stream(s)));
        self
    }

    pub fn failure(mut self, status: StatusCode) -> Self {
        let reason = status.canonical_reason().unwrap_or("<unknown status code>");

        self.response = Some(
            Response::builder()
                .status(status)
                .body(Body::from(reason))
                .expect("invalid builder"),
        );
        self
    }
}

impl Handler for OnSelect {
    fn handle(&mut self, _req: Request<Body>) -> Response<Body> {
        self.response
            .take()
            .expect("success() or failure() must be called")
        // TODO: or just return an empty response?
    }
}

// TODO: OnInsert
