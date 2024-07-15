use std::{
    collections::VecDeque,
    error::Error,
    net::SocketAddr,
    sync::{Arc, Mutex},
    thread,
};

use bytes::Bytes;
use http_body_util::{BodyExt as _, Full};
use hyper::{body::Incoming, server::conn, service, Request, Response};
use hyper_util::rt::{TokioIo, TokioTimer};
use tokio::{net::TcpListener, task::AbortHandle};

use super::{Handler, HandlerFn};

/// A mock server for testing.
pub struct Mock {
    url: String,
    shared: Arc<Mutex<Shared>>,
    non_exhaustive: bool,
    server_handle: AbortHandle,
}

/// Shared between the server and the test.
#[derive(Default)]
struct Shared {
    handlers: VecDeque<HandlerFn>,
    /// An error from the background server task.
    /// Propagated as a panic in test cases.
    error: Option<Box<dyn Error + Send + Sync>>,
}

impl Mock {
    /// Starts a new test server and returns a handle to it.
    #[track_caller]
    pub fn new() -> Self {
        let (addr, listener) = {
            let addr = SocketAddr::from(([127, 0, 0, 1], 0));
            let listener = std::net::TcpListener::bind(addr).expect("cannot bind a listener");
            listener
                .set_nonblocking(true)
                .expect("cannot set non-blocking mode");
            let addr = listener.local_addr().expect("cannot get a local address");
            let listener = TcpListener::from_std(listener).expect("cannot convert to tokio");
            (addr, listener)
        };

        let shared = Arc::new(Mutex::new(Shared::default()));
        let server_handle = tokio::spawn(server(listener, shared.clone()));

        Self {
            url: format!("http://{addr}"),
            shared,
            non_exhaustive: false,
            server_handle: server_handle.abort_handle(),
        }
    }

    /// Returns a test server's URL to provide into [`Client`].
    ///
    /// [`Client`]: crate::Client::with_url
    pub fn url(&self) -> &str {
        &self.url
    }

    /// Adds a handler to the test server for the next request.
    ///
    /// Can be called multiple times to enqueue multiple handlers.
    ///
    /// If [`Mock::non_exhaustive()`] is not called, the destructor will panic
    /// if not all handlers are called by the end of the test.
    #[track_caller]
    pub fn add<H: Handler>(&self, handler: H) -> H::Control {
        self.propagate_server_error();

        if self.server_handle.is_finished() {
            panic!("impossible to add a handler: the test server is terminated");
        }

        let (handler, control) = handler.make();
        self.shared.lock().unwrap().handlers.push_back(handler);
        control
    }

    /// Allows unused handlers to be left after the test ends.
    pub fn non_exhaustive(&mut self) {
        self.non_exhaustive = true;
    }

    #[track_caller]
    fn propagate_server_error(&self) {
        if let Some(error) = &self.shared.lock().unwrap().error {
            panic!("server error: {error}");
        }
    }
}

impl Default for Mock {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for Mock {
    fn drop(&mut self) {
        self.server_handle.abort();

        if thread::panicking() {
            return;
        }

        self.propagate_server_error();

        if !self.non_exhaustive && !self.shared.lock().unwrap().handlers.is_empty() {
            panic!("test ended, but not all responses have been consumed");
        }
    }
}

async fn server(listener: TcpListener, shared: Arc<Mutex<Shared>>) {
    let error = loop {
        let stream = match listener.accept().await {
            Ok((stream, _)) => stream,
            Err(err) => break err.into(),
        };

        let serving = conn::http1::Builder::new()
            .timer(TokioTimer::new())
            .keep_alive(false)
            .serve_connection(
                TokioIo::new(stream),
                service::service_fn(|request| handle(request, &shared)),
            );

        if let Err(err) = serving.await {
            break if let Some(source) = err.source() {
                source.to_string().into()
            } else {
                err.into()
            };
        }
    };

    shared.lock().unwrap().error.get_or_insert(error);
}

async fn handle(
    request: Request<Incoming>,
    shared: &Mutex<Shared>,
) -> Result<Response<Full<Bytes>>, Box<dyn Error + Send + Sync>> {
    let Some(handler) = shared.lock().unwrap().handlers.pop_front() else {
        return Err("no installed handler for an incoming request".into());
    };

    let (parts, body) = request.into_parts();
    let body = body.collect().await?.to_bytes();

    let request = Request::from_parts(parts, body);
    let response = handler(request).map(Full::new);

    Ok(response)
}
