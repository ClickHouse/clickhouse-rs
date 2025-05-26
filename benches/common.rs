#![allow(dead_code)] // typical for common test/bench modules :(

use std::{
    convert::Infallible,
    future::Future,
    net::SocketAddr,
    pin::Pin,
    sync::atomic::{AtomicU32, Ordering},
    thread,
    time::Duration,
};

use bytes::Bytes;
use futures::stream::StreamExt;
use http_body_util::BodyExt;
use hyper::{
    body::{Body, Incoming},
    server::conn,
    service, Request, Response,
};
use hyper_util::rt::{TokioIo, TokioTimer};
use tokio::{
    net::TcpListener,
    runtime,
    sync::{mpsc, oneshot},
};

use clickhouse::error::Result;

pub(crate) struct ServerHandle;

pub(crate) fn start_server<S, F, B>(addr: SocketAddr, serve: S) -> ServerHandle
where
    S: Fn(Request<Incoming>) -> F + Send + Sync + 'static,
    F: Future<Output = Response<B>> + Send,
    B: Body<Data = Bytes, Error = Infallible> + Send + 'static,
{
    let serving = async move {
        let listener = TcpListener::bind(addr).await.unwrap();

        loop {
            let (stream, _) = listener.accept().await.unwrap();

            let service =
                service::service_fn(|request| async { Ok::<_, Infallible>(serve(request).await) });

            // SELECT benchmark doesn't read the whole body, so ignore possible errors.
            let _ = conn::http1::Builder::new()
                .timer(TokioTimer::new())
                .serve_connection(TokioIo::new(stream), service)
                .await;
        }
    };

    run_on_st_runtime("server", serving);
    ServerHandle
}

pub(crate) async fn skip_incoming(request: Request<Incoming>) {
    let mut body = request.into_body().into_data_stream();

    // Read and skip all frames.
    while let Some(result) = body.next().await {
        result.unwrap();
    }
}

pub(crate) struct RunnerHandle {
    tx: mpsc::UnboundedSender<Run>,
}

struct Run {
    future: Pin<Box<dyn Future<Output = Result<Duration>> + Send>>,
    callback: oneshot::Sender<Result<Duration>>,
}

impl RunnerHandle {
    pub(crate) fn run(
        &self,
        f: impl Future<Output = Result<Duration>> + Send + 'static,
    ) -> Duration {
        let (tx, rx) = oneshot::channel();

        self.tx
            .send(Run {
                future: Box::pin(f),
                callback: tx,
            })
            .unwrap();

        rx.blocking_recv().unwrap().unwrap()
    }
}

pub(crate) fn start_runner() -> RunnerHandle {
    let (tx, mut rx) = mpsc::unbounded_channel::<Run>();

    run_on_st_runtime("testee", async move {
        while let Some(run) = rx.recv().await {
            let result = run.future.await;
            let _ = run.callback.send(result);
        }
    });

    RunnerHandle { tx }
}

fn run_on_st_runtime(name: &str, f: impl Future + Send + 'static) {
    let name = name.to_string();
    thread::Builder::new()
        .name(name.clone())
        .spawn(move || {
            let no = AtomicU32::new(0);
            runtime::Builder::new_current_thread()
                .enable_all()
                .thread_name_fn(move || {
                    let no = no.fetch_add(1, Ordering::Relaxed);
                    format!("{name}-{no}")
                })
                .build()
                .unwrap()
                .block_on(f);
        })
        .unwrap();
}
