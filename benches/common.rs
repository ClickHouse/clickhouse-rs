use std::{convert::Infallible, future::Future, net::SocketAddr, thread};

use bytes::Bytes;
use futures::stream::StreamExt;
use http_body_util::BodyExt;
use hyper::{
    body::{Body, Incoming},
    server::conn,
    service, Request, Response,
};
use hyper_util::rt::{TokioIo, TokioTimer};
use tokio::{net::TcpListener, runtime};

pub struct ServerHandle;

pub fn start_server<S, F, B>(addr: SocketAddr, serve: S) -> ServerHandle
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

    thread::spawn(move || {
        runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(serving);
    });

    ServerHandle
}

pub async fn skip_incoming(request: Request<Incoming>) {
    let mut body = request.into_body().into_data_stream();

    // Read and skip all frames.
    while let Some(result) = body.next().await {
        result.unwrap();
    }
}
