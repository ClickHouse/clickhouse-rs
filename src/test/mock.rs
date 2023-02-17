use std::{
    convert::Infallible,
    net::SocketAddr,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    thread,
    time::Duration,
};

use futures::{
    channel::{self, mpsc::UnboundedSender},
    lock::Mutex,
    StreamExt,
};
use hyper::service::{make_service_fn, service_fn};
use hyper::Server;
use tokio::time::timeout;

use super::{Handler, HandlerFn};

const MAX_WAIT_TIME: Duration = Duration::from_millis(150);

pub struct Mock {
    url: String,
    tx: UnboundedSender<HandlerFn>,
    responses_left: Arc<AtomicUsize>,
    non_exhaustive: bool,
}

impl Mock {
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        let addr = SocketAddr::from(([127, 0, 0, 1], 0));

        let (tx, rx) = channel::mpsc::unbounded::<HandlerFn>();
        let rx = Arc::new(Mutex::new(rx));
        let responses_left = Arc::new(AtomicUsize::new(0));
        let responses_left_0 = responses_left.clone();

        // Hm, here is one of the ugliest code that I've written ever.
        let make_service = make_service_fn(move |_conn| {
            let rx1 = rx.clone();
            let responses_left_1 = responses_left.clone();
            async move {
                let rx2 = rx1.clone();
                let responses_left_2 = responses_left_1.clone();
                Ok::<_, Infallible>(service_fn(move |req| {
                    let rx3 = rx2.clone();
                    let responses_left = responses_left_2.clone();
                    async move {
                        let handler_fn = {
                            let mut rx = rx3.lock().await;

                            // TODO: should we use `std::time::Instant` instead?
                            match timeout(MAX_WAIT_TIME, rx.next()).await {
                                Ok(Some(res)) => {
                                    responses_left.fetch_sub(1, Ordering::Relaxed);
                                    res
                                }
                                _ => panic!("unexpected request, no predefined responses left"),
                            }
                        };
                        Ok::<_, Infallible>(handler_fn(req))
                    }
                }))
            }
        });

        let server = Server::bind(&addr).serve(make_service);
        let addr = server.local_addr();
        // TODO: handle error
        tokio::spawn(server);

        Self {
            url: format!("http://{addr}"),
            tx,
            responses_left: responses_left_0,
            non_exhaustive: false,
        }
    }

    pub fn url(&self) -> &str {
        &self.url
    }

    pub fn add<H: Handler>(&self, mut handler: H) -> H::Control {
        let (h_fn, control) = handler.make();
        self.responses_left.fetch_add(1, Ordering::Relaxed);
        self.tx
            .unbounded_send(h_fn)
            .expect("the test server is down");
        control
    }

    pub fn non_exhaustive(&mut self) {
        self.non_exhaustive = true;
    }
}

impl Drop for Mock {
    fn drop(&mut self) {
        if !self.non_exhaustive
            && !thread::panicking()
            && self.responses_left.load(Ordering::Relaxed) > 0
        {
            panic!("test ended, but not all responses have been consumed");
        }
    }
}
