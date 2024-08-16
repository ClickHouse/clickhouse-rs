use hyper::Request;
use hyper_util::client::legacy::{connect::Connect, Client, ResponseFuture};
use lazy_static::lazy_static;
use prometheus::{CounterVec, register_counter_vec};
use sealed::sealed;

use crate::request_body::RequestBody;

/// A trait for underlying HTTP client.
///
/// Firstly, now it is implemented only for
/// `hyper_util::client::legacy::Client`, it's impossible to use another HTTP
/// client.
///
/// Secondly, although it's stable in terms of semver, it will be changed in the
/// future (e.g. to support more runtimes, not only tokio). Thus, prefer to open
/// a feature request instead of implementing this trait manually.
///
lazy_static! {
    static ref SEND_REQUEST_COUNT: CounterVec = register_counter_vec!(
        "send_request_count",
        "send_request_count.",
        &["type"]
    ).unwrap();
}
#[sealed]
pub trait HttpClient: Send + Sync + 'static {
    fn request(&self, req: Request<RequestBody>) -> ResponseFuture;
}

#[sealed]
impl<C> HttpClient for Client<C, RequestBody>
where
    C: Connect + Clone + Send + Sync + 'static,
{
    fn request(&self, req: Request<RequestBody>) -> ResponseFuture {
        SEND_REQUEST_COUNT.with_label_values(&["real"]).inc();
        self.request(req)
    }
}
