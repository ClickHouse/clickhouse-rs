use hyper::Request;
use hyper_util::client::legacy::{connect::Connect, Client, ResponseFuture};
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
        self.request(req)
    }
}
