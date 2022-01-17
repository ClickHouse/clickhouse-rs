use hyper::{
    client::{connect::Connect, ResponseFuture},
    Body, Request,
};
use sealed::sealed;

#[sealed]
pub trait HttpClient: Send + Sync + 'static {
    fn _request(&self, req: Request<Body>) -> ResponseFuture;
}

#[sealed]
impl<C> HttpClient for hyper::Client<C>
where
    C: Connect + Clone + Send + Sync + 'static,
{
    fn _request(&self, req: Request<Body>) -> ResponseFuture {
        self.request(req)
    }
}
