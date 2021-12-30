use hyper::{
    client::{connect::Connect, ResponseFuture},
    Body, Request,
};

use crate::sealed::Sealed;

pub trait HttpClient: Sealed + Send + Sync + 'static {
    fn _request(&self, req: Request<Body>) -> ResponseFuture;
}

impl<C> Sealed for hyper::Client<C> {}

impl<C> HttpClient for hyper::Client<C>
where
    C: Connect + Clone + Send + Sync + 'static,
{
    fn _request(&self, req: Request<Body>) -> ResponseFuture {
        self.request(req)
    }
}
