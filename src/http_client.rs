use hyper::{
    client::{connect::Connect, ResponseFuture},
    Body, Request,
};

use crate::sealed::Sealed;

pub type BoxHttpClient = Box<dyn HttpClient + Send + Sync + 'static>;

pub trait HttpClient: Sealed + CloneBoxHttpClient {
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

pub trait CloneBoxHttpClient {
    fn clone_box_http_client(&self) -> BoxHttpClient;
}

impl<T> CloneBoxHttpClient for T
where
    T: 'static + HttpClient + Clone + Send + Sync + 'static,
{
    fn clone_box_http_client(&self) -> BoxHttpClient {
        Box::new(self.clone())
    }
}

impl Clone for BoxHttpClient {
    fn clone(&self) -> Self {
        self.clone_box_http_client()
    }
}
