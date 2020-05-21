use hyper::{body, client::ResponseFuture, Body, StatusCode};

use crate::error::{Error, Result};

pub enum Response {
    Waiting(ResponseFuture),
    Loading(Body),
}

impl From<ResponseFuture> for Response {
    fn from(future: ResponseFuture) -> Self {
        Self::Waiting(future)
    }
}

impl Response {
    pub async fn resolve(&mut self) -> Result<&mut Body> {
        if let Self::Waiting(response) = self {
            let response = response.await?;

            if response.status() != StatusCode::OK {
                let bytes = body::to_bytes(response.into_body()).await?;
                let reason = String::from_utf8_lossy(&bytes).trim().into();

                return Err(Error::BadResponse(reason));
            }

            let body = response.into_body();
            *self = Self::Loading(body);
        }

        match self {
            Self::Waiting(_) => unreachable!(),
            Self::Loading(body) => Ok(body),
        }
    }
}
