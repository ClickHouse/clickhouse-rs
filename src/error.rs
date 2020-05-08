use std::{error::Error as StdError, fmt, result};

use serde::ser;

pub type Result<T, E = Error> = result::Result<T, E>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("invalid params: {0}")]
    InvalidParams(#[source] Box<dyn StdError>),
    #[error("network error: {0}")]
    Network(#[source] Box<dyn StdError>),
    #[error("sequences must have a knowable size ahead of time")]
    SequenceMustHaveLength,
    #[error("a custom error message from serde: {0}")]
    Custom(String),
    #[error("bad response: {0}")]
    BadResponse(String),
}

impl From<hyper::Error> for Error {
    fn from(error: hyper::Error) -> Self {
        Self::Network(Box::new(error))
    }
}

impl ser::Error for Error {
    fn custom<T: fmt::Display>(msg: T) -> Self {
        Self::Custom(msg.to_string())
    }
}
