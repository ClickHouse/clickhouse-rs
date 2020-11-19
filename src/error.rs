use std::{error::Error as StdError, fmt, io, result, str::Utf8Error};

use serde::{de, ser};
use static_assertions::assert_impl_all;

pub type Result<T, E = Error> = result::Result<T, E>;

#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    #[error("invalid params: {0}")]
    InvalidParams(#[source] Box<dyn StdError + Send + Sync>),
    #[error("network error: {0}")]
    Network(#[source] Box<dyn StdError + Send + Sync>),
    #[error("decompression error: {0}")]
    Decompression(#[source] Box<dyn StdError + Send + Sync>),
    #[error("no rows returned by a query that expected to return at least one row")]
    RowNotFound,
    #[error("sequences must have a knowable size ahead of time")]
    SequenceMustHaveLength,
    #[error("`deserialize_any` is not supported")]
    DeserializeAnyNotSupported,
    #[error("not enough data. Probably a row type mismatches a database schema")]
    NotEnoughData,
    #[error("string is not valid utf8")]
    InvalidUtf8Encoding(#[from] Utf8Error),
    #[error("tag for enum is not valid")]
    InvalidTagEncoding(usize),
    #[error("a custom error message from serde: {0}")]
    Custom(String),
    #[error("bad response: {0}")]
    BadResponse(String),
}

assert_impl_all!(Error: StdError, Send, Sync);

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

impl de::Error for Error {
    fn custom<T: fmt::Display>(msg: T) -> Self {
        Self::Custom(msg.to_string())
    }
}

impl Error {
    pub(crate) fn into_io(self) -> io::Error {
        io::Error::new(io::ErrorKind::Other, self)
    }

    pub(crate) fn decode_io(err: io::Error) -> Self {
        if err.get_ref().map(|r| r.is::<Error>()).unwrap_or(false) {
            *err.into_inner().unwrap().downcast::<Error>().unwrap()
        } else {
            Self::Decompression(Box::new(err))
        }
    }
}

#[test]
fn roundtrip_io_error() {
    let orig = Error::NotEnoughData;
    let io = orig.into_io();
    let err = Error::decode_io(io);
    assert!(matches!(err, Error::NotEnoughData));
}
