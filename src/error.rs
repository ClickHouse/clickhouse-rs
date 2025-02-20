//! Contains [`Error`] and corresponding [`Result`].

use std::{error::Error as StdError, fmt, io, result, str::Utf8Error};

use serde::{de, ser};

/// A result with a specified [`Error`] type.
pub type Result<T, E = Error> = result::Result<T, E>;

type BoxedError = Box<dyn StdError + Send + Sync>;

/// Represents all possible errors.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
#[allow(missing_docs)]
pub enum Error {
    #[error("invalid params: {0}")]
    InvalidParams(#[source] BoxedError),
    #[error("network error: {0}")]
    Network(#[source] BoxedError),
    #[error("compression error: {0}")]
    Compression(#[source] BoxedError),
    #[error("decompression error: {0}")]
    Decompression(#[source] BoxedError),
    #[error("no rows returned by a query that expected to return at least one row")]
    RowNotFound,
    #[error("sequences must have a known size ahead of time")]
    SequenceMustHaveLength,
    #[error("`deserialize_any` is not supported")]
    DeserializeAnyNotSupported,
    #[error("not enough data, probably a row type mismatches a database schema")]
    NotEnoughData,
    #[error("string is not valid utf8")]
    InvalidUtf8Encoding(#[from] Utf8Error),
    #[error("tag for enum is not valid")]
    InvalidTagEncoding(usize),
    #[error("max number of types in the Variant data type is 255, got {0}")]
    VariantDiscriminatorIsOutOfBound(usize),
    #[error("a custom error message from serde: {0}")]
    Custom(String),
    #[error("bad response: {0}")]
    BadResponse(String),
    #[error("timeout expired")]
    TimedOut,
    #[error("unsupported: {0}")]
    Unsupported(String),
    #[error("{0}")]
    Other(BoxedError),
}

assert_impl_all!(Error: StdError, Send, Sync);

impl From<hyper::Error> for Error {
    fn from(error: hyper::Error) -> Self {
        Self::Network(Box::new(error))
    }
}

impl From<hyper_util::client::legacy::Error> for Error {
    fn from(error: hyper_util::client::legacy::Error) -> Self {
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

impl From<Error> for io::Error {
    fn from(error: Error) -> Self {
        io::Error::new(io::ErrorKind::Other, error)
    }
}

impl From<io::Error> for Error {
    fn from(error: io::Error) -> Self {
        // TODO: after MSRV 1.79 replace with `io::Error::downcast`.
        if error.get_ref().is_some_and(|r| r.is::<Error>()) {
            *error.into_inner().unwrap().downcast::<Error>().unwrap()
        } else {
            Self::Other(error.into())
        }
    }
}

#[test]
fn roundtrip_io_error() {
    let orig = Error::NotEnoughData;

    // Error -> io::Error
    let orig_str = orig.to_string();
    let io = io::Error::from(orig);
    assert_eq!(io.kind(), io::ErrorKind::Other);
    assert_eq!(io.to_string(), orig_str);

    // io::Error -> Error
    let orig = Error::from(io);
    assert!(matches!(orig, Error::NotEnoughData));
}
