use std::{error::Error as StdError, fmt, result};

use serde::ser;

pub type Result<T, E = Error> = result::Result<T, E>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("network error: {0}")]
    Network(Box<dyn StdError>),
    #[error("sequences must have a knowable size ahead of time")]
    SequenceMustHaveLength,
    #[error("a custom error message from serde: {0}")]
    Custom(String),
}

impl ser::Error for Error {
    fn custom<T: fmt::Display>(msg: T) -> Self {
        Error::Custom(msg.to_string())
    }
}
