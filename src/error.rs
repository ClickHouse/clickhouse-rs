//! Contains [`Error`] and corresponding [`Result`].

use serde::{de, ser};
use std::{error::Error as StdError, fmt, io, result, str::Utf8Error};

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
    /// A typed ClickHouse server exception, parsed from the server's response.
    ///
    /// Constructed when the response body or `X-ClickHouse-Exception-Code`
    /// header carries a recognisable `Code: <num>. DB::<Class>: <message>
    /// (NAME) (version ...)` payload. Falls back to [`Error::BadResponse`]
    /// for responses that cannot be parsed (proxy errors, non-ClickHouse
    /// HTTP responses, malformed payloads).
    ///
    /// Use this variant for code/name-aware control flow:
    ///
    /// ```ignore
    /// match err {
    ///     Error::ServerError { code: 252, .. } => alert_on_too_many_parts(),
    ///     Error::ServerError { code, .. } if (43..=53).contains(&code) =>
    ///         report_caller_error(),
    ///     _ => report_internal_error(err),
    /// }
    /// ```
    #[error("server error: Code: {code}{}: {message}", name.as_deref().map(|n| format!(" ({n})")).unwrap_or_default())]
    ServerError {
        /// Numeric ClickHouse exception code (e.g. `252` for `TOO_MANY_PARTS`).
        ///
        /// The full table of codes lives in
        /// [src/Common/ErrorCodes.cpp](https://github.com/ClickHouse/ClickHouse/blob/master/src/Common/ErrorCodes.cpp)
        /// in the ClickHouse server tree.
        code: u32,
        /// Human-readable exception name (e.g. `"TOO_MANY_PARTS"`), extracted
        /// from the trailing parenthesised tag in the server message.
        ///
        /// `None` when the response is the bare `Code: NNN` form (e.g. a
        /// header-only fast-fail) or when the trailing tag is absent.
        name: Option<String>,
        /// The exception message — the text between `DB::<Class>:` and the
        /// trailing `(NAME) (version ...)` metadata, trimmed.
        message: String,
    },
    #[error("timeout expired")]
    TimedOut,
    #[error("error while parsing columns header from the response: {0}")]
    InvalidColumnsHeader(#[source] BoxedError),
    #[error("schema mismatch: {0}")]
    SchemaMismatch(String),
    #[error("unsupported: {0}")]
    Unsupported(String),
    #[error("{0}")]
    Other(BoxedError),
}

impl From<clickhouse_types::error::TypesError> for Error {
    fn from(err: clickhouse_types::error::TypesError) -> Self {
        Self::InvalidColumnsHeader(Box::new(err))
    }
}

impl From<hyper::Error> for Error {
    fn from(error: hyper::Error) -> Self {
        Self::Network(Box::new(error))
    }
}

impl From<hyper_util::client::legacy::Error> for Error {
    fn from(error: hyper_util::client::legacy::Error) -> Self {
        #[cfg(not(any(feature = "rustls-tls", feature = "native-tls")))]
        if error.is_connect() {
            static SCHEME_IS_NOT_HTTP: &str = "invalid URL, scheme is not http";

            let src = error.source().unwrap();
            // Unfortunately, this seems to be the only way, as `INVALID_NOT_HTTP` is not public.
            // See https://github.com/hyperium/hyper-util/blob/v0.1.14/src/client/legacy/connect/http.rs#L491-L495
            if src.to_string() == SCHEME_IS_NOT_HTTP {
                return Self::Unsupported(format!(
                    "{SCHEME_IS_NOT_HTTP}; if you are trying to connect via HTTPS, \
                    consider enabling `native-tls` or `rustls-tls` feature"
                ));
            }
        }
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
        io::Error::other(error)
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

impl Error {
    /// https://opentelemetry.io/docs/specs/semconv/registry/attributes/error/#error-type
    #[cfg(feature = "opentelemetry")]
    pub(crate) fn error_type(&self) -> &str {
        match self {
            Error::InvalidParams(_) => "InvalidParams",
            Error::Network(_) => "Network",
            Error::Compression(_) => "Compression",
            Error::Decompression(_) => "Decompression",
            Error::RowNotFound => "RowNotFound",
            Error::SequenceMustHaveLength => "SequenceMustHaveLength",
            Error::DeserializeAnyNotSupported => "DeserializeAnyNotSupported",
            Error::NotEnoughData => "NotEnoughData",
            Error::InvalidUtf8Encoding(_) => "InvalidUtf8Encoding",
            Error::InvalidTagEncoding(_) => "InvalidTagEncoding",
            Error::VariantDiscriminatorIsOutOfBound(_) => "VariantDiscriminatorIsOutOfBound",
            Error::Custom(_) => "Custom",
            Error::BadResponse(_) => "BadResponse",
            Error::ServerError { .. } => "ServerError",
            Error::TimedOut => "TimedOut",
            Error::InvalidColumnsHeader(_) => "InvalidColumnsHeader",
            Error::SchemaMismatch(_) => "SchemaMismatch",
            Error::Unsupported(_) => "Unsupported",
            Error::Other(_) => "Other",
        }
    }

    /// Record this `Error` in the context of the current `tracing::Span`,
    /// setting the OpenTelemetry conventional fields if the `opentelemetry` feature is enabled.
    pub(crate) fn record_in_current_span(&self, msg: &str) {
        // Span fields that remain unpopulated are not reported,
        // so we can avoid adding noise to logs if the user isn't utilizing this feature.
        #[cfg(feature = "opentelemetry")]
        tracing::record_all!(
            tracing::Span::current(),
            otel.status_code = "Error",
            otel.status_description = format!("{msg}: {self}"),
            error.type = self.error_type(),
        );

        tracing::debug!(error=%self, "{msg}");
    }
}

#[cfg(tests)]
mod tests {
    use crate::error::Error;
    use std::io;

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

    #[test]
    fn error_traits() {
        fn assert_traits<T: std::error::Error + Send + Sync>() {}

        assert_traits::<Error>();
    }
}
