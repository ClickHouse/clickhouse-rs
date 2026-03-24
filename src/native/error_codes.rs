//! Server exception mapping for ClickHouse native protocol.
//!
//! Maps ClickHouse error codes to severity levels to distinguish
//! fatal server errors from recoverable client/query errors.

use std::fmt;

use crate::native::protocol::ServerException;

/// Severity classification for server exceptions.
#[derive(Debug, Clone)]
pub(crate) enum Severity {
    /// Fatal server-side error — connection should be dropped.
    Server(ServerErrorKind),
    /// Non-fatal query/client error — connection can be reused.
    Client(ClientErrorKind),
}

impl fmt::Display for Severity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Severity::Server(kind) => write!(f, "Server({kind:?})"),
            Severity::Client(kind) => write!(f, "Client({kind:?})"),
        }
    }
}

#[derive(Debug, Clone)]
#[allow(unused)]
pub(crate) enum ServerErrorKind {
    Internal,
    Timeout,
    ResourceExhausted,
    Other,
}

#[derive(Debug, Clone)]
#[allow(unused)]
pub(crate) enum ClientErrorKind {
    Syntax,
    Type,
    NotFound,
    Auth,
    Other,
}

/// A mapped server error with severity classification.
#[derive(Debug, Clone)]
pub(crate) struct ServerError {
    pub(crate) severity: Severity,
    pub(crate) code: i32,
    pub(crate) name: String,
    pub(crate) message: String,
    pub(crate) stack_trace: String,
}

impl ServerError {
    #[allow(dead_code)] // Future error classification — used when pool recycler inspects exception severity
    pub(crate) fn is_fatal(&self) -> bool {
        matches!(self.severity, Severity::Server(_))
    }
}

impl fmt::Display for ServerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "ClickHouse exception: {severity} code={code} {name}: {message}",
            severity = self.severity,
            code = self.code,
            name = self.name,
            message = self.message,
        )?;
        if !self.stack_trace.is_empty() {
            write!(f, "\nStack trace:\n")?;
            for line in self.stack_trace.lines() {
                writeln!(f, "  {line}")?;
            }
        }
        Ok(())
    }
}

/// Map a raw server exception to a classified `ServerError`.
pub(crate) fn map_exception_to_error(exception: ServerException) -> ServerError {
    let severity = map_error_code(exception.code);
    ServerError {
        severity,
        code: exception.code,
        name: exception.name,
        message: exception.message,
        stack_trace: exception.stack_trace,
    }
}

/// Classify an error code into severity.
///
/// Based on ClickHouse error codes from ErrorCodes.h.
/// Only the most common codes are mapped; everything else defaults to Client(Other).
fn map_error_code(code: i32) -> Severity {
    match code {
        // Server-side fatal errors
        1 => Severity::Server(ServerErrorKind::Internal),      // UNSUPPORTED_METHOD
        48 => Severity::Server(ServerErrorKind::Internal),     // NOT_IMPLEMENTED
        76 => Severity::Server(ServerErrorKind::Internal),     // LOGICAL_ERROR
        159 => Severity::Server(ServerErrorKind::Timeout),     // TIMEOUT_EXCEEDED
        241 => Severity::Server(ServerErrorKind::ResourceExhausted), // MEMORY_LIMIT_EXCEEDED
        252 => Severity::Server(ServerErrorKind::ResourceExhausted), // TOO_MANY_SIMULTANEOUS_QUERIES
        // Client / query errors
        27 => Severity::Client(ClientErrorKind::NotFound),     // UNKNOWN_DATABASE
        36 => Severity::Client(ClientErrorKind::Type),         // TYPE_MISMATCH
        47 => Severity::Client(ClientErrorKind::Syntax),       // UNKNOWN_IDENTIFIER
        60 => Severity::Client(ClientErrorKind::NotFound),     // UNKNOWN_TABLE
        62 => Severity::Client(ClientErrorKind::Syntax),       // SYNTAX_ERROR
        192 => Severity::Client(ClientErrorKind::Auth),        // AUTHENTICATION_FAILED
        516 => Severity::Client(ClientErrorKind::Auth),        // AUTHENTICATION_FAILED (v2)
        _ => Severity::Client(ClientErrorKind::Other),
    }
}
