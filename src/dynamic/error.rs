//! Error types for dynamic (schema-driven) inserts.

use std::fmt;

/// Errors specific to dynamic schema-driven inserts.
#[derive(Debug)]
pub enum DynamicError {
    /// Column type string could not be parsed.
    UnsupportedType { column: String, type_str: String },
    /// Value could not be encoded for the target column type.
    EncodingError { column: String, message: String },
    /// Schema mismatch detected — server rejected the insert.
    SchemaMismatch { table: String, message: String },
    /// Schema fetch from system.columns failed.
    SchemaFetch {
        table: String,
        source: crate::error::Error,
    },
    /// Table has no columns (or does not exist).
    EmptySchema { table: String },
}

impl fmt::Display for DynamicError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnsupportedType { column, type_str } => {
                write!(f, "unsupported type '{type_str}' for column '{column}'")
            }
            Self::EncodingError { column, message } => {
                write!(f, "encoding error for column '{column}': {message}")
            }
            Self::SchemaMismatch { table, message } => {
                write!(f, "schema mismatch for table '{table}': {message}")
            }
            Self::SchemaFetch { table, source } => {
                write!(f, "failed to fetch schema for '{table}': {source}")
            }
            Self::EmptySchema { table } => {
                write!(f, "table '{table}' has no columns or does not exist")
            }
        }
    }
}

impl std::error::Error for DynamicError {}

impl From<DynamicError> for crate::error::Error {
    fn from(e: DynamicError) -> Self {
        crate::error::Error::Custom(e.to_string())
    }
}
