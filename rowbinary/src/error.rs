#[derive(Debug, thiserror::Error)]
pub enum ColumnsParserError {
    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),

    #[error("Expected LF at position {0}")]
    ExpectedLF(usize),

    #[error("Invalid integer encoding at position {0}")]
    InvalidIntegerEncoding(usize),

    #[error("Incomplete column data at position {0}")]
    IncompleteColumnData(usize),

    #[error("Invalid column spec at position {0}: {1}")]
    InvalidColumnSpec(usize, String),

    #[error("Type parsing error: {0}")]
    TypeParsingError(String),
}
