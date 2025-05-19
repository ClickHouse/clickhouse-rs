// FIXME: better errors
#[derive(Debug, thiserror::Error)]
pub enum ParserError {
    #[error("Not enough data: {0}")]
    NotEnoughData(String),

    #[error("Header parsing error: {0}")]
    HeaderParsingError(String),

    #[error("Type parsing error: {0}")]
    TypeParsingError(String),
}
