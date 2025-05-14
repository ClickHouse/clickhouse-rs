#[non_exhaustive]
#[derive(Clone)]
pub enum OutputFormat {
    RowBinary,
    RowBinaryWithNamesAndTypes,
}

impl Default for OutputFormat {
    fn default() -> Self {
        Self::RowBinary
    }
}

impl std::fmt::Display for OutputFormat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OutputFormat::RowBinary => write!(f, "RowBinary"),
            OutputFormat::RowBinaryWithNamesAndTypes => {
                write!(f, "RowBinaryWithNamesAndTypes")
            }
        }
    }
}
