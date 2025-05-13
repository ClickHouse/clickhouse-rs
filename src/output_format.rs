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
