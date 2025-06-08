/// The preferred mode of validation for struct (de)serialization.
/// It also affects which format is used by the client when sending queries.
///
/// - [`ValidationMode::First`] enables validation _only for the first `N` rows_
///   emitted by a cursor. For the following rows, validation is skipped.
///   Format: `RowBinaryWithNamesAndTypes`.
/// - [`ValidationMode::Each`] enables validation _for all rows_ emitted by a cursor.
///   This is the slowest mode. Format: `RowBinaryWithNamesAndTypes`.
///
/// # Default
///
/// By default, [`ValidationMode::First`] with value `1` is used,
/// meaning that only the first row will be validated against the database schema,
/// which is extracted from the `RowBinaryWithNamesAndTypes` format header.
/// It is done to minimize the performance impact of the validation,
/// while still providing reasonable safety guarantees by default.
///
/// While it is expected that the default validation mode is sufficient for most use cases,
/// in certain corner case scenarios there still can be schema mismatches after the first rows,
/// e.g., when a field is `Nullable(T)`, and the first value is `NULL`. In that case,
/// consider increasing the number of rows in [`ValidationMode::First`],
/// or even using [`ValidationMode::Each`] instead.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ValidationMode {
    First(usize),
    Each,
}

impl Default for ValidationMode {
    fn default() -> Self {
        Self::First(1)
    }
}
