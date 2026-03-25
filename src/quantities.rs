// -----------------------------------------------------------------------
// Shared insert statistics — used by both HTTP and native inserters.
//
// Extracted to avoid duplicating the same struct in `inserter.rs` and
// `native/inserter.rs`.  Both re-export `Quantities` so existing code
// using `crate::inserter::Quantities` or `crate::native::inserter::Quantities`
// continues to work unchanged.
// -----------------------------------------------------------------------

/// Statistics about pending or inserted data.
///
/// Returned by inserter commit/flush operations to report how much
/// data was written.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Quantities {
    /// Approximate number of uncompressed bytes written.
    pub bytes: u64,
    /// Number of rows written.
    pub rows: u64,
    /// Number of non-empty transactions (INSERT statements) committed.
    pub transactions: u64,
}

impl Quantities {
    /// All-zero quantities.
    pub const ZERO: Quantities = Quantities {
        bytes: 0,
        rows: 0,
        transactions: 0,
    };
}
