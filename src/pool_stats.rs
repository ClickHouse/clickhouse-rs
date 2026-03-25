//! Connection pool statistics.

/// Snapshot of connection pool utilisation.
///
/// Obtain via [`crate::native::NativeClient::pool_stats`] or
/// [`crate::unified::UnifiedClient::pool_stats`].
///
/// All values are eventually-consistent -- they reflect the pool state at the
/// moment of the call but may already be stale by the time they are read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PoolStats {
    /// Maximum number of connections (idle + in-use) the pool will hold.
    pub max_size: usize,
    /// Current number of connections (idle + in-use).
    pub size: usize,
    /// Number of connections currently idle and available for checkout.
    pub available: usize,
    /// Number of tasks waiting for a connection to become available.
    pub waiting: usize,
}
