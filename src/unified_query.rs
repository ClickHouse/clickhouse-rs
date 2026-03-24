//! [`UnifiedQuery`] — query builder that dispatches to either HTTP or native transport.
//!
//! Returned by [`crate::unified::UnifiedClient::query`].

use crate::error::Result;
use crate::row::{RowOwned, RowRead};

// ---------------------------------------------------------------------------
// Inner enum
// ---------------------------------------------------------------------------

enum QueryInner {
    Http(crate::query::Query),
    #[cfg(feature = "native-transport")]
    Native(crate::native::NativeQuery),
}

// ---------------------------------------------------------------------------
// UnifiedQuery
// ---------------------------------------------------------------------------

/// A query builder returned by [`crate::unified::UnifiedClient::query`].
///
/// Dispatches `execute`, `fetch_all`, `fetch_one`, and `fetch_optional` to
/// whichever transport is active.
///
/// # Parameter binding
///
/// [`UnifiedQuery::bind`] accepts any [`std::fmt::Display`] value. For the
/// HTTP transport the display string is forwarded as a serialised argument;
/// for the native transport it is substituted directly into the SQL string.
#[must_use]
pub struct UnifiedQuery {
    inner: QueryInner,
}

impl UnifiedQuery {
    pub(crate) fn from_http(query: crate::query::Query) -> Self {
        Self {
            inner: QueryInner::Http(query),
        }
    }

    #[cfg(feature = "native-transport")]
    pub(crate) fn from_native(query: crate::native::NativeQuery) -> Self {
        Self {
            inner: QueryInner::Native(query),
        }
    }

    // -----------------------------------------------------------------------
    // Builder methods
    // -----------------------------------------------------------------------

    /// Bind the next `?` placeholder in the query to `value`.
    ///
    /// Uses [`std::fmt::Display`] as the common interface across both
    /// transports.  For the HTTP transport the value is converted to a
    /// `String` first (which implements `serde::Serialize`) and passed to the
    /// underlying [`crate::query::Query::bind`].
    pub fn bind(self, value: impl std::fmt::Display) -> Self {
        match self.inner {
            QueryInner::Http(q) => Self {
                inner: QueryInner::Http(q.bind(value.to_string())),
            },
            #[cfg(feature = "native-transport")]
            QueryInner::Native(q) => Self {
                inner: QueryInner::Native(q.bind(value)),
            },
        }
    }

    // -----------------------------------------------------------------------
    // Terminal methods
    // -----------------------------------------------------------------------

    /// Execute a DDL or non-SELECT statement and discard any results.
    pub async fn execute(self) -> Result<()> {
        match self.inner {
            QueryInner::Http(q) => q.execute().await,
            #[cfg(feature = "native-transport")]
            QueryInner::Native(q) => q.execute().await,
        }
    }

    /// Execute a SELECT query and collect all rows into a `Vec`.
    pub async fn fetch_all<T>(self) -> Result<Vec<T>>
    where
        T: RowOwned + RowRead,
    {
        match self.inner {
            QueryInner::Http(q) => q.fetch_all::<T>().await,
            #[cfg(feature = "native-transport")]
            QueryInner::Native(q) => q.fetch_all::<T>().await,
        }
    }

    /// Execute a SELECT query and return exactly one row.
    ///
    /// Returns [`crate::error::Error::RowNotFound`] if the result set is empty.
    pub async fn fetch_one<T>(self) -> Result<T>
    where
        T: RowOwned + RowRead,
    {
        match self.inner {
            QueryInner::Http(q) => q.fetch_one::<T>().await,
            #[cfg(feature = "native-transport")]
            QueryInner::Native(q) => q.fetch_one::<T>().await,
        }
    }

    /// Execute a SELECT query and return at most one row.
    pub async fn fetch_optional<T>(self) -> Result<Option<T>>
    where
        T: RowOwned + RowRead,
    {
        match self.inner {
            QueryInner::Http(q) => q.fetch_optional::<T>().await,
            #[cfg(feature = "native-transport")]
            QueryInner::Native(q) => q.fetch_optional::<T>().await,
        }
    }
}
