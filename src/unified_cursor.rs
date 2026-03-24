//! [`UnifiedCursor`] — streaming row cursor that dispatches to either HTTP or
//! native TCP transport.
//!
//! Returned by [`crate::unified_query::UnifiedQuery::fetch`].

use crate::error::Result;
use crate::row::{RowOwned, RowRead};

// ---------------------------------------------------------------------------
// Inner enum
// ---------------------------------------------------------------------------

enum CursorInner<T: RowOwned + RowRead> {
    Http(crate::cursors::RowCursor<T>),
    #[cfg(feature = "native-transport")]
    Native(crate::native::NativeRowCursor<T>),
}

// ---------------------------------------------------------------------------
// UnifiedCursor
// ---------------------------------------------------------------------------

/// A streaming cursor over query results, backed by either the HTTP or native
/// TCP transport.
///
/// Returned by [`crate::unified_query::UnifiedQuery::fetch`].
///
/// `T` must be [`RowOwned`] — the deserialized value must not borrow from the
/// network buffer.  This is the common case for all derived `Row` types.
///
/// # Examples
///
/// ```no_run
/// # use clickhouse::unified::UnifiedClient;
/// # use clickhouse::{Row, RowOwned, RowRead};
/// # use serde::Deserialize;
/// # async fn example() -> clickhouse::error::Result<()> {
/// #[derive(Row, Deserialize)]
/// struct MyRow { id: u64 }
///
/// let client = UnifiedClient::http().with_url("http://localhost:8123").build();
/// let mut cursor = client.query("SELECT id FROM t").fetch::<MyRow>()?;
/// while let Some(row) = cursor.next().await? {
///     println!("{}", row.id);
/// }
/// # Ok(()) }
/// ```
pub struct UnifiedCursor<T: RowOwned + RowRead> {
    inner: CursorInner<T>,
}

impl<T: RowOwned + RowRead> UnifiedCursor<T> {
    pub(crate) fn from_http(cursor: crate::cursors::RowCursor<T>) -> Self {
        Self {
            inner: CursorInner::Http(cursor),
        }
    }

    #[cfg(feature = "native-transport")]
    pub(crate) fn from_native(cursor: crate::native::NativeRowCursor<T>) -> Self {
        Self {
            inner: CursorInner::Native(cursor),
        }
    }

    /// Return the next row, or `None` at the end of the result set.
    ///
    /// For the HTTP transport `T::Value<'_>` resolves to `T` because
    /// `T: RowOwned`.  For the native transport `next()` already returns `T`
    /// directly.  Both arms therefore produce an owned `T`.
    ///
    /// # Cancel safety
    ///
    /// This method is cancellation safe.
    pub async fn next(&mut self) -> Result<Option<T>> {
        match &mut self.inner {
            CursorInner::Http(c) => c.next().await,
            #[cfg(feature = "native-transport")]
            CursorInner::Native(c) => c.next().await,
        }
    }
}
