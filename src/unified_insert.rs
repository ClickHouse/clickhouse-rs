//! [`UnifiedInsert`] — INSERT handle that dispatches to either HTTP or native
//! TCP transport.
//!
//! Returned by [`crate::unified::UnifiedClient::insert`].

use crate::error::Result;
use crate::row::{Row, RowWrite};

// ---------------------------------------------------------------------------
// Inner enum
// ---------------------------------------------------------------------------

enum InsertInner<T> {
    Http(crate::insert::Insert<T>),
    #[cfg(feature = "native-transport")]
    Native(crate::native::NativeInsert<T>),
}

// ---------------------------------------------------------------------------
// UnifiedInsert
// ---------------------------------------------------------------------------

/// A single in-flight INSERT statement, backed by either the HTTP or native
/// TCP transport.
///
/// Returned by [`crate::unified::UnifiedClient::insert`].
///
/// Call [`write`](UnifiedInsert::write) for each row, then
/// [`end`](UnifiedInsert::end) to commit the INSERT.  Dropping without calling
/// `end` silently aborts the INSERT.
///
/// # Examples
///
/// ```no_run
/// # use clickhouse::unified::UnifiedClient;
/// # use clickhouse::{Row, RowOwned, RowWrite};
/// # use serde::Serialize;
/// # async fn example() -> clickhouse::error::Result<()> {
/// #[derive(Row, Serialize)]
/// struct Event { id: u64, name: String }
///
/// let client = UnifiedClient::http().with_url("http://localhost:8123").build();
/// let mut insert = client.insert::<Event>("events").await?;
/// insert.write(&Event { id: 1, name: "foo".into() }).await?;
/// insert.end().await?;
/// # Ok(()) }
/// ```
#[must_use]
pub struct UnifiedInsert<T> {
    inner: InsertInner<T>,
}

impl<T> UnifiedInsert<T> {
    pub(crate) fn from_http(insert: crate::insert::Insert<T>) -> Self {
        Self {
            inner: InsertInner::Http(insert),
        }
    }

    #[cfg(feature = "native-transport")]
    pub(crate) fn from_native(insert: crate::native::NativeInsert<T>) -> Self {
        Self {
            inner: InsertInner::Native(insert),
        }
    }

    /// Serialise `row` into the internal buffer and flush if above threshold.
    ///
    /// The future does not borrow `row` after it returns.
    pub async fn write(&mut self, row: &T::Value<'_>) -> Result<()>
    where
        T: RowWrite,
    {
        match &mut self.inner {
            InsertInner::Http(i) => i.write(row).await,
            #[cfg(feature = "native-transport")]
            InsertInner::Native(i) => i.write(row).await,
        }
    }

    /// Flush remaining buffered rows and signal end of INSERT to the server.
    ///
    /// Must be called to commit the INSERT.  If not called, the INSERT is
    /// silently aborted when this value is dropped.
    pub async fn end(self) -> Result<()>
    where
        T: Row,
    {
        match self.inner {
            InsertInner::Http(i) => i.end().await,
            #[cfg(feature = "native-transport")]
            InsertInner::Native(i) => i.end().await,
        }
    }
}
