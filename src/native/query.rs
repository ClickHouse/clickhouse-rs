//! Native query builder — mirrors `crate::query::Query` for the native transport.

use crate::error::{Error, Result};
use crate::native::client::NativeClient;
use crate::native::cursor::NativeRowCursor;
use crate::row::{RowOwned, RowRead};

/// A query being built for native transport execution.
///
/// Follows the same builder pattern as [`crate::query::Query`].
#[must_use]
pub struct NativeQuery {
    client: NativeClient,
    sql: String,
}

impl NativeQuery {
    pub(crate) fn new(client: NativeClient, sql: &str) -> Self {
        Self {
            client,
            sql: sql.to_string(),
        }
    }

    /// Bind a parameter using simple string substitution.
    ///
    /// Replaces the next `?` placeholder in the SQL string.
    ///
    /// For production use, prefer parameterized queries with ClickHouse's
    /// `{name: Type}` syntax via the HTTP client.
    pub fn bind(mut self, value: impl std::fmt::Display) -> Self {
        if let Some(pos) = self.sql.find('?') {
            self.sql = format!(
                "{}{}{}",
                &self.sql[..pos],
                value,
                &self.sql[pos + 1..]
            );
        }
        self
    }

    /// Execute a DDL or non-SELECT query (CREATE, DROP, INSERT, etc.).
    pub async fn execute(self) -> Result<()> {
        let mut conn = self.client.acquire().await?;
        conn.execute_query(&self.sql).await
    }

    /// Execute a SELECT query, returning a cursor over deserialized rows.
    ///
    /// # Type support
    ///
    /// The native transport supports: Int/UInt 8/16/32/64/128/256, Float32/64,
    /// String, FixedString(N), UUID, Date, Date32, DateTime, DateTime64,
    /// Nullable(T), and LowCardinality(T).
    ///
    /// Complex types (Array, Map, Tuple) are not yet supported.
    /// Execute a SELECT query, returning a cursor over deserialized rows.
    ///
    /// `T` must be [`RowOwned`] — the deserialized value must not borrow from
    /// the network buffer.
    ///
    /// # Type support
    ///
    /// Supported: Int/UInt 8/16/32/64/128/256, Float32/64, String, FixedString(N),
    /// UUID, Date, Date32, DateTime, DateTime64, Nullable(T), LowCardinality(T).
    ///
    /// Not yet supported: Array, Map, Tuple.
    pub fn fetch<T>(self) -> Result<NativeRowCursor<T>>
    where
        T: RowOwned + RowRead,
    {
        Ok(NativeRowCursor::new(self.client, self.sql))
    }

    /// Fetch a single row.
    pub async fn fetch_one<T>(self) -> Result<T>
    where
        T: RowOwned + RowRead,
    {
        let mut cursor = self.fetch::<T>()?;
        let row = match cursor.next().await {
            Ok(Some(row)) => row,
            Ok(None) => return Err(Error::RowNotFound),
            Err(err) => return Err(err),
        };
        // Drain remaining packets so the connection is returned to the pool
        // in a clean state rather than mid-stream.
        cursor.drain().await?;
        Ok(row)
    }

    /// Fetch all rows into a Vec.
    pub async fn fetch_all<T>(self) -> Result<Vec<T>>
    where
        T: RowOwned + RowRead,
    {
        let mut result = Vec::new();
        let mut cursor = self.fetch::<T>()?;
        while let Some(row) = cursor.next().await? {
            result.push(row);
        }
        Ok(result)
    }

    /// Fetch at most one row.
    pub async fn fetch_optional<T>(self) -> Result<Option<T>>
    where
        T: RowOwned + RowRead,
    {
        let mut cursor = self.fetch::<T>()?;
        let row = cursor.next().await?;
        // Drain if we got a row without reaching EndOfStream.
        cursor.drain().await?;
        Ok(row)
    }
}
