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
    /// Optional query ID sent in the query packet header.
    ///
    /// If `None`, an empty string is sent and the server generates its own ID.
    query_id: Option<String>,
    /// Per-query settings that override (or extend) the client-level settings.
    settings: Vec<(String, String)>,
}

impl NativeQuery {
    pub(crate) fn new(client: NativeClient, sql: &str) -> Self {
        Self {
            client,
            sql: sql.to_string(),
            query_id: None,
            settings: Vec::new(),
        }
    }

    /// Set the query ID sent to the server in the query packet header.
    ///
    /// ClickHouse records this ID in `system.query_log` and returns it in
    /// progress/profile packets.  If not set, an empty string is sent and the
    /// server generates its own UUID.
    pub fn with_query_id(mut self, id: impl Into<String>) -> Self {
        self.query_id = Some(id.into());
        self
    }

    /// Add per-query settings that override client-level settings for this
    /// query only.
    ///
    /// Settings are sent in the query packet and apply only to this query.
    /// They are merged with (and take precedence over) any settings configured
    /// on the [`NativeClient`] via [`NativeClient::with_setting`].
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use clickhouse::native::NativeClient;
    /// # async fn example() -> clickhouse::error::Result<()> {
    /// let client = NativeClient::default();
    /// let rows = client
    ///     .query("SELECT * FROM large_table")
    ///     .with_settings([("max_rows_to_read", "1000000")])
    ///     .fetch_all::<(u64,)>()
    ///     .await?;
    /// # Ok(()) }
    /// ```
    pub fn with_settings(
        mut self,
        settings: impl IntoIterator<Item = (impl Into<String>, impl Into<String>)>,
    ) -> Self {
        self.settings
            .extend(settings.into_iter().map(|(k, v)| (k.into(), v.into())));
        self
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

    /// Merge client-level settings with per-query settings.
    ///
    /// Per-query settings take precedence: if the same key appears in both,
    /// the per-query value wins.
    fn merged_settings(&self) -> Vec<(String, String)> {
        if self.settings.is_empty() {
            return self.client.settings().to_vec();
        }
        // Start with client-level settings, then override with per-query ones.
        let mut merged: Vec<(String, String)> = self.client.settings().to_vec();
        for (k, v) in &self.settings {
            if let Some(existing) = merged.iter_mut().find(|(ek, _)| ek == k) {
                existing.1 = v.clone();
            } else {
                merged.push((k.clone(), v.clone()));
            }
        }
        merged
    }

    /// Execute a DDL or non-SELECT query (CREATE, DROP, INSERT, etc.).
    pub async fn execute(self) -> Result<()> {
        let query_id = self.query_id.as_deref().unwrap_or("");
        let settings = self.merged_settings();
        let mut conn = self.client.acquire().await?;
        conn.execute_query_with(query_id, &self.sql, &settings).await
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
        let query_id = self.query_id.clone().unwrap_or_default();
        let settings = self.merged_settings();
        Ok(NativeRowCursor::new(self.client, self.sql, query_id, settings))
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
