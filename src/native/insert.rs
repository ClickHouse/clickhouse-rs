//! `NativeInsert<T>` — a single INSERT statement over the native TCP protocol.
//!
//! Mirrors the public API of [`crate::insert::Insert`] so code using the HTTP
//! client can switch to the native transport with minimal changes.
//!
//! # Usage
//!
//! ```no_run
//! # async fn example() -> clickhouse::error::Result<()> {
//! use clickhouse::{Row, native::NativeClient};
//! use serde::Serialize;
//!
//! #[derive(Row, Serialize)]
//! struct Event { id: u64, name: String }
//!
//! let client = NativeClient::default();
//! let mut insert = client.insert::<Event>("events");
//! insert.write(&Event { id: 1, name: "foo".into() }).await?;
//! insert.end().await?;
//! # Ok(()) }
//! ```
//!
//! # Behaviour
//!
//! - Connection is opened lazily on the first call to [`write`](NativeInsert::write).
//! - Rows are serialised to RowBinary and buffered in memory.
//! - The buffer is flushed (transposed to native columnar format and sent) when
//!   it exceeds ~256 KiB, and also when [`end`](NativeInsert::end) is called.
//! - [`end`](NativeInsert::end) must be called to commit the INSERT.  Dropping
//!   without calling `end` silently aborts (connection dropped).

use std::marker::PhantomData;
use std::time::Duration;

use bytes::BytesMut;

use crate::error::{Error, Result};
use crate::native::client::NativeClient;
use crate::native::encode::{ColumnSchema, encode_columns};
use crate::native::pool::PooledConnection;
use crate::row::{self, Row, RowWrite};
use crate::rowbinary::serialize_row_binary;

/// Desired flush threshold (~256 KiB uncompressed).
const BUFFER_SIZE: usize = 256 * 1024;
/// Soft flush limit — slightly below `BUFFER_SIZE` to avoid one extra allocation.
const MIN_CHUNK_SIZE: usize = BUFFER_SIZE - 2048;

/// A single in-flight native INSERT statement.
///
/// Call [`write`](NativeInsert::write) for each row, then
/// [`end`](NativeInsert::end) to commit.  Dropping without `end` aborts.
#[must_use]
pub struct NativeInsert<T> {
    client: NativeClient,
    /// `INSERT INTO table(col1, col2, …) FORMAT Native`
    sql: String,
    /// Table name, used to populate the schema cache after handshake.
    table: String,
    /// Pooled connection; `None` until the first `write`.
    conn: Option<PooledConnection>,
    /// Column schema received from the server after `begin_insert`.
    columns: Vec<ColumnSchema>,
    /// Buffered rows as RowBinary, one `Vec<u8>` per row.
    row_buf: Vec<Vec<u8>>,
    /// Total bytes across all buffered rows (used for flush threshold).
    row_bytes: usize,
    /// If set, each `send_insert_block` call is bounded by this duration.
    /// On expiry the connection is poisoned and `Error::TimedOut` is returned.
    send_timeout: Option<Duration>,
    /// If set, the final `finish_insert` call (including server-side processing)
    /// is bounded by this duration.  On expiry the connection is poisoned and
    /// `Error::TimedOut` is returned.
    end_timeout: Option<Duration>,
    _marker: PhantomData<fn() -> T>,
}

impl<T: Row> NativeInsert<T> {
    /// Create a new `NativeInsert`.  Connection is deferred until first write.
    pub(crate) fn new(client: NativeClient, table: &str) -> Self {
        let fields = row::join_column_names::<T>()
            .expect("the row type must be a struct or a wrapper around it");
        let sql = format!("INSERT INTO {table}({fields}) FORMAT Native");
        Self {
            client,
            sql,
            table: table.to_string(),
            conn: None,
            columns: Vec::new(),
            row_buf: Vec::new(),
            row_bytes: 0,
            send_timeout: None,
            end_timeout: None,
            _marker: PhantomData,
        }
    }

    /// Serialise `row` into the internal buffer and flush if above threshold.
    ///
    /// The future does not borrow `row` after it returns.
    pub async fn write(&mut self, row: &T::Value<'_>) -> Result<()>
    where
        T: RowWrite,
    {
        // Ensure connection is open and we have the column schema.
        self.ensure_connected().await?;

        // Serialise to RowBinary.
        let mut rb = BytesMut::new();
        if let Err(e) = serialize_row_binary(&mut rb, row) {
            self.abort();
            return Err(e);
        }
        let rb = rb.freeze().to_vec();
        self.row_bytes += rb.len();
        self.row_buf.push(rb);

        // Flush when the buffer is large enough.
        if self.row_bytes >= MIN_CHUNK_SIZE {
            if let Err(e) = self.flush().await {
                self.abort();
                return Err(e);
            }
        }
        Ok(())
    }

    /// Flush remaining buffered rows and signal end of INSERT to the server.
    ///
    /// Must be called to commit the INSERT.  On error the connection is dropped.
    pub async fn end(mut self) -> Result<()> {
        if self.conn.is_none() {
            // Nothing was written — open a connection and immediately close it cleanly.
            if let Err(e) = self.ensure_connected().await {
                return Err(e);
            }
        }
        if !self.row_buf.is_empty() {
            if let Err(e) = self.flush().await {
                return Err(e);
            }
        }
        let finish = self
            .conn
            .as_mut()
            .expect("conn must be open")
            .finish_insert();
        let result = if let Some(timeout) = self.end_timeout {
            match tokio::time::timeout(timeout, finish).await {
                Ok(r) => r,
                Err(_elapsed) => {
                    // Poison the connection — the protocol exchange is incomplete.
                    self.conn.as_mut().expect("conn must be open").discard();
                    Err(Error::TimedOut)
                }
            }
        } else {
            finish.await
        };
        if result.is_ok() {
            // Take the connection out so our Drop impl does not discard it.
            // Dropping the PooledConnection here returns it to the idle pool.
            let _ = self.conn.take();
        }
        result
    }

    async fn ensure_connected(&mut self) -> Result<()> {
        if self.conn.is_some() {
            return Ok(());
        }
        let mut conn = self.client.acquire().await?;
        let headers = conn.begin_insert(&self.sql).await?;
        self.client.cache_schema(&self.table, &headers);
        self.columns = ColumnSchema::from_headers(&headers).map_err(|e| {
            Error::BadResponse(format!("native INSERT: bad schema from server: {e}"))
        })?;
        self.conn = Some(conn);
        Ok(())
    }

    async fn flush(&mut self) -> Result<()> {
        let rows = std::mem::take(&mut self.row_buf);
        let n = rows.len();
        self.row_bytes = 0;
        if n == 0 {
            return Ok(());
        }
        let conn = self.conn.as_mut().expect("conn must be open during flush");
        let revision = conn.server_revision();
        let column_bytes = encode_columns(&rows, &self.columns, revision)?;
        // Drive the send future, applying send_timeout if configured.
        // `conn` (a reborrow of `self.conn`) is consumed by `send`; once
        // `send` is consumed by `timeout`/`await` the reborrow is released,
        // allowing the subsequent `self.conn` borrow for poisoning.
        let send = conn.send_insert_block(&column_bytes, self.columns.len(), n);
        let result: Result<()> = if let Some(timeout) = self.send_timeout {
            match tokio::time::timeout(timeout, send).await {
                Ok(r) => r,
                Err(_elapsed) => Err(Error::TimedOut),
            }
        } else {
            send.await
        };
        if result.is_err() {
            // Poison the connection on any error (including timeout) so it is
            // never returned to the pool mid-INSERT.
            self.conn.as_mut().expect("conn must be open during flush").discard();
        }
        result
    }

}

impl<T> NativeInsert<T> {
    /// Set per-operation timeouts for this INSERT.
    ///
    /// `send_timeout` bounds each individual block send (`write` flush).
    /// `end_timeout` bounds the final `end()` call, which includes waiting for
    /// the server to acknowledge the INSERT (materialized views, quorum, etc.).
    ///
    /// `None` disables the corresponding timeout (the default).
    ///
    /// On timeout the connection is poisoned and [`Error::TimedOut`] is returned.
    pub fn with_timeouts(
        mut self,
        send_timeout: Option<Duration>,
        end_timeout: Option<Duration>,
    ) -> Self {
        self.send_timeout = send_timeout;
        self.end_timeout = end_timeout;
        self
    }

    /// Abort the INSERT: discard the connection and clear the buffer.
    ///
    /// The server-side INSERT is incomplete — we must not return this
    /// connection to the pool as subsequent protocol exchanges would be
    /// misaligned.
    fn abort(&mut self) {
        if let Some(mut conn) = self.conn.take() {
            conn.discard();
        }
        self.row_buf.clear();
        self.row_bytes = 0;
    }
}

impl<T> Drop for NativeInsert<T> {
    /// If [`end`](NativeInsert::end) was not called, discard the connection so
    /// it is never returned to the pool mid-INSERT.
    fn drop(&mut self) {
        self.abort();
    }
}
