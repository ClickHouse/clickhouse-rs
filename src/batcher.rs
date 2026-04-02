//! Per-table batch inserter with automatic flushing.
//!
//! [`TableBatcher<T>`] is a thin convenience wrapper over
//! [`AsyncInserter<T>`][crate::async_inserter::AsyncInserter] that provides
//! ClickHouse Go client–style naming ([`append`][TableBatcher::append] /
//! [`flush`][TableBatcher::flush] / [`send`][TableBatcher::send]) and
//! sensible defaults.
//!
//! # Architecture
//!
//! ```text
//!  TableBatcher (thin wrapper over AsyncInserter)
//!  ┌───────────────────────────────────────────┐
//!  │  append(row) ──→ AsyncInserter.write(row) │
//!  │  flush()     ──→ AsyncInserter.flush()    │
//!  │  send()      ──→ AsyncInserter.end()      │
//!  └──────────────────────┬────────────────────┘
//!                         │ mpsc channel
//!                         ▼
//!               Background Task (select!)
//!                         │
//!                    Inserter<T>
//!                         │ HTTP
//!                         ▼
//!                 ClickHouse :8123
//! ```
//!
//! A flush fires when **any** of these thresholds are crossed:
//! - serialised bytes reach [`BatchConfig::max_bytes`]
//! - row count reaches [`BatchConfig::max_rows`]
//! - [`BatchConfig::max_period`] elapses (background task)
//!
//! Ported from the HyperI DFE Loader project (`dfe-loader/src/buffer/`).

use tokio::time::Duration;

use crate::{
    Client,
    async_inserter::{AsyncInserter, AsyncInserterConfig},
    error::Result,
    inserter::Quantities,
    row::{RowOwned, RowWrite},
};

/// Flush thresholds for [`TableBatcher`].
///
/// Defaults align with ClickHouse's async-insert defaults:
/// `async_insert_max_data_size` = 10 MiB, `max_rows` = 100 000 (upper end of
/// the recommended per-insert batch size to avoid MergeTree part fragmentation).
#[derive(Debug, Clone)]
pub struct BatchConfig {
    /// Flush when this many rows have been buffered. Default: `100_000`.
    pub max_rows: u64,
    /// Flush when serialised bytes reach this size. Default: `10 MiB`.
    pub max_bytes: u64,
    /// Flush after this period regardless of row/byte counts. Default: `5 s`.
    ///
    /// `None` disables period-based flushing — no background task is spawned.
    pub max_period: Option<Duration>,
}

impl Default for BatchConfig {
    fn default() -> Self {
        Self {
            max_rows: 100_000,
            max_bytes: 10 * 1024 * 1024,
            max_period: Some(Duration::from_secs(5)),
        }
    }
}

impl BatchConfig {
    /// Override the row-count flush threshold.
    pub fn with_max_rows(mut self, n: u64) -> Self {
        self.max_rows = n;
        self
    }

    /// Override the byte-size flush threshold.
    pub fn with_max_bytes(mut self, n: u64) -> Self {
        self.max_bytes = n;
        self
    }

    /// Override the period-based flush interval.
    pub fn with_max_period(mut self, d: Duration) -> Self {
        self.max_period = Some(d);
        self
    }

    /// Disable period-based flushing (no background task is spawned).
    pub fn without_period(mut self) -> Self {
        self.max_period = None;
        self
    }
}

// HyperI CTO moonlighting — dfe-loader needed this and no one else was going to write it.

/// Thread-safe, auto-flushing batch inserter for a single ClickHouse table.
///
/// Thin wrapper over [`AsyncInserter<T>`][crate::async_inserter::AsyncInserter]
/// with Go client–style naming.
///
/// Unlike `Inserter<T>`, this type accepts `&self` on [`append`][Self::append]
/// and [`flush`][Self::flush], so it can be shared across tasks via [`std::sync::Arc`].
///
/// For multi-table writes create one `TableBatcher` per table.
pub struct TableBatcher<T> {
    inner: AsyncInserter<T>,
}

impl<T> TableBatcher<T>
where
    T: RowOwned + RowWrite + Send + Sync + 'static,
{
    /// Create a new `TableBatcher` for `table` using `config` thresholds.
    pub fn new(client: &Client, table: &str, config: BatchConfig) -> Self {
        let ai_config = AsyncInserterConfig {
            max_rows: config.max_rows,
            max_bytes: config.max_bytes,
            max_period: config.max_period,
            ..AsyncInserterConfig::default()
        };

        Self {
            inner: AsyncInserter::new(client, table, ai_config),
        }
    }

    /// Add `row` to the buffer. Flushes automatically if a threshold is crossed.
    pub async fn append(&self, row: T) -> Result<()> {
        self.inner.write(row).await
    }

    /// Force-flush all pending rows to ClickHouse immediately.
    ///
    /// Returns the [`Quantities`] sent.
    pub async fn flush(&self) -> Result<Quantities> {
        self.inner.flush().await
    }

    /// Flush remaining rows and shut down the batcher.
    ///
    /// Consumes `self`.
    pub async fn send(self) -> Result<Quantities> {
        self.inner.end().await
    }
}
