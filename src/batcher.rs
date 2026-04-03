//! Per-table batch inserter with automatic flushing.
//!
//! [`TableBatcher<T>`] wraps [`Inserter<T>`][crate::inserter::Inserter] with
//! a shared buffer and a background task that handles period-based flushes,
//! so callers don't need to poll `time_left()`.
//!
//! API names follow the ClickHouse Go client's
//! [`Batch`](https://pkg.go.dev/github.com/ClickHouse/clickhouse-go/v2/lib/driver#Batch)
//! interface: [`append`][TableBatcher::append] / [`flush`][TableBatcher::flush] /
//! [`send`][TableBatcher::send].
//!
//! A flush fires when **any** of these thresholds are crossed:
//! - serialised bytes reach [`BatchConfig::max_bytes`]
//! - row count reaches [`BatchConfig::max_rows`]
//! - [`BatchConfig::max_period`] elapses (background task)

use std::sync::Arc;

use tokio::sync::Mutex;
use tokio::time::Duration;

use crate::{
    Client,
    error::Result,
    inserter::{Inserter, Quantities},
    row::{Row, RowWrite},
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

struct BatcherInner<T> {
    inserter: Inserter<T>,
}

/// Thread-safe, auto-flushing batch inserter for a single ClickHouse table.
///
/// Wraps [`Inserter<T>`][crate::inserter::Inserter] behind an `Arc<Mutex>` for
/// concurrent writes and spawns a background task to handle period-based flushes.
///
/// Unlike `Inserter<T>`, this type accepts `&self` on [`append`][Self::append]
/// and [`flush`][Self::flush], so it can be shared across tasks via [`Arc`].
///
/// Concurrent appends are serialised through the internal mutex. On the hot path
/// (limits not yet reached) the lock covers only RowBinary serialisation. A flush
/// holds the lock across the network round-trip, but that happens at most once per batch.
///
/// For multi-table writes create one `TableBatcher` per table and share via `Arc`.
pub struct TableBatcher<T> {
    inner: Arc<Mutex<BatcherInner<T>>>,
    flush_task: tokio::task::JoinHandle<()>,
}

impl<T> TableBatcher<T>
where
    T: Row + RowWrite + Send + 'static,
    for<'a> <T as Row>::Value<'a>: Send,
{
    /// Create a new `TableBatcher` for `table` using `config` thresholds.
    ///
    /// If `config.max_period` is `Some`, a background tokio task is spawned
    /// to handle periodic flushes.
    pub fn new(client: &Client, table: &str, config: BatchConfig) -> Self {
        let inserter = client
            .inserter::<T>(table)
            .with_max_rows(config.max_rows)
            .with_max_bytes(config.max_bytes)
            .with_period(config.max_period);

        let inner = Arc::new(Mutex::new(BatcherInner { inserter }));
        let inner_bg = Arc::clone(&inner);
        let period = config.max_period;

        let flush_task = tokio::spawn(async move {
            let Some(p) = period else { return };

            let mut interval = tokio::time::interval(p);
            // Skip ticks that arrive while a flush is already in progress.
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            // First tick fires immediately at t=0; skip it to avoid flushing an empty buffer.
            interval.tick().await;

            loop {
                interval.tick().await;
                let mut guard = inner_bg.lock().await;
                if let Err(_err) = guard.inserter.commit().await {
                    // Errors surface on the next explicit append/flush/send call.
                    // Background tasks can't propagate errors to callers.
                }
            }
        });

        Self { inner, flush_task }
    }

    /// Add `row` to the buffer. Flushes automatically if a threshold is crossed.
    pub async fn append(&self, row: &<T as Row>::Value<'_>) -> Result<()> {
        let mut guard = self.inner.lock().await;
        guard.inserter.write(row).await?;
        guard.inserter.commit().await?;
        Ok(())
    }

    /// Force-flush all pending rows to ClickHouse immediately.
    ///
    /// Returns the [`Quantities`] sent. Useful when shutting down a subsystem
    /// while other clones of this batcher are still alive.
    pub async fn flush(&self) -> Result<Quantities> {
        let mut guard = self.inner.lock().await;
        guard.inserter.force_commit().await
    }

    /// Flush remaining rows and shut down the batcher.
    ///
    /// Aborts the background flush task, waits for it to exit (so its `Arc`
    /// clone is dropped), then finalises the INSERT via
    /// [`Inserter::end`][crate::inserter::Inserter::end].
    ///
    /// All other `Arc` holders over the same inner buffer must be dropped before
    /// calling `send`. If any remain after the abort, a force-flush is done
    /// instead of a clean `end`.
    pub async fn send(self) -> Result<Quantities> {
        self.flush_task.abort();
        let _ = self.flush_task.await;

        match Arc::try_unwrap(self.inner) {
            Ok(mutex) => mutex.into_inner().inserter.end().await,
            Err(arc) => {
                // Another Arc clone still exists — force-commit what we can.
                let mut guard = arc.lock().await;
                guard.inserter.force_commit().await
            }
        }
    }
}
