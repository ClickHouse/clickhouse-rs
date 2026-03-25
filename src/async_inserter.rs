//! Concurrent, auto-flushing inserter with background task (HTTP transport).
//!
//! [`AsyncInserter<T>`] moves serialisation, limit-checking, and periodic
//! flushing into a dedicated tokio task that communicates with callers via an
//! MPSC channel.  Multiple tasks can call [`write`][AsyncInserter::write]
//! concurrently -- the bounded channel provides natural backpressure.
//!
//! Ported from the HyperI DFE Loader project (`dfe-loader/src/buffer/`)
//! where a similar architecture (per-table buffer + background flush task +
//! orchestrator select! loop) was used to feed ClickHouse from Kafka at
//! sustained throughput.  The key improvement here is that the batching
//! policy is embedded in the library rather than requiring each consumer to
//! re-implement the orchestrator pattern.
//!
//! # Architecture
//!
//! ```text
//! +- Task A --+  +- Task B --+  +- Task C --+
//! | tx.send() |  | tx.send() |  | tx.send() |
//! +-----+-----+  +-----+-----+  +-----+-----+
//!       +---------------+---------------+
//!                       |
//!                bounded mpsc channel
//!                       |
//!           +-----------v------------+
//!           |   Background Task      |
//!           |                        |
//!           |  select! {             |
//!           |    cmd = rx.recv()     |
//!           |    _ = interval.tick() |
//!           |  }                     |
//!           |                        |
//!           |  serialize -> buffer    |
//!           |  check limits -> flush  |
//!           +----------+-------------+
//!                      | HTTP
//!                      v
//!              ClickHouse :8123
//! ```
//!
//! This module keeps batch inserts purely
//! caller-driven (no background goroutines).  This design goes further  -- 
//! providing the concurrent, auto-flushing inserter that Go users typically
//! build themselves with goroutines and channels.

use tokio::sync::{mpsc, oneshot};
use tokio::time::Duration;

use crate::{
    Client,
    error::Result,
    inserter::{Inserter, Quantities},
    row::{RowOwned, RowWrite},
};

const DEFAULT_CHANNEL_CAPACITY: usize = 8192;

// ---------------------------------------------------------------------------
// Commands sent over the MPSC channel
// ---------------------------------------------------------------------------

enum Command<T> {
    Write(T, oneshot::Sender<Result<()>>),
    Flush(oneshot::Sender<Result<Quantities>>),
    End(oneshot::Sender<Result<Quantities>>),
}

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Configuration for [`AsyncInserter`].
///
/// Defaults align with ClickHouse's recommended batch sizes and the
/// `async_insert_max_data_size` server setting.
#[derive(Debug, Clone)]
pub struct AsyncInserterConfig {
    /// Flush when this many rows have been buffered.  Default: `100_000`.
    pub max_rows: u64,
    /// Flush when serialised bytes reach this size.  Default: `10 MiB`.
    pub max_bytes: u64,
    /// Flush after this period regardless of row/byte counts.  Default: `5 s`.
    ///
    /// `None` disables period-based flushing.
    pub max_period: Option<Duration>,
    /// Bounded channel capacity.  Default: `8192`.
    ///
    /// Controls backpressure: producers block when the channel is full.
    pub channel_capacity: usize,
}

impl Default for AsyncInserterConfig {
    fn default() -> Self {
        Self {
            max_rows: 100_000,
            max_bytes: 10 * 1024 * 1024,
            max_period: Some(Duration::from_secs(5)),
            channel_capacity: DEFAULT_CHANNEL_CAPACITY,
        }
    }
}

impl AsyncInserterConfig {
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

    /// Disable period-based flushing.
    pub fn without_period(mut self) -> Self {
        self.max_period = None;
        self
    }

    /// Override the bounded channel capacity.
    pub fn with_channel_capacity(mut self, cap: usize) -> Self {
        self.channel_capacity = cap;
        self
    }
}

// ---------------------------------------------------------------------------
// AsyncInserter -- HTTP transport
// ---------------------------------------------------------------------------

/// Concurrent, auto-flushing inserter for a single ClickHouse table (HTTP).
///
/// Unlike [`Inserter<T>`][crate::inserter::Inserter], this type:
///
/// - Accepts `&self` on [`write`][Self::write] and [`flush`][Self::flush],
///   so it can be shared across tasks via `Arc` (or via cheap
///   [`handle()`][Self::handle] clones).
/// - Moves serialisation and network I/O to a background tokio task.
/// - Flushes automatically when row/byte/period limits are reached.
/// - Provides backpressure via a bounded MPSC channel.
///
/// # Note: `RowOwned` requirement
///
/// Because rows are sent over an MPSC channel, `T` must be [`RowOwned`]
/// (i.e. `T::Value<'a> = T` for all lifetimes).  This is automatically
/// satisfied by any `#[derive(Row)]` struct that owns its fields.
///
/// Ported from HyperI DFE Loader's per-table buffer + orchestrator pattern.
pub struct AsyncInserter<T> {
    tx: mpsc::Sender<Command<T>>,
    handle: tokio::task::JoinHandle<()>,
}

/// A cheap, clonable handle for writing rows to an [`AsyncInserter`].
///
/// Obtained via [`AsyncInserter::handle`].  Multiple handles can write
/// concurrently.  The background task exits when all handles and the
/// original `AsyncInserter` are dropped.
#[derive(Clone)]
pub struct AsyncInserterHandle<T> {
    tx: mpsc::Sender<Command<T>>,
}

fn channel_closed_err() -> crate::error::Error {
    crate::error::Error::Custom("AsyncInserter background task gone".into())
}

impl<T> AsyncInserter<T>
where
    T: RowOwned + RowWrite + Send + Sync + 'static,
{
    /// Create a new `AsyncInserter` for `table` using `config` thresholds.
    ///
    /// Spawns a background tokio task immediately.
    pub fn new(client: &Client, table: &str, config: AsyncInserterConfig) -> Self {
        let (tx, rx) = mpsc::channel(config.channel_capacity);

        let inserter = client
            .inserter::<T>(table)
            .with_max_rows(config.max_rows)
            .with_max_bytes(config.max_bytes)
            .with_period(config.max_period);

        let period = config.max_period;
        let handle = tokio::spawn(background_task(inserter, rx, period));

        Self { tx, handle }
    }

    /// Obtain a cheap, clonable write handle.
    pub fn handle(&self) -> AsyncInserterHandle<T> {
        AsyncInserterHandle {
            tx: self.tx.clone(),
        }
    }

    /// Serialize and buffer a row.
    ///
    /// Blocks (asynchronously) if the channel is full (backpressure).
    /// Returns once the row has been serialised into the internal buffer.
    pub async fn write(&self, row: T) -> Result<()> {
        let (resp_tx, resp_rx) = oneshot::channel();
        self.tx
            .send(Command::Write(row, resp_tx))
            .await
            .map_err(|_| channel_closed_err())?;
        resp_rx.await.map_err(|_| channel_closed_err())?
    }

    /// Force-flush all buffered rows to ClickHouse.
    pub async fn flush(&self) -> Result<Quantities> {
        let (resp_tx, resp_rx) = oneshot::channel();
        self.tx
            .send(Command::Flush(resp_tx))
            .await
            .map_err(|_| channel_closed_err())?;
        resp_rx.await.map_err(|_| channel_closed_err())?
    }

    /// Graceful shutdown: flush remaining rows, end the current INSERT,
    /// and stop the background task.
    ///
    /// Consumes `self`.  All cloned handles become inert after this call.
    pub async fn end(self) -> Result<Quantities> {
        let (resp_tx, resp_rx) = oneshot::channel();
        if self.tx.send(Command::End(resp_tx)).await.is_err() {
            return Ok(Quantities::ZERO);
        }
        drop(self.tx);
        let result = resp_rx.await.map_err(|_| channel_closed_err())?;
        let _ = self.handle.await;
        result
    }
}

impl<T> AsyncInserterHandle<T>
where
    T: RowOwned + RowWrite + Send + Sync + 'static,
{
    /// Serialize and buffer a row (same as [`AsyncInserter::write`]).
    pub async fn write(&self, row: T) -> Result<()> {
        let (resp_tx, resp_rx) = oneshot::channel();
        self.tx
            .send(Command::Write(row, resp_tx))
            .await
            .map_err(|_| channel_closed_err())?;
        resp_rx.await.map_err(|_| channel_closed_err())?
    }

    /// Force-flush all buffered rows to ClickHouse.
    pub async fn flush(&self) -> Result<Quantities> {
        let (resp_tx, resp_rx) = oneshot::channel();
        self.tx
            .send(Command::Flush(resp_tx))
            .await
            .map_err(|_| channel_closed_err())?;
        resp_rx.await.map_err(|_| channel_closed_err())?
    }
}

// ---------------------------------------------------------------------------
// Background task
// ---------------------------------------------------------------------------

async fn background_task<T>(
    mut inserter: Inserter<T>,
    mut rx: mpsc::Receiver<Command<T>>,
    period: Option<Duration>,
) where
    T: RowOwned + RowWrite + Send + Sync + 'static,
{
    let mut interval = period.map(|p| {
        let mut iv = tokio::time::interval(p);
        iv.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        iv
    });

    // Skip the immediate first tick.
    if let Some(ref mut iv) = interval {
        iv.tick().await;
    }

    loop {
        let tick_fut = async {
            match interval {
                Some(ref mut iv) => iv.tick().await,
                None => std::future::pending().await,
            }
        };

        tokio::select! {
            biased;

            cmd = rx.recv() => {
                match cmd {
                    Some(Command::Write(row, resp)) => {
                        let result = inserter.write(&row).await;
                        if result.is_ok() {
                            let _ = inserter.commit().await;
                        }
                        let _ = resp.send(result);
                    }
                    Some(Command::Flush(resp)) => {
                        let _ = resp.send(inserter.force_commit().await);
                    }
                    Some(Command::End(resp)) => {
                        let _ = resp.send(inserter.end().await);
                        return;
                    }
                    None => {
                        let _ = inserter.end().await;
                        return;
                    }
                }
            }

            _ = tick_fut => {
                let _ = inserter.commit().await;
            }
        }
    }
}
