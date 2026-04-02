//! Concurrent, auto-flushing inserter with background task (native TCP transport).
//!
//! [`AsyncNativeInserter<T>`] is the native TCP equivalent of
//! [`crate::async_inserter::AsyncInserter<T>`] (HTTP).  It wraps
//! [`NativeInserter<T>`] in a background tokio task with an MPSC channel,
//! providing concurrent writes, backpressure, and automatic periodic flushing.
//!
//! Ported from the HyperI DFE Loader project (`dfe-loader/src/buffer/`).
//!
//! # Architecture
//!
//! ```text
//! ┌─ Task A ──┐  ┌─ Task B ──┐  ┌─ Task C ──┐
//! │ tx.send() │  │ tx.send() │  │ tx.send() │
//! └─────┬─────┘  └─────┬─────┘  └─────┬─────┘
//!       └───────────────┴───────────────┘
//!                       │
//!                bounded mpsc channel
//!                       │
//!           ┌───────────▼────────────┐
//!           │   Background Task      │
//!           │                        │
//!           │  select! {             │
//!           │    cmd = rx.recv()     │
//!           │    _ = interval.tick() │
//!           │  }                     │
//!           │                        │
//!           │  serialize → buffer    │
//!           │  check limits → flush  │
//!           └──────────┬─────────────┘
//!                      │ native TCP
//!                      ▼
//!              ClickHouse :9000
//! ```

use std::time::Duration;

use tokio::sync::{mpsc, oneshot};

use crate::error::Result;
use crate::native::client::NativeClient;
use crate::native::inserter::{NativeInserter, Quantities};
use crate::row::{RowOwned, RowWrite};

const DEFAULT_CHANNEL_CAPACITY: usize = 8192;

// ---------------------------------------------------------------------------
// Commands
// ---------------------------------------------------------------------------

enum Command<T> {
    Write(T, oneshot::Sender<Result<()>>),
    Flush(oneshot::Sender<Result<Quantities>>),
    End(oneshot::Sender<Result<Quantities>>),
}

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Configuration for [`AsyncNativeInserter`].
///
/// Same defaults as [`crate::async_inserter::AsyncInserterConfig`].
#[derive(Debug, Clone)]
pub struct AsyncNativeInserterConfig {
    /// Flush when this many rows have been buffered.  Default: `100_000`.
    pub max_rows: u64,
    /// Flush when serialised bytes reach this size.  Default: `10 MiB`.
    pub max_bytes: u64,
    /// Flush after this period regardless of row/byte counts.  Default: `5 s`.
    ///
    /// `None` disables period-based flushing.
    pub max_period: Option<Duration>,
    /// Bounded channel capacity.  Default: `8192`.
    pub channel_capacity: usize,
}

impl Default for AsyncNativeInserterConfig {
    fn default() -> Self {
        Self {
            max_rows: 100_000,
            max_bytes: 10 * 1024 * 1024,
            max_period: Some(Duration::from_secs(5)),
            channel_capacity: DEFAULT_CHANNEL_CAPACITY,
        }
    }
}

impl AsyncNativeInserterConfig {
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
// AsyncNativeInserter — native TCP transport
// ---------------------------------------------------------------------------

/// Concurrent, auto-flushing inserter for a single ClickHouse table (native TCP).
///
/// This is the native transport equivalent of
/// [`AsyncInserter<T>`][crate::async_inserter::AsyncInserter].
///
/// - Accepts `&self` on [`write`][Self::write] and [`flush`][Self::flush].
/// - Moves serialisation and network I/O to a background tokio task.
/// - Flushes automatically when row/byte/period limits are reached.
/// - Provides backpressure via a bounded MPSC channel.
///
/// Ported from HyperI DFE Loader's per-table buffer + orchestrator pattern.
pub struct AsyncNativeInserter<T> {
    tx: mpsc::Sender<Command<T>>,
    handle: tokio::task::JoinHandle<()>,
}

/// A cheap, clonable handle for writing rows to an [`AsyncNativeInserter`].
#[derive(Clone)]
pub struct AsyncNativeInserterHandle<T> {
    tx: mpsc::Sender<Command<T>>,
}

fn channel_closed_err() -> crate::error::Error {
    crate::error::Error::Custom("AsyncNativeInserter background task gone".into())
}

impl<T> AsyncNativeInserter<T>
where
    T: RowOwned + RowWrite + Send + Sync + 'static,
{
    /// Create a new `AsyncNativeInserter` for `table` using `config` thresholds.
    ///
    /// Spawns a background tokio task immediately.
    pub fn new(client: &NativeClient, table: &str, config: AsyncNativeInserterConfig) -> Self {
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
    pub fn handle(&self) -> AsyncNativeInserterHandle<T> {
        AsyncNativeInserterHandle {
            tx: self.tx.clone(),
        }
    }

    /// Serialize and buffer a row.
    ///
    /// Blocks (asynchronously) if the channel is full (backpressure).
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

impl<T> AsyncNativeInserterHandle<T>
where
    T: RowOwned + RowWrite + Send + Sync + 'static,
{
    /// Serialize and buffer a row (same as [`AsyncNativeInserter::write`]).
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
    mut inserter: NativeInserter<T>,
    mut rx: mpsc::Receiver<Command<T>>,
    period: Option<Duration>,
) where
    T: RowOwned + RowWrite + Send + Sync + 'static,
{
    let mut interval = period.map(|p| {
        let mut iv = tokio::time::interval(tokio::time::Duration::from(p));
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
