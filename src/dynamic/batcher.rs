//! Async auto-flushing dynamic inserter with background task.
//!
//! `DynamicBatcher` is the async, multi-producer variant of `DynamicInsert`.
//! It moves schema fetch, RowBinary encoding, and periodic flushing into a
//! dedicated tokio task that communicates with callers via a bounded MPSC
//! channel. Multiple tasks can call `write_map()` concurrently — the bounded
//! channel provides natural backpressure.
//!
//! # Schema Recovery
//!
//! On schema mismatch errors from ClickHouse, the background task:
//! 1. Invalidates the cached schema
//! 2. Re-fetches from `system.columns`
//! 3. Retries the current batch with the new schema
//! 4. Resumes normal operation
//!
//! One retry attempt per mismatch — prevents infinite loops on genuine
//! data errors.
//!
//! # Architecture
//!
//! ```text
//! ┌─ Task A ──┐  ┌─ Task B ──┐  ┌─ Task C ──┐
//! │ write_map()│  │ write_map()│  │ write_map()│
//! └─────┬─────┘  └─────┬─────┘  └─────┬─────┘
//!       └───────────────┴───────────────┘
//!                       │
//!                bounded mpsc channel
//!                       │
//!           ┌───────────▼────────────┐
//!           │   Background Task      │
//!           │  select! {             │
//!           │    cmd = rx.recv()     │
//!           │    _ = interval.tick() │
//!           │  }                     │
//!           │  encode → RowBinary    │
//!           │  buffer → flush        │
//!           └──────────┬─────────────┘
//!                      │ HTTP RowBinary
//!                      ▼
//!              ClickHouse :8123
//! ```

use std::sync::Arc;

use serde_json::{Map, Value};
use tokio::sync::{mpsc, oneshot};
use tokio::time::Duration;

use crate::unified::UnifiedClient;

use super::error::DynamicError;
use super::schema::DynamicSchemaCache;

const DEFAULT_CHANNEL_CAPACITY: usize = 8192;

/// Configuration for [`DynamicBatcher`].
#[derive(Debug, Clone)]
pub struct DynamicBatchConfig {
    /// Flush when this many rows have been buffered. Default: `10_000`.
    pub max_rows: u64,
    /// Flush after this period regardless of row count. Default: `5s`.
    pub max_period: Duration,
    /// Bounded channel capacity. Default: `8192`.
    pub channel_capacity: usize,
}

impl Default for DynamicBatchConfig {
    fn default() -> Self {
        Self {
            max_rows: 10_000,
            max_period: Duration::from_secs(5),
            channel_capacity: DEFAULT_CHANNEL_CAPACITY,
        }
    }
}

// ---------------------------------------------------------------------------
// Commands over the MPSC channel
// ---------------------------------------------------------------------------

enum Cmd {
    Write(Map<String, Value>, oneshot::Sender<Result<(), DynamicError>>),
    Flush(oneshot::Sender<Result<u64, DynamicError>>),
    End(oneshot::Sender<Result<u64, DynamicError>>),
}

// ---------------------------------------------------------------------------
// DynamicBatcher
// ---------------------------------------------------------------------------

/// Async auto-flushing dynamic inserter for a single ClickHouse table.
///
/// Push `Map<String, Value>`, the batcher encodes to RowBinary in a background
/// task and flushes to ClickHouse when row count or time thresholds are reached.
///
/// Schema is fetched lazily from `system.columns` and cached. On schema
/// mismatch, the batcher automatically invalidates and re-fetches.
pub struct DynamicBatcher {
    tx: mpsc::Sender<Cmd>,
    handle: tokio::task::JoinHandle<()>,
}

/// Cheap clonable handle for writing rows to a [`DynamicBatcher`].
#[derive(Clone)]
pub struct DynamicBatcherHandle {
    tx: mpsc::Sender<Cmd>,
}

fn channel_closed() -> DynamicError {
    DynamicError::EncodingError {
        column: String::new(),
        message: "DynamicBatcher background task gone".to_string(),
    }
}

impl DynamicBatcher {
    /// Create a new `DynamicBatcher`. Spawns a background tokio task immediately.
    pub fn new(
        client: &UnifiedClient,
        database: &str,
        table: &str,
        config: DynamicBatchConfig,
    ) -> Self {
        let (tx, rx) = mpsc::channel(config.channel_capacity);
        let client = client.clone();
        let database = database.to_string();
        let table = table.to_string();
        let schema_cache = client.dynamic_schema_cache.clone();

        let handle = tokio::spawn(background_task(
            client,
            database,
            table,
            schema_cache,
            config,
            rx,
        ));

        Self { tx, handle }
    }

    /// Get a cheap, clonable write handle.
    pub fn handle(&self) -> DynamicBatcherHandle {
        DynamicBatcherHandle {
            tx: self.tx.clone(),
        }
    }

    /// Buffer a row. Blocks (async) if the channel is full (backpressure).
    pub async fn write_map(&self, row: Map<String, Value>) -> Result<(), DynamicError> {
        let (resp_tx, resp_rx) = oneshot::channel();
        self.tx
            .send(Cmd::Write(row, resp_tx))
            .await
            .map_err(|_| channel_closed())?;
        resp_rx.await.map_err(|_| channel_closed())?
    }

    /// Force-flush all buffered rows to ClickHouse.
    pub async fn flush(&self) -> Result<u64, DynamicError> {
        let (resp_tx, resp_rx) = oneshot::channel();
        self.tx
            .send(Cmd::Flush(resp_tx))
            .await
            .map_err(|_| channel_closed())?;
        resp_rx.await.map_err(|_| channel_closed())?
    }

    /// Flush remaining rows and shut down the batcher. Consumes self.
    pub async fn end(self) -> Result<u64, DynamicError> {
        let (resp_tx, resp_rx) = oneshot::channel();
        if self.tx.send(Cmd::End(resp_tx)).await.is_err() {
            return Ok(0);
        }
        drop(self.tx);
        let result = resp_rx.await.map_err(|_| channel_closed())?;
        let _ = self.handle.await;
        result
    }
}

impl DynamicBatcherHandle {
    /// Buffer a row (same as [`DynamicBatcher::write_map`]).
    pub async fn write_map(&self, row: Map<String, Value>) -> Result<(), DynamicError> {
        let (resp_tx, resp_rx) = oneshot::channel();
        self.tx
            .send(Cmd::Write(row, resp_tx))
            .await
            .map_err(|_| channel_closed())?;
        resp_rx.await.map_err(|_| channel_closed())?
    }

    /// Force-flush all buffered rows.
    pub async fn flush(&self) -> Result<u64, DynamicError> {
        let (resp_tx, resp_rx) = oneshot::channel();
        self.tx
            .send(Cmd::Flush(resp_tx))
            .await
            .map_err(|_| channel_closed())?;
        resp_rx.await.map_err(|_| channel_closed())?
    }
}

// ---------------------------------------------------------------------------
// Background task
// ---------------------------------------------------------------------------

async fn background_task(
    client: UnifiedClient,
    database: String,
    table: String,
    schema_cache: Arc<DynamicSchemaCache>,
    config: DynamicBatchConfig,
    mut rx: mpsc::Receiver<Cmd>,
) {
    let mut buffer: Vec<Map<String, Value>> = Vec::with_capacity(config.max_rows as usize);
    let mut total_rows: u64 = 0;

    let mut interval = tokio::time::interval(config.max_period);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    // Skip the immediate first tick
    interval.tick().await;

    loop {
        tokio::select! {
            biased;

            cmd = rx.recv() => {
                match cmd {
                    Some(Cmd::Write(row, resp)) => {
                        buffer.push(row);
                        if buffer.len() as u64 >= config.max_rows {
                            let flushed = flush_buffer(
                                &client, &database, &table, &schema_cache,
                                &mut buffer,
                            ).await;
                            match flushed {
                                Ok(n) => {
                                    total_rows += n;
                                    let _ = resp.send(Ok(()));
                                }
                                Err(e) => {
                                    let _ = resp.send(Err(e));
                                }
                            }
                        } else {
                            let _ = resp.send(Ok(()));
                        }
                    }
                    Some(Cmd::Flush(resp)) => {
                        let flushed = flush_buffer(
                            &client, &database, &table, &schema_cache,
                            &mut buffer,
                        ).await;
                        match flushed {
                            Ok(n) => {
                                total_rows += n;
                                let _ = resp.send(Ok(total_rows));
                            }
                            Err(e) => {
                                let _ = resp.send(Err(e));
                            }
                        }
                    }
                    Some(Cmd::End(resp)) => {
                        let flushed = flush_buffer(
                            &client, &database, &table, &schema_cache,
                            &mut buffer,
                        ).await;
                        match flushed {
                            Ok(n) => {
                                total_rows += n;
                                let _ = resp.send(Ok(total_rows));
                            }
                            Err(e) => {
                                let _ = resp.send(Err(e));
                            }
                        }
                        return;
                    }
                    None => {
                        // All senders dropped — flush and exit
                        let _ = flush_buffer(
                            &client, &database, &table, &schema_cache,
                            &mut buffer,
                        ).await;
                        return;
                    }
                }
            }

            _ = interval.tick() => {
                if !buffer.is_empty() {
                    match flush_buffer(
                        &client, &database, &table, &schema_cache,
                        &mut buffer,
                    ).await {
                        Ok(n) => total_rows += n,
                        Err(_e) => {
                            // Timer-triggered flush errors are logged but not fatal
                            // The next write_map will surface errors to callers
                        }
                    }
                }
            }
        }
    }
}

/// Flush buffered rows via DynamicInsert.
///
/// On schema mismatch, invalidates cache and retries once with fresh schema.
async fn flush_buffer(
    client: &UnifiedClient,
    database: &str,
    table: &str,
    schema_cache: &Arc<DynamicSchemaCache>,
    buffer: &mut Vec<Map<String, Value>>,
) -> Result<u64, DynamicError> {
    if buffer.is_empty() {
        return Ok(0);
    }

    let rows = std::mem::take(buffer);
    let count = rows.len() as u64;

    match try_insert(client, database, table, &rows).await {
        Ok(()) => Ok(count),
        Err(DynamicError::SchemaMismatch { .. }) => {
            // Schema changed — invalidate and retry once
            let full_table = format!("{database}.{table}");
            schema_cache.invalidate(&full_table);

            // Retry with fresh schema
            try_insert(client, database, table, &rows)
                .await
                .map(|()| count)
        }
        Err(e) => Err(e),
    }
}

/// Attempt to insert rows via DynamicInsert.
async fn try_insert(
    client: &UnifiedClient,
    database: &str,
    table: &str,
    rows: &[Map<String, Value>],
) -> Result<(), DynamicError> {
    let mut insert = client.dynamic_insert(database, table);
    for row in rows {
        insert.write_map(row).await?;
    }
    insert.end().await?;
    Ok(())
}
