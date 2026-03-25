//! `NativeInserter<T>` — multi-batch INSERT wrapper for the native transport.
//!
//! Mirrors the public API of [`crate::inserter::Inserter`] (HTTP transport)
//! without requiring the `inserter` crate feature.

use std::mem;
use std::time::Duration;

use crate::error::Result;
use crate::native::client::NativeClient;
use crate::native::insert::NativeInsert;
use crate::row::{Row, RowWrite};
use crate::ticks::Ticks;

// Re-export from shared module for backwards compatibility.
pub use crate::quantities::Quantities;

/// Multi-batch native INSERT manager.
///
/// Wraps [`NativeInsert`] to produce multiple consecutive INSERT statements
/// bounded by configurable thresholds.  See [`crate::inserter::Inserter`] for
/// the equivalent HTTP version and full documentation.
#[must_use]
pub struct NativeInserter<T> {
    client: NativeClient,
    table: String,
    max_bytes: u64,
    max_rows: u64,
    insert: Option<NativeInsert<T>>,
    ticks: Ticks,
    pending: Quantities,
    in_transaction: bool,
    #[allow(clippy::type_complexity)]
    on_commit: Option<Box<dyn FnMut(&Quantities) + Send + 'static>>,
}

impl<T: Row> NativeInserter<T> {
    pub(crate) fn new(client: &NativeClient, table: &str) -> Self {
        Self {
            client: client.clone(),
            table: table.into(),
            max_bytes: u64::MAX,
            max_rows: u64::MAX,
            insert: None,
            ticks: Ticks::default(),
            pending: Quantities::ZERO,
            in_transaction: false,
            on_commit: None,
        }
    }

    /// Maximum uncompressed bytes per INSERT statement (soft limit).
    pub fn with_max_bytes(mut self, threshold: u64) -> Self {
        self.max_bytes = threshold;
        self
    }

    /// Maximum rows per INSERT statement (soft limit).
    pub fn with_max_rows(mut self, threshold: u64) -> Self {
        self.max_rows = threshold;
        self
    }

    /// Maximum elapsed time between INSERT commits.
    pub fn with_period(mut self, period: Option<Duration>) -> Self {
        self.ticks.set_period(period);
        self.ticks.reschedule();
        self
    }

    /// Add a bias to the period for jitter (API-compatible; bias is ignored in MVP).
    pub fn with_period_bias(mut self, bias: f64) -> Self {
        self.ticks.set_period_bias(bias);
        self
    }

    /// Register a callback invoked after each successful non-empty commit.
    pub fn with_commit_callback(
        mut self,
        callback: impl FnMut(&Quantities) + Send + 'static,
    ) -> Self {
        self.on_commit = Some(Box::new(callback));
        self
    }

    /// See [`with_max_bytes`](Self::with_max_bytes).
    pub fn set_max_bytes(&mut self, threshold: u64) {
        self.max_bytes = threshold;
    }

    /// See [`with_max_rows`](Self::with_max_rows).
    pub fn set_max_rows(&mut self, threshold: u64) {
        self.max_rows = threshold;
    }

    /// See [`with_period`](Self::with_period).
    pub fn set_period(&mut self, period: Option<Duration>) {
        self.ticks.set_period(period);
        self.ticks.reschedule();
    }

    /// See [`with_period_bias`](Self::with_period_bias).
    pub fn set_period_bias(&mut self, bias: f64) {
        self.ticks.set_period_bias(bias);
    }

    /// How much time remains until the next tick.  `None` if no period is set.
    pub fn time_left(&mut self) -> Option<Duration> {
        self.ticks.time_left()
    }

    /// Statistics about rows/bytes not yet committed.
    pub fn pending(&self) -> &Quantities {
        &self.pending
    }

    /// Serialise `row` into the internal buffer.
    ///
    /// Flushes to the network when the active `NativeInsert`'s buffer is full.
    /// Call [`commit`](Self::commit) or [`force_commit`](Self::force_commit)
    /// to check limits and end the current INSERT.
    pub async fn write(&mut self, row: &T::Value<'_>) -> Result<()>
    where
        T: RowWrite,
    {
        if self.insert.is_none() {
            self.init_insert();
        }

        match self.insert.as_mut().unwrap().write(row).await {
            Ok(()) => {
                self.pending.rows += 1;
                if !self.in_transaction {
                    self.pending.transactions += 1;
                    self.in_transaction = true;
                }
                Ok(())
            }
            Err(e) => {
                self.pending = Quantities::ZERO;
                self.insert = None;
                Err(e)
            }
        }
    }

    /// Check limits; if reached, end the active INSERT.
    ///
    /// Returns [`Quantities::ZERO`] when limits have not been reached.
    pub async fn commit(&mut self) -> Result<Quantities> {
        if !self.limits_reached() {
            self.in_transaction = false;
            return Ok(Quantities::ZERO);
        }
        self.force_commit().await
    }

    /// End the active INSERT unconditionally, regardless of limits.
    pub async fn force_commit(&mut self) -> Result<Quantities> {
        let q = self.do_commit().await?;
        self.ticks.reschedule();
        Ok(q)
    }

    /// End the active INSERT and consume the `NativeInserter`.
    ///
    /// Must be called to flush the final batch.
    pub async fn end(mut self) -> Result<Quantities> {
        self.do_commit().await
    }

    fn limits_reached(&self) -> bool {
        self.pending.rows >= self.max_rows || self.ticks.reached()
    }

    async fn do_commit(&mut self) -> Result<Quantities> {
        self.in_transaction = false;
        let quantities = mem::replace(&mut self.pending, Quantities::ZERO);

        if let Some(insert) = self.insert.take() {
            insert.end().await?;
        }

        if let Some(cb) = &mut self.on_commit {
            if quantities.transactions > 0 {
                (cb)(&quantities);
            }
        }

        Ok(quantities)
    }

    #[inline(never)]
    fn init_insert(&mut self) {
        debug_assert!(self.insert.is_none());
        self.insert = Some(NativeInsert::new(self.client.clone(), &self.table));
    }
}
