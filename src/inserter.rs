use std::{future::Future, mem};

use serde::Serialize;
use tokio::time::{Duration, Instant};

use crate::{error::Result, insert::Insert, row::Row, ticks::Ticks, Client};

const DEFAULT_MAX_ENTRIES: u64 = 250_000;

/// Performs multiple consecutive `INSERT`s.
///
/// Rows are being sent progressively to spread network load.
#[must_use]
pub struct Inserter<T> {
    client: Client,
    table: String,
    max_entries: u64,
    send_timeout: Option<Duration>,
    end_timeout: Option<Duration>,
    insert: Insert<T>,
    ticks: Ticks,
    committed: Quantities,
    uncommitted_entries: u64,
}

/// Statistics about inserted rows.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Quantities {
    /// How many rows ([`Inserter::write`]) have been inserted.
    pub entries: u64,
    /// How many nonempty transactions ([`Inserter::commit`]) have been inserted.
    pub transactions: u64,
}

impl Quantities {
    /// Just zero quantities, nothing special.
    pub const ZERO: Quantities = Quantities {
        entries: 0,
        transactions: 0,
    };
}

impl<T> Inserter<T>
where
    T: Row,
{
    pub(crate) fn new(client: &Client, table: &str) -> Result<Self> {
        Ok(Self {
            client: client.clone(),
            table: table.into(),
            max_entries: DEFAULT_MAX_ENTRIES,
            send_timeout: None,
            end_timeout: None,
            insert: client.insert(table)?,
            ticks: Ticks::default(),
            committed: Quantities::ZERO,
            uncommitted_entries: 0,
        })
    }

    /// See [`Insert::with_max_entries()`].
    pub fn with_timeouts(
        mut self,
        send_timeout: Option<Duration>,
        end_timeout: Option<Duration>,
    ) -> Self {
        self.set_timeouts(send_timeout, end_timeout);
        self
    }

    /// The maximum number of rows in one `INSERT` statement.
    ///
    /// Note: ClickHouse inserts batches atomically only if all rows fit in the same partition
    /// and their number is less [`max_insert_block_size`](https://clickhouse.tech/docs/en/operations/settings/settings/#settings-max_insert_block_size).
    ///
    /// `250_000` by default.
    pub fn with_max_entries(mut self, threshold: u64) -> Self {
        self.set_max_entries(threshold);
        self
    }

    /// The time between `INSERT`s.
    ///
    /// Note that [`Inserter`] doesn't spawn tasks or threads to check the elapsed time,
    /// all checks are performend only on [`Inserter::commit()`] calls.
    /// However, it's possible to use [`Inserter::time_left()`] and set a timer up
    /// to call [`Inserter::commit()`] to check passed time again.
    ///
    /// `None` by default.
    pub fn with_period(mut self, period: Option<Duration>) -> Self {
        self.set_period(period);
        self
    }

    /// Adds a bias to the period. The actual period will be in the following range:
    /// ```ignore
    ///   [period * (1 - bias), period * (1 + bias)]
    /// ```
    ///
    /// It helps to avoid producing a lot of `INSERT`s at the same time by multiple inserters.
    pub fn with_period_bias(mut self, bias: f64) -> Self {
        self.set_period_bias(bias);
        self
    }

    #[deprecated(note = "use `with_period()` instead")]
    pub fn with_max_duration(mut self, threshold: Duration) -> Self {
        self.set_period(Some(threshold));
        self
    }

    /// See [`Inserter::with_timeouts()`].
    pub fn set_timeouts(&mut self, send_timeout: Option<Duration>, end_timeout: Option<Duration>) {
        self.send_timeout = send_timeout;
        self.end_timeout = end_timeout;
        self.insert.set_timeouts(send_timeout, end_timeout);
    }

    /// See [`Inserter::with_max_entries()`].
    pub fn set_max_entries(&mut self, threshold: u64) {
        self.max_entries = threshold;
    }

    /// See [`Inserter::with_period()`].
    pub fn set_period(&mut self, period: Option<Duration>) {
        self.ticks.set_period(period);
        self.ticks.reschedule();
    }

    /// See [`Inserter::with_period_bias()`].
    pub fn set_period_bias(&mut self, bias: f64) {
        self.ticks.set_period_bias(bias);
        self.ticks.reschedule();
    }

    #[deprecated(note = "use `set_period()` instead")]
    pub fn set_max_duration(&mut self, threshold: Duration) {
        self.ticks.set_period(Some(threshold));
    }

    /// How much time we have until the next tick.
    ///
    /// `None` if the period isn't configured.
    pub fn time_left(&mut self) -> Option<Duration> {
        Some(
            self.ticks
                .next_at()?
                .saturating_duration_since(Instant::now()),
        )
    }

    /// Serializes and writes to the socket a provided row.
    ///
    /// # Panics
    /// If called after previous call returned an error.
    #[inline]
    pub fn write<'a>(&'a mut self, row: &T) -> impl Future<Output = Result<()>> + 'a + Send
    where
        T: Serialize,
    {
        self.uncommitted_entries += 1;
        let fut = self.insert.write(row);
        async move { fut.await }
    }

    /// Checks limits and ends a current `INSERT` if they are reached.
    pub async fn commit(&mut self) -> Result<Quantities> {
        if self.uncommitted_entries > 0 {
            self.committed.entries += self.uncommitted_entries;
            self.committed.transactions += 1;
            self.uncommitted_entries = 0;
        }

        let now = Instant::now();

        Ok(if self.is_threshold_reached(now) {
            let quantities = mem::replace(&mut self.committed, Quantities::ZERO);
            let result = self.insert().await;
            self.ticks.reschedule();
            result?;
            quantities
        } else {
            Quantities::ZERO
        })
    }

    /// Ends a current `INSERT` and whole `Inserter` unconditionally.
    ///
    /// If it isn't called, the current `INSERT` is aborted.
    pub async fn end(self) -> Result<Quantities> {
        self.insert.end().await?;
        Ok(self.committed)
    }

    fn is_threshold_reached(&self, now: Instant) -> bool {
        self.committed.entries >= self.max_entries
            || self.ticks.next_at().map_or(false, |next_at| now >= next_at)
    }

    async fn insert(&mut self) -> Result<()> {
        let mut new_insert = self.client.insert(&self.table)?; // Actually it mustn't fail.
        new_insert.set_timeouts(self.send_timeout, self.end_timeout);
        let insert = mem::replace(&mut self.insert, new_insert);
        insert.end().await
    }
}
