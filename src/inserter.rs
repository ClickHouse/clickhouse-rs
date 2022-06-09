use std::{future::Future, mem};

use serde::Serialize;
use tokio::time::{Duration, Instant};

use crate::{error::Result, insert::Insert, row::Row, ticks::Ticks, Client};

const DEFAULT_MAX_ENTRIES: u64 = 250_000;

#[must_use]
pub struct Inserter<T> {
    client: Client,
    table: String,
    max_entries: u64,
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
            insert: client.insert(table)?,
            ticks: Ticks::default(),
            committed: Quantities::ZERO,
            uncommitted_entries: 0,
        })
    }

    pub fn with_max_entries(mut self, threshold: u64) -> Self {
        self.set_max_entries(threshold);
        self
    }

    pub fn with_period(mut self, period: Option<Duration>) -> Self {
        self.set_period(period);
        self
    }

    pub fn with_period_bias(mut self, bias: f64) -> Self {
        self.set_period_bias(bias);
        self
    }

    #[deprecated(note = "use `with_period()` instead")]
    pub fn with_max_duration(mut self, threshold: Duration) -> Self {
        self.set_period(Some(threshold));
        self
    }

    pub fn set_max_entries(&mut self, threshold: u64) {
        self.max_entries = threshold;
    }

    pub fn set_period(&mut self, period: Option<Duration>) {
        self.ticks.set_period(period);
        self.ticks.reschedule();
    }

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

    #[inline]
    pub fn write<'a>(&'a mut self, row: &T) -> impl Future<Output = Result<()>> + 'a + Send
    where
        T: Serialize,
    {
        self.uncommitted_entries += 1;
        let fut = self.insert.write(row);
        async move { fut.await }
    }

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

    pub async fn end(self) -> Result<Quantities> {
        self.insert.end().await?;
        Ok(self.committed)
    }

    fn is_threshold_reached(&self, now: Instant) -> bool {
        self.committed.entries >= self.max_entries
            || self.ticks.next_at().map_or(false, |next_at| now >= next_at)
    }

    async fn insert(&mut self) -> Result<()> {
        let new_insert = self.client.insert(&self.table)?; // Actually it mustn't fail.
        let insert = mem::replace(&mut self.insert, new_insert);
        insert.end().await
    }
}
