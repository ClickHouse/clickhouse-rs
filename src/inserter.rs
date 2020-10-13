use std::{future::Future, mem};

use serde::Serialize;
use tokio::time::{Duration, Instant};

use crate::{error::Result, insert::Insert, introspection::Reflection, Client};

const DEFAULT_ENTRY_THRESHOLD: u64 = 250_000;
const DEFAULT_TIME_THRESHOLD: Duration = Duration::from_secs(10);
const MAX_TIME_BIAS: f64 = 0.10; // % of `time_threshold`

pub struct Inserter<T> {
    client: Client,
    table: String,
    entry_threshold: u64,
    time_threshold: Duration,
    insert: Insert<T>,
    quantities: Quantities,
    next_insert_at: Instant,
}

//#[derive(Debug, Clone, Add, AddAssign, Sub, SubAssign, PartialEq, Eq, Serialize)]
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Quantities {
    pub entries: u64,
    pub transactions: u64,
}

impl Quantities {
    pub const ZERO: Quantities = Quantities {
        entries: 0,
        transactions: 0,
    };
}

impl<T> Inserter<T>
where
    T: Reflection,
{
    pub fn new(client: &Client, table: &str) -> Result<Self> {
        Ok(Self {
            client: client.clone(),
            table: table.into(),
            entry_threshold: DEFAULT_ENTRY_THRESHOLD,
            time_threshold: DEFAULT_TIME_THRESHOLD,
            insert: client.insert(table)?,
            quantities: Quantities::ZERO,
            next_insert_at: Instant::now() + DEFAULT_TIME_THRESHOLD,
        })
    }

    pub fn with_entry_threshold(mut self, threshold: u64) -> Self {
        self.set_entry_threshold(threshold);
        self
    }

    pub fn with_time_threshold(mut self, threshold: Duration) -> Self {
        self.set_time_threshold(threshold);
        self
    }

    pub fn set_entry_threshold(&mut self, threshold: u64) {
        self.entry_threshold = threshold;
    }

    pub fn set_time_threshold(&mut self, threshold: Duration) {
        let prev_insert_at = self
            .next_insert_at
            .checked_sub(self.time_threshold)
            .unwrap_or_else(Instant::now);
        self.next_insert_at = prev_insert_at + threshold;
        self.time_threshold = threshold;
    }

    #[inline]
    pub fn write<'a>(&'a mut self, row: &T) -> impl Future<Output = Result<()>> + 'a + Send
    where
        T: Serialize,
    {
        self.quantities.entries += 1;
        let fut = self.insert.write(row);
        async move { fut.await }
    }

    pub async fn commit(&mut self) -> Result<Quantities> {
        self.quantities.transactions += 1;

        let now = Instant::now();

        Ok(if self.is_threshold_reached(now) {
            self.next_insert_at = shifted_next_time(now, self.next_insert_at, self.time_threshold);
            let new_insert = self.client.insert(&self.table)?; // Actually it mustn't fail.
            let insert = mem::replace(&mut self.insert, new_insert);
            insert.end().await?;
            mem::replace(&mut self.quantities, Quantities::ZERO)
        } else {
            Quantities::ZERO
        })
    }

    pub async fn end(self) -> Result<Quantities> {
        self.insert.end().await?;
        Ok(self.quantities)
    }

    fn is_threshold_reached(&self, now: Instant) -> bool {
        self.quantities.entries >= self.entry_threshold || now >= self.next_insert_at
    }
}

fn shifted_next_time(now: Instant, prev: Instant, threshold: Duration) -> Instant {
    const MAX_TIME_BIAS_255: u32 = (MAX_TIME_BIAS * 255. + 0.5) as u32;

    let coef = (now.max(prev) - now.min(prev)).subsec_nanos() & 0xff;
    let max_bias = threshold * MAX_TIME_BIAS_255 / 255;
    let bias = max_bias * coef / 255;

    prev + threshold + 2 * bias - max_bias
}
