//! Aggregation of the `ProfileEvents` packets of the native protocol.
//!
//! The server sends the counters of the query (for example `ReadCompressedBytes` or
//! `SelectedRows`) as increments since the previous packet, and gauges (for example
//! `MemoryTrackerPeakUsage`) as current values. The rows with `thread_id = 0` have the values
//! of the whole query; older servers also send a row for each thread.

use crate::native::Block;
use std::collections::HashMap;

/// `ProfileEvents::Type::INCREMENT`
const INCREMENT: i8 = 1;
/// `ProfileEvents::Type::GAUGE`
const GAUGE: i8 = 2;

#[derive(Debug, Default, Clone)]
pub(crate) struct ProfileEvents {
    counters: HashMap<String, i64>,
    /// The last value of each gauge for each host.
    gauges: HashMap<(String, String), i64>,
}

impl ProfileEvents {
    /// Add the values of a `ProfileEvents` block. Blocks with other columns are ignored.
    pub(crate) fn add(&mut self, block: &Block) {
        let (Some(host), Some(thread), Some(kind), Some(name), Some(value)) = (
            block.column_by_name("host_name"),
            block.column_by_name("thread_id"),
            block.column_by_name("type"),
            block.column_by_name("name"),
            block.column_by_name("value"),
        ) else {
            return;
        };

        let (Some((8, threads)), Some((1, kinds)), Some((8, values))) =
            (thread.fixed_data(), kind.fixed_data(), value.fixed_data())
        else {
            return;
        };

        let (Ok(hosts), Ok(names)) = (host.iter::<&str>(), name.iter::<&str>()) else {
            return;
        };

        let rows = threads
            .chunks_exact(8)
            .zip(kinds)
            .zip(values.chunks_exact(8))
            .zip(hosts.zip(names));

        for (((thread, &kind), value), (host, name)) in rows {
            let (Ok(host), Ok(name)) = (host, name) else {
                continue;
            };

            let read = |bytes: &[u8]| <[u8; 8]>::try_from(bytes).map(i64::from_le_bytes).ok();
            let (Some(thread), Some(value)) = (read(thread), read(value)) else {
                continue;
            };

            if thread != 0 {
                continue;
            }

            match i8::from_le_bytes([kind]) {
                INCREMENT => {
                    let counter = self.counters.entry(name.to_owned()).or_default();
                    *counter = counter.saturating_add(value);
                }
                GAUGE => {
                    self.gauges
                        .insert((host.to_owned(), name.to_owned()), value);
                }
                _ => {}
            }
        }
    }

    /// The value of each event: the sum of increments, or the sum of gauges over hosts.
    pub(crate) fn values(&self) -> HashMap<&str, i64> {
        let mut values: HashMap<&str, i64> = self
            .counters
            .iter()
            .map(|(name, value)| (name.as_str(), *value))
            .collect();

        for ((_, name), value) in &self.gauges {
            let total = values.entry(name.as_str()).or_default();
            *total = total.saturating_add(*value);
        }

        values
    }
}
