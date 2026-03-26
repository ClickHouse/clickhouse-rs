//! Schema cache for the native transport.
//!
//! Caches `(column_name, type_name)` pairs fetched from `system.columns` so
//! that consumers (e.g. dfe-loader) can inspect table schemas without a
//! round-trip on every insert.
//!
//! The cache is shared across clones of [`crate::native::NativeClient`] via `Arc`.

use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use rustc_hash::FxHashMap;

/// A cached schema entry.
struct Entry {
    /// Ordered `(name, type_name)` pairs.
    columns: Vec<(String, String)>,
    fetched_at: Instant,
}

/// TTL-based schema cache shared across [`crate::native::NativeClient`] clones.
pub(crate) struct NativeSchemaCache {
    inner: RwLock<FxHashMap<String, Entry>>,
    ttl: Duration,
}

impl NativeSchemaCache {
    /// Create a new cache wrapped in `Arc`.
    ///
    /// A TTL of 300 s (5 minutes) is a sensible default.
    pub(crate) fn new(ttl_secs: u64) -> Arc<Self> {
        Arc::new(Self {
            inner: RwLock::new(FxHashMap::default()),
            ttl: Duration::from_secs(ttl_secs),
        })
    }

    /// Return cached columns if the entry exists and has not expired.
    pub(crate) fn get(&self, table: &str) -> Option<Vec<(String, String)>> {
        let guard = self.inner.read().unwrap();
        guard.get(table).and_then(|e| {
            if e.fetched_at.elapsed() < self.ttl {
                Some(e.columns.clone())
            } else {
                None
            }
        })
    }

    /// Insert or refresh a schema entry.
    pub(crate) fn insert(&self, table: String, columns: Vec<(String, String)>) {
        let mut guard = self.inner.write().unwrap();
        guard.insert(
            table,
            Entry {
                columns,
                fetched_at: Instant::now(),
            },
        );
    }

    /// Remove a schema from the cache, forcing a refresh on next access.
    pub(crate) fn invalidate(&self, table: &str) {
        self.inner.write().unwrap().remove(table);
    }

    /// Remove all cached schemas.
    pub(crate) fn invalidate_all(&self) {
        self.inner.write().unwrap().clear();
    }
}
