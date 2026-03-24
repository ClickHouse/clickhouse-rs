//! Connection pool for the native TCP transport.
//!
//! Thin wrapper around [`deadpool::managed`].  A [`NativeConnectionManager`]
//! teaches deadpool how to open and recycle [`NativeConnection`]s; all pool
//! mechanics (semaphore, idle queue, timeouts, metrics) are handled by
//! deadpool.
//!
//! # Discard pattern
//!
//! When an I/O error or incomplete protocol exchange leaves a connection in an
//! unrecoverable state, call [`PooledConnection::discard`].  This sets the
//! `poisoned` flag on the underlying [`NativeConnection`]; deadpool's
//! `recycle()` hook sees the flag and drops the connection instead of
//! returning it to the idle queue.

use std::net::SocketAddr;
use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicUsize, Ordering};

use deadpool::managed::{self, RecycleError, RecycleResult};

use crate::error::{Error, Result};
use crate::native::connection::NativeConnection;
use crate::native::protocol::NativeCompressionMethod;

/// Parameters needed to open a new connection.
pub(crate) struct PoolConfig {
    /// One or more server addresses for round-robin failover.
    ///
    /// The pool cycles through addresses in order, so connections are
    /// spread across all listed hosts.  A single-element vec is the
    /// common case and behaves identically to the original single-addr design.
    pub(crate) addrs: Vec<SocketAddr>,
    pub(crate) database: String,
    pub(crate) username: String,
    pub(crate) password: String,
    pub(crate) compression: NativeCompressionMethod,
    pub(crate) settings: Vec<(String, String)>,
}

/// deadpool [`Manager`](managed::Manager) for [`NativeConnection`].
pub(crate) struct NativeConnectionManager {
    config: PoolConfig,
    /// Monotonically increasing counter used for round-robin address selection.
    next_addr: AtomicUsize,
}

impl managed::Manager for NativeConnectionManager {
    type Type = NativeConnection;
    type Error = Error;

    async fn create(&self) -> Result<NativeConnection> {
        let addrs = &self.config.addrs;
        let idx = self.next_addr.fetch_add(1, Ordering::Relaxed) % addrs.len();
        let addr = &addrs[idx];
        NativeConnection::open(
            addr,
            &self.config.database,
            &self.config.username,
            &self.config.password,
            self.config.compression,
            self.config.settings.clone(),
        )
        .await
    }

    async fn recycle(
        &self,
        conn: &mut NativeConnection,
        _: &managed::Metrics,
    ) -> RecycleResult<Error> {
        if !conn.check_alive() {
            return Err(RecycleError::message("connection dead or dirty"));
        }
        Ok(())
    }
}

/// A bounded connection pool backed by deadpool.
pub(crate) type NativePool = managed::Pool<NativeConnectionManager>;

/// Build a new pool with the given config and connection cap.
pub(crate) fn build_pool(config: PoolConfig, max_size: usize) -> NativePool {
    let mgr = NativeConnectionManager {
        config,
        next_addr: AtomicUsize::new(0),
    };
    managed::Pool::builder(mgr)
        .max_size(max_size)
        .build()
        .expect("pool config is always valid")
}

/// A connection borrowed from the pool.
///
/// Dereferences to [`NativeConnection`] for transparent method access.
/// Returns the connection to the idle queue on drop, unless
/// [`discard`](PooledConnection::discard) was called first.
pub(crate) struct PooledConnection {
    inner: managed::Object<NativeConnectionManager>,
}

impl PooledConnection {
    pub(crate) fn new(inner: managed::Object<NativeConnectionManager>) -> Self {
        Self { inner }
    }

    /// Mark this connection as broken.
    ///
    /// The connection will be closed on drop rather than returned to the pool.
    /// Call this after any I/O error or incomplete protocol exchange that
    /// leaves the connection in an unrecoverable state.
    pub(crate) fn discard(&mut self) {
        self.inner.poisoned = true;
    }
}

impl Deref for PooledConnection {
    type Target = NativeConnection;
    fn deref(&self) -> &NativeConnection {
        &self.inner
    }
}

impl DerefMut for PooledConnection {
    fn deref_mut(&mut self) -> &mut NativeConnection {
        &mut self.inner
    }
}
