//! Connection pool for the native TCP transport.
//!
//! Holds a bounded set of idle [`NativeConnection`]s so successive queries
//! and inserts can reuse TCP connections instead of paying handshake overhead
//! on every operation.
//!
//! # Model
//!
//! - A [`Semaphore`] caps the total number of connections (idle + in-use) to
//!   `max_size`.  Callers block on [`NativePool::acquire`] when the pool is
//!   full until a permit becomes available.
//! - Idle connections are stored in a `Mutex<VecDeque>` (FIFO, so recently
//!   used connections are preferred).
//! - On [`PooledConnection`] drop the connection is returned to the idle
//!   queue unless [`PooledConnection::discard`] was called, in which case it
//!   is closed and the semaphore slot is released.

use std::collections::VecDeque;
use std::net::SocketAddr;
use std::ops::{Deref, DerefMut};
use std::sync::{Arc, Mutex};

use tokio::sync::Semaphore;

use crate::error::{Error, Result};
use crate::native::connection::NativeConnection;
use crate::native::protocol::NativeCompressionMethod;

/// Parameters needed to open a new connection.
pub(crate) struct PoolConfig {
    pub(crate) addr: SocketAddr,
    pub(crate) database: String,
    pub(crate) username: String,
    pub(crate) password: String,
    pub(crate) compression: NativeCompressionMethod,
    pub(crate) settings: Vec<(String, String)>,
}

/// A bounded idle-connection pool.
pub(crate) struct NativePool {
    idle: Mutex<VecDeque<NativeConnection>>,
    /// Total in-use + idle connections must not exceed `max_size`.
    semaphore: Semaphore,
    config: PoolConfig,
    max_size: usize,
}

impl NativePool {
    pub(crate) fn new(config: PoolConfig, max_size: usize) -> Arc<Self> {
        Arc::new(Self {
            idle: Mutex::new(VecDeque::new()),
            semaphore: Semaphore::new(max_size),
            config,
            max_size,
        })
    }

    /// Acquire a connection.  Waits if the pool is at capacity.
    ///
    /// Tries an idle connection first; opens a new one if none are available.
    pub(crate) async fn acquire(self: &Arc<Self>) -> Result<PooledConnection> {
        let permit = self
            .semaphore
            .acquire()
            .await
            .map_err(|_| Error::Custom("connection pool closed".into()))?;
        // We manage permits manually — forget the RAII guard.
        permit.forget();

        let conn = {
            let mut idle = self.idle.lock().expect("pool mutex");
            idle.pop_front()
        };

        let conn = match conn {
            Some(c) => c,
            None => {
                NativeConnection::open(
                    &self.config.addr,
                    &self.config.database,
                    &self.config.username,
                    &self.config.password,
                    self.config.compression,
                    self.config.settings.clone(),
                )
                .await
                .inspect_err(|_| {
                    // Opening failed — release the permit so the pool slot isn't lost.
                    self.semaphore.add_permits(1);
                })?
            }
        };

        Ok(PooledConnection {
            conn: Some(conn),
            pool: Arc::clone(self),
            return_to_pool: true,
        })
    }

    /// Return a connection to the idle queue and release its semaphore slot.
    fn return_conn(&self, conn: NativeConnection) {
        {
            let mut idle = self.idle.lock().expect("pool mutex");
            // Guard against exceeding max_size in the idle queue.
            if idle.len() < self.max_size {
                idle.push_back(conn);
            }
            // If somehow over capacity, just drop the connection.
        }
        self.semaphore.add_permits(1);
    }

    /// Release a semaphore permit without returning the connection (broken path).
    fn release_permit(&self) {
        self.semaphore.add_permits(1);
    }
}

/// A connection borrowed from a [`NativePool`].
///
/// Dereferences to [`NativeConnection`] for transparent method calls.
/// When dropped, the connection is returned to the pool — unless
/// [`discard`](PooledConnection::discard) was called, in which case the
/// connection is closed and the pool slot is freed.
pub(crate) struct PooledConnection {
    conn: Option<NativeConnection>,
    pool: Arc<NativePool>,
    return_to_pool: bool,
}

impl PooledConnection {
    /// Mark this connection as broken — it will be closed on drop instead of
    /// returned to the pool.  Call this when an I/O error or incomplete
    /// protocol exchange leaves the connection in an unrecoverable state.
    pub(crate) fn discard(&mut self) {
        self.return_to_pool = false;
    }
}

impl Deref for PooledConnection {
    type Target = NativeConnection;
    fn deref(&self) -> &NativeConnection {
        self.conn
            .as_ref()
            .expect("PooledConnection invariant: conn is always Some while borrowed")
    }
}

impl DerefMut for PooledConnection {
    fn deref_mut(&mut self) -> &mut NativeConnection {
        self.conn
            .as_mut()
            .expect("PooledConnection invariant: conn is always Some while borrowed")
    }
}

impl Drop for PooledConnection {
    fn drop(&mut self) {
        if let Some(conn) = self.conn.take() {
            if self.return_to_pool {
                self.pool.return_conn(conn);
            } else {
                drop(conn);
                self.pool.release_permit();
            }
        }
    }
}
