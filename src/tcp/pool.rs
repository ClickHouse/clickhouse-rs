//! A pool of native TCP connections.
//!
//! A connection is a server session: `SET`, `USE` and temporary tables persist in it.
//! A query that changes the session closes its connection afterwards, so that pooled
//! connections behave like stateless HTTP requests. The `session_id` setting reserves one
//! connection for the session, like the sessions of the HTTP interface.

use crate::error::{Error, Result};
use crate::tcp::Address;
use crate::tcp::connection::{Connection, Hello, HelloAuth, ServerCompression};
use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};
use tokio::net::TcpStream;

/// Options of the connection pool of the native TCP transport.
///
/// Use with [`Client::with_tcp_pool()`](crate::Client::with_tcp_pool).
///
/// # Example
/// ```
/// # use clickhouse::{Client, TcpPoolOptions};
/// # use std::time::Duration;
/// let client = Client::default()
///     .with_url("tcp://localhost:9000")
///     .with_tcp_pool(
///         TcpPoolOptions::default()
///             .with_max_idle_connections(128)
///             .with_idle_timeout(Duration::from_secs(30)),
///     );
/// ```
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct TcpPoolOptions {
    /// The maximum number of idle connections for each server and user. Connections that are
    /// released when this number is reached are closed. Default: `32`.
    pub max_idle_connections: usize,
    /// Idle connections are closed after this time. Default: 60 seconds.
    pub idle_timeout: Duration,
}

impl Default for TcpPoolOptions {
    fn default() -> Self {
        Self {
            max_idle_connections: 32,
            idle_timeout: Duration::from_secs(60),
        }
    }
}

impl TcpPoolOptions {
    /// Set [`Self::max_idle_connections`].
    ///
    /// Set it to the number of concurrent queries to reuse a connection for each query.
    #[must_use]
    pub fn with_max_idle_connections(mut self, max_idle_connections: usize) -> Self {
        self.max_idle_connections = max_idle_connections;
        self
    }

    /// Set [`Self::idle_timeout`].
    #[must_use]
    pub fn with_idle_timeout(mut self, idle_timeout: Duration) -> Self {
        self.idle_timeout = idle_timeout;
        self
    }
}

/// The default value of the `session_timeout` setting of the HTTP interface.
pub(crate) const DEFAULT_SESSION_TIMEOUT: Duration = Duration::from_secs(60);

/// Everything that is sent in the `Hello` packet: connections with different keys
/// cannot be used for each other's queries.
#[derive(Clone, PartialEq, Eq, Hash)]
pub(crate) struct PoolKey {
    pub(crate) address: Address,
    pub(crate) database: String,
    pub(crate) auth: KeyAuth,
    pub(crate) quota_key: String,
    pub(crate) client_name: String,
}

#[derive(Clone, PartialEq, Eq, Hash)]
pub(crate) enum KeyAuth {
    Password { user: String, password: String },
    Jwt(String),
}

#[derive(Default)]
pub(crate) struct Pool {
    options: TcpPoolOptions,
    state: Mutex<PoolState>,
}

#[derive(Default)]
struct PoolState {
    idle: HashMap<PoolKey, Vec<IdleConnection>>,
    sessions: HashMap<(PoolKey, String), Session>,
    /// The compression settings of the server for each user, to probe them once.
    server_compression: HashMap<PoolKey, ServerCompression>,
}

struct IdleConnection {
    conn: Connection,
    since: Instant,
}

struct Session {
    /// `None` while a query uses the session.
    conn: Option<Connection>,
    expires: Instant,
}

/// Options of the `session_id` setting.
#[derive(Clone)]
pub(crate) struct SessionOptions {
    pub(crate) id: String,
    pub(crate) timeout: Duration,
    /// `session_check = 1`: the session must exist.
    pub(crate) check: bool,
    /// `close_session = 1`: close the session after the query.
    pub(crate) close: bool,
}

/// A connection taken from the pool.
///
/// Return it with [`PooledConnection::release()`]; a dropped connection is closed.
pub(crate) struct PooledConnection {
    conn: Option<Connection>,
    pool: Arc<Pool>,
    key: PoolKey,
    session: Option<SessionOptions>,
}

impl Pool {
    pub(crate) fn new(options: TcpPoolOptions) -> Self {
        Self {
            options,
            state: Mutex::default(),
        }
    }

    fn lock(&self) -> MutexGuard<'_, PoolState> {
        // The state stays consistent if a thread panics while holding the lock.
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    pub(crate) async fn acquire(
        self: &Arc<Self>,
        key: &PoolKey,
        session: Option<SessionOptions>,
    ) -> Result<PooledConnection> {
        let reused = match &session {
            Some(session) => self.take_session(key, session)?,
            None => self.take_idle(key),
        };

        let conn = match reused {
            Some(conn) => conn,
            None => {
                let mut conn = connect(key).await?;
                conn.server_compression = self.lock().server_compression.get(key).cloned();
                conn
            }
        };

        Ok(PooledConnection {
            conn: Some(conn),
            pool: self.clone(),
            key: key.clone(),
            session,
        })
    }

    fn take_idle(&self, key: &PoolKey) -> Option<Connection> {
        let mut state = self.lock();
        let idle = state.idle.get_mut(key)?;

        while let Some(mut idle_conn) = idle.pop() {
            if idle_conn.since.elapsed() < self.options.idle_timeout && idle_conn.conn.is_reusable()
            {
                return Some(idle_conn.conn);
            }
        }

        None
    }

    fn take_session(&self, key: &PoolKey, options: &SessionOptions) -> Result<Option<Connection>> {
        let mut state = self.lock();
        let now = Instant::now();
        state
            .sessions
            .retain(|_, session| session.conn.is_none() || session.expires > now);

        let session_key = (key.clone(), options.id.clone());

        let existing = match state.sessions.get_mut(&session_key) {
            Some(session) => match session.conn.take() {
                Some(conn) => Some(conn),
                None => {
                    return Err(Error::BadResponse(format!(
                        "Code: 373. DB::Exception: Session {} is locked by a concurrent client. \
                         (SESSION_IS_LOCKED)",
                        options.id
                    )));
                }
            },
            None => None,
        };

        let existing = existing.and_then(|mut conn| conn.is_reusable().then_some(conn));

        match existing {
            Some(conn) => return Ok(Some(conn)),
            // The server closed the connection, and the session with it.
            None if options.check => {
                state.sessions.remove(&session_key);
                return Err(session_not_found(&options.id));
            }
            None => {}
        }

        // Reserve the session until the connection is released.
        state.sessions.insert(
            session_key,
            Session {
                conn: None,
                expires: now + options.timeout,
            },
        );
        Ok(None)
    }
}

fn session_not_found(id: &str) -> Error {
    Error::BadResponse(format!(
        "Code: 372. DB::Exception: Session {id} not found. (SESSION_NOT_FOUND)"
    ))
}

async fn connect(key: &PoolKey) -> Result<Connection> {
    let stream = TcpStream::connect((key.address.host.as_str(), key.address.port))
        .await
        .map_err(|e| Error::Network(Box::new(e)))?;
    stream
        .set_nodelay(true)
        .map_err(|e| Error::Network(Box::new(e)))?;

    let stream = crate::tcp::tls::wrap(stream, &key.address).await?;

    let auth = match &key.auth {
        KeyAuth::Password { user, password } => HelloAuth::Password { user, password },
        KeyAuth::Jwt(token) => HelloAuth::Jwt(token),
    };

    Connection::handshake(
        stream,
        &Hello {
            client_name: &key.client_name,
            database: &key.database,
            auth,
            quota_key: &key.quota_key,
        },
    )
    .await
}

impl PooledConnection {
    pub(crate) fn conn(&mut self) -> Result<&mut Connection> {
        self.conn
            .as_mut()
            .ok_or_else(|| Error::Other("BUG: the connection was already released".into()))
    }

    /// Return the connection to the pool if `reusable`, or close it.
    pub(crate) fn release(mut self, reusable: bool) {
        let Some(conn) = self.conn.take() else {
            return;
        };

        let mut state = self.pool.lock();

        if let Some(known) = &conn.server_compression {
            state
                .server_compression
                .entry(self.key.clone())
                .or_insert_with(|| known.clone());
        }

        match self.session.take() {
            Some(session) => {
                let session_key = (self.key.clone(), session.id);

                if reusable && !session.close {
                    state.sessions.insert(
                        session_key,
                        Session {
                            conn: Some(conn),
                            expires: Instant::now() + session.timeout,
                        },
                    );
                } else {
                    state.sessions.remove(&session_key);
                }
            }
            None if reusable => {
                let options = &self.pool.options;
                let idle = state.idle.entry(self.key.clone()).or_default();
                idle.retain(|idle_conn| idle_conn.since.elapsed() < options.idle_timeout);

                if idle.len() < options.max_idle_connections {
                    idle.push(IdleConnection {
                        conn,
                        since: Instant::now(),
                    });
                }
            }
            None => {}
        }
    }
}

impl Drop for PooledConnection {
    fn drop(&mut self) {
        if self.conn.is_some() {
            // The connection is in an unknown state.
            if let Some(session) = self.session.take() {
                self.pool
                    .lock()
                    .sessions
                    .remove(&(self.key.clone(), session.id));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tcp::protocol::CLIENT_REVISION;
    use crate::tcp::server_tests::connect;

    fn key() -> PoolKey {
        PoolKey {
            address: Address {
                host: "localhost".into(),
                port: 9000,
                secure: false,
            },
            database: String::new(),
            auth: KeyAuth::Password {
                user: "default".into(),
                password: String::new(),
            },
            quota_key: String::new(),
            client_name: String::new(),
        }
    }

    /// Release `count` connections to `pool`, keeping the scripted servers open.
    async fn release(pool: &Arc<Pool>, count: usize) -> Vec<impl Sized> {
        let mut servers = Vec::new();

        for _ in 0..count {
            let (conn, server) = connect(CLIENT_REVISION).await;
            servers.push(server);

            PooledConnection {
                conn: Some(conn),
                pool: pool.clone(),
                key: key(),
                session: None,
            }
            .release(true);
        }

        servers
    }

    fn idle_count(pool: &Pool) -> usize {
        pool.lock().idle.get(&key()).map_or(0, Vec::len)
    }

    #[tokio::test]
    async fn keeps_at_most_max_idle_connections() {
        for max in [0, 1, 3] {
            let pool = Arc::new(Pool::new(
                TcpPoolOptions::default().with_max_idle_connections(max),
            ));
            let _servers = release(&pool, 5).await;
            assert_eq!(idle_count(&pool), max);

            // Every idle connection is reused, then the pool is empty.
            for _ in 0..max {
                assert!(pool.take_idle(&key()).is_some());
            }
            assert!(pool.take_idle(&key()).is_none());
        }
    }

    #[tokio::test]
    async fn default_options() {
        let pool = Arc::new(Pool::default());
        let _servers = release(&pool, 40).await;
        assert_eq!(idle_count(&pool), 32);
    }

    #[tokio::test]
    async fn closes_expired_connections() {
        let pool = Arc::new(Pool::new(
            TcpPoolOptions::default().with_idle_timeout(Duration::ZERO),
        ));
        let _servers = release(&pool, 2).await;
        assert!(pool.take_idle(&key()).is_none());
        assert_eq!(idle_count(&pool), 0);
    }

    #[tokio::test]
    async fn closed_connections_are_not_reused() {
        let pool = Arc::new(Pool::default());
        let servers = release(&pool, 2).await;
        drop(servers);
        assert!(pool.take_idle(&key()).is_none());
    }
}
