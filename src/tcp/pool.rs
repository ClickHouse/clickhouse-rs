//! A pool of native TCP connections.
//!
//! A connection is a server session: `SET`, `USE` and temporary tables persist in it.
//! A query that changes the session closes its connection afterwards, so that pooled
//! connections behave like stateless HTTP requests. The `session_id` setting reserves one
//! connection for the session, like the sessions of the HTTP interface.

use crate::error::{Error, Result};
use crate::tcp::Address;
use crate::tcp::connection::{Connection, Hello, HelloAuth};
use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};
use tokio::net::TcpStream;

/// The maximum number of idle connections for each server and user.
const MAX_IDLE_CONNECTIONS: usize = 32;

/// Idle connections older than this are closed.
const IDLE_TIMEOUT: Duration = Duration::from_secs(60);

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
    state: Mutex<PoolState>,
}

#[derive(Default)]
struct PoolState {
    idle: HashMap<PoolKey, Vec<IdleConnection>>,
    sessions: HashMap<(PoolKey, String), Session>,
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
            None => connect(key).await?,
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
            if idle_conn.since.elapsed() < IDLE_TIMEOUT && idle_conn.conn.is_reusable() {
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
                let idle = state.idle.entry(self.key.clone()).or_default();
                idle.retain(|idle_conn| idle_conn.since.elapsed() < IDLE_TIMEOUT);

                if idle.len() < MAX_IDLE_CONNECTIONS {
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
