//! Public `NativeClient` — a ClickHouse client using the native TCP protocol.
//!
//! Mirrors the basic API of [`crate::Client`] so integration tests can switch
//! between transports with minimal changes.
//!
//! # Connection model
//!
//! Each `NativeClient` owns a lazily-initialised connection pool (default
//! size: 10).  Connections are returned to the pool after each query/insert
//! and reused by subsequent operations.  Use [`with_pool_size`] to tune the
//! cap.  The pool is per-client-instance; clones share the same pool.
//!
//! Builder methods that affect connection parameters (`with_addr`,
//! `with_database`, `with_user`, `with_password`, `with_setting`, `with_lz4`)
//! reset the pool so the next `acquire` opens fresh connections with the
//! updated config.
//!
//! [`with_pool_size`]: NativeClient::with_pool_size

use std::net::{SocketAddr, ToSocketAddrs};
use std::sync::{Arc, OnceLock};

use crate::error::Result;
use crate::native::insert::NativeInsert;
use crate::native::inserter::NativeInserter;
use crate::native::pool::{NativePool, PoolConfig, PooledConnection};
use crate::native::protocol::NativeCompressionMethod;
use crate::native::query::NativeQuery;
use crate::native::schema::NativeSchemaCache;
use crate::row::Row;

/// A ClickHouse client using the native binary TCP protocol (port 9000).
///
/// # Example
///
/// ```no_run
/// # async fn example() -> clickhouse::error::Result<()> {
/// use clickhouse::native::NativeClient;
///
/// let client = NativeClient::default()
///     .with_addr("localhost:9000")
///     .with_database("default")
///     .with_user("default")
///     .with_password("");
///
/// client.query("CREATE TABLE t (n UInt32) ENGINE = Memory").execute().await?;
/// # Ok(()) }
/// ```
/// Default connection pool size.
const DEFAULT_POOL_SIZE: usize = 10;

#[derive(Clone)]
pub struct NativeClient {
    addr: SocketAddr,
    database: String,
    username: String,
    password: String,
    compression: NativeCompressionMethod,
    /// Shared schema cache (TTL 300 s by default).
    schema_cache: Arc<NativeSchemaCache>,
    /// Per-query settings sent with every query on this client.
    settings: Arc<Vec<(String, String)>>,
    /// Maximum connections (idle + in-use) in the pool.
    pool_size: usize,
    /// Lazily-initialised pool; reset whenever connection params change.
    ///
    /// Wrapped in `Arc` so `Clone` shares the same pool across copies of a
    /// fully-configured client.
    pool: Arc<OnceLock<Arc<NativePool>>>,
}

impl Default for NativeClient {
    fn default() -> Self {
        Self {
            addr: "127.0.0.1:9000".parse().expect("valid default addr"),
            database: "default".to_string(),
            username: "default".to_string(),
            password: String::new(),
            compression: NativeCompressionMethod::None,
            schema_cache: NativeSchemaCache::new(300),
            settings: Arc::new(Vec::new()),
            pool_size: DEFAULT_POOL_SIZE,
            pool: Arc::new(OnceLock::new()),
        }
    }
}

impl NativeClient {
    /// Set the server address (host:port).
    ///
    /// # Panics
    ///
    /// If `addr` cannot be resolved to a socket address.
    #[must_use]
    pub fn with_addr(mut self, addr: impl ToSocketAddrs) -> Self {
        self.addr = addr
            .to_socket_addrs()
            .expect("invalid address")
            .next()
            .expect("no address resolved");
        self.pool = Arc::new(OnceLock::new());
        self
    }

    /// Set the database name.
    #[must_use]
    pub fn with_database(mut self, database: impl Into<String>) -> Self {
        self.database = database.into();
        self.pool = Arc::new(OnceLock::new());
        self
    }

    /// Set the username.
    #[must_use]
    pub fn with_user(mut self, user: impl Into<String>) -> Self {
        self.username = user.into();
        self.pool = Arc::new(OnceLock::new());
        self
    }

    /// Set the password.
    #[must_use]
    pub fn with_password(mut self, password: impl Into<String>) -> Self {
        self.password = password.into();
        self.pool = Arc::new(OnceLock::new());
        self
    }

    /// Enable LZ4 compression for query data.
    #[must_use]
    pub fn with_lz4(mut self) -> Self {
        self.compression = NativeCompressionMethod::Lz4;
        self.pool = Arc::new(OnceLock::new());
        self
    }

    /// Set the maximum number of connections (idle + in-use) in the pool.
    ///
    /// Defaults to 10.  Must be called before the first query/insert —
    /// changing it after the pool has been initialised has no effect.
    #[must_use]
    pub fn with_pool_size(mut self, size: usize) -> Self {
        self.pool_size = size;
        self.pool = Arc::new(OnceLock::new());
        self
    }

    /// Add a session-level setting sent with every query on this client.
    ///
    /// Settings are sent in the query packet and apply to all query types
    /// (SELECT, INSERT, DDL).  Common examples:
    ///
    /// ```no_run
    /// # use clickhouse::native::NativeClient;
    /// let client = NativeClient::default()
    ///     // Read-after-write consistency on replicated tables:
    ///     .with_setting("select_sequential_consistency", "1")
    ///     // Require N replicas to acknowledge an INSERT before returning:
    ///     .with_setting("insert_quorum", "2");
    /// ```
    #[must_use]
    pub fn with_setting(
        mut self,
        name: impl Into<String>,
        value: impl Into<String>,
    ) -> Self {
        Arc::make_mut(&mut self.settings).push((name.into(), value.into()));
        self.pool = Arc::new(OnceLock::new());
        self
    }

    /// Return all session-level settings configured on this client.
    pub(crate) fn settings(&self) -> &[(String, String)] {
        &self.settings
    }

    /// Start a query.
    pub fn query(&self, sql: &str) -> NativeQuery {
        NativeQuery::new(self.clone(), sql)
    }

    /// Begin a single INSERT statement for rows of type `T`.
    ///
    /// The connection is opened lazily on the first call to
    /// [`NativeInsert::write`].  Call [`NativeInsert::end`] to commit.
    ///
    /// ```no_run
    /// # async fn example() -> clickhouse::error::Result<()> {
    /// use clickhouse::{Row, native::NativeClient};
    /// use serde::Serialize;
    ///
    /// #[derive(Row, Serialize)]
    /// struct Event { id: u64, name: String }
    ///
    /// let client = NativeClient::default();
    /// let mut insert = client.insert::<Event>("events");
    /// insert.write(&Event { id: 1, name: "foo".into() }).await?;
    /// insert.end().await?;
    /// # Ok(()) }
    /// ```
    pub fn insert<T: Row>(&self, table: &str) -> NativeInsert<T> {
        NativeInsert::new(self.clone(), table)
    }

    /// Create a multi-batch inserter for rows of type `T`.
    ///
    /// Mirrors [`crate::inserter::Inserter`] for the native transport.
    ///
    /// ```no_run
    /// # async fn example() -> clickhouse::error::Result<()> {
    /// use clickhouse::{Row, native::NativeClient};
    /// use serde::Serialize;
    /// use std::time::Duration;
    ///
    /// #[derive(Row, Serialize)]
    /// struct Event { id: u64, name: String }
    ///
    /// let client = NativeClient::default();
    /// let mut ins = client.inserter::<Event>("events")
    ///     .with_max_rows(100_000)
    ///     .with_period(Some(Duration::from_secs(5)));
    ///
    /// ins.write(&Event { id: 1, name: "foo".into() }).await?;
    /// ins.commit().await?;
    /// ins.end().await?;
    /// # Ok(()) }
    /// ```
    pub fn inserter<T: Row>(&self, table: &str) -> NativeInserter<T> {
        NativeInserter::new(self, table)
    }

    /// Return the cached schema for `table` if it has been populated.
    ///
    /// The cache is populated automatically during INSERT operations when the
    /// server sends column headers.  To fetch the schema proactively, use
    /// [`NativeClient::fetch_schema`].
    pub fn cached_schema(&self, table: &str) -> Option<Vec<(String, String)>> {
        self.schema_cache.get(table)
    }

    /// Fetch column schema for `table` from `system.columns`, bypassing the cache.
    ///
    /// Parses the result at the RowBinary level so no serde derive is required.
    /// The result is stored in the TTL cache for future calls to [`cached_schema`].
    ///
    /// [`cached_schema`]: NativeClient::cached_schema
    pub async fn fetch_schema(
        &self,
        table: &str,
    ) -> Result<Vec<(String, String)>> {
        if let Some(cached) = self.schema_cache.get(table) {
            return Ok(cached);
        }
        let db = &self.database;
        let sql = format!(
            "SELECT name, type \
             FROM system.columns \
             WHERE database = '{db}' AND table = '{table}' \
             ORDER BY position"
        );
        let columns = fetch_string_pairs(self, &sql).await?;
        self.schema_cache.insert(table.to_string(), columns.clone());
        Ok(columns)
    }

    /// Remove `table`'s schema from the cache, forcing a refresh on next access.
    pub fn clear_cached_schema(&self, table: &str) {
        self.schema_cache.invalidate(table);
    }

    /// Remove all cached schemas.
    pub fn clear_all_cached_schemas(&self) {
        self.schema_cache.invalidate_all();
    }

    /// Populate the schema cache entry for `table` from the given column headers.
    ///
    /// Called internally after a successful `begin_insert` to cache the schema
    /// the server reported.
    pub(crate) fn cache_schema(
        &self,
        table: &str,
        columns: &[(String, String)],
    ) {
        self.schema_cache
            .insert(table.to_string(), columns.to_vec());
    }

    /// Acquire a connection from the pool, opening a new one if needed.
    ///
    /// The pool is created lazily on first call with a snapshot of the
    /// current connection parameters.
    pub(crate) async fn acquire(&self) -> Result<PooledConnection> {
        let pool = self.pool.get_or_init(|| {
            NativePool::new(
                PoolConfig {
                    addr: self.addr,
                    database: self.database.clone(),
                    username: self.username.clone(),
                    password: self.password.clone(),
                    compression: self.compression,
                    settings: self.settings.as_ref().clone(),
                },
                self.pool_size,
            )
        });
        pool.acquire().await
    }

    /// Ping the server.
    pub async fn ping(&self) -> Result<()> {
        let mut conn = self.acquire().await?;
        conn.ping().await
    }
}

/// Execute a query expected to return two `String` columns and collect all rows
/// as `Vec<(String, String)>`, parsing RowBinary directly without serde.
async fn fetch_string_pairs(
    client: &NativeClient,
    sql: &str,
) -> Result<Vec<(String, String)>> {
    use crate::native::reader::ServerPacket;

    let mut conn = client.acquire().await?;
    let revision = conn.server_revision();
    let compression = conn.compression();

    crate::native::writer::send_query(
        conn.writer_mut(),
        "",
        sql,
        client.settings(),
        revision,
        compression,
    )
    .await?;
    crate::native::writer::send_empty_block(conn.writer_mut(), compression).await?;

    let mut result = Vec::new();

    loop {
        let packet = crate::native::reader::read_packet(
            conn.reader_mut(),
            revision,
            compression,
        )
        .await?;
        match packet {
            ServerPacket::Data(block) if block.num_rows > 0 => {
                // Each element in row_data is one complete RowBinary row.
                // Two String columns: parse varuint(len)+bytes twice per row.
                for row in &block.row_data {
                    let (a, rest) = rb_read_string(row)?;
                    let (b, _) = rb_read_string(rest)?;
                    result.push((a, b));
                }
            }
            ServerPacket::EndOfStream => break,
            ServerPacket::Exception(err) => {
                return Err(crate::error::Error::BadResponse(err.to_string()));
            }
            _ => {}
        }
    }

    Ok(result)
}

/// Parse one RowBinary-encoded `String` from the start of `bytes`.
/// Returns `(value, remaining_bytes)`.
fn rb_read_string(bytes: &[u8]) -> crate::error::Result<(String, &[u8])> {
    if bytes.is_empty() {
        return Err(crate::error::Error::NotEnoughData);
    }
    let mut len = 0u64;
    let mut shift = 0u32;
    let mut i = 0usize;
    loop {
        if i >= bytes.len() {
            return Err(crate::error::Error::NotEnoughData);
        }
        let b = bytes[i];
        i += 1;
        len |= u64::from(b & 0x7F) << shift;
        shift += 7;
        if b & 0x80 == 0 {
            break;
        }
    }
    let len = len as usize;
    if i + len > bytes.len() {
        return Err(crate::error::Error::NotEnoughData);
    }
    let s = String::from_utf8_lossy(&bytes[i..i + len]).into_owned();
    Ok((s, &bytes[i + len..]))
}
