//! Public `NativeClient` -- a ClickHouse client using the native TCP protocol.
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
//! `with_database`, `with_user`, `with_password`, `with_setting`, `with_lz4`,
//! `with_roles`) reset the pool so the next `acquire` opens fresh connections
//! with the updated config.
//!
//! [`with_pool_size`]: NativeClient::with_pool_size

use std::net::{SocketAddr, ToSocketAddrs};
use std::sync::Arc;

use crate::error::{Error, Result};
use crate::native::connection::TlsConfig;
use crate::native::insert::NativeInsert;
use crate::native::inserter::NativeInserter;
use crate::native::pool::{NativePool, PoolConfig, PooledConnection, build_pool};
use crate::native::protocol::NativeCompressionMethod;
use crate::native::query::NativeQuery;
use crate::native::schema::NativeSchemaCache;
use crate::pool_stats::PoolStats;
use crate::row::Row;
use crate::server_info::ServerVersion;

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
    /// One or more server addresses for round-robin failover.
    ///
    /// Set via [`with_addr`] (single) or [`with_addrs`] (multiple).
    ///
    /// [`with_addr`]: NativeClient::with_addr
    /// [`with_addrs`]: NativeClient::with_addrs
    addrs: Vec<SocketAddr>,
    database: String,
    username: String,
    password: String,
    compression: NativeCompressionMethod,
    /// TLS configuration.  When set, all connections use TLS (port 9440).
    /// When `None` / `()` (depending on feature), plain TCP (port 9000).
    tls: TlsConfig,
    /// Shared schema cache (TTL 300 s by default).
    schema_cache: Arc<NativeSchemaCache>,
    /// Per-query settings sent with every query on this client.
    settings: Arc<Vec<(String, String)>>,
    /// Roles to activate on each new connection via `SET ROLE`.
    ///
    /// ClickHouse roles are session-scoped: `SET ROLE` must be issued once per
    /// connection, immediately after the handshake.  The pool manager sends
    /// `SET ROLE role1, role2, ...` before returning a new connection.
    ///
    /// An empty vec means "use the server default roles for this user"  -- 
    /// equivalent to `SET ROLE DEFAULT`.
    roles: Vec<String>,
    /// Maximum connections (idle + in-use) in the pool.
    pool_size: usize,
    /// Deadpool-backed connection pool.  Already Arc-backed internally, so
    /// cloning this client shares the same pool across all copies.
    pool: NativePool,
}

/// Default TLS config: no TLS.
fn default_tls_config() -> TlsConfig {
    #[cfg(feature = "native-tls-rustls")]
    { None }
    #[cfg(not(feature = "native-tls-rustls"))]
    { () }
}

impl Default for NativeClient {
    fn default() -> Self {
        let default_addr: SocketAddr = "127.0.0.1:9000".parse().expect("valid default addr");
        let addrs = vec![default_addr];
        let database = "default".to_string();
        let username = "default".to_string();
        let password = String::new();
        let compression = NativeCompressionMethod::None;
        let settings: Vec<(String, String)> = Vec::new();
        let roles: Vec<String> = Vec::new();
        let tls = default_tls_config();
        let pool = build_pool(
            PoolConfig {
                addrs: addrs.clone(),
                database: database.clone(),
                username: username.clone(),
                password: password.clone(),
                compression,
                settings: settings.clone(),
                roles: roles.clone(),
                tls: tls.clone(),
            },
            DEFAULT_POOL_SIZE,
        );
        Self {
            addrs,
            database,
            username,
            password,
            compression,
            tls,
            schema_cache: NativeSchemaCache::new(300),
            settings: Arc::new(settings),
            roles,
            pool_size: DEFAULT_POOL_SIZE,
            pool,
        }
    }
}

impl NativeClient {
    /// Rebuild the connection pool from the current client configuration.
    /// Called internally whenever a connection parameter changes.
    fn rebuild_pool(&mut self) {
        self.pool = build_pool(
            PoolConfig {
                addrs: self.addrs.clone(),
                database: self.database.clone(),
                username: self.username.clone(),
                password: self.password.clone(),
                compression: self.compression,
                settings: self.settings.as_ref().clone(),
                roles: self.roles.clone(),
                tls: self.tls.clone(),
            },
            self.pool_size,
        );
    }

    /// Set the server address (host:port).
    ///
    /// Replaces any previously configured addresses with a single address.
    /// To configure multiple addresses for round-robin failover, use
    /// [`with_addrs`](NativeClient::with_addrs).
    ///
    /// # Panics
    ///
    /// If `addr` cannot be resolved to a socket address.
    #[must_use]
    pub fn with_addr(mut self, addr: impl ToSocketAddrs) -> Self {
        let resolved = addr
            .to_socket_addrs()
            .expect("invalid address")
            .next()
            .expect("no address resolved");
        self.addrs = vec![resolved];
        self.rebuild_pool();
        self
    }

    /// Set multiple server addresses for round-robin failover.
    ///
    /// The pool cycles through the provided addresses in order, distributing
    /// new connections across all listed hosts.  When a connection to one host
    /// fails, the next `acquire` will try the following address in the list.
    ///
    /// Replaces any previously configured addresses.
    ///
    /// # Panics
    ///
    /// If `addrs` is empty.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use clickhouse::native::NativeClient;
    /// let client = NativeClient::default()
    ///     .with_addrs(vec![
    ///         "10.0.0.1:9000".parse().unwrap(),
    ///         "10.0.0.2:9000".parse().unwrap(),
    ///     ]);
    /// ```
    #[must_use]
    pub fn with_addrs(mut self, addrs: Vec<std::net::SocketAddr>) -> Self {
        assert!(!addrs.is_empty(), "with_addrs: address list must not be empty");
        self.addrs = addrs;
        self.rebuild_pool();
        self
    }

    /// Set the database name.
    #[must_use]
    pub fn with_database(mut self, database: impl Into<String>) -> Self {
        self.database = database.into();
        self.rebuild_pool();
        self
    }

    /// Set the username.
    #[must_use]
    pub fn with_user(mut self, user: impl Into<String>) -> Self {
        self.username = user.into();
        self.rebuild_pool();
        self
    }

    /// Set the password.
    #[must_use]
    pub fn with_password(mut self, password: impl Into<String>) -> Self {
        self.password = password.into();
        self.rebuild_pool();
        self
    }

    /// Enable LZ4 compression for query data.
    #[must_use]
    pub fn with_lz4(mut self) -> Self {
        self.compression = NativeCompressionMethod::Lz4;
        self.rebuild_pool();
        self
    }

    /// Enable TLS for all connections.
    ///
    /// Loads both webpki (public CA) and native OS root certificates,
    /// so connections work against both public ClickHouse Cloud and
    /// internal deployments with private CAs.
    ///
    /// The `server_name` is used for SNI and certificate verification  -- 
    /// typically the hostname of the ClickHouse server.
    /// Connect to ClickHouse's native TLS port (9440 by default).
    ///
    /// # Example
    ///
    /// ```no_run
    /// # async fn example() -> clickhouse::error::Result<()> {
    /// use clickhouse::native::NativeClient;
    ///
    /// let client = NativeClient::default()
    ///     .with_addr("clickhouse.example.com:9440")
    ///     .with_tls("clickhouse.example.com")
    ///     .with_database("default");
    /// # Ok(()) }
    /// ```
    #[cfg(feature = "native-tls-rustls")]
    #[must_use]
    pub fn with_tls(mut self, server_name: &str) -> Self {
        let root_store = build_root_cert_store();

        let tls_config = rustls::ClientConfig::builder()
            .with_root_certificates(root_store)
            .with_no_client_auth();

        let sni = rustls::pki_types::ServerName::try_from(server_name.to_owned())
            .expect("valid DNS server name for TLS SNI");

        self.tls = Some((std::sync::Arc::new(tls_config), sni));
        self.rebuild_pool();
        self
    }

    /// Set the maximum number of connections (idle + in-use) in the pool.
    ///
    /// Defaults to 10.  Must be called before the first query/insert  -- 
    /// changing it after the pool has been initialised has no effect.
    #[must_use]
    pub fn with_pool_size(mut self, size: usize) -> Self {
        self.pool_size = size;
        self.rebuild_pool();
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
        self.rebuild_pool();
        self
    }

    /// Activate one or more ClickHouse roles for all connections on this client.
    ///
    /// Roles are session-scoped in ClickHouse: each new connection opened by
    /// the pool will execute `SET ROLE role1, role2, ...` immediately after the
    /// handshake, before the connection is handed to any query or insert.
    ///
    /// Replaces any roles previously set by this method.  Call
    /// [`with_default_roles`] to revert to the user's default role set.
    ///
    /// [`with_default_roles`]: NativeClient::with_default_roles
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use clickhouse::native::NativeClient;
    /// // Single role
    /// let client = NativeClient::default().with_roles(["readonly"]);
    ///
    /// // Multiple roles
    /// let client = NativeClient::default().with_roles(["analyst", "reporting"]);
    /// ```
    #[must_use]
    pub fn with_roles(mut self, roles: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.roles = roles.into_iter().map(Into::into).collect();
        self.rebuild_pool();
        self
    }

    /// Clear any explicitly set roles, reverting to the user's default role set.
    ///
    /// New connections will not send `SET ROLE`, so ClickHouse uses whatever
    /// roles are configured as defaults for the authenticated user.
    ///
    /// Overrides any roles previously set by [`with_roles`].
    ///
    /// [`with_roles`]: NativeClient::with_roles
    #[must_use]
    pub fn with_default_roles(mut self) -> Self {
        self.roles.clear();
        self.rebuild_pool();
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
    pub(crate) async fn acquire(&self) -> Result<PooledConnection> {
        use deadpool::managed::PoolError;
        self.pool
            .get()
            .await
            .map(PooledConnection::new)
            .map_err(|e| match e {
                PoolError::Backend(e) => e,
                e => Error::Custom(format!("pool: {e}")),
            })
    }

    /// Ping the server.
    pub async fn ping(&self) -> Result<()> {
        let mut conn = self.acquire().await?;
        conn.ping().await
    }

    /// Return server version information from the native protocol handshake.
    ///
    /// Acquires a pooled connection (opening one if the pool is empty), reads
    /// the cached [`ServerHello`](crate::native::protocol::ServerHello), and
    /// immediately returns the connection to the pool.
    pub async fn server_version(&self) -> Result<ServerVersion> {
        let conn = self.acquire().await?;
        let hello = conn.server_hello();
        Ok(ServerVersion {
            name: hello.server_name.clone(),
            major: hello.version.0,
            minor: hello.version.1,
            patch: hello.version.2,
            revision: hello.revision_version,
            timezone: hello.timezone.clone(),
            display_name: hello.display_name.clone(),
        })
    }

    /// Return a snapshot of connection pool statistics.
    ///
    /// Values are eventually-consistent -- they reflect the pool state at the
    /// moment of the call.
    pub fn pool_stats(&self) -> PoolStats {
        let status = self.pool.status();
        PoolStats {
            max_size: status.max_size,
            size: status.size,
            available: status.available,
            waiting: status.waiting,
        }
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

/// Build a root certificate store with both webpki (public) and native (OS)
/// root certs.  This ensures TLS works against both ClickHouse Cloud (public
/// certs) and internal deployments using private CAs (certs in the OS store).
#[cfg(feature = "native-tls-rustls")]
fn build_root_cert_store() -> rustls::RootCertStore {
    let mut root_store = rustls::RootCertStore::empty();

    // 1. webpki roots -- covers ClickHouse Cloud and all public CAs.
    root_store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());

    // 2. Native OS roots -- covers internal/private CAs (e.g. cert-manager,
    //    OpenBao PKI, corporate CAs).  Errors loading individual certs are
    //    non-fatal: webpki roots alone are sufficient for public endpoints.
    let native = rustls_native_certs::load_native_certs();
    for cert in native.certs {
        let _ = root_store.add(cert);
    }

    root_store
}
