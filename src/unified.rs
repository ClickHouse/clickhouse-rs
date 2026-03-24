//! Unified client wrapper that dispatches to either the HTTP or native TCP transport.
//!
//! # Design — Approach C (additive wrapper, both backends untouched)
//!
//! [`UnifiedClient`] holds a [`Transport`] enum and delegates every operation
//! to whichever variant is active.  Neither [`crate::Client`] nor
//! [`crate::native::NativeClient`] is modified; they remain fully independent
//! and can still be used directly.
//!
//! ## Extension point
//!
//! `Transport` is the single extension point for future transports.  To add
//! a new transport (e.g. gRPC):
//!
//! 1. Add a variant to `Transport` (feature-gate it if appropriate):
//!    ```ignore
//!    #[cfg(feature = "grpc-transport")]
//!    Grpc(GrpcClient),
//!    ```
//! 2. Add a builder struct (`UnifiedGrpcBuilder`) that wraps `GrpcClient`'s
//!    builder pattern and terminates in `.build() -> UnifiedClient`.
//! 3. Add an `UnifiedClient::grpc() -> UnifiedGrpcBuilder` constructor and,
//!    optionally, an `as_grpc() -> Option<&GrpcClient>` accessor.

use std::sync::Arc;

use crate::Client;
use crate::dynamic::{DynamicBatchConfig, DynamicBatcher, DynamicSchemaCache};
use crate::dynamic::insert::DynamicInsert;
use crate::error::Result;
use crate::pool_stats::PoolStats;
use crate::row::Row;
use crate::server_info::ServerVersion;
use crate::unified_insert::UnifiedInsert;
use crate::unified_query::UnifiedQuery;

#[cfg(feature = "native-transport")]
use crate::native::NativeClient;

// ---------------------------------------------------------------------------
// Transport enum
// ---------------------------------------------------------------------------

/// Selects the wire protocol used by a [`UnifiedClient`].
///
/// Variants are additive — new transports can be introduced without breaking
/// existing code that already pattern-matches on this enum (add `#[non_exhaustive]`
/// if upstream opts in to that stability guarantee).
#[derive(Clone)]
pub enum Transport {
    /// HTTP interface (default ClickHouse port 8123).
    Http(Client),

    /// Native binary TCP protocol (default ClickHouse port 9000).
    #[cfg(feature = "native-transport")]
    Native(NativeClient),
}

// ---------------------------------------------------------------------------
// UnifiedClient
// ---------------------------------------------------------------------------

/// A ClickHouse client that can use either the HTTP or native TCP transport,
/// selected at runtime via the [`Transport`] enum.
///
/// # Construction
///
/// Use the transport-specific constructors for a fluent builder experience:
///
/// ```no_run
/// use clickhouse::unified::UnifiedClient;
///
/// // HTTP transport
/// let client = UnifiedClient::http()
///     .with_url("http://localhost:8123")
///     .with_database("default")
///     .build();
///
/// // Native TCP transport (requires feature = "native-transport")
/// # #[cfg(feature = "native-transport")]
/// let client = UnifiedClient::native()
///     .with_addr("localhost:9000")
///     .with_database("default")
///     .build();
/// ```
///
/// Or wrap an already-configured client directly:
///
/// ```no_run
/// use clickhouse::{Client, unified::{Transport, UnifiedClient}};
///
/// let http = Client::default().with_url("http://localhost:8123");
/// let client = UnifiedClient::new(Transport::Http(http));
/// ```
#[derive(Clone)]
pub struct UnifiedClient {
    transport: Transport,
    pub(crate) dynamic_schema_cache: Arc<DynamicSchemaCache>,
}

impl UnifiedClient {
    /// Wrap an existing [`Transport`] value.
    ///
    /// For the HTTP transport, the dynamic schema cache is shared with the
    /// underlying [`Client`].  For the native transport, a new independent
    /// cache is created (5 min TTL, same default as HTTP).
    pub fn new(transport: Transport) -> Self {
        let dynamic_schema_cache = match &transport {
            Transport::Http(c) => c.dynamic_schema_cache.clone(),
            #[cfg(feature = "native-transport")]
            Transport::Native(_) => DynamicSchemaCache::new(std::time::Duration::from_secs(300)),
        };
        Self {
            transport,
            dynamic_schema_cache,
        }
    }

    /// Start building an HTTP-transport client.
    pub fn http() -> UnifiedHttpBuilder {
        UnifiedHttpBuilder::default()
    }

    /// Start building a native-TCP-transport client.
    #[cfg(feature = "native-transport")]
    pub fn native() -> UnifiedNativeBuilder {
        UnifiedNativeBuilder::default()
    }

    // -----------------------------------------------------------------------
    // Accessors
    // -----------------------------------------------------------------------

    /// Return a reference to the underlying [`Transport`].
    pub fn transport(&self) -> &Transport {
        &self.transport
    }

    /// Return the inner [`Client`] if this is an HTTP transport, otherwise `None`.
    pub fn as_http(&self) -> Option<&Client> {
        match &self.transport {
            Transport::Http(c) => Some(c),
            #[cfg(feature = "native-transport")]
            Transport::Native(_) => None,
        }
    }

    /// Return the inner [`NativeClient`] if this is a native transport, otherwise `None`.
    #[cfg(feature = "native-transport")]
    pub fn as_native(&self) -> Option<&NativeClient> {
        match &self.transport {
            Transport::Native(c) => Some(c),
            Transport::Http(_) => None,
        }
    }

    // -----------------------------------------------------------------------
    // Query
    // -----------------------------------------------------------------------

    /// Start building a SELECT or DDL query.
    ///
    /// Returns a [`UnifiedQuery`] that dispatches to whichever transport is
    /// active.  Use [`UnifiedQuery::bind`] to fill `?` placeholders, then
    /// call one of the terminal methods (`execute`, `fetch_all`, `fetch_one`,
    /// `fetch_optional`).
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use clickhouse::unified::UnifiedClient;
    /// # async fn example() -> clickhouse::error::Result<()> {
    /// let client = UnifiedClient::http().with_url("http://localhost:8123").build();
    /// client.query("CREATE TABLE IF NOT EXISTS t (x UInt32) ENGINE=Memory")
    ///     .execute()
    ///     .await?;
    /// # Ok(()) }
    /// ```
    pub fn query(&self, sql: &str) -> UnifiedQuery {
        match &self.transport {
            Transport::Http(c) => UnifiedQuery::from_http(c.query(sql)),
            #[cfg(feature = "native-transport")]
            Transport::Native(c) => UnifiedQuery::from_native(c.query(sql)),
        }
    }

    // -----------------------------------------------------------------------
    // INSERT
    // -----------------------------------------------------------------------

    /// Start an INSERT for rows of type `T`.
    ///
    /// Returns a [`UnifiedInsert`] that dispatches to whichever transport is
    /// active.  Call [`UnifiedInsert::write`] for each row, then
    /// [`UnifiedInsert::end`] to commit.
    ///
    /// This method is `async` because the HTTP transport opens the request at
    /// this point; the native transport defers the connection until the first
    /// [`write`](UnifiedInsert::write) call.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use clickhouse::unified::UnifiedClient;
    /// # use clickhouse::{Row, RowOwned, RowWrite};
    /// # use serde::Serialize;
    /// # async fn example() -> clickhouse::error::Result<()> {
    /// #[derive(Row, Serialize)]
    /// struct Event { id: u64, name: String }
    ///
    /// let client = UnifiedClient::http().with_url("http://localhost:8123").build();
    /// let mut insert = client.insert::<Event>("events").await?;
    /// insert.write(&Event { id: 1, name: "foo".into() }).await?;
    /// insert.end().await?;
    /// # Ok(()) }
    /// ```
    pub async fn insert<T: Row>(&self, table: &str) -> Result<UnifiedInsert<T>> {
        match &self.transport {
            Transport::Http(c) => Ok(UnifiedInsert::from_http(c.insert::<T>(table).await?)),
            #[cfg(feature = "native-transport")]
            Transport::Native(c) => Ok(UnifiedInsert::from_native(c.insert::<T>(table))),
        }
    }

    /// Start an INSERT sending pre-formatted data (HTTP transport only).
    ///
    /// `sql` should be an `INSERT INTO ... FORMAT <format name>` statement.
    ///
    /// Returns [`crate::error::Error::UnsupportedTransport`] if the active
    /// transport is not HTTP.
    pub fn insert_formatted_with(
        &self,
        sql: impl Into<String>,
    ) -> Result<crate::insert_formatted::InsertFormatted> {
        match &self.transport {
            Transport::Http(c) => Ok(c.insert_formatted_with(sql)),
            #[cfg(feature = "native-transport")]
            Transport::Native(_) => Err(crate::error::Error::UnsupportedTransport(
                "InsertFormatted requires HTTP transport".into(),
            )),
        }
    }

    // -----------------------------------------------------------------------
    // Dynamic INSERT
    // -----------------------------------------------------------------------

    /// Start a schema-driven dynamic insert for a table.
    ///
    /// Fetches the schema from `system.columns` (cached with TTL) and encodes
    /// `Map<String, Value>` to RowBinary.  The query path works on both HTTP
    /// and native transports, but the actual insert data path currently requires
    /// the HTTP transport (native returns an error at write time).
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let mut insert = client.dynamic_insert("mydb", "mytable");
    /// insert.write_map(&row).await?;
    /// insert.write_map(&row2).await?;
    /// let rows_written = insert.end().await?;
    /// ```
    pub fn dynamic_insert(&self, database: &str, table: &str) -> DynamicInsert {
        DynamicInsert::new(
            self.clone(),
            database.to_string(),
            table.to_string(),
            self.dynamic_schema_cache.clone(),
        )
    }

    /// Start an async auto-flushing dynamic batcher for a table.
    ///
    /// Same as [`dynamic_insert`][Self::dynamic_insert] but with a background
    /// task that auto-flushes on row count and time thresholds.  Multiple tasks
    /// can write concurrently via [`DynamicBatcherHandle`][crate::dynamic::DynamicBatcherHandle].
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let batcher = client.dynamic_batcher("mydb", "mytable", Default::default());
    /// let handle = batcher.handle();
    /// handle.write_map(row).await?;
    /// batcher.end().await?;
    /// ```
    pub fn dynamic_batcher(
        &self,
        database: &str,
        table: &str,
        config: DynamicBatchConfig,
    ) -> DynamicBatcher {
        DynamicBatcher::new(self, database, table, config)
    }

    // -----------------------------------------------------------------------
    // Query cancellation
    // -----------------------------------------------------------------------

    /// Cancel a running query by its ID.
    ///
    /// Sends `KILL QUERY WHERE query_id = '{id}'` on a fresh connection so
    /// the in-flight query is not interrupted mid-stream.  ClickHouse will
    /// attempt to cancel the query asynchronously — cancellation is
    /// best-effort and not guaranteed to be immediate.
    ///
    /// The `query_id` should be the same value passed to
    /// [`UnifiedQuery::with_query_id`] when the query was started.  Because
    /// `query_id` is caller-supplied (not end-user input), a plain format
    /// string is used rather than a parameterized query.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use clickhouse::unified::UnifiedClient;
    /// # async fn example() -> clickhouse::error::Result<()> {
    /// let client = UnifiedClient::http().with_url("http://localhost:8123").build();
    /// client
    ///     .query("SELECT sleep(30)")
    ///     .with_query_id("my-long-query")
    ///     .execute();  // fire-and-forget in a separate task
    ///
    /// // ... later, from another task:
    /// client.cancel_query("my-long-query").await?;
    /// # Ok(()) }
    /// ```
    pub async fn cancel_query(&self, query_id: &str) -> Result<()> {
        let sql = format!("KILL QUERY WHERE query_id = '{query_id}'");
        self.query(&sql).execute().await
    }

    // -----------------------------------------------------------------------
    // Ping
    // -----------------------------------------------------------------------

    /// Ping the server to verify connectivity.
    ///
    /// For the HTTP transport this issues `SELECT 1`.  For the native transport
    /// it sends the native ping packet.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use clickhouse::unified::UnifiedClient;
    /// # async fn example() -> clickhouse::error::Result<()> {
    /// let client = UnifiedClient::http().with_url("http://localhost:8123").build();
    /// client.ping().await?;
    /// # Ok(()) }
    /// ```
    pub async fn ping(&self) -> Result<()> {
        match &self.transport {
            Transport::Http(c) => c.query("SELECT 1").execute().await,
            #[cfg(feature = "native-transport")]
            Transport::Native(c) => c.ping().await,
        }
    }

    // -----------------------------------------------------------------------
    // Server metadata
    // -----------------------------------------------------------------------

    /// Return server version information from the native TCP handshake.
    ///
    /// Returns `None` for the HTTP transport (no handshake with version info).
    /// For the native transport this acquires a pooled connection, reads the
    /// cached [`ServerVersion`], and immediately returns the connection.
    pub async fn server_version(&self) -> Option<ServerVersion> {
        match &self.transport {
            Transport::Http(_) => None,
            #[cfg(feature = "native-transport")]
            Transport::Native(c) => c.server_version().await.ok(),
        }
    }

    // -----------------------------------------------------------------------
    // Pool statistics
    // -----------------------------------------------------------------------

    /// Return a snapshot of connection pool statistics.
    ///
    /// Returns `None` for the HTTP transport (no managed connection pool).
    pub fn pool_stats(&self) -> Option<PoolStats> {
        match &self.transport {
            Transport::Http(_) => None,
            #[cfg(feature = "native-transport")]
            Transport::Native(c) => Some(c.pool_stats()),
        }
    }
}

// ---------------------------------------------------------------------------
// HTTP builder
// ---------------------------------------------------------------------------

/// Fluent builder for an HTTP-backed [`UnifiedClient`].
///
/// Delegates to [`Client`]'s builder methods.  Call `.build()` to finish.
pub struct UnifiedHttpBuilder {
    inner: Client,
}

impl Default for UnifiedHttpBuilder {
    fn default() -> Self {
        Self {
            inner: Client::default(),
        }
    }
}

impl UnifiedHttpBuilder {
    /// Set the ClickHouse HTTP endpoint URL.
    ///
    /// # Examples
    /// ```
    /// use clickhouse::unified::UnifiedClient;
    /// let client = UnifiedClient::http().with_url("http://localhost:8123").build();
    /// ```
    #[must_use]
    pub fn with_url(mut self, url: impl Into<String>) -> Self {
        self.inner = self.inner.with_url(url);
        self
    }

    /// Set the database name.
    #[must_use]
    pub fn with_database(mut self, database: impl Into<String>) -> Self {
        self.inner = self.inner.with_database(database);
        self
    }

    /// Set the username.
    #[must_use]
    pub fn with_user(mut self, user: impl Into<String>) -> Self {
        self.inner = self.inner.with_user(user);
        self
    }

    /// Set the password.
    #[must_use]
    pub fn with_password(mut self, password: impl Into<String>) -> Self {
        self.inner = self.inner.with_password(password);
        self
    }

    /// Consume the builder and return a [`UnifiedClient`].
    pub fn build(self) -> UnifiedClient {
        UnifiedClient::new(Transport::Http(self.inner))
    }
}

impl From<UnifiedHttpBuilder> for UnifiedClient {
    fn from(b: UnifiedHttpBuilder) -> Self {
        b.build()
    }
}

// ---------------------------------------------------------------------------
// Native builder
// ---------------------------------------------------------------------------

/// Fluent builder for a native-TCP-backed [`UnifiedClient`].
///
/// Delegates to [`NativeClient`]'s builder methods.  Call `.build()` to finish.
#[cfg(feature = "native-transport")]
pub struct UnifiedNativeBuilder {
    inner: NativeClient,
}

#[cfg(feature = "native-transport")]
impl Default for UnifiedNativeBuilder {
    fn default() -> Self {
        Self {
            inner: NativeClient::default(),
        }
    }
}

#[cfg(feature = "native-transport")]
impl UnifiedNativeBuilder {
    /// Set the server address (`host:port`).
    ///
    /// # Panics
    ///
    /// If `addr` cannot be resolved to a socket address.
    ///
    /// # Examples
    /// ```no_run
    /// use clickhouse::unified::UnifiedClient;
    /// let client = UnifiedClient::native().with_addr("localhost:9000").build();
    /// ```
    #[must_use]
    pub fn with_addr(mut self, addr: impl std::net::ToSocketAddrs) -> Self {
        self.inner = self.inner.with_addr(addr);
        self
    }

    /// Set the database name.
    #[must_use]
    pub fn with_database(mut self, database: impl Into<String>) -> Self {
        self.inner = self.inner.with_database(database);
        self
    }

    /// Set the username.
    #[must_use]
    pub fn with_user(mut self, user: impl Into<String>) -> Self {
        self.inner = self.inner.with_user(user);
        self
    }

    /// Set the password.
    #[must_use]
    pub fn with_password(mut self, password: impl Into<String>) -> Self {
        self.inner = self.inner.with_password(password);
        self
    }

    /// Enable LZ4 compression for query data.
    #[must_use]
    pub fn with_lz4(mut self) -> Self {
        self.inner = self.inner.with_lz4();
        self
    }

    /// Set multiple server addresses for round-robin failover.
    ///
    /// Delegates to [`NativeClient::with_addrs`].
    ///
    /// # Panics
    ///
    /// If `addrs` is empty.
    #[must_use]
    pub fn with_addrs(mut self, addrs: Vec<std::net::SocketAddr>) -> Self {
        self.inner = self.inner.with_addrs(addrs);
        self
    }

    /// Consume the builder and return a [`UnifiedClient`].
    pub fn build(self) -> UnifiedClient {
        UnifiedClient::new(Transport::Native(self.inner))
    }
}

#[cfg(feature = "native-transport")]
impl From<UnifiedNativeBuilder> for UnifiedClient {
    fn from(b: UnifiedNativeBuilder) -> Self {
        b.build()
    }
}
