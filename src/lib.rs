#![doc = include_str!("../README.md")]
#![cfg_attr(docsrs, feature(doc_cfg))]

pub use self::{
    compression::Compression,
    row::{Row, RowOwned, RowRead, RowWrite},
};
use self::{error::Result, http_client::HttpClient};
use crate::row_metadata::{AccessType, ColumnDefaultKind, InsertMetadata, RowMetadata};
use crate::server_info::ServerVersion;

#[doc = include_str!("row_derive.md")]
pub use clickhouse_macros::Row;
use clickhouse_types::{Column, DataTypeNode};

use crate::_priv::row_insert_metadata_query;
use std::collections::HashSet;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::{fmt::Display, sync::Arc};

use rustc_hash::FxHashMap;
use tokio::sync::RwLock;

#[cfg(feature = "async-inserter")]
pub mod async_inserter;
#[cfg(feature = "batcher")]
pub mod batcher;
pub mod error;
pub mod insert;
pub mod insert_formatted;
#[cfg(feature = "inserter")]
pub mod inserter;
pub mod query;
pub mod serde;
pub mod sql;
#[cfg(feature = "test-util")]
pub mod test;

pub mod types;

mod bytes_ext;
mod compression;
mod cursors;
mod headers;
mod http_client;
pub mod quantities;
mod request_body;
mod response;
mod row;
mod row_metadata;
mod rowbinary;
#[cfg(any(feature = "inserter", feature = "native-transport"))]
pub(crate) mod ticks;

#[cfg(feature = "native-transport")]
pub mod native;

pub mod dynamic;

pub mod pool_stats;
pub mod server_info;
pub mod unified;
pub mod unified_cursor;
pub mod unified_insert;
pub mod unified_query;
pub use pool_stats::PoolStats;
pub use unified::{Transport, UnifiedClient};

/// A client containing HTTP pool.
///
/// ### Cloning behavior
/// Clones share the same HTTP transport but store their own configurations.
/// Any `with_*` configuration method (e.g., [`Client::with_option`]) applies
/// only to future clones, because [`Client::clone`] creates a deep copy
/// of the [`Client`] configuration, except the transport.
///
/// The round-robin URL counter (`next_url_index`) is shared across clones so
/// that all copies of a client advance through the same host rotation.
#[derive(Clone)]
pub struct Client {
    http: Arc<dyn HttpClient>,

    /// The ordered list of ClickHouse HTTP endpoints.
    ///
    /// Always contains at least one entry after [`Client::with_url`] or
    /// [`Client::with_urls`] is called. May be empty for a default-constructed
    /// client that has not yet had a URL set (preserving backwards compat).
    urls: Vec<String>,

    /// Shared counter for round-robin URL selection across all clones.
    ///
    /// `Arc` so that all clones advance the same counter; `AtomicUsize` so
    /// that there is no lock contention on the hot path.
    next_url_index: Arc<AtomicUsize>,

    database: Option<String>,
    authentication: Authentication,
    compression: Compression,
    roles: HashSet<String>,
    options: FxHashMap<String, String>,
    headers: FxHashMap<String, String>,
    products_info: Vec<ProductInfo>,
    validation: bool,
    insert_metadata_cache: Arc<InsertMetadataCache>,
    pub(crate) dynamic_schema_cache: Arc<dynamic::DynamicSchemaCache>,

    #[cfg(feature = "test-util")]
    mocked: bool,
}

#[derive(Clone)]
struct ProductInfo {
    name: String,
    version: String,
}

impl Display for ProductInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}", self.name, self.version)
    }
}

#[derive(Clone, PartialEq)]
pub(crate) enum Authentication {
    Credentials {
        user: Option<String>,
        password: Option<String>,
    },
    Jwt {
        access_token: String,
    },
}

// Manual Debug impl to redact secrets from log/panic output.
impl std::fmt::Debug for Authentication {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Credentials { user, .. } => f
                .debug_struct("Credentials")
                .field("user", user)
                .field("password", &"[REDACTED]")
                .finish(),
            Self::Jwt { .. } => f
                .debug_struct("Jwt")
                .field("access_token", &"[REDACTED]")
                .finish(),
        }
    }
}

impl Default for Authentication {
    fn default() -> Self {
        Self::Credentials {
            user: None,
            password: None,
        }
    }
}

impl Default for Client {
    fn default() -> Self {
        Self::with_http_client(http_client::default())
    }
}

/// Cache for [`RowMetadata`] to avoid allocating it for the same struct more than once
/// during the application lifecycle. Key: fully qualified table name (e.g. `database.table`).
#[derive(Default)]
pub(crate) struct InsertMetadataCache(RwLock<FxHashMap<String, Arc<InsertMetadata>>>);

impl Client {
    /// Creates a new client with a specified underlying HTTP client.
    ///
    /// See `HttpClient` for details.
    pub fn with_http_client(client: impl HttpClient) -> Self {
        Self {
            http: Arc::new(client),
            urls: Vec::new(),
            next_url_index: Arc::new(AtomicUsize::new(0)),
            database: None,
            authentication: Authentication::default(),
            compression: Compression::default(),
            roles: HashSet::new(),
            options: FxHashMap::default(),
            headers: FxHashMap::default(),
            products_info: Vec::default(),
            validation: true,
            insert_metadata_cache: Arc::new(InsertMetadataCache::default()),
            dynamic_schema_cache: dynamic::DynamicSchemaCache::new(std::time::Duration::from_secs(
                300,
            )),
            #[cfg(feature = "test-util")]
            mocked: false,
        }
    }

    /// Specifies ClickHouse's url. Should point to HTTP endpoint.
    ///
    /// Automatically [clears the metadata cache][Self::clear_cached_metadata]
    /// for this instance only.
    ///
    /// # Examples
    /// ```
    /// # use clickhouse::Client;
    /// let client = Client::default().with_url("http://localhost:8123");
    /// ```
    pub fn with_url(mut self, url: impl Into<String>) -> Self {
        let mut url = url.into();

        // `with_mock()` didn't exist previously, so to not break existing usages,
        // we need to be able to detect a mocked server using nothing but the URL.
        #[cfg(feature = "test-util")]
        if let Some(real_url) = test::Mock::mocked_url_to_real(&url) {
            url = real_url;
            self.mocked = true;
        }

        self.urls = vec![url];

        // Assume our cached metadata is invalid.
        self.insert_metadata_cache = Default::default();

        self
    }

    /// Specifies multiple ClickHouse HTTP endpoints for round-robin failover.
    ///
    /// On each request the client picks the next URL from the list using a
    /// shared atomic counter, cycling through the hosts in order. This provides
    /// simple load distribution across a set of ClickHouse nodes.
    ///
    /// All clones of the client share the same counter so the rotation is
    /// co-ordinated across copies.
    ///
    /// Automatically [clears the metadata cache][Self::clear_cached_metadata]
    /// for this instance only.
    ///
    /// # Panics
    ///
    /// If `urls` is empty.
    ///
    /// # Examples
    /// ```
    /// # use clickhouse::Client;
    /// let client = Client::default().with_urls(vec![
    ///     "http://ch-1:8123".to_string(),
    ///     "http://ch-2:8123".to_string(),
    ///     "http://ch-3:8123".to_string(),
    /// ]);
    /// ```
    pub fn with_urls(mut self, urls: Vec<String>) -> Self {
        assert!(!urls.is_empty(), "with_urls: URL list must not be empty");
        self.urls = urls;

        // Assume our cached metadata is invalid.
        self.insert_metadata_cache = Default::default();

        self
    }

    /// Specifies a database name.
    ///
    /// Automatically [clears the metadata cache][Self::clear_cached_metadata]
    /// for this instance only.
    ///
    /// # Examples
    /// ```
    /// # use clickhouse::Client;
    /// let client = Client::default().with_database("test");
    /// ```
    pub fn with_database(mut self, database: impl Into<String>) -> Self {
        self.database = Some(database.into());

        // Assume our cached metadata is invalid.
        self.insert_metadata_cache = Default::default();

        self
    }

    /// Specifies a user.
    ///
    /// # Panics
    /// If called after [`Client::with_access_token`].
    ///
    /// # Examples
    /// ```
    /// # use clickhouse::Client;
    /// let client = Client::default().with_user("test");
    /// ```
    pub fn with_user(mut self, user: impl Into<String>) -> Self {
        match self.authentication {
            Authentication::Jwt { .. } => {
                panic!("`user` cannot be set together with `access_token`");
            }
            Authentication::Credentials { password, .. } => {
                self.authentication = Authentication::Credentials {
                    user: Some(user.into()),
                    password,
                };
            }
        }
        self
    }

    /// Specifies a password.
    ///
    /// # Panics
    /// If called after [`Client::with_access_token`].
    ///
    /// # Examples
    /// ```
    /// # use clickhouse::Client;
    /// let client = Client::default().with_password("secret");
    /// ```
    pub fn with_password(mut self, password: impl Into<String>) -> Self {
        match self.authentication {
            Authentication::Jwt { .. } => {
                panic!("`password` cannot be set together with `access_token`");
            }
            Authentication::Credentials { user, .. } => {
                self.authentication = Authentication::Credentials {
                    user,
                    password: Some(password.into()),
                };
            }
        }
        self
    }

    /// Configure the [roles] to use when executing statements with this `Client` instance.
    ///
    /// Overrides any roles previously set by this method or [`Client::with_option`].
    ///
    /// Call [`Client::with_default_roles`] to clear any explicitly set roles.
    ///
    /// This setting is copied into cloned clients.
    ///
    /// [roles]: https://clickhouse.com/docs/operations/access-rights#role-management
    ///
    /// # Examples
    ///
    /// ```
    /// # use clickhouse::Client;
    ///
    /// // Single role
    /// let client = Client::default().with_roles(["foo"]);
    ///
    /// // Multiple roles
    /// let client = Client::default().with_roles(["foo", "bar", "baz"]);
    /// ```
    pub fn with_roles(mut self, roles: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.set_roles(roles);
        self
    }

    /// Clear any explicitly set [roles] from this `Client` instance.
    ///
    /// Overrides any roles previously set by [`Client::with_roles`] or [`Client::with_option`].
    ///
    /// [roles]: https://clickhouse.com/docs/operations/access-rights#role-management
    pub fn with_default_roles(mut self) -> Self {
        self.clear_roles();
        self
    }

    /// A JWT access token to authenticate with ClickHouse.
    /// JWT token authentication is supported in ClickHouse Cloud only.
    /// Should not be called after [`Client::with_user`] or
    /// [`Client::with_password`].
    ///
    /// # Panics
    /// If called after [`Client::with_user`] or [`Client::with_password`].
    ///
    /// # Examples
    /// ```
    /// # use clickhouse::Client;
    /// let client = Client::default().with_access_token("jwt");
    /// ```
    pub fn with_access_token(mut self, access_token: impl Into<String>) -> Self {
        match self.authentication {
            Authentication::Credentials { user, password }
                if user.is_some() || password.is_some() =>
            {
                panic!("`access_token` cannot be set together with `user` or `password`");
            }
            _ => {
                self.authentication = Authentication::Jwt {
                    access_token: access_token.into(),
                }
            }
        }
        self
    }

    /// Specifies a compression mode. See [`Compression`] for details.
    /// By default, `Lz4` is used.
    ///
    /// # Examples
    /// ```
    /// # use clickhouse::{Client, Compression};
    /// # #[cfg(feature = "lz4")]
    /// let client = Client::default().with_compression(Compression::Lz4);
    /// ```
    pub fn with_compression(mut self, compression: Compression) -> Self {
        self.compression = compression;
        self
    }

    /// Used to specify options that will be passed to all queries.
    ///
    /// # Example
    /// ```
    /// # use clickhouse::Client;
    /// Client::default().with_option("allow_nondeterministic_mutations", "1");
    /// ```
    pub fn with_option(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.options.insert(name.into(), value.into());
        self
    }

    /// Used to specify a header that will be passed to all queries.
    ///
    /// # Example
    /// ```
    /// # use clickhouse::Client;
    /// Client::default().with_header("Cookie", "A=1");
    /// ```
    pub fn with_header(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.headers.insert(name.into(), value.into());
        self
    }

    /// Specifies the product name and version that will be included
    /// in the default User-Agent header. Multiple products are supported.
    /// This could be useful for the applications built on top of this client.
    ///
    /// # Examples
    ///
    /// Sample default User-Agent header:
    ///
    /// ```plaintext
    /// clickhouse-rs/0.12.2 (lv:rust/1.67.0, os:macos)
    /// ```
    ///
    /// Sample User-Agent with a single product information:
    ///
    /// ```
    /// # use clickhouse::Client;
    /// let client = Client::default().with_product_info("MyDataSource", "v1.0.0");
    /// ```
    ///
    /// ```plaintext
    /// MyDataSource/v1.0.0 clickhouse-rs/0.12.2 (lv:rust/1.67.0, os:macos)
    /// ```
    ///
    /// Sample User-Agent with multiple products information
    /// (NB: the products are added in the reverse order of
    /// [`Client::with_product_info`] calls, which could be useful to add
    /// higher abstraction layers first):
    ///
    /// ```
    /// # use clickhouse::Client;
    /// let client = Client::default()
    ///     .with_product_info("MyDataSource", "v1.0.0")
    ///     .with_product_info("MyApp", "0.0.1");
    /// ```
    ///
    /// ```plaintext
    /// MyApp/0.0.1 MyDataSource/v1.0.0 clickhouse-rs/0.12.2 (lv:rust/1.67.0, os:macos)
    /// ```
    pub fn with_product_info(
        mut self,
        product_name: impl Into<String>,
        product_version: impl Into<String>,
    ) -> Self {
        self.products_info.push(ProductInfo {
            name: product_name.into(),
            version: product_version.into(),
        });
        self
    }

    /// Set an option on this instance of [`Client`].
    ///
    /// Returns the previous value for the option, if one was set.
    pub fn set_option(
        &mut self,
        name: impl Into<String>,
        value: impl Into<String>,
    ) -> Option<String> {
        self.options.insert(name.into(), value.into())
    }

    /// Get an option that was previously set on this `Client`.
    pub fn get_option(&self, name: impl AsRef<str>) -> Option<&str> {
        self.options.get(name.as_ref()).map(String::as_str)
    }

    /// Starts a new INSERT statement.
    ///
    /// # Validation
    ///
    /// If validation is enabled (default), `RowBinaryWithNamesAndTypes` input format is used.
    /// When [`Client::insert`] method is called for this `table` for the first time,
    /// it will fetch the table schema from the server, allowing to validate the serialized rows,
    /// as well as write the names and types of the columns in the request header.
    ///
    /// Fetching the schema will happen only once per `table`,
    /// as the schema is cached by the client internally.
    ///
    /// With disabled validation, the schema is not fetched,
    /// and the rows serialized with `RowBinary` input format.
    ///
    /// # Panics
    ///
    /// If `T` has unnamed fields, e.g. tuples.
    pub async fn insert<T: Row>(&self, table: &str) -> Result<insert::Insert<T>> {
        if self.get_validation() {
            let metadata = self.get_insert_metadata(table).await?;
            let row = metadata.to_row::<T>()?;
            return Ok(insert::Insert::new(self, table, Some(row)));
        }
        Ok(insert::Insert::new(self, table, None))
    }

    /// Creates an inserter to perform multiple INSERT statements.
    #[cfg(feature = "inserter")]
    pub fn inserter<T: Row>(&self, table: &str) -> inserter::Inserter<T> {
        inserter::Inserter::new(self, table)
    }

    /// Start an `INSERT` statement sending pre-formatted data.
    ///
    /// `sql` should be an `INSERT INTO ... FORMAT <format name>` statement.
    /// Any other type of statement may produce incorrect results.
    ///
    /// The statement is not issued until the first call to
    /// [`.send()`][insert_formatted::InsertFormatted::send].
    ///
    /// # Note: Not Validated
    /// Unlike [`Insert`][insert::Insert] and [`Inserter`][inserter::Inserter],
    /// this does not perform any validation on the submitted data.
    ///
    /// Only the use of self-describing formats (e.g. CSV, TabSeparated, JSON) is recommended.
    ///
    /// See the [list of supported formats](https://clickhouse.com/docs/interfaces/formats)
    /// for details.
    pub fn insert_formatted_with(
        &self,
        sql: impl Into<String>,
    ) -> insert_formatted::InsertFormatted {
        insert_formatted::InsertFormatted::new(self, sql.into())
    }

    /// Start a dynamic INSERT for a table with runtime schema.
    ///
    /// Fetches the schema from `system.columns` (cached with TTL) and encodes
    /// `Map<String, Value>` to RowBinary. As simple as JSONEachRow to use, but
    /// ClickHouse skips JSON parsing entirely -- significant CPU savings on the
    /// cluster at scale.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let mut insert = client.dynamic_insert("mydb", "mytable");
    /// insert.write_map(&row).await?;
    /// insert.write_map(&row2).await?;
    /// let rows_written = insert.end().await?;
    /// ```
    pub fn dynamic_insert(&self, database: &str, table: &str) -> dynamic::insert::DynamicInsert {
        let unified =
            crate::unified::UnifiedClient::new(crate::unified::Transport::Http(self.clone()));
        unified.dynamic_insert(database, table)
    }

    /// Start an async auto-flushing dynamic batcher for a table.
    ///
    /// Same as [`dynamic_insert`][Self::dynamic_insert] but with a background
    /// task that auto-flushes on row count and time thresholds. Multiple tasks
    /// can write concurrently via [`DynamicBatcherHandle`][dynamic::DynamicBatcherHandle].
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
        config: dynamic::DynamicBatchConfig,
    ) -> dynamic::DynamicBatcher {
        let unified =
            crate::unified::UnifiedClient::new(crate::unified::Transport::Http(self.clone()));
        unified.dynamic_batcher(database, table, config)
    }

    /// Starts a new SELECT/DDL query.
    pub fn query(&self, query: &str) -> query::Query {
        query::Query::new(self, query)
    }

    /// Enables or disables [`Row`] data types validation against the database schema
    /// at the cost of performance. Validation is enabled by default, and in this mode,
    /// the client will use `RowBinaryWithNamesAndTypes` format.
    ///
    /// If you are looking to maximize performance, you could disable validation using this method.
    /// When validation is disabled, the client switches to `RowBinary` format usage instead.
    ///
    /// The downside with plain `RowBinary` is that instead of clearer error messages,
    /// a mismatch between [`Row`] and database schema will result
    /// in a [`error::Error::NotEnoughData`] error without specific details.
    ///
    /// However, depending on the dataset, there might be x1.1 to x3 performance improvement,
    /// but that highly depends on the shape and volume of the dataset.
    ///
    /// It is always recommended to measure the performance impact of validation
    /// in your specific use case. Additionally, writing smoke tests to ensure that
    /// the row types match the ClickHouse schema is highly recommended,
    /// if you plan to disable validation in your application.
    ///
    /// # Note: Mocking
    /// When using [`test::Mock`] with the `test-util` feature, validation is forced off.
    ///
    /// This applies either when using [`Client::with_mock()`], or [`Client::with_url()`]
    /// with a URL from [`test::Mock::url()`].
    ///
    /// As of writing, the mocking facilities are unable to generate the `RowBinaryWithNamesAndTypes`
    /// header required for validation to function.
    pub fn with_validation(mut self, enabled: bool) -> Self {
        self.validation = enabled;
        self
    }

    /// Checks connectivity to the ClickHouse server.
    ///
    /// Executes `SELECT 1` and discards the result. Returns `Ok(())` if the
    /// server responds successfully, or an error if the connection fails or the
    /// server returns an exception.
    ///
    /// Works with all ClickHouse deployments including those behind HTTP proxies
    /// that may not forward the `/ping` endpoint.
    pub async fn ping(&self) -> Result<()> {
        self.query("SELECT 1").execute().await
    }

    /// Returns version information for the connected ClickHouse server.
    ///
    /// Executes `SELECT version(), timezone()` and parses the result into a
    /// [`ServerVersion`]. The version string is expected in the format returned
    /// by ClickHouse: `"major.minor.patch.revision"` (e.g. `"24.3.1.123"`).
    ///
    /// `display_name` is always `None` for the HTTP transport -- the server
    /// display name is only available via the native TCP handshake.
    ///
    /// # Errors
    ///
    /// Returns an error if the server is unreachable, the query fails, or the
    /// version string cannot be parsed.
    pub async fn server_version(&self) -> Result<ServerVersion> {
        let (version_str, timezone): (String, String) = self
            .query("SELECT version(), timezone()")
            .fetch_one()
            .await?;

        // Parse "major.minor.patch.revision" -- ClickHouse always emits all
        // four components. Any missing component defaults to 0 so that future
        // format changes degrade gracefully rather than returning an error.
        let mut parts = version_str.splitn(4, '.');
        let major = parts.next().unwrap_or("0").parse::<u64>().unwrap_or(0);
        let minor = parts.next().unwrap_or("0").parse::<u64>().unwrap_or(0);
        let patch = parts.next().unwrap_or("0").parse::<u64>().unwrap_or(0);
        let revision = parts.next().unwrap_or("0").parse::<u64>().unwrap_or(0);

        Ok(ServerVersion {
            name: "ClickHouse".to_string(),
            major,
            minor,
            patch,
            revision,
            timezone: Some(timezone),
            display_name: None,
        })
    }

    /// Clear table metadata that was previously received and cached.
    ///
    /// [`Insert`][crate::insert::Insert] uses cached metadata when sending data with validation.
    /// If the table schema changes, this metadata needs to re-fetched.
    ///
    /// This method clears the metadata cache, causing future insert queries to re-fetch metadata.
    /// This applies to all cloned instances of this `Client` (using the same URL and database)
    /// as well.
    ///
    /// This may need to wait to acquire a lock if a query is concurrently writing into the cache.
    ///
    /// Cancel-safe.
    pub async fn clear_cached_metadata(&self) {
        self.insert_metadata_cache.0.write().await.clear();
    }

    /// Used internally to check if the validation mode is enabled,
    /// as it takes into account the `test-util` feature flag.
    #[inline]
    pub(crate) fn get_validation(&self) -> bool {
        #[cfg(feature = "test-util")]
        if self.mocked {
            return false;
        }
        self.validation
    }

    pub(crate) fn set_roles(&mut self, roles: impl IntoIterator<Item = impl Into<String>>) {
        self.clear_roles();
        self.roles.extend(roles.into_iter().map(Into::into));
    }

    #[inline]
    pub(crate) fn clear_roles(&mut self) {
        // Make sure we overwrite any role manually set by the user via `with_option()`.
        self.options.remove(settings::ROLE);
        self.roles.clear();
    }

    /// Use a mock server for testing purposes.
    ///
    /// # Note
    ///
    /// The client will always use `RowBinary` format instead of `RowBinaryWithNamesAndTypes`,
    /// as otherwise it'd be required to provide RBWNAT header in the mocks,
    /// which is pointless in that kind of tests.
    #[cfg(feature = "test-util")]
    pub fn with_mock(mut self, mock: &test::Mock) -> Self {
        self.urls = vec![mock.real_url().to_string()];
        self.mocked = true;
        self
    }

    /// Pick the next URL from the round-robin list.
    ///
    /// If only one URL is configured the counter is never incremented -- no
    /// unnecessary atomic write on the hot path. If no URL has been configured
    /// (default-constructed client) an empty string is returned, matching the
    /// original behaviour of the unset `url: String` field.
    #[inline]
    pub(crate) fn pick_url(&self) -> &str {
        match self.urls.len() {
            0 => "",
            1 => &self.urls[0],
            n => {
                // Relaxed ordering is fine here: we only need the counter to
                // advance monotonically across calls; there is no dependent
                // memory that needs to be synchronised alongside this load.
                let idx = self.next_url_index.fetch_add(1, Ordering::Relaxed);
                &self.urls[idx % n]
            }
        }
    }

    async fn get_insert_metadata(&self, table_name: &str) -> Result<Arc<InsertMetadata>> {
        {
            let read_lock = self.insert_metadata_cache.0.read().await;

            // FIXME: `table_name` is not necessarily fully qualified here
            if let Some(metadata) = read_lock.get(table_name) {
                return Ok(metadata.clone());
            }
        }

        // TODO: should it be moved to a cold function?
        let mut write_lock = self.insert_metadata_cache.0.write().await;
        let db = match self.database {
            Some(ref db) => db,
            None => "default",
        };

        let mut columns_cursor = self
            .query(&row_insert_metadata_query(db, table_name))
            .fetch::<(String, String, String)>()?;

        let mut columns = Vec::new();
        let mut column_default_kinds = Vec::new();
        let mut column_lookup = rustc_hash::FxHashMap::default();

        while let Some((name, type_, default_kind)) = columns_cursor.next().await? {
            let data_type = DataTypeNode::new(&type_)?;
            let default_kind = default_kind.parse::<ColumnDefaultKind>()?;

            column_lookup.insert(name.clone(), columns.len());

            columns.push(Column { name, data_type });

            column_default_kinds.push(default_kind);
        }

        let metadata = Arc::new(InsertMetadata {
            row_metadata: RowMetadata {
                columns,
                access_type: AccessType::WithSeqAccess, // ignored on insert
            },
            column_default_kinds,
            column_lookup,
        });

        write_lock.insert(table_name.to_string(), metadata.clone());
        Ok(metadata)
    }
}

mod formats {
    pub(crate) const ROW_BINARY: &str = "RowBinary";
    pub(crate) const ROW_BINARY_WITH_NAMES_AND_TYPES: &str = "RowBinaryWithNamesAndTypes";
}

mod settings {
    pub(crate) const DATABASE: &str = "database";
    pub(crate) const DEFAULT_FORMAT: &str = "default_format";
    pub(crate) const COMPRESS: &str = "compress";
    pub(crate) const DECOMPRESS: &str = "decompress";
    pub(crate) const ROLE: &str = "role";
    pub(crate) const QUERY: &str = "query";
}

/// This is a private API exported only for internal purposes.
/// Do not use it in your code directly, it doesn't follow semver.
#[doc(hidden)]
pub mod _priv {
    pub use crate::row::RowKind;

    #[cfg(feature = "lz4")]
    pub fn lz4_compress(uncompressed: &[u8]) -> super::Result<bytes::Bytes> {
        crate::compression::lz4::compress(uncompressed)
    }

    // Also needed by `it::insert::cache_row_metadata()`
    pub fn row_insert_metadata_query(db: &str, table: &str) -> String {
        let mut out = "SELECT \
            name, \
            type, \
            default_kind \
         FROM system.columns \
         WHERE database = "
            .to_string();

        crate::sql::escape::string(db, &mut out).unwrap();

        out.push_str(" AND table = ");

        crate::sql::escape::string(table, &mut out).unwrap();

        out
    }
}

#[cfg(test)]
mod client_tests {
    use crate::_priv::RowKind;
    use crate::row_metadata::{AccessType, RowMetadata};
    use crate::{Authentication, Client, Row};
    use clickhouse_types::{Column, DataTypeNode};

    #[test]
    fn it_can_use_credentials_auth() {
        assert_eq!(
            Client::default()
                .with_user("bob")
                .with_password("secret")
                .authentication,
            Authentication::Credentials {
                user: Some("bob".into()),
                password: Some("secret".into()),
            }
        );
    }

    #[test]
    fn it_can_use_credentials_auth_user_only() {
        assert_eq!(
            Client::default().with_user("alice").authentication,
            Authentication::Credentials {
                user: Some("alice".into()),
                password: None,
            }
        );
    }

    #[test]
    fn it_can_use_credentials_auth_password_only() {
        assert_eq!(
            Client::default().with_password("secret").authentication,
            Authentication::Credentials {
                user: None,
                password: Some("secret".into()),
            }
        );
    }

    #[test]
    fn it_can_override_credentials_auth() {
        assert_eq!(
            Client::default()
                .with_user("bob")
                .with_password("secret")
                .with_user("alice")
                .with_password("something_else")
                .authentication,
            Authentication::Credentials {
                user: Some("alice".into()),
                password: Some("something_else".into()),
            }
        );
    }

    #[test]
    fn it_can_use_jwt_auth() {
        assert_eq!(
            Client::default().with_access_token("my_jwt").authentication,
            Authentication::Jwt {
                access_token: "my_jwt".into(),
            }
        );
    }

    #[test]
    fn it_can_override_jwt_auth() {
        assert_eq!(
            Client::default()
                .with_access_token("my_jwt")
                .with_access_token("my_jwt_2")
                .authentication,
            Authentication::Jwt {
                access_token: "my_jwt_2".into(),
            }
        );
    }

    #[test]
    #[should_panic(expected = "`access_token` cannot be set together with `user` or `password`")]
    fn it_cannot_use_jwt_after_with_user() {
        let _ = Client::default()
            .with_user("bob")
            .with_access_token("my_jwt");
    }

    #[test]
    #[should_panic(expected = "`access_token` cannot be set together with `user` or `password`")]
    fn it_cannot_use_jwt_after_with_password() {
        let _ = Client::default()
            .with_password("secret")
            .with_access_token("my_jwt");
    }

    #[test]
    #[should_panic(expected = "`access_token` cannot be set together with `user` or `password`")]
    fn it_cannot_use_jwt_after_both_with_user_and_with_password() {
        let _ = Client::default()
            .with_user("alice")
            .with_password("secret")
            .with_access_token("my_jwt");
    }

    #[test]
    #[should_panic(expected = "`user` cannot be set together with `access_token`")]
    fn it_cannot_use_with_user_after_jwt() {
        let _ = Client::default()
            .with_access_token("my_jwt")
            .with_user("alice");
    }

    #[test]
    #[should_panic(expected = "`password` cannot be set together with `access_token`")]
    fn it_cannot_use_with_password_after_jwt() {
        let _ = Client::default()
            .with_access_token("my_jwt")
            .with_password("secret");
    }

    #[test]
    fn it_sets_validation_mode() {
        let client = Client::default();
        assert!(client.validation);
        let client = client.with_validation(false);
        assert!(!client.validation);
        let client = client.with_validation(true);
        assert!(client.validation);
    }

    #[derive(Debug, Clone, PartialEq)]
    struct SystemRolesRow {
        name: String,
        id: uuid::Uuid,
        storage: String,
    }

    impl SystemRolesRow {
        fn columns() -> Vec<Column> {
            vec![
                Column::new("name".to_string(), DataTypeNode::String),
                Column::new("id".to_string(), DataTypeNode::UUID),
                Column::new("storage".to_string(), DataTypeNode::String),
            ]
        }
    }

    impl Row for SystemRolesRow {
        const NAME: &'static str = "SystemRolesRow";
        const KIND: RowKind = RowKind::Struct;
        const COLUMN_COUNT: usize = 3;
        const COLUMN_NAMES: &'static [&'static str] = &["name", "id", "storage"];
        type Value<'a> = SystemRolesRow;
    }

    #[test]
    fn get_row_metadata() {
        let metadata =
            RowMetadata::new_for_cursor::<SystemRolesRow>(SystemRolesRow::columns()).unwrap();
        assert_eq!(metadata.columns, SystemRolesRow::columns());
        assert_eq!(metadata.access_type, AccessType::WithSeqAccess);

        // the order is shuffled => map access
        let columns = vec![
            Column::new("id".to_string(), DataTypeNode::UUID),
            Column::new("storage".to_string(), DataTypeNode::String),
            Column::new("name".to_string(), DataTypeNode::String),
        ];
        let metadata = RowMetadata::new_for_cursor::<SystemRolesRow>(columns.clone()).unwrap();
        assert_eq!(metadata.columns, columns);
        assert_eq!(
            metadata.access_type,
            AccessType::WithMapAccess(vec![1, 2, 0]) // see COLUMN_NAMES above
        );
    }

    #[test]
    fn it_does_follow_previous_configuration() {
        let client = Client::default().with_option("async_insert", "1");
        assert_eq!(client.options, client.clone().options,);
    }

    #[test]
    fn it_does_not_follow_future_configuration() {
        let client = Client::default();
        let client_clone = client.clone();
        let client = client.with_option("async_insert", "1");
        assert_ne!(client.options, client_clone.options,);
    }

    #[test]
    fn it_gets_and_sets_options() {
        let mut client = Client::default();

        assert_eq!(client.set_option("foo", "foo"), None);
        assert_eq!(client.set_option("bar", "bar"), None);

        assert_eq!(client.get_option("foo"), Some("foo"));
        assert_eq!(client.get_option("bar"), Some("bar"));
        assert_eq!(client.get_option("baz"), None);

        assert_eq!(client.set_option("foo", "foo_2"), Some("foo".to_string()));
        assert_eq!(client.set_option("bar", "bar_2"), Some("bar".to_string()));
    }
}
