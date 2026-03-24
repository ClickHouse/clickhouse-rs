//! [`UnifiedQuery`] — query builder that dispatches to either HTTP or native transport.
//!
//! Returned by [`crate::unified::UnifiedClient::query`].

use crate::error::Result;
use crate::row::{RowOwned, RowRead};
use crate::unified_cursor::UnifiedCursor;

// ---------------------------------------------------------------------------
// Inner enum
// ---------------------------------------------------------------------------

enum QueryInner {
    Http(crate::query::Query),
    #[cfg(feature = "native-transport")]
    Native(crate::native::NativeQuery),
}

// ---------------------------------------------------------------------------
// UnifiedQuery
// ---------------------------------------------------------------------------

/// A query builder returned by [`crate::unified::UnifiedClient::query`].
///
/// Dispatches `execute`, `fetch_all`, `fetch_one`, and `fetch_optional` to
/// whichever transport is active.
///
/// # Parameter binding
///
/// [`UnifiedQuery::bind`] accepts any [`std::fmt::Display`] value. For the
/// HTTP transport the display string is forwarded as a serialised argument;
/// for the native transport it is substituted directly into the SQL string.
#[must_use]
pub struct UnifiedQuery {
    inner: QueryInner,
}

impl UnifiedQuery {
    pub(crate) fn from_http(query: crate::query::Query) -> Self {
        Self {
            inner: QueryInner::Http(query),
        }
    }

    #[cfg(feature = "native-transport")]
    pub(crate) fn from_native(query: crate::native::NativeQuery) -> Self {
        Self {
            inner: QueryInner::Native(query),
        }
    }

    // -----------------------------------------------------------------------
    // Builder methods
    // -----------------------------------------------------------------------

    /// Bind a ClickHouse named parameter using the `{name:Type}` placeholder syntax.
    ///
    /// For the HTTP transport, delegates to [`crate::query::Query::param`].
    /// For the native transport, sends the parameter as a `param_<name>`
    /// query setting (the same mechanism used by the Go client).
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use clickhouse::unified::UnifiedClient;
    /// # async fn example() -> clickhouse::error::Result<()> {
    /// let client = UnifiedClient::http().with_url("http://localhost:8123").build();
    /// let rows = client
    ///     .query("SELECT * FROM t WHERE id = {id:UInt32}")
    ///     .param("id", 42u32)
    ///     .fetch_all::<(u32,)>()
    ///     .await?;
    /// # Ok(()) }
    /// ```
    pub fn param(self, name: &str, value: impl std::fmt::Display) -> Self {
        match self.inner {
            QueryInner::Http(q) => Self {
                inner: QueryInner::Http(q.param(name, value.to_string())),
            },
            #[cfg(feature = "native-transport")]
            QueryInner::Native(q) => Self {
                inner: QueryInner::Native(q.param(name, value)),
            },
        }
    }

    /// Bind the next `?` placeholder in the query to `value`.
    ///
    /// Uses [`std::fmt::Display`] as the common interface across both
    /// transports.  For the HTTP transport the value is converted to a
    /// `String` first (which implements `serde::Serialize`) and passed to the
    /// underlying [`crate::query::Query::bind`].
    pub fn bind(self, value: impl std::fmt::Display) -> Self {
        match self.inner {
            QueryInner::Http(q) => Self {
                inner: QueryInner::Http(q.bind(value.to_string())),
            },
            #[cfg(feature = "native-transport")]
            QueryInner::Native(q) => Self {
                inner: QueryInner::Native(q.bind(value)),
            },
        }
    }

    /// Set a query ID that ClickHouse will associate with this query.
    ///
    /// For the HTTP transport, this sets the `query_id` URL parameter.
    /// For the native transport, the ID is sent in the query packet header.
    ///
    /// If not set, the server generates its own query ID.
    pub fn with_query_id(self, id: impl Into<String>) -> Self {
        let id = id.into();
        match self.inner {
            QueryInner::Http(q) => Self {
                inner: QueryInner::Http(q.with_option("query_id", &id)),
            },
            #[cfg(feature = "native-transport")]
            QueryInner::Native(q) => Self {
                inner: QueryInner::Native(q.with_query_id(id)),
            },
        }
    }

    /// Register a callback invoked for each [`Progress`] packet received from the server.
    ///
    /// Only available when the `native-transport` feature is enabled. For the
    /// HTTP transport this method is a no-op — HTTP responses do not carry
    /// inline Progress packets.
    #[cfg(feature = "native-transport")]
    pub fn with_progress(
        self,
        f: impl Fn(&crate::native::protocol::Progress) + Send + Sync + 'static,
    ) -> Self {
        match self.inner {
            QueryInner::Http(q) => Self {
                inner: QueryInner::Http(q),
            },
            QueryInner::Native(q) => Self {
                inner: QueryInner::Native(q.with_progress(f)),
            },
        }
    }

    /// Register a callback invoked when the server sends a [`ProfileInfo`] packet.
    ///
    /// Only available when the `native-transport` feature is enabled. For the
    /// HTTP transport this method is a no-op — HTTP responses do not carry
    /// inline ProfileInfo packets.
    #[cfg(feature = "native-transport")]
    pub fn with_profile_info(
        self,
        f: impl Fn(&crate::native::protocol::ProfileInfo) + Send + Sync + 'static,
    ) -> Self {
        match self.inner {
            QueryInner::Http(q) => Self {
                inner: QueryInner::Http(q),
            },
            QueryInner::Native(q) => Self {
                inner: QueryInner::Native(q.with_profile_info(f)),
            },
        }
    }

    /// Add per-query settings that override any client-level settings for this
    /// query only.
    ///
    /// For the HTTP transport, each setting is forwarded as a URL parameter
    /// (ClickHouse accepts settings as query parameters on the HTTP interface).
    /// For the native transport, settings are sent in the query packet.
    pub fn with_settings(
        self,
        settings: impl IntoIterator<Item = (impl Into<String>, impl Into<String>)>,
    ) -> Self {
        match self.inner {
            QueryInner::Http(q) => {
                let mut q = q;
                for (k, v) in settings {
                    q = q.with_option(k.into(), v.into());
                }
                Self {
                    inner: QueryInner::Http(q),
                }
            }
            #[cfg(feature = "native-transport")]
            QueryInner::Native(q) => Self {
                inner: QueryInner::Native(q.with_settings(settings)),
            },
        }
    }

    // -----------------------------------------------------------------------
    // Private helpers
    // -----------------------------------------------------------------------

    /// Return a static label for the active transport, used in tracing spans.
    fn transport_name(&self) -> &'static str {
        match &self.inner {
            QueryInner::Http(_) => "http",
            #[cfg(feature = "native-transport")]
            QueryInner::Native(_) => "native",
        }
    }

    // -----------------------------------------------------------------------
    // Terminal methods
    // -----------------------------------------------------------------------

    /// Execute a SELECT query and return a streaming [`UnifiedCursor`].
    ///
    /// The cursor yields one owned row at a time via [`UnifiedCursor::next`].
    /// Use this when the result set may be large and you want to process rows
    /// without buffering them all in memory.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use clickhouse::unified::UnifiedClient;
    /// # use clickhouse::{Row, RowOwned, RowRead};
    /// # use serde::Deserialize;
    /// # async fn example() -> clickhouse::error::Result<()> {
    /// #[derive(Row, Deserialize)]
    /// struct MyRow { id: u64 }
    ///
    /// let client = UnifiedClient::http().with_url("http://localhost:8123").build();
    /// let mut cursor = client.query("SELECT id FROM t").fetch::<MyRow>()?;
    /// while let Some(row) = cursor.next().await? {
    ///     println!("{}", row.id);
    /// }
    /// # Ok(()) }
    /// ```
    pub fn fetch<T>(self) -> Result<UnifiedCursor<T>>
    where
        T: RowOwned + RowRead,
    {
        match self.inner {
            QueryInner::Http(q) => Ok(UnifiedCursor::from_http(q.fetch::<T>()?)),
            #[cfg(feature = "native-transport")]
            QueryInner::Native(q) => Ok(UnifiedCursor::from_native(q.fetch::<T>()?)),
        }
    }

    /// Execute a DDL or non-SELECT statement and discard any results.
    pub async fn execute(self) -> Result<()> {
        #[cfg(feature = "tracing")]
        tracing::debug!(transport = self.transport_name(), "executing query");

        match self.inner {
            QueryInner::Http(q) => q.execute().await,
            #[cfg(feature = "native-transport")]
            QueryInner::Native(q) => q.execute().await,
        }
    }

    /// Execute a SELECT query and collect all rows into a `Vec`.
    pub async fn fetch_all<T>(self) -> Result<Vec<T>>
    where
        T: RowOwned + RowRead,
    {
        #[cfg(feature = "tracing")]
        tracing::debug!(transport = self.transport_name(), "fetching all rows");

        match self.inner {
            QueryInner::Http(q) => q.fetch_all::<T>().await,
            #[cfg(feature = "native-transport")]
            QueryInner::Native(q) => q.fetch_all::<T>().await,
        }
    }

    /// Execute a SELECT query and return exactly one row.
    ///
    /// Returns [`crate::error::Error::RowNotFound`] if the result set is empty.
    pub async fn fetch_one<T>(self) -> Result<T>
    where
        T: RowOwned + RowRead,
    {
        match self.inner {
            QueryInner::Http(q) => q.fetch_one::<T>().await,
            #[cfg(feature = "native-transport")]
            QueryInner::Native(q) => q.fetch_one::<T>().await,
        }
    }

    /// Execute a SELECT query and return at most one row.
    pub async fn fetch_optional<T>(self) -> Result<Option<T>>
    where
        T: RowOwned + RowRead,
    {
        match self.inner {
            QueryInner::Http(q) => q.fetch_optional::<T>().await,
            #[cfg(feature = "native-transport")]
            QueryInner::Native(q) => q.fetch_optional::<T>().await,
        }
    }
}
