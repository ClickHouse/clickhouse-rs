use std::{fmt::Write, time::Duration};

use serde::Deserialize;
use sha1::{Digest, Sha1};

use crate::{
    cursor::JsonCursor,
    error::{Error, Result},
    row::Row,
    sql::{Bind, SqlBuilder},
    Client, Compression,
};

#[must_use]
pub struct Watch<V = Rows> {
    client: Client,
    sql: SqlBuilder,
    refresh: Option<Duration>,
    limit: Option<usize>,
    _kind: V,
}

pub struct Rows;
pub struct Events;

impl<V> Watch<V> {
    pub fn bind(mut self, value: impl Bind) -> Self {
        self.sql.bind_arg(value);
        self
    }

    /// Limits the number of updates after initial one.
    pub fn limit(mut self, limit: impl Into<Option<usize>>) -> Self {
        self.limit = limit.into();
        self
    }

    /// See [docs](https://clickhouse.com/docs/en/sql-reference/statements/create/view#with-refresh-clause).
    ///
    /// Makes sense only for SQL queries (`client.watch("SELECT X")`).
    pub fn refresh(mut self, interval: impl Into<Option<Duration>>) -> Self {
        self.refresh = interval.into();
        self
    }

    // TODO: `groups()` for `(Version, &[T])`.

    fn cursor<T: Row>(mut self, only_events: bool) -> Result<CursorWithInit<T>> {
        self.sql.bind_fields::<T>();
        let sql = self.sql.finish()?;
        let (sql, view) = if is_table_name(&sql) {
            (None, sql)
        } else {
            let view = make_live_view_name(&sql);
            (Some(sql), view)
        };

        let params = WatchParams {
            sql,
            view,
            refresh: self.refresh,
            limit: self.limit,
            only_events,
        };

        Ok(CursorWithInit::Preparing(self.client, params))
    }
}

impl Watch<Rows> {
    pub(crate) fn new(client: &Client, template: &str) -> Self {
        let client = client
            .clone()
            // TODO: check again.
            // It seems `WATCH` and compression are incompatible.
            .with_compression(Compression::None)
            .with_option("max_execution_time", "0")
            .with_option("allow_experimental_live_view", "1")
            .with_option("output_format_json_quote_64bit_integers", "0");

        Self {
            client,
            sql: SqlBuilder::new(template),
            refresh: None,
            limit: None,
            _kind: Rows,
        }
    }

    pub fn only_events(self) -> Watch<Events> {
        Watch {
            client: self.client,
            sql: self.sql,
            refresh: self.refresh,
            limit: self.limit,
            _kind: Events,
        }
    }

    /// # Panics
    /// Panics if `T` are rows without specified names.
    /// Only structs are supported in this API.
    #[track_caller]
    pub fn fetch<T: Row>(self) -> Result<RowCursor<T>> {
        assert!(
            !T::COLUMN_NAMES.is_empty(),
            "only structs are supported in the watch API"
        );

        Ok(RowCursor(self.cursor(false)?))
    }

    pub async fn fetch_one<T>(self) -> Result<(Version, T)>
    where
        T: Row + for<'b> Deserialize<'b>,
    {
        match self.limit(1).fetch()?.next().await {
            Ok(Some(row)) => Ok(row),
            Ok(None) => Err(Error::RowNotFound),
            Err(err) => Err(err),
        }
    }
}

impl Watch<Events> {
    pub fn fetch(self) -> Result<EventCursor> {
        Ok(EventCursor(self.cursor(true)?))
    }

    pub async fn fetch_one(self) -> Result<Version> {
        match self.limit(1).fetch()?.next().await {
            Ok(Some(row)) => Ok(row),
            Ok(None) => Err(Error::RowNotFound),
            Err(err) => Err(err),
        }
    }
}

pub type Version = u64; // TODO: NonZeroU64

// === EventCursor ===

/// A cursor that emits only versions.
pub struct EventCursor(CursorWithInit<EventPayload>);

#[derive(Deserialize)]
struct EventPayload {
    version: Version,
}

impl Row for EventPayload {
    const COLUMN_NAMES: &'static [&'static str] = &[];
}

impl EventCursor {
    /// Emits the next version.
    pub async fn next(&mut self) -> Result<Option<Version>> {
        Ok(self.0.next().await?.map(|payload| payload.version))
    }
}

// === RowCursor ===

/// A cursor that emits `(Version, T)`.
pub struct RowCursor<T>(CursorWithInit<RowPayload<T>>);

#[derive(Deserialize)]
struct RowPayload<T> {
    _version: Version,
    #[serde(flatten)]
    data: T,
}

impl<T: Row> Row for RowPayload<T> {
    const COLUMN_NAMES: &'static [&'static str] = T::COLUMN_NAMES;
}

impl<T> RowCursor<T> {
    /// Emits the next row.
    pub async fn next<'a, 'b: 'a>(&'a mut self) -> Result<Option<(Version, T)>>
    where
        T: Deserialize<'b> + Row,
    {
        Ok(self
            .0
            .next()
            .await?
            .map(|payload| (payload._version, payload.data)))
    }
}

// === CursorWithInit ===

#[allow(clippy::large_enum_variant)]
enum CursorWithInit<T> {
    Preparing(Client, WatchParams),
    Fetching(JsonCursor<T>),
}

struct WatchParams {
    sql: Option<String>,
    view: String,
    refresh: Option<Duration>,
    limit: Option<usize>,
    only_events: bool,
}

impl<T> CursorWithInit<T> {
    async fn next<'a, 'b: 'a>(&'a mut self) -> Result<Option<T>>
    where
        T: Deserialize<'b>,
    {
        if let Self::Preparing(client, params) = self {
            let cursor = init_cursor(client, params).await?;
            *self = Self::Fetching(cursor);
        }

        match self {
            Self::Fetching(cursor) => cursor.next().await,
            Self::Preparing(..) => unreachable!(),
        }
    }
}

#[cold]
async fn init_cursor<T>(client: &Client, params: &WatchParams) -> Result<JsonCursor<T>> {
    if let Some(sql) = &params.sql {
        let refresh_sql = params
            .refresh
            .map_or_else(String::new, |d| format!(" REFRESH {}", d.as_secs()));

        let create_sql = format!(
            "CREATE LIVE VIEW IF NOT EXISTS {}{} AS {}",
            params.view, refresh_sql, sql
        );

        client.query(&create_sql).execute().await?;
    }

    let events = if params.only_events { " EVENTS" } else { "" };
    let mut watch_sql = format!("WATCH {}{}", params.view, events);

    if let Some(limit) = params.limit {
        let _ = write!(&mut watch_sql, " LIMIT {limit}");
    }

    watch_sql.push_str(" FORMAT JSONEachRowWithProgress");

    let response = client.query(&watch_sql).do_execute(true)?;
    Ok(JsonCursor::new(response))
}

fn is_table_name(sql: &str) -> bool {
    // TODO: support quoted identifiers.
    sql.split_ascii_whitespace().take(2).count() == 1
}

fn make_live_view_name(sql: &str) -> String {
    let mut hasher = Sha1::new();
    hasher.update(sql.as_bytes());
    let result = hasher.finalize();

    let mut name = String::with_capacity(40);
    for word in &result[..] {
        let _ = write!(&mut name, "{word:02x}");
    }

    format!("lv_{name}")
}

#[test]
fn it_makes_live_view_name() {
    let a = make_live_view_name("SELECT 1");
    let b = make_live_view_name("SELECT 2");

    assert_ne!(a, b);
    assert_eq!(a.len(), 3 + 40);
    assert_eq!(b.len(), 3 + 40);
}
