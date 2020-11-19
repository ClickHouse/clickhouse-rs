use std::fmt::Write;

use serde::Deserialize;
use sha1::{Digest, Sha1};

use crate::{
    error::{Error, Result},
    introspection::Reflection,
    query,
    sql_builder::{Bind, SqlBuilder},
    Client, Compression,
};

pub struct Watch<V = Rows> {
    client: Client,
    sql: SqlBuilder,
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

    // TODO: `timeout()`.

    pub fn limit(mut self, limit: impl Into<Option<usize>>) -> Self {
        self.limit = limit.into();
        self
    }

    // TODO: `groups()` for `(Version, &[T])`.

    fn cursor<T: Reflection>(self, only_events: bool) -> Result<RawCursor<T>> {
        let sql = self.sql.finish()?;
        let (sql, view) = if is_table_name(&sql) {
            (None, sql)
        } else {
            let view = make_live_view_name(&sql);
            (Some(sql), view)
        };

        Ok(RawCursor::Preparing {
            client: self.client,
            sql,
            view,
            limit: self.limit,
            only_events,
        })
    }
}

impl Watch<Rows> {
    pub(crate) fn new(client: &Client, template: &str) -> Self {
        Self {
            client: client.clone(),
            sql: SqlBuilder::new(template),
            limit: None,
            _kind: Rows,
        }
    }

    #[deprecated(since = "0.4.0", note = "use `Watch::fetch()` instead")]
    pub fn rows<T: Reflection>(self) -> Result<RowCursor<T>> {
        self.fetch()
    }

    #[deprecated(since = "0.4.0", note = "use `Watch::only_events().fetch()` instead")]
    pub fn events(self) -> Result<EventCursor> {
        self.only_events().fetch()
    }

    pub fn only_events(self) -> Watch<Events> {
        Watch {
            client: self.client,
            sql: self.sql,
            limit: self.limit,
            _kind: Events,
        }
    }

    pub fn fetch<T: Reflection>(self) -> Result<RowCursor<T>> {
        Ok(RowCursor(self.cursor(false)?))
    }

    pub async fn fetch_one<T>(self) -> Result<(Version, T)>
    where
        T: Reflection + for<'b> Deserialize<'b>,
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

pub struct EventCursor(RawCursor<()>);

impl EventCursor {
    pub async fn next(&mut self) -> Result<Option<Version>> {
        Ok(self.0.next().await?.map(|(_, version)| version))
    }
}

pub struct RowCursor<T>(RawCursor<T>);

impl<T> RowCursor<T> {
    pub async fn next<'a, 'b: 'a>(&'a mut self) -> Result<Option<(Version, T)>>
    where
        T: Deserialize<'b> + Reflection,
    {
        Ok(self.0.next().await?.map(|(row, version)| (version, row)))
    }
}

enum RawCursor<T> {
    Preparing {
        client: Client,
        sql: Option<String>,
        view: String,
        limit: Option<usize>,
        only_events: bool,
    },
    Fetching(query::RowCursor<(T, Version)>),
}

impl<T> RawCursor<T> {
    pub async fn next<'a, 'b: 'a>(&'a mut self) -> Result<Option<(T, Version)>>
    where
        T: Deserialize<'b> + Reflection,
    {
        if let RawCursor::Preparing {
            client,
            sql,
            view,
            limit,
            only_events,
        } = self
        {
            // It seems `WATCH` and compression are incompatible.
            if client.compression != Compression::None {
                take_mut::take(client, |client| client.with_compression(Compression::None))
            }

            if let Some(sql) = sql {
                let create_sql = format!(
                    "CREATE LIVE VIEW IF NOT EXISTS {} WITH TIMEOUT AS {}",
                    view, sql
                );

                client.query(&create_sql).execute().await?;
            }

            let events = if *only_events { " EVENTS" } else { "" };
            let watch_sql = match limit {
                Some(limit) => format!("WATCH {}{} LIMIT {}", view, events, limit),
                None => format!("WATCH {}{}", view, events),
            };

            let cursor = client.query(&watch_sql).fetch()?;
            *self = RawCursor::Fetching(cursor);
        }

        match self {
            RawCursor::Preparing { .. } => unreachable!(),
            RawCursor::Fetching(cursor) => Ok(cursor.next().await?),
        }
    }
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
        let _ = write!(&mut name, "{:02x}", word);
    }

    format!("lv_{}", name)
}

#[test]
fn it_makes_live_view_name() {
    let a = make_live_view_name("SELECT 1");
    let b = make_live_view_name("SELECT 2");

    assert_ne!(a, b);
    assert_eq!(a.len(), 3 + 40);
    assert_eq!(b.len(), 3 + 40);
}
