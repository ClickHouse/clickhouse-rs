use std::fmt::Write;

use serde::Deserialize;
use sha1::{Digest, Sha1};

use crate::{
    error::Result,
    introspection::Reflection,
    query,
    sql_builder::{Bind, SqlBuilder},
    Client,
};

pub struct Watch {
    client: Client,
    sql: SqlBuilder,
    limit: Option<usize>,
}

impl Watch {
    pub(crate) fn new(client: &Client, template: &str) -> Self {
        Self {
            client: client.clone(),
            sql: SqlBuilder::new(template),
            limit: None,
        }
    }

    pub fn bind(mut self, value: impl Bind) -> Self {
        self.sql.bind_arg(value);
        self
    }

    // TODO: `timeout()`.

    pub fn limit(mut self, limit: impl Into<Option<usize>>) -> Self {
        self.limit = limit.into();
        self
    }

    pub fn rows<T: Reflection>(self) -> Result<RowCursor<T>> {
        Ok(RowCursor(self.cursor(false)?))
    }

    pub fn events(self) -> Result<EventCursor> {
        Ok(EventCursor(self.cursor(true)?))
    }

    // TODO: `groups()` for `(Version, &[T])`.

    fn cursor<T: Reflection>(self, only_events: bool) -> Result<RawCursor<T>> {
        let sql = self.sql.finish()?;
        let view = make_live_view_name(&sql);

        Ok(RawCursor::Preparing {
            client: self.client,
            sql,
            view,
            limit: self.limit,
            only_events,
        })
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
        sql: String,
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
            let create_sql = format!(
                "CREATE LIVE VIEW IF NOT EXISTS {} WITH TIMEOUT AS {}",
                view, sql
            );
            client.query(&create_sql).execute().await?;

            let events = if *only_events { " EVENTS" } else { "" };
            let watch_sql = match limit {
                Some(limit) => format!("WATCH {}{} LIMIT {}", view, events, limit),
                None => format!("WATCH {}{}", view, events),
            };

            let cursor = client.query(&watch_sql).rows()?;
            *self = RawCursor::Fetching(cursor);
        }

        match self {
            RawCursor::Preparing { .. } => unreachable!(),
            RawCursor::Fetching(cursor) => Ok(cursor.next().await?),
        }
    }
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
