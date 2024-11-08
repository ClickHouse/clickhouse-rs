use std::marker::PhantomData;

use bytes::Bytes;
use futures::TryStreamExt;
use serde::Deserialize;

use crate::{
    bytes_ext::BytesExt,
    error::{Error, Result},
    response::{Chunks, Response, ResponseFuture},
    rowbinary,
};

// === RawCursor ===

pub struct RawCursor(RawCursorInner);

enum RawCursorInner {
    Waiting(ResponseFuture),
    Loading(RawCursorLoading),
}

struct RawCursorLoading {
    chunks: Chunks,
    net_size: u64,
    data_size: u64,
}

impl RawCursor {
    pub(crate) fn new(response: Response) -> Self {
        Self(RawCursorInner::Waiting(response.into_future()))
    }

    pub async fn next(&mut self) -> Result<Option<Bytes>> {
        if matches!(self.0, RawCursorInner::Waiting(_)) {
            self.resolve().await?;
        }

        let state = match &mut self.0 {
            RawCursorInner::Loading(state) => state,
            RawCursorInner::Waiting(_) => unreachable!(),
        };

        match state.chunks.try_next().await {
            Ok(Some(chunk)) => {
                state.net_size += chunk.net_size as u64;
                state.data_size += chunk.data.len() as u64;
                Ok(Some(chunk.data))
            }
            Ok(None) => Ok(None),
            Err(err) => Err(err),
        }
    }

    async fn resolve(&mut self) -> Result<()> {
        if let RawCursorInner::Waiting(future) = &mut self.0 {
            let chunks = future.await;
            self.0 = RawCursorInner::Loading(RawCursorLoading {
                chunks: chunks?,
                net_size: 0,
                data_size: 0,
            });
        }
        Ok(())
    }

    fn received_bytes(&self) -> u64 {
        match &self.0 {
            RawCursorInner::Waiting(_) => 0,
            RawCursorInner::Loading(state) => state.net_size,
        }
    }

    fn decoded_bytes(&self) -> u64 {
        match &self.0 {
            RawCursorInner::Waiting(_) => 0,
            RawCursorInner::Loading(state) => state.data_size,
        }
    }
}

// XXX: it was a workaround for https://github.com/rust-lang/rust/issues/51132,
//      but introduced #24 and must be fixed.
fn workaround_51132<'a, T: ?Sized>(ptr: &T) -> &'a T {
    // SAFETY: actually, it leads to unsoundness, see #24
    unsafe { &*(ptr as *const T) }
}

// === RowCursor ===

/// A cursor that emits rows.
#[must_use]
pub struct RowCursor<T> {
    raw: RawCursor,
    bytes: BytesExt,
    _marker: PhantomData<T>,
}

impl<T> RowCursor<T> {
    pub(crate) fn new(response: Response) -> Self {
        Self {
            raw: RawCursor::new(response),
            bytes: BytesExt::default(),
            _marker: PhantomData,
        }
    }

    /// Emits the next row.
    ///
    /// The result is unspecified if it's called after `Err` is returned.
    pub async fn next<'a, 'b: 'a>(&'a mut self) -> Result<Option<T>>
    where
        T: Deserialize<'b>,
    {
        loop {
            let mut slice = workaround_51132(self.bytes.slice());

            match rowbinary::deserialize_from(&mut slice) {
                Ok(value) => {
                    self.bytes.set_remaining(slice.len());
                    return Ok(Some(value));
                }
                Err(Error::NotEnoughData) => {}
                Err(err) => return Err(err),
            }

            match self.raw.next().await? {
                Some(chunk) => self.bytes.extend(chunk),
                None => return Ok(None),
            }
        }
    }

    /// Returns the total size in bytes received from the CH server since
    /// the cursor was created.
    ///
    /// This method counts only size without HTTP headers for now.
    /// It can be changed in the future without notice.
    #[inline]
    pub fn received_bytes(&self) -> u64 {
        self.raw.received_bytes()
    }

    /// Returns the total size in bytes decompressed since the cursor was
    /// created.
    #[inline]
    pub fn decoded_bytes(&self) -> u64 {
        self.raw.decoded_bytes()
    }
}

// === JsonCursor ===

#[cfg(feature = "watch")]
pub(crate) struct JsonCursor<T> {
    raw: RawCursor,
    bytes: BytesExt,
    line: String,
    _marker: PhantomData<T>,
}

// We use `JSONEachRowWithProgress` to avoid infinite HTTP connections.
// See https://github.com/ClickHouse/ClickHouse/issues/22996 for details.
#[cfg(feature = "watch")]
#[derive(Deserialize)]
#[serde(rename_all = "lowercase")]
enum JsonRow<T> {
    Row(T),
    Progress {},
}

#[cfg(feature = "watch")]
impl<T> JsonCursor<T> {
    const INITIAL_BUFFER_SIZE: usize = 1024;

    pub(crate) fn new(response: Response) -> Self {
        Self {
            raw: RawCursor::new(response),
            bytes: BytesExt::default(),
            line: String::with_capacity(Self::INITIAL_BUFFER_SIZE),
            _marker: PhantomData,
        }
    }

    pub(crate) async fn next<'a, 'b: 'a>(&'a mut self) -> Result<Option<T>>
    where
        T: Deserialize<'b>,
    {
        use bytes::Buf;
        use std::io::BufRead;

        loop {
            self.line.clear();

            let read = match self.bytes.slice().reader().read_line(&mut self.line) {
                Ok(read) => read,
                Err(err) => return Err(Error::Custom(err.to_string())),
            };

            if let Some(line) = self.line.strip_suffix('\n') {
                self.bytes.advance(read);

                match serde_json::from_str(workaround_51132(line)) {
                    Ok(JsonRow::Row(value)) => return Ok(Some(value)),
                    Ok(JsonRow::Progress { .. }) => continue,
                    Err(err) => return Err(Error::BadResponse(err.to_string())),
                }
            }

            match self.raw.next().await? {
                Some(chunk) => self.bytes.extend(chunk),
                None => return Ok(None),
            }
        }
    }
}
