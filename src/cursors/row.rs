#[cfg(feature = "futures03")]
use crate::RowOwned;
use crate::row_metadata::RowMetadata;
use crate::{
    RowRead,
    bytes_ext::BytesExt,
    cursors::RawCursor,
    error::{Error, Result},
    query_summary::QuerySummary,
    response::Response,
    rowbinary,
};
use bytes::Buf;
use clickhouse_types::error::TypesError;
use clickhouse_types::parse_rbwnat_columns_header;
use std::marker::PhantomData;
use std::pin::Pin;
use std::task::{Context, Poll, ready};

/// A cursor that emits rows deserialized as structures from RowBinary.
#[must_use]
pub struct RowCursor<T> {
    raw: RawCursor,
    bytes: BytesExt,
    validation: bool,
    /// [`None`] until the first call to [`RowCursor::next()`],
    /// as [`RowCursor::new`] is not `async`, so it loads lazily.
    row_metadata: Option<RowMetadata>,
    _marker: PhantomData<fn() -> T>,
}

impl<T> RowCursor<T> {
    pub(crate) fn new(response: Response, validation: bool) -> Self {
        Self {
            _marker: PhantomData,
            raw: RawCursor::new(response),
            bytes: BytesExt::default(),
            row_metadata: None,
            validation,
        }
    }

    #[cold]
    #[inline(never)]
    fn poll_read_columns(&mut self, cx: &mut Context<'_>) -> Poll<Result<()>>
    where
        T: RowRead,
    {
        loop {
            if self.bytes.remaining() > 0 {
                let mut slice = self.bytes.slice();

                // Can't pass `&mut self.bytes` because the parsing may partially consume the buffer
                match parse_rbwnat_columns_header(&mut slice) {
                    Ok(columns) if !columns.is_empty() => {
                        self.bytes.set_remaining(slice.len());
                        let row_metadata = RowMetadata::new_for_cursor::<T>(columns)?;
                        self.row_metadata = Some(row_metadata);
                        return Poll::Ready(Ok(()));
                    }
                    Ok(_) => {
                        // This does not panic, as it could be a network issue
                        // or a malformed response from the server or LB,
                        // and a simple retry might help in certain cases.
                        return Poll::Ready(Err(Error::BadResponse(
                            "Expected at least one column in the header".to_string(),
                        )));
                    }
                    Err(TypesError::NotEnoughData(_)) => {}
                    Err(err) => {
                        return Poll::Ready(Err(Error::InvalidColumnsHeader(err.into())));
                    }
                }
            }
            match ready!(self.raw.poll_next(cx))? {
                Some(chunk) => self.bytes.extend(chunk),
                None if self.row_metadata.is_none() => {
                    // Similar to the other BadResponse branch above
                    return Poll::Ready(Err(Error::BadResponse(
                        "Could not read columns header".to_string(),
                    )));
                }
                // if the result set is empty, there is only the columns header
                None => return Poll::Ready(Ok(())),
            }
        }
    }

    /// Emits the next row.
    ///
    /// The result is unspecified if it's called after `Err` is returned.
    ///
    /// # Cancel safety
    ///
    /// This method is cancellation safe.
    pub async fn next(&mut self) -> Result<Option<T::Value<'_>>>
    where
        T: RowRead,
    {
        Next::new(self).await
    }

    // -----------------------------------------------------------------------
    // Why the unsafe reborrow?
    //
    // We hate unsafe. Genuinely. But NLL (the current borrow checker) can't
    // see that `bytes` is dead in the NotEnoughData branch of this loop.
    // The returned value borrows from `bytes`, so NLL extends that borrow
    // to the function's return lifetime — blocking the `bytes.extend()`
    // that only runs when no value exists. Classic Polonius limitation:
    //   https://github.com/rust-lang/rust/issues/51132
    //
    // This used to be the `polonius-the-crab` crate, which wraps the exact
    // same raw-pointer reborrow behind a macro. We dropped it because
    // polonius-the-crab has so many abandonment issues it needs therapy:
    //   - `paste` transitive dep: RUSTSEC-2024-0436 (unmaintained)
    //   - `polonius-the-crab` itself: no meaningful commits in 12+ months
    //   - `higher-kinded-types`, `macro_rules_attribute`: same story
    // Four stagnant crates, two RustSec advisories, all for a macro that
    // expands to one line of unsafe. Two lines of unsafe instead of four
    // crates is a good return.
    //
    // We properly tried to avoid this:
    //   - TryRow enum (borrow still escapes via return type — same error)
    //   - async-only next() + poll_next_owned for Stream (same NLL issue)
    //   - interior mutability in BytesExt via UnsafeCell (3x the diff,
    //     same amount of actual unsafe, just hidden — not actually better)
    //   - double deserialisation / probe-then-extract (~2x deser cost on
    //     the happy path — non-starter for a perf-sensitive cursor)
    // None compiled without unsafe somewhere, or had unacceptable costs.
    //
    // When Polonius lands in stable rustc, rip this out. We'll buy it a beer.
    // -----------------------------------------------------------------------

    #[inline]
    fn poll_next(&mut self, cx: &mut Context<'_>) -> Poll<Result<Option<T::Value<'_>>>>
    where
        T: RowRead,
    {
        if self.validation && self.row_metadata.is_none() {
            ready!(self.poll_read_columns(cx))?;
            debug_assert!(self.row_metadata.is_some());
        }

        let bytes = &mut self.bytes;

        loop {
            // SAFETY: we create a second &mut to `bytes` via raw pointer so the
            // borrow checker releases the original. This is sound because:
            //   - On Ok: we return immediately — only one &mut is live.
            //   - On NotEnoughData: the deserialized value doesn't exist, the
            //     reborrow is dead, and we fall through to extend().
            //   - On Err: we return immediately.
            // Polonius would prove this automatically. NLL can't (yet).
            let reborrowed = unsafe { &mut *(bytes as *mut BytesExt) };

            if reborrowed.remaining() > 0 {
                let mut slice = reborrowed.slice();
                let result = rowbinary::deserialize_row::<T::Value<'_>>(
                    &mut slice,
                    self.row_metadata.as_ref(),
                );

                match result {
                    Ok(value) => {
                        reborrowed.set_remaining(slice.len());
                        return Poll::Ready(Ok(Some(value)));
                    }
                    Err(Error::NotEnoughData) => {}
                    Err(err) => return Poll::Ready(Err(err)),
                }
            }

            match ready!(self.raw.poll_next(cx))? {
                Some(chunk) => bytes.extend(chunk),
                None if bytes.remaining() > 0 => {
                    // If some data is left, we have an incomplete row in the buffer.
                    // This is usually a schema mismatch on the client side.
                    return Poll::Ready(Err(Error::NotEnoughData));
                }
                None => return Poll::Ready(Ok(None)),
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

    /// Returns the total size in bytes decompressed since the cursor was created.
    #[inline]
    pub fn decoded_bytes(&self) -> u64 {
        self.raw.decoded_bytes()
    }

    /// Returns the parsed `X-ClickHouse-Summary` response header, if
    /// present. Available once the response headers have been received.
    ///
    /// Note: the summary values may be incomplete unless the query was
    /// executed with `wait_end_of_query=1`.
    #[inline]
    pub fn summary(&self) -> Option<&QuerySummary> {
        self.raw.summary()
    }
}

#[cfg(feature = "futures03")]
impl<T> futures_util::stream::Stream for RowCursor<T>
where
    T: RowOwned + RowRead,
{
    type Item = Result<T>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        Self::poll_next(self.get_mut(), cx).map(Result::transpose)
    }
}

struct Next<'a, T> {
    cursor: Option<&'a mut RowCursor<T>>,
}

impl<'a, T> Next<'a, T> {
    fn new(cursor: &'a mut RowCursor<T>) -> Self {
        Self {
            cursor: Some(cursor),
        }
    }
}

impl<'a, T> std::future::Future for Next<'a, T>
where
    T: RowRead,
{
    type Output = Result<Option<T::Value<'a>>>;

    #[inline]
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // Take cursor out so poll_next's return value gets lifetime 'a
        // (not the anonymous reborrow lifetime of &mut self).
        let cursor = self.cursor.take().expect("Future polled after completion");

        // SAFETY: same pattern as poll_next above — we create a second &mut
        // via raw pointer. On Ready the reborrow escapes via the return value
        // and cursor is consumed. On Pending the reborrow is dead and we put
        // cursor back. Sound for the same reasons; Polonius would accept this.
        let reborrowed = unsafe { &mut *(cursor as *mut RowCursor<T>) };

        match reborrowed.poll_next(cx) {
            Poll::Ready(value) => Poll::Ready(value),
            Poll::Pending => {
                self.cursor = Some(cursor);
                Poll::Pending
            }
        }
    }
}
