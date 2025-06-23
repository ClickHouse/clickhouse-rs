use crate::row_metadata::RowMetadata;
use crate::{
    bytes_ext::BytesExt,
    cursors::RawCursor,
    error::{Error, Result},
    response::Response,
    rowbinary, Row,
};
use clickhouse_types::error::TypesError;
use clickhouse_types::parse_rbwnat_columns_header;
use serde::Deserialize;
use std::marker::PhantomData;

/// A cursor that emits rows deserialized as structures from RowBinary.
#[must_use]
pub struct RowCursor<T> {
    raw: RawCursor,
    bytes: BytesExt,
    validation: bool,
    /// [`None`] until the first call to [`RowCursor::next()`],
    /// as [`RowCursor::new`] is not `async`, so it loads lazily.
    row_metadata: Option<RowMetadata>,
    _marker: PhantomData<T>,
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
    async fn read_columns(&mut self) -> Result<()>
    where
        T: Row,
    {
        loop {
            if self.bytes.remaining() > 0 {
                let mut slice = self.bytes.slice();
                match parse_rbwnat_columns_header(&mut slice) {
                    Ok(columns) if !columns.is_empty() => {
                        self.bytes.set_remaining(slice.len());
                        self.row_metadata = Some(RowMetadata::new::<T>(columns));
                        return Ok(());
                    }
                    Ok(_) => {
                        // This does not panic, as it could be a network issue
                        // or a malformed response from the server or LB,
                        // and a simple retry might help in certain cases.
                        return Err(Error::BadResponse(
                            "Expected at least one column in the header".to_string(),
                        ));
                    }
                    Err(TypesError::NotEnoughData(_)) => {}
                    Err(err) => {
                        return Err(Error::InvalidColumnsHeader(err.into()));
                    }
                }
            }
            match self.raw.next().await? {
                Some(chunk) => self.bytes.extend(chunk),
                None if self.row_metadata.is_none() => {
                    // Similar to the other BadResponse branch above
                    return Err(Error::BadResponse(
                        "Could not read columns header".to_string(),
                    ));
                }
                // if the result set is empty, there is only the columns header
                None => return Ok(()),
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
    pub async fn next<'cursor, 'data: 'cursor>(&'cursor mut self) -> Result<Option<T>>
    where
        T: Deserialize<'data> + Row,
    {
        loop {
            if self.bytes.remaining() > 0 {
                let mut slice: &[u8];
                let result = if self.validation {
                    if self.row_metadata.is_none() {
                        self.read_columns().await?;
                        if self.bytes.remaining() == 0 {
                            continue;
                        }
                    }
                    slice = super::workaround_51132(self.bytes.slice());
                    rowbinary::deserialize_row_with_validation::<T>(
                        &mut slice,
                        // handled above
                        self.row_metadata.as_ref().unwrap(),
                    )
                } else {
                    slice = super::workaround_51132(self.bytes.slice());
                    rowbinary::deserialize_row::<T>(&mut slice)
                };
                match result {
                    Err(Error::NotEnoughData) => {}
                    Ok(value) => {
                        self.bytes.set_remaining(slice.len());
                        return Ok(Some(value));
                    }
                    Err(err) => return Err(err),
                }
            }

            match self.raw.next().await? {
                Some(chunk) => self.bytes.extend(chunk),
                None if self.bytes.remaining() > 0 => {
                    // If some data is left, we have an incomplete row in the buffer.
                    // This is usually a schema mismatch on the client side.
                    return Err(Error::NotEnoughData);
                }
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

    /// Returns the total size in bytes decompressed since the cursor was created.
    #[inline]
    pub fn decoded_bytes(&self) -> u64 {
        self.raw.decoded_bytes()
    }
}
