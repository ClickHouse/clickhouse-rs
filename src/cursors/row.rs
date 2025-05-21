use crate::validation_mode::ValidationMode;
use crate::{
    bytes_ext::BytesExt,
    cursors::RawCursor,
    error::{Error, Result},
    response::Response,
    rowbinary,
};
use clickhouse_rowbinary::data_types::Column;
use clickhouse_rowbinary::parse_rbwnat_columns_header;
use serde::Deserialize;
use std::marker::PhantomData;

/// A cursor that emits rows deserialized as structures from RowBinary.
#[must_use]
pub struct RowCursor<T> {
    raw: RawCursor,
    bytes: BytesExt,
    validation_mode: ValidationMode,
    columns: Option<Vec<Column>>,
    rows_emitted: u64,
    _marker: PhantomData<T>,
}

impl<T> RowCursor<T> {
    pub(crate) fn new(response: Response, validation_mode: ValidationMode) -> Self {
        Self {
            _marker: PhantomData,
            raw: RawCursor::new(response),
            bytes: BytesExt::default(),
            columns: None,
            rows_emitted: 0,
            validation_mode,
        }
    }

    /// Emits the next row.
    ///
    /// The result is unspecified if it's called after `Err` is returned.
    ///
    /// # Cancel safety
    ///
    /// This method is cancellation safe.
    pub async fn next<'a, 'b: 'a>(&'a mut self) -> Result<Option<T>>
    where
        T: Deserialize<'b>,
    {
        let should_validate = match self.validation_mode {
            ValidationMode::Disabled => false,
            ValidationMode::Each => true,
            ValidationMode::First(n) => self.rows_emitted < (n as u64),
        };

        loop {
            if self.bytes.remaining() > 0 {
                let mut slice = super::workaround_51132(self.bytes.slice());
                let deserialize_result = if should_validate {
                    match &self.columns {
                        // TODO: can it be moved to `new` instead?
                        None => {
                            let columns = parse_rbwnat_columns_header(&mut slice)?;
                            self.bytes.set_remaining(slice.len());
                            self.columns = Some(columns);
                            let columns = self.columns.as_ref().unwrap();
                            // usually, the header arrives as a separate first chunk
                            if self.bytes.remaining() > 0 {
                                rowbinary::deserialize_from_and_validate(&mut slice, columns)
                            } else {
                                Err(Error::NotEnoughData)
                            }
                        }
                        Some(columns) => {
                            rowbinary::deserialize_from_and_validate(&mut slice, columns)
                        }
                    }
                } else {
                    rowbinary::deserialize_from(&mut slice)
                };

                match deserialize_result {
                    Ok(value) => {
                        self.bytes.set_remaining(slice.len());
                        self.rows_emitted += 1;
                        return Ok(Some(value));
                    }
                    Err(Error::NotEnoughData) => {}
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

    /// Returns the number of rows emitted via [`Self::next`] since the cursor was created.
    #[inline]
    pub fn rows_emitted(&self) -> u64 {
        self.rows_emitted
    }
}
