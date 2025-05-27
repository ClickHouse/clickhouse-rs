use crate::validation_mode::ValidationMode;
use crate::{
    bytes_ext::BytesExt,
    cursors::RawCursor,
    error::{Error, Result},
    response::Response,
    rowbinary,
};
use clickhouse_types::data_types::Column;
use clickhouse_types::parse_rbwnat_columns_header;
use serde::Deserialize;
use std::marker::PhantomData;

/// A cursor that emits rows deserialized as structures from RowBinary.
#[must_use]
pub struct RowCursor<T> {
    raw: RawCursor,
    bytes: BytesExt,
    columns: Vec<Column>,
    rows_to_validate: u64,
    _marker: PhantomData<T>,
}

impl<T> RowCursor<T> {
    pub(crate) fn new(response: Response, validation_mode: ValidationMode) -> Self {
        Self {
            _marker: PhantomData,
            raw: RawCursor::new(response),
            bytes: BytesExt::default(),
            columns: Vec::new(),
            rows_to_validate: match validation_mode {
                ValidationMode::First(n) => n as u64,
                ValidationMode::Each => u64::MAX,
            },
        }
    }

    #[cold]
    #[inline(never)]
    fn read_columns(&mut self, mut slice: &[u8]) -> Result<()> {
        let columns = parse_rbwnat_columns_header(&mut slice)?;
        debug_assert!(!columns.is_empty());
        self.bytes.set_remaining(slice.len());
        self.columns = columns;
        Ok(())
    }

    #[inline(always)]
    fn deserialize_with_validation<'cursor, 'data: 'cursor>(
        &'cursor mut self,
        slice: &mut &'data [u8],
    ) -> (Result<T>, bool)
    where
        T: Deserialize<'data>,
    {
        let result = rowbinary::deserialize_from_and_validate::<T>(slice, &self.columns);
        self.rows_to_validate -= 1;
        result
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
        T: Deserialize<'data>,
    {
        loop {
            if self.bytes.remaining() > 0 {
                let mut slice = super::workaround_51132(self.bytes.slice());
                if self.columns.is_empty() {
                    self.read_columns(slice)?;
                } else {
                    let (result, not_enough_data) = match self.rows_to_validate {
                        0 => rowbinary::deserialize_from_and_validate::<T>(&mut slice, &[]),
                        u64::MAX => {
                            rowbinary::deserialize_from_and_validate::<T>(&mut slice, &self.columns)
                        }
                        _ => {
                            // extracting to a separate method boosts performance for Each ~10%
                            self.deserialize_with_validation(&mut slice)
                        }
                    };
                    if !not_enough_data {
                        return match result {
                            Ok(value) => {
                                self.bytes.set_remaining(slice.len());
                                Ok(Some(value))
                            }
                            Err(err) => Err(err),
                        };
                    }
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
