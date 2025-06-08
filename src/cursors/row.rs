use crate::row_metadata::RowMetadata;
use crate::validation_mode::ValidationMode;
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
    /// [`None`] until the first call to [`RowCursor::next()`],
    /// as [`RowCursor::new`] is not `async`, so it loads lazily.
    row_metadata: Option<RowMetadata>,
    rows_to_validate: u64,
    _marker: PhantomData<T>,
}

impl<T> RowCursor<T> {
    pub(crate) fn new(response: Response, validation_mode: ValidationMode) -> Self {
        Self {
            _marker: PhantomData,
            raw: RawCursor::new(response),
            bytes: BytesExt::default(),
            row_metadata: None,
            rows_to_validate: match validation_mode {
                ValidationMode::First(n) => n as u64,
                ValidationMode::Each => u64::MAX,
            },
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
                        // TODO: or panic instead?
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
                if self.row_metadata.is_none() {
                    self.read_columns().await?;
                    if self.bytes.remaining() == 0 {
                        continue;
                    }
                }
                let mut slice = super::workaround_51132(self.bytes.slice());
                let (result, not_enough_data) = match self.rows_to_validate {
                    0 => rowbinary::deserialize_from::<T>(&mut slice, None),
                    u64::MAX => {
                        rowbinary::deserialize_from::<T>(&mut slice, self.row_metadata.as_ref())
                    }
                    _ => {
                        let result = rowbinary::deserialize_from::<T>(
                            &mut slice,
                            self.row_metadata.as_ref(),
                        );
                        self.rows_to_validate -= 1;
                        result
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
