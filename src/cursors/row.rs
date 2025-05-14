use crate::output_format::OutputFormat;
use crate::{
    bytes_ext::BytesExt,
    cursors::RawCursor,
    error::{Error, Result},
    response::Response,
    rowbinary,
};
use clickhouse_rowbinary::parse_columns_header;
use clickhouse_rowbinary::types::Column;
use serde::Deserialize;
use std::marker::PhantomData;

/// A cursor that emits rows deserialized as structures from RowBinary.
#[must_use]
pub struct RowCursor<T> {
    raw: RawCursor,
    bytes: BytesExt,
    format: OutputFormat,
    columns: Option<Vec<Column>>,
    _marker: PhantomData<T>,
}

impl<T> RowCursor<T> {
    pub(crate) fn new(response: Response, format: OutputFormat) -> Self {
        Self {
            _marker: PhantomData,
            raw: RawCursor::new(response),
            bytes: BytesExt::default(),
            columns: None,
            format,
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
        loop {
            let mut slice = super::workaround_51132(self.bytes.slice());
            match self.format {
                OutputFormat::RowBinary => match rowbinary::deserialize_from(&mut slice) {
                    Ok(value) => {
                        self.bytes.set_remaining(slice.len());
                        return Ok(Some(value));
                    }
                    Err(Error::NotEnoughData) => {}
                    Err(err) => return Err(err),
                },
                OutputFormat::RowBinaryWithNamesAndTypes => match self.columns.as_ref() {
                    // FIXME: move this branch to new?
                    None => {
                        if slice.len() > 0 {
                            let columns = parse_columns_header(&mut slice)?;
                            self.bytes.set_remaining(slice.len());
                            self.columns = Some(columns);
                        }
                    }
                    Some(columns) => {
                        match rowbinary::deserialize_from_rbwnat(&mut slice, columns) {
                            Ok(value) => {
                                self.bytes.set_remaining(slice.len());
                                return Ok(Some(value));
                            }
                            Err(Error::NotEnoughData) => {}
                            Err(err) => return Err(err),
                        }
                    }
                },
            }
            // }

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

    /// Returns the total size in bytes decompressed since the cursor was
    /// created.
    #[inline]
    pub fn decoded_bytes(&self) -> u64 {
        self.raw.decoded_bytes()
    }
}
