use crate::error::Error;
use crate::native::string::MaybeUtf8;
use crate::native::{Block, Column, Layout, LayoutKind, fixed_width};
use crate::response::Chunks;
use bytes::{Buf, Bytes, BytesMut};
use clickhouse_types::DataTypeNode;
use futures_util::StreamExt;
use std::cmp;
use std::ops::ControlFlow;

pub(crate) struct BlockReader {
    chunks: Chunks,
    last_chunk: Bytes,
}

impl BlockReader {
    pub(crate) fn new(chunks: Chunks) -> Self {
        BlockReader {
            chunks,
            last_chunk: Bytes::new(),
        }
    }

    /// **NOT** cancel-safe. Intended to be wrapped in an async task for cancel-safety.
    pub(crate) async fn read_block(&mut self) -> Result<Option<Block>, Error> {
        let num_columns = self.read_varuint().await?;
        let num_rows = self.read_varuint().await?;

        let num_rows = usize::try_from(num_rows).map_err(|_| {
            Error::Custom(format!(
                "number of rows in block is out of range: {num_rows}"
            ))
        })?;

        if num_columns == 0 {
            return Ok(None);
        }

        let mut columns = Vec::new();

        for _ in 0..num_columns {
            columns.push(self.read_column(num_rows).await?);
        }

        Ok(Some(Block::from_columns(columns, num_rows)))
    }

    async fn read_column(&mut self, num_rows: usize) -> Result<Column, Error> {
        let name = self.read_string().await?;
        let data_type = self.read_string().await?;

        let data_type = data_type.try_as_str().ok_or_else(|| {
            Error::Custom(format!(
                "invalid data type {data_type:?} of column {name:?}"
            ))
        })?;

        let data_type = DataTypeNode::new(data_type).map_err(|e| {
            Error::Custom(format!(
                "invalid data type {data_type:?} of column {name:?}: {e}"
            ))
        })?;

        let layout = self.read_type_data(&data_type, num_rows).await?;

        Ok(Column {
            name,
            data_type,
            layout,
            num_rows,
        })
    }

    async fn read_type_data(
        &mut self,
        data_type: &DataTypeNode,
        num_rows: usize,
    ) -> Result<Layout, Error> {
        let (data_type, nulls) = if let DataTypeNode::Nullable(inner) = data_type {
            // Read off the null bitmap
            (&**inner, Some(self.read_bytes(num_rows).await?))
        } else {
            (data_type, None)
        };

        if let Some(type_width) = fixed_width(data_type) {
            let total_bytes = type_width.checked_mul(num_rows).ok_or_else(|| {
                Error::Custom(format!(
                    "data size is too large: {num_rows} rows X {type_width} bytes"
                ))
            })?;

            return Ok(Layout {
                kind: LayoutKind::Fixed {
                    type_width,
                    data: self.read_bytes(total_bytes).await?,
                },
                nulls,
            });
        }

        match data_type {
            DataTypeNode::String => {
                // Assume default growth strategy is fine
                let mut data = BytesMut::new();
                let mut end_offsets = Vec::with_capacity(num_rows);

                for row in 0..num_rows {
                    let len = self.read_varuint().await?;

                    let len = usize::try_from(len).map_err(|_| {
                        Error::Custom(format!("string #{row} length {len} is out of range"))
                    })?;

                    self.read_bytes_into(len, &mut data).await?;
                    end_offsets.push(data.len());
                }

                Ok(Layout {
                    kind: LayoutKind::Variable {
                        data: data.freeze(),
                        end_offsets: end_offsets.into(),
                    },
                    nulls,
                })
            }
            DataTypeNode::Array(elem_type) => {
                let mut end_indices = Vec::with_capacity(num_rows);

                for _ in 0..num_rows {
                    // Note: cumulative length, last value is total number of elements
                    let length = u64::from_le_bytes(self.read_bytes_fixed().await?);

                    let length = usize::try_from(length).map_err(|_| {
                        Error::Custom(format!("array length out of range: {length}"))
                    })?;

                    end_indices.push(length);
                }

                let total_len = end_indices.last().copied().unwrap_or(0);

                // Recursive `async fn` currently requires boxing
                let elem_layout = Box::pin(self.read_type_data(elem_type, total_len)).await?;

                Ok(Layout {
                    kind: LayoutKind::Array {
                        end_indices: end_indices.into(),
                        elem_layout: Box::new(elem_layout),
                    },
                    nulls,
                })
            }
            _ => Err(Error::Custom(format!(
                "data type {data_type:?} not implemented"
            ))),
        }
    }

    async fn read_string(&mut self) -> Result<MaybeUtf8, Error> {
        let len = self.read_varuint().await?;

        let len = usize::try_from(len)
            .map_err(|_| Error::Custom(format!("string length too large: {len}")))?;

        Ok(self.read_bytes(len).await?.into())
    }

    async fn read_varuint(&mut self) -> Result<u64, Error> {
        let mut parser = ParseVarUInt::default();

        loop {
            if let ControlFlow::Break(val) = parser.feed(&mut self.last_chunk)? {
                return Ok(val);
            }

            self.read_chunk().await?;
        }
    }

    async fn read_bytes(&mut self, len: usize) -> Result<Bytes, Error> {
        self.read_chunk().await?;

        if self.last_chunk.len() >= len {
            return Ok(self.last_chunk.split_to(len));
        }

        let mut buf = BytesMut::with_capacity(len);
        self.read_bytes_into(len, &mut buf).await?;

        Ok(buf.freeze())
    }

    async fn read_bytes_into(
        &mut self,
        mut read_len: usize,
        buf: &mut BytesMut,
    ) -> Result<(), Error> {
        while read_len > 0 {
            self.read_chunk().await?;

            let data = self
                .last_chunk
                .split_to(cmp::min(read_len, self.last_chunk.len()));
            buf.extend_from_slice(&data);
            read_len -= data.len();
        }

        Ok(())
    }

    async fn read_bytes_fixed<const LEN: usize>(&mut self) -> Result<[u8; LEN], Error> {
        let mut read_len = 0;
        let mut buf = [0u8; LEN];

        while read_len < LEN {
            self.read_chunk().await?;

            let amt = cmp::min(LEN - read_len, self.last_chunk.len());
            buf[read_len..].copy_from_slice(&self.last_chunk[..amt]);
            read_len += amt;
            self.last_chunk.advance(amt);
        }

        Ok(buf)
    }

    async fn read_chunk(&mut self) -> Result<(), Error> {
        if self.last_chunk.is_empty() {
            self.last_chunk = self.chunks.next().await.ok_or(Error::NotEnoughData)??.data;
        }

        Ok(())
    }
}

#[derive(Default)]
pub(super) struct ParseVarUInt {
    accumulator: u64,
}

impl ParseVarUInt {
    pub fn parse_full(buf: impl Buf) -> Result<u64, Error> {
        Self::default()
            .feed(buf)?
            .break_value()
            .ok_or(Error::NotEnoughData)
    }

    pub fn feed(&mut self, mut buf: impl Buf) -> Result<ControlFlow<u64>, Error> {
        const MAX_LEN: usize = 10;

        for _ in 0..MAX_LEN {
            let Ok(b) = buf.try_get_u8() else {
                return Ok(ControlFlow::Continue(()));
            };

            self.accumulator |= u64::from(b & 0x7F);

            if b >= 0x80 {
                self.accumulator = self.accumulator.checked_shl(7).ok_or_else(|| {
                    Error::Custom(format!(
                        "VarUInt repr overflowed: {:016x} byte: {b:02x}",
                        self.accumulator
                    ))
                })?;
            } else {
                return Ok(ControlFlow::Break(self.accumulator));
            }
        }

        Err(Error::Custom(format!(
            "terminating byte missing in VarUInt encoding: {:016x}",
            self.accumulator,
        )))
    }
}
