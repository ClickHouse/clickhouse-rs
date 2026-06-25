use crate::error::Error;
use crate::native::string::MaybeUtf8;
use crate::native::{Block, Column, Layout};
use crate::response::{Chunk, Chunks};
use bytes::{Bytes, BytesMut};
use clickhouse_types::DataTypeNode;
use futures_util::StreamExt;
use std::ops::ControlFlow;

pub(crate) struct BlockReader {
    chunks: Chunks,
    buffer: Buffer,
}

struct Buffer {
    bytes: BytesMut,
    marker: usize,
}

impl BlockReader {
    pub(crate) fn new(chunks: Chunks) -> Self {
        BlockReader {
            chunks,
            buffer: Buffer {
                bytes: BytesMut::new(),
                marker: 0,
            },
        }
    }

    /// **NOT** cancel-safe. Intended to be wrapped in an async task for cancel-safety.
    pub(crate) async fn read_block(&mut self) -> Result<Option<Block>, Error> {
        if let Some(chunk) = self.chunks.next().await.transpose()? {
            self.buffer.push(chunk);
        } else if self.buffer.bytes.is_empty() {
            return Ok(None);
        };

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

        // Remove the block header from the buffer
        self.buffer.split();

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

        // Split the column header from the data
        self.buffer.split();

        if let Some(type_width) = super::fixed_width(&data_type) {
            let total_bytes = type_width
                .checked_mul(num_rows)
                .ok_or_else(|| Error::Custom(format!("data size of column {name:?} is too large: {num_rows} rows X {type_width} bytes")))?;

            self.read_bytes(total_bytes).await?;

            return Ok(Column {
                name,
                data_type,
                data: self.buffer.split().freeze(),
                layout: Layout::Fixed { type_width },
            });
        }

        match data_type {
            DataTypeNode::String => {
                let mut offsets = Vec::with_capacity(num_rows);

                for row in 0..num_rows {
                    offsets.push(self.buffer.marker);
                    let len = self.read_varuint().await?;

                    let len = usize::try_from(len).map_err(|_| {
                        Error::Custom(format!(
                            "string length {len} of row {row} of column {name:?} is out of range"
                        ))
                    })?;

                    self.read_bytes(len).await?;
                }

                Ok(Column {
                    name,
                    data_type,
                    data: self.buffer.split().freeze(),
                    layout: Layout::Offsets(offsets.into()),
                })
            }
            _ => Err(Error::Custom(format!(
                "data type {data_type:?} of column {name:?} not implemented"
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
        loop {
            match self.buffer.read_varuint() {
                ControlFlow::Break(res) => return res,
                ControlFlow::Continue(_) => {
                    let chunk = self
                        .chunks
                        .next()
                        .await
                        .transpose()?
                        .ok_or_else(|| Error::NotEnoughData)?;
                    self.buffer.push(chunk);
                }
            }
        }
    }

    async fn read_bytes(&mut self, len: usize) -> Result<&[u8], Error> {
        while self.buffer.tail().len() < len {
            let chunk = self
                .chunks
                .next()
                .await
                .transpose()?
                .ok_or_else(|| Error::NotEnoughData)?;

            self.buffer.push(chunk);
        }

        self.buffer.advance(len)
    }
}

impl Buffer {
    fn push(&mut self, chunk: Chunk) {
        self.bytes.extend_from_slice(&chunk.data);
    }

    fn split(&mut self) -> BytesMut {
        let ret = self.bytes.split_to(self.marker);
        self.marker = 0;
        ret
    }

    fn tail(&self) -> &[u8] {
        &self.bytes[self.marker..]
    }

    fn advance(&mut self, len: usize) -> Result<&[u8], Error> {
        let ret = &self.bytes[self.marker..]
            .get(..len)
            .ok_or(Error::NotEnoughData)?;

        self.marker += len;

        Ok(ret)
    }

    fn read_varuint(&mut self) -> ControlFlow<Result<u64, Error>, usize> {
        let mut tail = self.tail();

        match parse_varuint(&mut tail) {
            ControlFlow::Break(Ok(res)) => {
                self.marker = self.bytes.len() - tail.len();
                ControlFlow::Break(Ok(res))
            }
            other => other,
        }
    }
}

pub(super) fn parse_varuint(buf: &mut &[u8]) -> ControlFlow<Result<u64, Error>, usize> {
    const MAX_LEN: usize = 10;

    let mut accumulator = 0u64;

    let mut iter = buf.iter();

    for b in iter.by_ref().take(MAX_LEN) {
        accumulator |= u64::from(b & 0x7F);
        if *b >= 0x80 {
            accumulator <<= 7;
        } else {
            *buf = iter.as_slice();
            return ControlFlow::Break(Ok(accumulator));
        }
    }

    if buf.len() < MAX_LEN {
        ControlFlow::Continue(MAX_LEN - buf.len())
    } else {
        // TODO: better errors
        ControlFlow::Break(Err(Error::Custom(format!(
            "terminating byte missing in VarUInt encoding: {buf:?}",
        ))))
    }
}
