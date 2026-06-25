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

        if num_columns == 0 {
            return Ok(None);
        }

        // Remove the block header from the buffer
        self.buffer.commit();

        let mut columns = Vec::new();

        for _ in 0..num_columns {
            columns.push(self.read_column(num_rows).await?);
        }

        Ok(Some(Block::from_columns(columns)))
    }

    async fn read_column(&mut self, num_rows: u64) -> Result<Column, Error> {
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

        let layout = if let Some(type_width) = super::fixed_width(&data_type) {
            Layout::Fixed { type_width }
        } else if let DataTypeNode::LowCardinality(_) = data_type {
            Layout::LowCardinality(todo!())
        } else {
            Layout::Offsets(todo!())
        };

        todo!()
    }

    async fn read_string(&mut self) -> Result<MaybeUtf8, Error> {
        todo!()
    }

    async fn read_varuint(&mut self) -> Result<u64, Error> {
        loop {
            match self.buffer.parse_varuint() {
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
}

impl Buffer {
    fn push(&mut self, chunk: Chunk) {
        self.bytes.extend(chunk.data);
    }

    fn commit(&mut self) -> BytesMut {
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

    fn parse_varuint(&mut self) -> ControlFlow<Result<u64, Error>, usize> {
        const MAX_LEN: usize = 10;

        let mut accumulator = 0u64;

        for (pos, b) in self.tail().iter().enumerate().take(MAX_LEN) {
            accumulator |= u64::from(b & 0x7F);
            if b & 0x80 != 0 {
                accumulator <<= 7;
            } else {
                self.marker += pos;
                return ControlFlow::Break(Ok(accumulator));
            }
        }

        if self.tail().len() < MAX_LEN {
            ControlFlow::Continue(MAX_LEN - self.tail().len())
        } else {
            // TODO: better errors
            ControlFlow::Break(Err(Error::Custom(format!(
                "terminating byte missing in VarUInt encoding: {:?}",
                self.tail()
            ))))
        }
    }
}
