use crate::error::Error;
use crate::native::string::MaybeUtf8;
use crate::native::{Block, Column, Layout, LayoutKind, LayoutLowCardinality, fixed_width};
use crate::response::Chunks;
use bytes::{Buf, Bytes, BytesMut};
use clickhouse_types::DataTypeNode;
use futures_util::StreamExt;
use hashbrown::{HashMap, hash_map};
use std::cmp;
use std::ops::ControlFlow;
use std::sync::Arc;

pub(crate) struct BlockReader {
    inner: ReaderInner,
    lc_state: LcState,
}

struct ReaderInner {
    chunks: Chunks,
    last_chunk: Bytes,
}

/// `LowCardinality` decode state for the current stream.
struct LcState {
    by_column: HashMap<MaybeUtf8, ColumnLcState>,
}

struct ColumnLcState {
    global_dict: Option<Arc<Layout>>,
}

#[repr(u64)]
#[derive(Copy, Clone, PartialEq, Eq, Debug)]
enum LcKeyType {
    UInt8 = 0,
    UInt16,
    UInt32,
    UInt64,
}

impl BlockReader {
    pub(crate) fn new(chunks: Chunks) -> Self {
        BlockReader {
            inner: ReaderInner {
                chunks,
                last_chunk: Bytes::new(),
            },
            lc_state: LcState {
                by_column: HashMap::new(),
            },
        }
    }

    /// **NOT** cancel-safe. Intended to be wrapped in an async task for cancel-safety.
    pub(crate) async fn read_block(&mut self) -> Result<Option<Block>, Error> {
        let num_columns = self.inner.read_varuint().await?;
        let num_rows = self.inner.read_varuint().await?;

        let num_rows = usize::try_from(num_rows).map_err(|_| {
            Error::Custom(format!(
                "number of rows in block is out of range: {num_rows}"
            ))
        })?;

        if num_columns == 0 {
            return Ok(None);
        }

        let mut columns = Vec::new();

        for col in 0..num_columns {
            columns.push(self.read_column(num_rows).await.inspect_err(|e| {
                tracing::debug!(col, "error reading column: {e:?}");
            })?);
        }

        Ok(Some(Block::from_columns(columns, num_rows)))
    }

    async fn read_column(&mut self, num_rows: usize) -> Result<Column, Error> {
        let name = self.inner.read_string().await?;
        let data_type = self.inner.read_string().await?;

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

        let layout = self.read_type_data(&name, &data_type, num_rows).await?;

        Ok(Column {
            name,
            data_type,
            layout,
        })
    }

    async fn read_type_data(
        &mut self,
        column_name: &MaybeUtf8,
        data_type: &DataTypeNode,
        num_values: usize,
    ) -> Result<Layout, Error> {
        let (data_type, nulls) = if let DataTypeNode::Nullable(inner) = data_type {
            // Read off the null bitmap
            (&**inner, Some(self.inner.read_bytes(num_values).await?))
        } else {
            (data_type, None)
        };

        if let Some(type_width) = fixed_width(data_type) {
            let total_bytes = type_width.checked_mul(num_values).ok_or_else(|| {
                Error::Custom(format!(
                    "data size of column {column_name:?} is too large: {num_values} rows X {type_width} bytes"
                ))
            })?;

            return Ok(Layout {
                kind: LayoutKind::Fixed {
                    type_width,
                    data: self.inner.read_bytes(total_bytes).await?,
                },
                nulls,
                num_values,
            });
        }

        match data_type {
            DataTypeNode::String => {
                // Assume default growth strategy is fine
                let mut data = BytesMut::new();
                let mut end_offsets = Vec::with_capacity(num_values);

                for row in 0..num_values {
                    let len = self.inner.read_varuint().await?;

                    let len = usize::try_from(len).map_err(|_| {
                        Error::Custom(format!("string #{row} length {len} is out of range"))
                    })?;

                    self.inner.read_bytes_into(len, &mut data).await?;
                    end_offsets.push(data.len());
                }

                Ok(Layout {
                    kind: LayoutKind::Variable {
                        data: data.freeze(),
                        end_offsets: end_offsets.into(),
                    },
                    nulls,
                    num_values,
                })
            }
            DataTypeNode::Array(elem_type) => {
                let end_indices = self.inner.read_array_indices(num_values).await?;

                let total_len = end_indices.last().copied().unwrap_or(0);

                // Recursive `async fn` currently requires boxing
                let elem_layout =
                    Box::pin(self.read_type_data(column_name, elem_type, total_len)).await?;

                Ok(Layout {
                    kind: LayoutKind::Array {
                        end_indices,
                        elem_layout: Box::new(elem_layout),
                    },
                    nulls,
                    num_values,
                })
            }
            DataTypeNode::Tuple(types) => {
                let mut layouts = Vec::with_capacity(types.len());

                for ty in types {
                    layouts.push(Box::pin(self.read_type_data(column_name, ty, num_values)).await?);
                }

                Ok(Layout {
                    kind: LayoutKind::Tuple {
                        layouts: layouts.into(),
                    },
                    nulls,
                    num_values,
                })
            }
            DataTypeNode::Map([key_ty, val_ty]) => {
                // Maps are just `Array(Tuple(K, V))` but the way this code is structured,
                // it's easier to create a specialized routine to read them
                let end_indices = self.inner.read_array_indices(num_values).await?;

                let total_len = end_indices.last().copied().unwrap_or(0);

                // Keys semantically cannot be `LowCardinality` or `Nullable`
                // but that doesn't really affect decoding
                let key_layout =
                    Box::pin(self.read_type_data(column_name, key_ty, total_len)).await?;
                let value_layout =
                    Box::pin(self.read_type_data(column_name, val_ty, total_len)).await?;

                Ok(Layout {
                    kind: LayoutKind::Map {
                        key_val_layouts: Box::new([key_layout, value_layout]),
                        end_indices,
                    },
                    nulls,
                    num_values,
                })
            }
            DataTypeNode::LowCardinality(inner_type) => {
                Ok(Layout {
                    kind: LayoutKind::LowCardinality(
                        self.read_low_cardinality(column_name, inner_type, num_values)
                            .await?,
                    ),
                    // Can be `Some` if the type is `Nullable(LowCardinality(...))`
                    // even though `LowCardinality(Nullable(...))` is more efficient
                    nulls,
                    num_values,
                })
            }
            _ => Err(Error::Custom(format!(
                "data type {data_type:?} not implemented"
            ))),
        }
    }

    async fn read_low_cardinality(
        &mut self,
        column_name: &MaybeUtf8,
        inner_type: &DataTypeNode,
        num_rows: usize,
    ) -> Result<LayoutLowCardinality, Error> {
        const HAS_ADDITIONAL_KEYS_BIT: u64 = 0x200;
        const NEEDS_UPDATE_DICTIONARY_BIT: u64 = 0x400;
        const NEED_GLOBAL_DICTIONARY_BIT: u64 = 0x800;

        if num_rows == 0 {
            todo!("empty result contains no prefix");
        }

        if let hash_map::EntryRef::Vacant(vacant) = self.lc_state.by_column.entry_ref(column_name) {
            // We haven't seen the state prefix for this query yet
            let prefix = self.inner.read_int64().await?;

            if prefix != 1 {
                // sharedDictionariesWithAdditionalKeys
                return Err(Error::Custom(format!(
                    "unexpected/unsupported serialization version of type \
                             LowCardinality({inner_type}) of column {column_name:?}: \
                             expected 1, got {prefix}"
                )));
            }

            vacant.insert(ColumnLcState { global_dict: None });
        };

        let metadata = self.inner.read_uint64().await?;

        let key_type = LcKeyType::try_from_metadata(metadata)?;

        let has_additional_keys = metadata & HAS_ADDITIONAL_KEYS_BIT != 0;
        let needs_update_dictionary = metadata & NEEDS_UPDATE_DICTIONARY_BIT != 0;
        let need_global_dictionary = metadata & NEED_GLOBAL_DICTIONARY_BIT != 0;

        let dict_len = self.inner.read_uint64().await?;
        let dict_len = usize::try_from(dict_len)
            .map_err(|_| Error::Custom(format!("column {column_name:?} type LowCardinality({inner_type}) dictionary size too large: {dict_len}")))?;

        // `LowCardinality(Nullable)` doesn't emit a null map, instead `dict[1]` is the null value
        let (non_nullable, is_nullable) = if let DataTypeNode::Nullable(inner) = inner_type {
            (&**inner, true)
        } else {
            (inner_type, false)
        };

        // called from `self.read_type_data()` so we have to box
        let dict = Box::pin(self.read_type_data(column_name, non_nullable, dict_len)).await?;

        let keys_len = self.inner.read_uint64().await?;
        let keys_len = usize::try_from(keys_len)
            .map_err(|_| Error::Custom(format!("column {column_name:?} type LowCardinality({inner_type}) keys count too large: {keys_len}")))?;

        if keys_len != num_rows {
            return Err(Error::Custom(format!(
                "column {column_name:?} type LowCardinality({inner_type}) keys count does not match number of rows in block: {keys_len} vs {num_rows}; this likely means a bug or corrupted data"
            )));
        }

        let keys = self.inner.read_lc_keys(key_type, num_rows).await?;

        // Cannot be borrowed across other calls to `self` so we have to do a second lookup;
        // we could amortize by using `IndexMap` but two lookups per column is unlikely to be an issue
        let state = self
            .lc_state
            .by_column
            .get_mut(column_name)
            .unwrap_or_else(|| panic!("BUG: `lc_state` should have been initialized"));

        let (global_dict, local_dict) = if need_global_dictionary {
            let global_dict = state.global_dict
                .clone()
                .ok_or_else(|| Error::Custom(format!("column {column_name:?} type LowCardinality({inner_type}) set NEEDS_GLOBAL_DICTIONARY_BIT for a block but no global dictionary was read")))?;

            (
                Some(global_dict),
                has_additional_keys.then(|| Box::new(dict)),
            )
        } else if needs_update_dictionary {
            (Some(state.global_dict.insert(Arc::new(dict)).clone()), None)
        } else {
            (None, Some(Box::new(dict)))
        };

        Ok(LayoutLowCardinality {
            global_dict,
            local_dict,
            keys,
            is_nullable,
        })
    }
}

impl ReaderInner {
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

    async fn read_array_indices(&mut self, num_rows: usize) -> Result<Box<[usize]>, Error> {
        let mut end_indices = Vec::with_capacity(num_rows);

        for _ in 0..num_rows {
            // Note: cumulative length, last value is total number of elements
            let length = self.read_uint64().await?;

            let length = usize::try_from(length)
                .map_err(|_| Error::Custom(format!("array length out of range: {length}")))?;

            end_indices.push(length);
        }

        Ok(end_indices.into())
    }

    async fn read_lc_keys(
        &mut self,
        key_type: LcKeyType,
        num_rows: usize,
    ) -> Result<Box<[usize]>, Error> {
        let mut keys = Vec::with_capacity(num_rows);

        for i in 0..num_rows {
            let mut buf = [0u8; LcKeyType::MAX_BYTE_WIDTH];

            self.fill_buf(&mut buf[..key_type.byte_width()]).await?;

            let key = u64::from_le_bytes(buf);

            let key = usize::try_from(key).map_err(|_| {
                Error::Custom(format!(
                    "LowCardinality dictionary key out of range at index {i}: {key}"
                ))
            })?;

            keys.push(key);
        }

        Ok(keys.into())
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

    async fn read_bytes_into(&mut self, mut amt: usize, buf: &mut BytesMut) -> Result<(), Error> {
        while amt > 0 {
            self.read_chunk().await?;

            let read_len = cmp::min(self.last_chunk.len(), amt);

            buf.extend_from_slice(&self.last_chunk[..read_len]);

            self.consume(read_len);

            amt -= read_len;
        }

        Ok(())
    }

    async fn read_bytes_fixed<const LEN: usize>(&mut self) -> Result<[u8; LEN], Error> {
        let mut buf = [0u8; LEN];

        self.fill_buf(&mut buf).await?;

        Ok(buf)
    }

    async fn fill_buf(&mut self, mut buf: &mut [u8]) -> Result<(), Error> {
        while !buf.is_empty() {
            self.read_chunk().await?;

            let read_len = cmp::min(buf.len(), self.last_chunk.len());

            let dst = buf
                .split_off_mut(..read_len)
                .expect("BUG: we just validated `read_len` is in bounds");

            dst.copy_from_slice(&self.last_chunk[..read_len]);

            self.consume(read_len);
        }

        Ok(())
    }

    async fn read_uint64(&mut self) -> Result<u64, Error> {
        Ok(u64::from_le_bytes(self.read_bytes_fixed().await?))
    }

    async fn read_int64(&mut self) -> Result<i64, Error> {
        Ok(i64::from_le_bytes(self.read_bytes_fixed().await?))
    }

    async fn read_chunk(&mut self) -> Result<(), Error> {
        if self.last_chunk.is_empty() {
            self.last_chunk = self.chunks.next().await.ok_or(Error::NotEnoughData)??.data;
        }

        Ok(())
    }

    fn consume(&mut self, len: usize) {
        self.last_chunk.advance(len);

        // `Bytes::advance()` doesn't decrement the refcount when it's empty,
        // preventing the buffer from being reused
        if self.last_chunk.is_empty() {
            self.last_chunk = Bytes::new();
        }
    }
}

#[derive(Default)]
struct ParseVarUInt {
    accumulator: u64,
    shift: u32,
}

impl ParseVarUInt {
    fn feed(&mut self, mut buf: impl Buf) -> Result<ControlFlow<u64>, Error> {
        const MAX_LEN: usize = 10;

        for _ in 0..MAX_LEN {
            let Ok(b) = buf.try_get_u8() else {
                return Ok(ControlFlow::Continue(()));
            };

            self.accumulator |= (b as u64 & 0x7F).checked_shl(self.shift).ok_or_else(|| {
                Error::Custom(format!(
                    "VarUInt repr overflowed: {:016x} byte: {b:02x}",
                    self.accumulator
                ))
            })?;

            if b <= 0x7F {
                return Ok(ControlFlow::Break(self.accumulator));
            }

            self.shift += 7;
        }

        Err(Error::Custom(format!(
            "terminating byte missing in VarUInt encoding: {:016x}",
            self.accumulator,
        )))
    }
}

impl LcKeyType {
    const MASK: u64 = 0xFF; // Low 8 bits

    const MAX_BYTE_WIDTH: usize = Self::UInt64.byte_width();

    fn try_from_metadata(metadata: u64) -> Result<Self, Error> {
        Ok(match metadata & Self::MASK {
            0 => Self::UInt8,
            1 => Self::UInt16,
            2 => Self::UInt32,
            3 => Self::UInt64,
            unknown => {
                return Err(Error::Custom(format!(
                    "unknown LowCardinality key type: {unknown}"
                )));
            }
        })
    }

    const fn byte_width(&self) -> usize {
        match self {
            LcKeyType::UInt8 => 1,
            LcKeyType::UInt16 => 2,
            LcKeyType::UInt32 => 4,
            LcKeyType::UInt64 => 8,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::ParseVarUInt;
    use bytes::Buf;
    use std::ops::ControlFlow;

    #[test]
    fn parse_varuint() {
        let encoded_and_decoded: &[(&[u8], u64)] = &[
            (&[0u8][..], 0u64),
            (&[1], 1),
            (&[127], 127),
            (&[0x80, 0x01], 1 << 7),
            (&[0x80, 0x80, 0x01], 1 << 14),
            (&[0x80, 0x80, 0x80, 0x01], 1 << 21),
            (&[0x80, 0x80, 0x80, 0x80, 0x01], 1 << 28),
            (&[0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x1F], 0xFF_FF_FF_FF_FF),
        ];

        let pad_len = 16usize;

        for (encoded, decoded) in encoded_and_decoded {
            // Pad with junk data that must be ignored
            let padded: Vec<_> = encoded
                .iter()
                .copied()
                .chain((0..).cycle())
                .take(pad_len)
                .collect();

            // Test feeding slices in different size chunks
            for chunk_size in 1..=padded.len() {
                let mut parser = ParseVarUInt::default();

                let mut slice = &padded[..];

                let mut last_remaining = slice.len();

                loop {
                    match parser.feed((&mut slice).take(chunk_size)) {
                        Ok(ControlFlow::Break(res)) => {
                            assert_eq!(
                                res, *decoded,
                                "invalid decoding; chunk_size: {chunk_size}, padded: {padded:?}, remaining: {slice:?}"
                            );
                            assert_eq!(
                                slice.len(),
                                padded.len() - encoded.len(),
                                "extra data consumed: {slice:?}"
                            );
                            break;
                        }
                        Ok(ControlFlow::Continue(())) => {
                            assert!(
                                !slice.is_empty(),
                                "full slice consumed without giving a result"
                            );
                            assert_ne!(
                                slice.len(),
                                last_remaining,
                                "parser failed to make progress"
                            );
                            last_remaining = slice.len();
                        }
                        Err(e) => {
                            panic!(
                                "error: {e:?}, chunk_size: {chunk_size}, padded: {padded:?}, remaining: {slice:?}"
                            );
                        }
                    }
                }
            }
        }
    }
}
