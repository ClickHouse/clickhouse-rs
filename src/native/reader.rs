use crate::error::Error;
use crate::native::string::MaybeUtf8;
use crate::native::utils::type_fixed_width;
use crate::native::varuint::ParseVarUInt;
use crate::native::{Block, Column, Layout, LayoutKind, LayoutLowCardinality};
use crate::response::Chunks;
use bytes::{Buf, Bytes, BytesMut};
use clickhouse_types::DataTypeNode;
use std::borrow::Cow;
use std::cmp;
use std::collections::VecDeque;
use std::fmt::{Display, Formatter};
use std::ops::ControlFlow;

/// A source of byte chunks for [`BlockReader`].
///
/// A chunk may end at any position: the reader keeps the unused rest of a chunk
/// and asks for more data only when it needs it.
pub(crate) trait ChunkSource: Send {
    /// Return the next chunk, or `None` at the end of data.
    fn next_chunk(&mut self) -> impl Future<Output = Option<Result<Bytes, Error>>> + Send;
}

impl ChunkSource for Chunks {
    async fn next_chunk(&mut self) -> Option<Result<Bytes, Error>> {
        use futures_util::StreamExt;

        self.next().await.map(|chunk| chunk.map(|chunk| chunk.data))
    }
}

/// The first protocol revision that sends a serialization kind for each column.
pub(crate) const REVISION_WITH_CUSTOM_SERIALIZATION: u64 = 54454;

/// Details of the serialization of blocks that depend on the peer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct WireFormat {
    /// Native protocol revision of the stream, or `0` for the `Native` format.
    ///
    /// The native TCP protocol adds a `BlockInfo` header to each block and, since revision
    /// [`REVISION_WITH_CUSTOM_SERIALIZATION`], the serialization kind of each column.
    pub(crate) revision: u64,
    /// `Variant` columns start with the serialization mode of discriminators (ClickHouse 24.7+).
    pub(crate) variant_mode_prefix: bool,
}

impl WireFormat {
    /// The `Native` format of current servers.
    pub(crate) const NATIVE: Self = Self {
        revision: 0,
        variant_mode_prefix: true,
    };
}

pub(crate) struct BlockReader<S = Chunks> {
    inner: ReaderInner<S>,
    // Since versioned type encoding is split into a "prefix phase" and a "data phase"
    // we need to read state prefixes separately and remember what we've seen
    prefix_queue: VecDeque<StatePrefix>,
    format: WireFormat,
}

/// Captures errors that may occur while decoding a [Native Format] block.
///
/// [Native Format]: https://clickhouse.com/docs/reference/interfaces/specs/NativeFormat
#[derive(Debug)]
pub struct BlockReadError {
    message: String,
    column_name: Option<MaybeUtf8>,
    column_type: Option<DataTypeNode>,
    source: Option<Box<dyn std::error::Error + Send + Sync + 'static>>,
}

struct ReaderInner<S> {
    chunks: S,
    last_chunk: Bytes,
}

#[derive(Debug)]
enum StatePrefix {
    // LowCardinality state prefix is just `version = 1`, no state necessary
    LowCardinality,
    /// `Variant` discriminators serialization mode.
    Variant(VariantMode),
    /// `JSON` serialized as one string per row.
    JsonString,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
enum VariantMode {
    /// One discriminator byte per row.
    Basic,
    /// Granules with either one discriminator byte per row or one discriminator for all rows.
    Compact,
}

/// `ColumnVariant::NULL_DISCRIMINATOR`
pub(crate) const VARIANT_NULL_DISCRIMINATOR: u8 = 255;

/// The serialization kind of a column, sent by the native TCP protocol.
#[derive(Debug, Clone, PartialEq, Eq)]
enum SerializationKind {
    Default,
    /// Only the non-default values are sent, with the offsets of their rows.
    Sparse,
    /// The kinds of the elements of a `Tuple`.
    Tuple(Box<SerializationKind>, Vec<SerializationKind>),
}

impl SerializationKind {
    fn is_default(&self) -> bool {
        match self {
            Self::Default => true,
            Self::Sparse => false,
            Self::Tuple(own, elements) => {
                own.is_default() && elements.iter().all(SerializationKind::is_default)
            }
        }
    }
}

#[repr(u64)]
#[derive(Copy, Clone, PartialEq, Eq, Debug)]
enum LcKeyType {
    UInt8 = 0,
    UInt16,
    UInt32,
    UInt64,
}

macro_rules! read_error {
    ($($format_args:tt)*) => {
        BlockReadError {
            message: format!($($format_args)*),
            column_name: None,
            column_type: None,
            source: None,
        }
    }
}

/// Maximum number of values allowed in any one column.
///
/// This is a hardcoded limit, not expected to be reached by any practical application,
/// meant to guard against allocating based on malicious or malformed data.
const SAFE_ALLOCATION_LIMIT: usize = 1 << 32;

impl BlockReader {
    pub(crate) fn new(chunks: Chunks) -> Self {
        Self::with_format(chunks, WireFormat::NATIVE)
    }
}

impl<S: ChunkSource> BlockReader<S> {
    /// Create a reader for blocks in the given format.
    pub(crate) fn with_format(chunks: S, format: WireFormat) -> Self {
        BlockReader {
            inner: ReaderInner {
                chunks,
                last_chunk: Bytes::new(),
            },
            prefix_queue: VecDeque::new(),
            format,
        }
    }

    /// Return the chunk source and the data that the reader received but did not use.
    #[cfg(feature = "tcp")]
    pub(crate) fn into_parts(self) -> (S, Bytes) {
        (self.inner.chunks, self.inner.last_chunk)
    }

    /// Read one block of the native TCP protocol.
    ///
    /// Unlike [`Self::read_block()`], an empty block is a valid result:
    /// the server sends blocks without rows to describe the columns of a result
    /// and to mark the end of data.
    ///
    /// **NOT** cancel-safe.
    #[cfg(feature = "tcp")]
    pub(crate) async fn read_protocol_block(&mut self) -> Result<Block, Error> {
        self.read_block_info().await?;

        let num_columns = self.inner.read_varuint().await?;
        let num_rows = self.inner.read_varuint().await?;

        let num_columns = usize::try_from(num_columns)
            .ok()
            .filter(|&n| n <= SAFE_ALLOCATION_LIMIT)
            .ok_or_else(|| {
                read_error!("number of columns in block is out of range: {num_columns}")
            })?;
        let num_rows = usize::try_from(num_rows)
            .ok()
            .filter(|&n| n <= SAFE_ALLOCATION_LIMIT)
            .ok_or_else(|| read_error!("number of rows in block is out of range: {num_rows}"))?;

        let mut columns = Vec::with_capacity(cmp::min(num_columns, 1024));

        for col in 0..num_columns {
            columns.push(self.read_column(num_rows).await.inspect_err(|e| {
                tracing::debug!(col, "error reading column: {e:?}");
            })?);
        }

        Ok(Block::from_columns(columns, num_rows))
    }

    /// Skip the `BlockInfo` structure: a list of `(field number, value)` pairs ending with `0`.
    #[cfg(feature = "tcp")]
    async fn read_block_info(&mut self) -> Result<(), Error> {
        if self.format.revision == 0 {
            return Ok(());
        }

        loop {
            match self.inner.read_varuint().await? {
                0 => return Ok(()),
                // `is_overflows: bool`
                1 => {
                    self.inner.read_bytes_fixed::<1>().await?;
                }
                // `bucket_num: Int32`
                2 => {
                    self.inner.read_bytes_fixed::<4>().await?;
                }
                other => {
                    return Err(read_error!("unknown BlockInfo field number: {other}").into());
                }
            }
        }
    }

    /// **NOT** cancel-safe. Intended to be wrapped in an async task for cancel-safety.
    pub(crate) async fn read_block(&mut self) -> Result<Option<Block>, Error> {
        if !self.inner.try_read_chunk().await? {
            // Native format over HTTP doesn't send a zero-row block on an empty resultset,
            // it just sends nothing at all.
            return Ok(None);
        }

        let num_columns = self.inner.read_varuint().await?;
        let num_rows = self.inner.read_varuint().await?;

        let num_rows = usize::try_from(num_rows)
            .map_err(|_| read_error!("number of rows in block is out of range: {num_rows}"))?;

        if num_rows > SAFE_ALLOCATION_LIMIT {
            return Err(read_error!(
                "number of rows in block exceeds safe limit: {num_rows} vs {SAFE_ALLOCATION_LIMIT}"
            )
            .into());
        }

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
        let type_name = self.inner.read_string().await?;

        let data_type = type_name.as_str().ok_or_else(|| {
            read_error!("invalid data type {type_name:?}").with_column(&name, None)
        })?;

        let data_type = DataTypeNode::new(data_type).map_err(|e| {
            read_error!("error parsing data type {data_type:?}")
                .with_column(&name, None)
                .with_source(e)
        })?;

        let kind = if self.format.revision >= REVISION_WITH_CUSTOM_SERIALIZATION {
            let [has_custom] = self.inner.read_bytes_fixed::<1>().await?;

            if has_custom != 0 {
                self.read_serialization_kind(&data_type)
                    .await
                    .err_with_column(&name, Some(&data_type))?
            } else {
                SerializationKind::Default
            }
        } else {
            SerializationKind::Default
        };

        if num_rows != 0 {
            self.read_state_prefix(&name, &data_type)
                .await
                // so every error message doesn't have to manually mention the column name/type
                .err_with_column(&name, Some(&data_type))?;
        }

        let layout = if kind.is_default() {
            self.read_data(&name, &data_type, num_rows, num_rows != 0)
                .await
        } else {
            self.read_data_with_kind(&name, &data_type, &kind, num_rows)
                .await
        }
        .err_with_column(&name, Some(&data_type))?;

        Ok(Column {
            name,
            data_type,
            type_name,
            layout,
        })
    }

    async fn read_state_prefix(
        &mut self,
        column_name: &MaybeUtf8,
        data_type: &DataTypeNode,
    ) -> Result<(), Error> {
        let data_type = wire_type(data_type);

        match &*data_type {
            DataTypeNode::LowCardinality(_) => {
                let prefix = self.inner.read_int64().await?;

                if prefix != 1 {
                    // sharedDictionariesWithAdditionalKeys
                    return Err(read_error!(
                        "unexpected/unsupported serialization version of LowCardinality: \
                         expected 1, got {prefix}"
                    )
                    .into());
                }

                self.prefix_queue.push_back(StatePrefix::LowCardinality);
            }
            // State prefix for composite types needs to be read _before_ the composite type layout
            DataTypeNode::Array(inner_type) | DataTypeNode::Nullable(inner_type) => {
                Box::pin(self.read_state_prefix(column_name, inner_type)).await?;
            }
            DataTypeNode::Tuple(types) => {
                for ty in types {
                    Box::pin(self.read_state_prefix(column_name, ty)).await?;
                }
            }
            DataTypeNode::Map([key_ty, val_ty]) => {
                Box::pin(self.read_state_prefix(column_name, key_ty)).await?;
                Box::pin(self.read_state_prefix(column_name, val_ty)).await?;
            }
            DataTypeNode::Variant(types) => {
                let mode = if !self.format.variant_mode_prefix {
                    VariantMode::Basic
                } else {
                    match self.inner.read_uint64().await? {
                        0 => VariantMode::Basic,
                        1 => VariantMode::Compact,
                        other => {
                            return Err(read_error!(
                                "unknown discriminators serialization mode of Variant: {other}"
                            )
                            .into());
                        }
                    }
                };

                self.prefix_queue.push_back(StatePrefix::Variant(mode));

                for ty in types {
                    Box::pin(self.read_state_prefix(column_name, ty)).await?;
                }
            }
            DataTypeNode::JSON | DataTypeNode::JsonWithHint(_) => {
                /// `SerializationObject::SerializationVersion::STRING`
                const JSON_AS_STRING: u64 = 1;

                let version = self.inner.read_uint64().await?;

                if version != JSON_AS_STRING {
                    return Err(read_error!(
                        "unsupported serialization version of JSON: {version}; \
                         only JSON serialized as strings is supported \
                         (setting `output_format_native_write_json_as_string = 1`)"
                    )
                    .into());
                }

                self.prefix_queue.push_back(StatePrefix::JsonString);
            }
            DataTypeNode::Dynamic | DataTypeNode::AggregateFunction(..) => {
                return Err(
                    read_error!("unimplemented deserialization of (sub)type {data_type}").into(),
                );
            }
            _ => (),
        }

        Ok(())
    }

    async fn read_serialization_kind(
        &mut self,
        data_type: &DataTypeNode,
    ) -> Result<SerializationKind, Error> {
        let [kind] = self.inner.read_bytes_fixed::<1>().await?;

        let kind = match kind {
            0 => SerializationKind::Default,
            1 => SerializationKind::Sparse,
            other => {
                return Err(read_error!("unsupported serialization kind: {other}").into());
            }
        };

        if let DataTypeNode::Tuple(types) = &*wire_type(data_type) {
            let mut elements = Vec::with_capacity(types.len());

            for ty in types {
                elements.push(Box::pin(self.read_serialization_kind(ty)).await?);
            }

            return Ok(SerializationKind::Tuple(Box::new(kind), elements));
        }

        Ok(kind)
    }

    /// Read a column that has a serialization kind other than the default.
    ///
    /// Only revisions below `DBMS_MIN_REVISION_WITH_SPARSE_SERIALIZATION` are used, so the server
    /// sends sparse columns only if it is older than this revision (ClickHouse 22.x and 23.x).
    async fn read_data_with_kind(
        &mut self,
        column_name: &MaybeUtf8,
        data_type: &DataTypeNode,
        kind: &SerializationKind,
        num_values: usize,
    ) -> Result<Layout, Error> {
        let block_nonempty = num_values != 0;

        match kind {
            SerializationKind::Default => {
                self.read_data(column_name, data_type, num_values, block_nonempty)
                    .await
            }
            SerializationKind::Sparse => {
                self.read_sparse_data(column_name, data_type, num_values)
                    .await
            }
            SerializationKind::Tuple(own, elements) => {
                let wire = wire_type(data_type);

                let (DataTypeNode::Tuple(types), SerializationKind::Default) = (&*wire, &**own)
                else {
                    return Err(read_error!(
                        "unsupported serialization kind {kind:?} for type {data_type}"
                    )
                    .into());
                };

                let mut layouts = Vec::with_capacity(types.len());

                for (ty, kind) in types.iter().zip(elements) {
                    layouts.push(
                        Box::pin(self.read_data_with_kind(column_name, ty, kind, num_values))
                            .await?,
                    );
                }

                Ok(Layout {
                    kind: LayoutKind::Tuple {
                        layouts: layouts.into(),
                    },
                    nulls: None,
                    num_values,
                })
            }
        }
    }

    /// Read a sparse column and expand it to the usual layout.
    ///
    /// The server sends the offsets of non-default rows, then the non-default values.
    async fn read_sparse_data(
        &mut self,
        column_name: &MaybeUtf8,
        data_type: &DataTypeNode,
        num_values: usize,
    ) -> Result<Layout, Error> {
        /// `SerializationSparse`: the group with this flag holds the trailing default rows.
        const END_OF_GRANULE_FLAG: u64 = 1 << 62;

        let mut value_rows = Vec::new();
        let mut row = 0usize;

        if num_values != 0 {
            loop {
                let group = self.inner.read_varuint().await?;
                let defaults = usize::try_from(group & !END_OF_GRANULE_FLAG)
                    .map_err(|_| read_error!("sparse offset is out of range: {group}"))?;

                row = row
                    .checked_add(defaults)
                    .filter(|&row| row <= num_values)
                    .ok_or_else(|| read_error!("sparse offsets exceed the number of rows"))?;

                if group & END_OF_GRANULE_FLAG != 0 {
                    break;
                }

                if row >= num_values {
                    return Err(read_error!("sparse offsets exceed the number of rows").into());
                }

                value_rows.push(row);
                row += 1;
            }
        }

        if row != num_values {
            return Err(read_error!(
                "sparse offsets cover {row} rows, but the block has {num_values} rows"
            )
            .into());
        }

        let values = self
            .read_data(column_name, data_type, value_rows.len(), num_values != 0)
            .await?;

        let kind = match values.kind {
            LayoutKind::Fixed { type_width, data } if values.nulls.is_none() && type_width > 0 => {
                let total_bytes = type_width.checked_mul(num_values).ok_or_else(|| {
                    read_error!("data size is too large: {num_values} rows X {type_width} bytes")
                })?;
                let mut dense = vec![0u8; total_bytes];

                // `row < num_values`, so the products cannot overflow
                for (chunk, &row) in data.chunks_exact(type_width).zip(&value_rows) {
                    let start = row * type_width;
                    if let Some(dst) = dense.get_mut(start..start + type_width) {
                        dst.copy_from_slice(chunk);
                    }
                }

                LayoutKind::Fixed {
                    type_width,
                    data: dense.into(),
                }
            }
            LayoutKind::Variable { end_offsets, data } if values.nulls.is_none() => {
                let mut dense_offsets = Vec::with_capacity(num_values);
                let mut next_value = value_rows.iter().zip(end_offsets.iter()).peekable();
                let mut end = 0;

                for row in 0..num_values {
                    if let Some((_, &value_end)) =
                        next_value.next_if(|&(&value_row, _)| value_row == row)
                    {
                        end = value_end;
                    }
                    dense_offsets.push(end);
                }

                LayoutKind::Variable {
                    end_offsets: dense_offsets.into(),
                    data,
                }
            }
            _ => {
                return Err(read_error!(
                    "sparse serialization of type {data_type} is not supported"
                )
                .into());
            }
        };

        Ok(Layout {
            kind,
            nulls: None,
            num_values,
        })
    }

    async fn read_data(
        &mut self,
        column_name: &MaybeUtf8,
        data_type: &DataTypeNode,
        num_values: usize,
        block_nonempty: bool,
    ) -> Result<Layout, Error> {
        if num_values > SAFE_ALLOCATION_LIMIT {
            // Ideally one of the other checks will give the user a more specific error message.
            return Err(read_error!(
                "number of values exceeds safe allocation limit: \
                 {num_values} vs {SAFE_ALLOCATION_LIMIT}"
            )
            .into());
        }

        let wire = wire_type(data_type);

        let (data_type, nulls) = if let DataTypeNode::Nullable(inner) = &*wire {
            // Read off the null bitmap
            (&**inner, Some(self.inner.read_bytes(num_values).await?))
        } else {
            (&*wire, None)
        };

        if let Some(type_width) = type_fixed_width(data_type) {
            let total_bytes = type_width.checked_mul(num_values).ok_or_else(|| {
                read_error!("data size is too large: {num_values} rows X {type_width} bytes")
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

        match data_type.remove_simple_aggregate_function() {
            DataTypeNode::String => Ok(Layout {
                kind: self.inner.read_strings(num_values).await?,
                nulls,
                num_values,
            }),
            DataTypeNode::JSON | DataTypeNode::JsonWithHint(_) => {
                if block_nonempty {
                    match self.prefix_queue.pop_front() {
                        Some(StatePrefix::JsonString) => (),
                        other => {
                            return Err(read_error!(
                                "error reading (sub)type {data_type}: \
                                 expected JSON state prefix, got {other:?}"
                            )
                            .into());
                        }
                    }
                }

                Ok(Layout {
                    kind: self.inner.read_strings(num_values).await?,
                    nulls,
                    num_values,
                })
            }
            DataTypeNode::Variant(types) => {
                let mode = if block_nonempty {
                    match self.prefix_queue.pop_front() {
                        Some(StatePrefix::Variant(mode)) => mode,
                        other => {
                            return Err(read_error!(
                                "error reading (sub)type {data_type}: \
                                 expected Variant state prefix, got {other:?}"
                            )
                            .into());
                        }
                    }
                } else {
                    VariantMode::Basic
                };

                let discriminators = self.read_variant_discriminators(mode, num_values).await?;

                let mut counts = vec![0usize; types.len()];

                for &discriminator in discriminators.iter() {
                    if discriminator == VARIANT_NULL_DISCRIMINATOR {
                        continue;
                    }

                    let count = counts.get_mut(usize::from(discriminator)).ok_or_else(|| {
                        read_error!(
                            "Variant discriminator {discriminator} is out of range \
                             for {} variant types",
                            types.len()
                        )
                    })?;
                    *count += 1;
                }

                let mut variants = Vec::with_capacity(types.len());

                for (ty, count) in types.iter().zip(counts) {
                    variants.push(
                        Box::pin(self.read_data(column_name, ty, count, block_nonempty)).await?,
                    );
                }

                Ok(Layout {
                    kind: LayoutKind::Variant {
                        discriminators,
                        variants: variants.into(),
                    },
                    nulls,
                    num_values,
                })
            }
            DataTypeNode::Array(elem_type) => {
                let end_indices = self.inner.read_array_indices(num_values).await?;

                let total_len = end_indices.last().copied().unwrap_or(0);

                if total_len > SAFE_ALLOCATION_LIMIT {
                    return Err(read_error!(
                        "total length of arrays in column exceeds safe limit: \
                         {total_len} vs {SAFE_ALLOCATION_LIMIT}"
                    )
                    .into());
                }

                // Recursive `async fn` currently requires boxing
                let elem_layout =
                    Box::pin(self.read_data(column_name, elem_type, total_len, block_nonempty))
                        .await?;

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
                    layouts.push(
                        Box::pin(self.read_data(column_name, ty, num_values, block_nonempty))
                            .await?,
                    );
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

                if total_len > SAFE_ALLOCATION_LIMIT {
                    return Err(read_error!(
                        "total number of map entries exceeds safe limit: \
                         {total_len} vs {SAFE_ALLOCATION_LIMIT}"
                    )
                    .into());
                }

                let key_layout =
                    Box::pin(self.read_data(column_name, key_ty, total_len, block_nonempty))
                        .await?;
                let value_layout =
                    Box::pin(self.read_data(column_name, val_ty, total_len, block_nonempty))
                        .await?;

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
                        self.read_lc_data(column_name, inner_type, num_values, block_nonempty)
                            .await?,
                    ),
                    // Server disallows `Nullable(LowCardinality(...))` but we don't gain much
                    // from asserting that here
                    nulls,
                    num_values,
                })
            }
            _ => Err(read_error!("data type {data_type:?} not implemented").into()),
        }
    }

    async fn read_variant_discriminators(
        &mut self,
        mode: VariantMode,
        num_values: usize,
    ) -> Result<Bytes, Error> {
        if mode == VariantMode::Basic || num_values == 0 {
            return self.inner.read_bytes(num_values).await;
        }

        // Compact mode: the native format writes the whole column as one granule.
        let granule_rows = self.inner.read_varuint().await?;

        if usize::try_from(granule_rows).ok() != Some(num_values) {
            return Err(read_error!(
                "Variant granule has {granule_rows} rows, but {num_values} rows were expected"
            )
            .into());
        }

        match self.inner.read_bytes_fixed::<1>().await? {
            // `CompactDiscriminatorsGranuleFormat::PLAIN`
            [0] => self.inner.read_bytes(num_values).await,
            // `CompactDiscriminatorsGranuleFormat::COMPACT`: one discriminator for all rows
            [1] => {
                let [discriminator] = self.inner.read_bytes_fixed::<1>().await?;
                Ok(Bytes::from(vec![discriminator; num_values]))
            }
            [other] => Err(read_error!("unknown Variant granule format: {other}").into()),
        }
    }

    async fn read_lc_data(
        &mut self,
        column_name: &MaybeUtf8,
        inner_type: &DataTypeNode,
        num_rows: usize,
        block_nonempty: bool,
    ) -> Result<LayoutLowCardinality, Error> {
        const HAS_ADDITIONAL_KEYS_BIT: u64 = 0x200;
        const NEEDS_UPDATE_DICTIONARY_BIT: u64 = 0x400;

        // NEED_GLOBAL_DICTIONARY_BIT (0x800) should NEVER be set
        const VALID_FLAG_BITS: u64 = HAS_ADDITIONAL_KEYS_BIT | NEEDS_UPDATE_DICTIONARY_BIT;

        // `LowCardinality(Nullable)` doesn't emit a null map, instead `dict[0]` is the null value
        let (non_nullable, is_nullable) = if let DataTypeNode::Nullable(inner) = inner_type {
            (&**inner, true)
        } else {
            (inner_type, false)
        };

        // Array(LowCardinality(...)) still emits a prefix even if the array itself is empty
        if block_nonempty {
            // Ensure that we read our version prefix even if it doesn't tell us anything
            match self.prefix_queue.pop_front() {
                Some(StatePrefix::LowCardinality) => (),
                other => {
                    return Err(read_error!(
                        "error reading (sub)type LowCardinality({inner_type}): \
                         expected state prefix 0x01, got {other:?}"
                    )
                    .into());
                }
            }
        }

        if num_rows == 0 {
            return Ok(LayoutLowCardinality {
                keys: Box::new([]),
                dict: Box::new(
                    // The easiest way to implement this is to delegate back to `read_type_data()`
                    Box::pin(self.read_data(column_name, non_nullable, num_rows, block_nonempty))
                        .await?,
                ),
                is_nullable,
            });
        }

        let metadata = self.inner.read_uint64().await?;

        let key_type = LcKeyType::try_from_metadata(metadata)?;

        let flags = metadata & !LcKeyType::MASK;

        if flags != VALID_FLAG_BITS {
            return Err(read_error!(
                "(sub)type LowCardinality({inner_type}) has \
                 invalid or unexpected metadata bits (metadata: {metadata:X}); \
                 expected: {VALID_FLAG_BITS:X}, received: {flags:X}",
            )
            .into());
        }

        let dict_len = self.inner.read_uint64().await?;
        let dict_len = usize::try_from(dict_len).map_err(|_| {
            read_error!("LowCardinality({inner_type}) dictionary size too large: {dict_len}")
        })?;

        // called from `self.read_type_data()` so we have to box
        let dict = Box::pin(self.read_data(column_name, non_nullable, dict_len, block_nonempty))
            .await
            .map_err(|e| {
                read_error!("error reading LowCardinality({inner_type}) dictionary").with_source(e)
            })?;

        let keys_len = self.inner.read_uint64().await?;
        let keys_len = usize::try_from(keys_len).map_err(|_| {
            read_error!("LowCardinality({inner_type}) keys count too large: {keys_len}")
        })?;

        if keys_len != num_rows {
            return Err(read_error!(
                "LowCardinality({inner_type}) keys count does not match number of rows in block: \
                 {keys_len} vs {num_rows}; this likely means a bug or corrupted data"
            )
            .into());
        }

        let keys = self.inner.read_lc_keys(key_type, num_rows).await?;

        Ok(LayoutLowCardinality {
            keys,
            dict: Box::new(dict),
            is_nullable,
        })
    }
}

impl<S: ChunkSource> ReaderInner<S> {
    /// Read `num_values` strings, each one prefixed with its length as a `VarUInt`.
    async fn read_strings(&mut self, num_values: usize) -> Result<LayoutKind, Error> {
        // Assume default growth strategy is fine
        let mut data = BytesMut::new();
        let mut end_offsets = Vec::with_capacity(cmp::min(num_values, 1 << 20));

        let mut row = 0;

        while row < num_values {
            // Fast path: read the strings that are complete in the current chunk.
            row += self.read_buffered_strings(num_values - row, &mut data, &mut end_offsets)?;

            if row == num_values {
                break;
            }

            let len = self.read_varuint().await?;

            let len = usize::try_from(len)
                .map_err(|_| read_error!("string #{row} length {len} is out of range"))?;

            self.read_bytes_into(len, &mut data).await?;
            end_offsets.push(data.len());
            row += 1;
        }

        Ok(LayoutKind::Variable {
            data: data.freeze(),
            end_offsets: end_offsets.into(),
        })
    }

    /// Read up to `max` strings that are complete in the current chunk, and return the number.
    fn read_buffered_strings(
        &mut self,
        max: usize,
        data: &mut BytesMut,
        end_offsets: &mut Vec<usize>,
    ) -> Result<usize, Error> {
        let mut input = &self.last_chunk[..];
        let mut count = 0;

        while count < max {
            let mut rest = input;

            let ControlFlow::Break(len) = ParseVarUInt::default()
                .feed(&mut rest)
                .map_err(|e| read_error!("error parsing VarUInt").with_source(e))?
            else {
                break;
            };

            let Some((value, rest)) = usize::try_from(len)
                .ok()
                .and_then(|len| rest.split_at_checked(len))
            else {
                break;
            };

            if data.len().saturating_add(value.len()) > SAFE_ALLOCATION_LIMIT {
                return Err(read_error!(
                    "safe allocation limit exceeded: {} vs {SAFE_ALLOCATION_LIMIT}",
                    data.len().saturating_add(value.len())
                )
                .into());
            }

            data.extend_from_slice(value);
            end_offsets.push(data.len());
            input = rest;
            count += 1;
        }

        let consumed = self.last_chunk.len() - input.len();
        self.consume(consumed);
        Ok(count)
    }

    async fn read_string(&mut self) -> Result<MaybeUtf8, Error> {
        let len = self.read_varuint().await?;

        let len =
            usize::try_from(len).map_err(|_| read_error!("string length too large: {len}"))?;

        if len > SAFE_ALLOCATION_LIMIT {
            return Err(read_error!(
                "string size exceeds safe allocation limit: {len} vs {SAFE_ALLOCATION_LIMIT}"
            )
            .into());
        }

        Ok(self.read_bytes(len).await?.into())
    }

    async fn read_varuint(&mut self) -> Result<u64, Error> {
        let mut parser = ParseVarUInt::default();

        loop {
            if let ControlFlow::Break(val) = parser
                .feed(&mut self.last_chunk)
                .map_err(|e| read_error!("error parsing VarUInt").with_source(e))?
            {
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
                .map_err(|_| read_error!("array length out of range: {length}"))?;

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
                Error::DataFormat(
                    format!("LowCardinality dictionary key out of range at index {i}: {key}")
                        .into(),
                )
            })?;

            keys.push(key);
        }

        Ok(keys.into())
    }

    async fn read_bytes(&mut self, len: usize) -> Result<Bytes, Error> {
        if len == 0 {
            return Ok(Bytes::new());
        }

        self.read_chunk().await?;

        if self.last_chunk.len() >= len {
            return Ok(self.last_chunk.split_to(len));
        }

        let mut buf = BytesMut::with_capacity(len);
        self.read_bytes_into(len, &mut buf).await?;

        Ok(buf.freeze())
    }

    async fn read_bytes_into(&mut self, mut amt: usize, buf: &mut BytesMut) -> Result<(), Error> {
        let expected_len = buf.len().saturating_add(amt);

        if expected_len > SAFE_ALLOCATION_LIMIT {
            return Err(read_error!(
                "safe allocation limit exceeded: {expected_len} vs {SAFE_ALLOCATION_LIMIT}"
            )
            .into());
        }

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
        if !self.try_read_chunk().await? {
            tracing::trace!("error: not enough data");
            return Err(Error::NotEnoughData);
        }

        Ok(())
    }

    async fn try_read_chunk(&mut self) -> Result<bool, Error> {
        if self.last_chunk.is_empty() {
            let Some(chunk) = self.chunks.next_chunk().await else {
                return Ok(false);
            };

            self.last_chunk = chunk?;
        }

        Ok(true)
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
                return Err(read_error!("unknown LowCardinality key type: {unknown}").into());
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

/// The type that describes how a column of `data_type` is serialized.
///
/// Removes wrappers that do not change the serialization and replaces geo types
/// with their equivalents.
pub(crate) fn wire_type(data_type: &DataTypeNode) -> Cow<'_, DataTypeNode> {
    match data_type {
        DataTypeNode::SimpleAggregateFunction(_, inner) => wire_type(inner),
        other => geo_equivalent(other).map_or(Cow::Borrowed(other), Cow::Owned),
    }
}

/// The serialization of geo types is the same as the serialization of these types:
/// <https://clickhouse.com/docs/sql-reference/data-types/geo>
pub(crate) fn geo_equivalent(data_type: &DataTypeNode) -> Option<DataTypeNode> {
    fn point() -> DataTypeNode {
        DataTypeNode::Tuple(vec![DataTypeNode::Float64, DataTypeNode::Float64])
    }

    fn array(inner: DataTypeNode) -> DataTypeNode {
        DataTypeNode::Array(Box::new(inner))
    }

    Some(match data_type {
        DataTypeNode::Point => point(),
        DataTypeNode::Ring | DataTypeNode::LineString => array(point()),
        DataTypeNode::Polygon | DataTypeNode::MultiLineString => array(array(point())),
        DataTypeNode::MultiPolygon => array(array(array(point()))),
        _ => return None,
    })
}

impl Display for BlockReadError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "error reading native block")?;

        match (&self.column_name, &self.column_type) {
            (Some(col_name), Some(col_type)) => {
                write!(f, " at column `{col_name} {col_type}`")?;
            }
            (Some(col_name), None) => {
                write!(f, " at column `{col_name}`")?;
            }
            _ => (),
        }

        write!(f, ": {}", self.message)
    }
}

impl std::error::Error for BlockReadError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        // `self.source.as_deref()` on its own doesn't trigger the correct coercion
        Some(self.source.as_deref()?)
    }
}

impl BlockReadError {
    fn with_column(mut self, name: &MaybeUtf8, ty: Option<&DataTypeNode>) -> Self {
        self.set_column(name, ty);
        self
    }

    fn set_column(&mut self, name: &MaybeUtf8, ty: Option<&DataTypeNode>) {
        self.column_name = Some(name.clone());
        self.column_type = ty.cloned();
    }

    fn with_source(
        mut self,
        source: impl Into<Box<dyn std::error::Error + Send + Sync + 'static>>,
    ) -> Self {
        self.source = Some(source.into());
        self
    }

    /// Get the name of the column that was being read when the error occurred.
    ///
    /// Returns `None` if a column was not being read yet, or if the column name is not UTF-8.
    ///
    /// See [`Self::column_name_bytes()`] if handling column names that may not be UTF-8.
    pub fn column_name(&self) -> Option<&str> {
        self.column_name.as_ref().and_then(MaybeUtf8::as_str)
    }

    /// Get the name of the column that was being read when the error occurred.
    ///
    /// Returns `None` if a column was not being read yet.
    pub fn column_name_bytes(&self) -> Option<&[u8]> {
        self.column_name.as_ref().map(MaybeUtf8::as_bytes)
    }
}

trait ResultExt {
    fn err_with_column(self, name: &MaybeUtf8, data_type: Option<&DataTypeNode>) -> Self;
}

impl<T> ResultExt for Result<T, crate::Error> {
    fn err_with_column(self, name: &MaybeUtf8, data_type: Option<&DataTypeNode>) -> Self {
        let Err(e) = self else {
            return self;
        };

        let Error::DataFormat(e) = e else {
            return Err(e);
        };

        Err(Error::DataFormat(
            e.downcast::<BlockReadError>().map_or_else(
                |e| e,
                |mut e| {
                    e.set_column(name, data_type);
                    e
                },
            ),
        ))
    }
}

impl<T> ResultExt for Result<T, BlockReadError> {
    fn err_with_column(self, name: &MaybeUtf8, data_type: Option<&DataTypeNode>) -> Self {
        self.map_err(|e| e.with_column(name, data_type))
    }
}
