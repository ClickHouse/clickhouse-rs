use crate::error::BoxedError;
use crate::native::encode::{Encode, ValueWriter};
use crate::native::string::MaybeUtf8;
use crate::native::utils::{DebugFixedData, DebugNullMap, DebugVariableData, type_fixed_width};
use crate::native::{Block, Column, Layout, LayoutKind};
use bytes::{BufMut, BytesMut};
use clickhouse_types::DataTypeNode;
use hashbrown::{HashMap, hash_map};
use std::collections::VecDeque;
use std::fmt::{Debug, Formatter};
use std::marker::PhantomData;
use std::mem;

#[derive(Default)]
pub struct BlockBuilder {
    column_names: HashMap<MaybeUtf8, usize>,
    columns: Vec<ColumnBuilderRaw>,
}

#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum BlockBuilderError {
    #[error(
        "attempting to overwrite existing column `{name} {existing_type}` with a different type: {new_type}"
    )]
    ColumnExists {
        name: String,
        existing_type: DataTypeNode,
        new_type: DataTypeNode,
    },
    #[error("unsupported type or subtype of column `{column_name}`:  `{data_type}`")]
    UnsupportedType {
        column_name: String,
        data_type: DataTypeNode,
    },
    #[error(
        "block contains columns of mismatched lengths; \
         longest column: `{longest_column}` (len: {longest_len}), \
         shortest column: `{shortest_column}` (len: {shortest_len})"
    )]
    MismatchedLengths {
        longest_column: String,
        longest_len: usize,
        shortest_column: String,
        shortest_len: usize,
    },
    #[error("column `{column_name} {column_type}` contains invalid data: {message}")]
    ColumnDataInvalid {
        column_name: String,
        column_type: DataTypeNode,
        message: String,
    },
    #[error("cannot build an empty block")]
    BlockEmpty,
}

impl BlockBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    /// Add an empty column to the block, or get a reference to an existing one.
    ///
    /// The given data type will have any `LowCardinality(_)` or `SimpleAggregateFunction(...)`
    /// wrappers erased for ease of implementation.
    ///
    /// # Errors
    /// * If a column with the same name already exists, but with a different type.
    /// * If the given type is not currently supported by the implementation.
    pub fn upsert_column<T: Encode>(
        &mut self,
        name: impl Into<String>,
    ) -> Result<ColumnBuilder<'_, T>, Box<BlockBuilderError>> {
        self.upsert_column_with(name, T::produces())
            .map(|inner| ColumnBuilder {
                inner,
                _marker: PhantomData,
            })
    }

    fn upsert_column_with(
        &mut self,
        name: impl Into<String>,
        data_type: DataTypeNode,
    ) -> Result<&mut ColumnBuilderRaw, Box<BlockBuilderError>> {
        let data_type = erase_wrappers(data_type);

        match self.column_names.entry(MaybeUtf8::from_string(name)) {
            hash_map::Entry::Occupied(existing) => {
                let col = &mut self.columns[*existing.get()];

                if col.data_type != data_type {
                    return Err(BlockBuilderError::ColumnExists {
                        name: col.name.to_string(),
                        existing_type: col.data_type.clone(),
                        new_type: data_type,
                    }
                    .into());
                }

                Ok(col)
            }
            hash_map::Entry::Vacant(vacant) => {
                let col = ColumnBuilderRaw {
                    layout: LayoutBuilder::new(vacant.key(), &data_type)?,
                    data_type,
                    name: vacant.key().clone(),
                };

                vacant.insert(self.columns.len());
                // FIXME: replace with `Vec::push_mut()` after Rust 1.95
                self.columns.push(col);
                Ok(self.columns.last_mut().unwrap())
            }
        }
    }

    pub fn build(&mut self) -> Result<Block, Box<BlockBuilderError>> {
        let mut num_rows = 0;

        // Check that all the columns have the same length
        if let Some((mut longest_col, columns)) = self.columns.split_first() {
            let mut len_mismatch = false;

            let mut shortest_col = longest_col;

            num_rows = longest_col.num_values();

            for col in columns {
                if col.num_values() > longest_col.num_values() {
                    longest_col = col;
                    len_mismatch = true;
                }

                if col.num_values() < shortest_col.num_values() {
                    shortest_col = col;
                    len_mismatch = true;
                }
            }

            if len_mismatch {
                return Err(BlockBuilderError::MismatchedLengths {
                    longest_column: longest_col.name.to_string(),
                    longest_len: longest_col.num_values(),
                    shortest_column: shortest_col.name.to_string(),
                    shortest_len: shortest_col.num_values(),
                }
                .into());
            }
        }

        if num_rows == 0 {
            return Err(BlockBuilderError::BlockEmpty.into());
        }

        // Note: try to perform as much validation as possible before consuming `self`
        for col in &self.columns {
            col.layout.validate(&col.data_type).map_err(|message| {
                BlockBuilderError::ColumnDataInvalid {
                    column_name: col.name.to_string(),
                    column_type: col.data_type.clone(),
                    message,
                }
            })?;
        }

        let columns = self
            .columns
            .drain(..)
            .map(|col| Column {
                name: col.name,
                data_type: col.data_type,
                layout: col.layout.into_layout(),
            })
            .collect();

        Ok(Block {
            columns,
            column_names: mem::take(&mut self.column_names),
            num_rows,
        })
    }
}

impl Debug for BlockBuilder {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BlockBuilder")
            // Ignore `column_names`, implementation detail
            .field("columns", &self.columns)
            .finish()
    }
}

pub struct ColumnBuilder<'a, T> {
    inner: &'a mut ColumnBuilderRaw,
    _marker: PhantomData<fn(T)>,
}

impl<T> ColumnBuilder<'_, T>
where
    T: Encode,
{
    pub fn num_values(&self) -> usize {
        self.inner.layout.num_values()
    }

    pub fn add(&mut self, value: T) -> Result<&mut Self, BoxedError> {
        // Compatibility checked when this was created
        self.inner.add_unchecked(value)?;
        Ok(self)
    }

    pub fn add_all<I>(&mut self, values: I) -> Result<&mut Self, BoxedError>
    where
        I: IntoIterator<Item = T>,
    {
        self.inner.add_all_unchecked(values)?;
        Ok(self)
    }
}

impl<T> Debug for ColumnBuilder<'_, T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.inner.fmt(f)
    }
}

#[derive(Debug)] // Derived impl works for us here
struct ColumnBuilderRaw {
    name: MaybeUtf8,
    data_type: DataTypeNode,
    layout: LayoutBuilder,
}

impl ColumnBuilderRaw {
    fn num_values(&self) -> usize {
        self.layout.num_values()
    }

    fn add_unchecked<T>(&mut self, value: T) -> Result<&mut Self, BoxedError>
    where
        T: Encode,
    {
        value.encode(&mut ValueWriter {
            data_type: &self.data_type,
            layout: &mut self.layout,
        })?;

        Ok(self)
    }

    fn add_all_unchecked<I>(&mut self, values: I) -> Result<&mut Self, BoxedError>
    where
        I: IntoIterator,
        I::Item: Encode,
    {
        let mut values = values.into_iter();

        while let Some(value) = values.next() {
            // Catches an infinite-length iterator that returns `usize::MAX` for its size hint
            // This is comparable to the default behavior of `impl Extend<T> for Vec<T>`
            let (lower_bound, _) = values.size_hint();
            self.layout.reserve(lower_bound.saturating_add(1));

            self.add_unchecked(value)?;
        }

        Ok(self)
    }
}

// These types may be identical to `Layout` but they need to use growable containers.
// `LowCardinality` is also deliberately omitted in the initial implementation
// since the server can do the transformation automatically.
pub(super) struct LayoutBuilder {
    pub(super) kind: LayoutBuilderKind,
    pub(super) nulls: Option<BytesMut>,
}

pub(super) enum LayoutBuilderKind {
    /// Fixed layout. Width of each cell depends only on [`DataTypeNode`].
    Fixed {
        type_width: usize,
        data: BytesMut,
    },
    /// Variable-length data (namely strings)
    Variable {
        /// Ending offset of each string in `data`.
        ///
        /// The offset of the first string is always `0` unless this is empty.
        end_offsets: Vec<usize>,
        // Each `Bytes` instance is 4 `usizes` (32 bytes on 64-bit),
        // so we save 24 bytes per string by linearizing the string data and storing offsets,
        // assuming many small strings instead of fewer big ones, which also amortizes allocations
        data: BytesMut,
    },
    /// Array data. Element data governed by `elem_layout`.
    Array {
        /// Ending index of each array in `elem_layout`.
        end_indices: Vec<usize>,
        elem_layout: Box<LayoutBuilder>,
    },
    Tuple {
        layouts: Box<[LayoutBuilder]>,
    },
    Map {
        key_val_layouts: Box<[LayoutBuilder; 2]>,
        end_indices: Vec<usize>,
    },
}

impl LayoutBuilder {
    fn new(
        column_name: &MaybeUtf8,
        data_type: &DataTypeNode,
    ) -> Result<Self, Box<BlockBuilderError>> {
        let (non_nullable, nulls) = if let DataTypeNode::Nullable(inner) = data_type {
            if is_forbidden_nullable(inner) {
                return Err(Box::new(BlockBuilderError::UnsupportedType {
                    column_name: column_name.to_string(),
                    data_type: data_type.clone(),
                }));
            }

            (&**inner, Some(BytesMut::new()))
        } else {
            (data_type, None)
        };

        if let Some(type_width) = type_fixed_width(non_nullable) {
            return Ok(Self {
                nulls,
                kind: LayoutBuilderKind::Fixed {
                    type_width,
                    data: Default::default(),
                },
            });
        };

        match non_nullable {
            DataTypeNode::String => Ok(Self {
                nulls,
                kind: LayoutBuilderKind::Variable {
                    end_offsets: vec![],
                    data: Default::default(),
                },
            }),
            DataTypeNode::Tuple(types) => Ok(Self {
                nulls,
                kind: LayoutBuilderKind::Tuple {
                    layouts: types
                        .iter()
                        .map(|ty| LayoutBuilder::new(column_name, ty))
                        .collect::<Result<_, _>>()?,
                },
            }),
            DataTypeNode::Array(elem_type) => Ok(Self {
                nulls,
                kind: LayoutBuilderKind::Array {
                    end_indices: vec![],
                    elem_layout: Box::new(LayoutBuilder::new(column_name, elem_type)?),
                },
            }),
            DataTypeNode::Map(key_val_types) => Ok(Self {
                nulls,
                kind: LayoutBuilderKind::Map {
                    key_val_layouts: Box::new([
                        LayoutBuilder::new(column_name, &key_val_types[0])?,
                        LayoutBuilder::new(column_name, &key_val_types[1])?,
                    ]),
                    end_indices: vec![],
                },
            }),
            _ => Err(Box::new(BlockBuilderError::UnsupportedType {
                column_name: column_name.to_string(),
                data_type: data_type.clone(),
            })),
        }
    }

    pub(super) fn num_values(&self) -> usize {
        match &self.kind {
            LayoutBuilderKind::Fixed { type_width, data } => data.len() / type_width,
            LayoutBuilderKind::Variable { end_offsets, .. } => end_offsets.len(),
            LayoutBuilderKind::Array { end_indices, .. } => end_indices.len(),
            LayoutBuilderKind::Tuple { layouts, .. } => {
                layouts.first().map_or(0, |layout| layout.num_values())
            }
            LayoutBuilderKind::Map { end_indices, .. } => end_indices.len(),
        }
    }

    pub(super) fn reserve(&mut self, additional: usize) {
        match &mut self.kind {
            LayoutBuilderKind::Fixed { type_width, data } => {
                data.reserve(type_width.saturating_mul(additional));
            }
            LayoutBuilderKind::Variable { end_offsets, .. } => {
                end_offsets.reserve(additional);
                // Don't reserve in `data` because we don't know the total additional size
            }
            LayoutBuilderKind::Array { .. } => {}
            LayoutBuilderKind::Tuple { .. } => {}
            LayoutBuilderKind::Map { .. } => {}
        }
    }

    /// Push a valid placeholder value
    pub(super) fn push_placeholder(&mut self) {
        match &mut self.kind {
            LayoutBuilderKind::Fixed { type_width, data } => {
                data.put_bytes(0, *type_width);
            }
            LayoutBuilderKind::Variable { end_offsets, data } => {
                end_offsets.push(data.len());
            }
            LayoutBuilderKind::Array { end_indices, .. } => {
                let end_index = end_indices.last().copied().unwrap_or(0);
                end_indices.push(end_index);
            }
            // This is only needed for `Nullable(Tuple(...))` which is currently experimental
            LayoutBuilderKind::Tuple { layouts } => {
                for layout in layouts {
                    layout.push_placeholder();
                }
            }
            LayoutBuilderKind::Map { end_indices, .. } => {
                let end_index = end_indices.last().copied().unwrap_or(0);
                end_indices.push(end_index);
            }
        }
    }

    /// Truncate to the given number of values.
    ///
    /// For arrays and maps, this truncates to the length of the array at `num_values`.
    pub(super) fn truncate(&mut self, num_values: usize) {
        match &mut self.kind {
            LayoutBuilderKind::Fixed { type_width, data } => {
                data.truncate(type_width.saturating_mul(num_values));
            }
            LayoutBuilderKind::Variable { end_offsets, data } => {
                end_offsets.truncate(num_values);

                let last_offset = end_offsets.last().copied().unwrap_or(0);
                data.truncate(last_offset);
            }
            LayoutBuilderKind::Array {
                end_indices,
                elem_layout,
            } => {
                end_indices.truncate(num_values);

                let last_index = end_indices.last().copied().unwrap_or(0);
                elem_layout.truncate(last_index);
            }
            LayoutBuilderKind::Tuple { layouts } => {
                for layout in layouts {
                    layout.truncate(num_values);
                }
            }
            LayoutBuilderKind::Map {
                key_val_layouts,
                end_indices,
            } => {
                end_indices.truncate(num_values);

                let last_index = end_indices.last().copied().unwrap_or(0);

                key_val_layouts[0].truncate(last_index);
                key_val_layouts[1].truncate(last_index);
            }
        }
    }

    fn validate(&self, data_type: &DataTypeNode) -> Result<(), String> {
        self.validate_nulls(data_type)?;

        let non_nullable = if let DataTypeNode::Nullable(inner) = data_type {
            inner
        } else {
            data_type
        };

        match &self.kind {
            LayoutBuilderKind::Fixed { type_width, data } => {
                let expected_width = type_fixed_width(non_nullable)
                    .ok_or_else(|| format!("data type {non_nullable} is not fixed-width but we encoded {} bytes of {type_width}-byte values", data.len()))?;

                if expected_width != *type_width {
                    return Err(format!(
                        "data type {non_nullable} has a fixed width of {expected_width} but we encoded {} bytes of {type_width}-byte values",
                        data.len()
                    ));
                }

                if !data.len().is_multiple_of(*type_width) {
                    return Err(format!(
                        "data length ({}) is not a multiple of type_width ({type_width})",
                        data.len()
                    ));
                }

                Ok(())
            }
            LayoutBuilderKind::Variable { end_offsets, data } => {
                for (i, &end_offset) in end_offsets.iter().enumerate() {
                    if end_offset > data.len() {
                        return Err(format!(
                            "string {i} end offset {end_offset} is out of bounds: {}",
                            data.len()
                        ));
                    }
                }

                Ok(())
            }
            LayoutBuilderKind::Array {
                end_indices,
                elem_layout,
            } => {
                let DataTypeNode::Array(elem_type) = non_nullable else {
                    return Err(format!("expected type Array(_), got {non_nullable}"));
                };

                let num_elements = elem_layout.num_values();

                for (i, &end_index) in end_indices.iter().enumerate() {
                    if end_index > num_elements {
                        return Err(format!(
                            "array {i} end index ({end_index}) out of bounds: {num_elements}"
                        ));
                    }
                }

                let last_index = end_indices.last().copied().unwrap_or(0);

                if last_index != num_elements {
                    // Most likely cause of this error is a leaked `ArrayWriter`
                    return Err(format!(
                        "last array index ({last_index}) out of sync with total elements: {num_elements}"
                    ));
                }

                elem_layout.validate(elem_type)
            }
            LayoutBuilderKind::Tuple { layouts } => {
                let DataTypeNode::Tuple(types) = non_nullable else {
                    return Err(format!("expected type Tuple(...), got {non_nullable}"));
                };

                let expected_len = layouts.first().map_or(0, LayoutBuilder::num_values);

                for (i, (ty, layout)) in types.iter().zip(layouts).enumerate() {
                    layout.validate(ty)?;

                    let actual_len = layout.num_values();

                    if layout.num_values() != expected_len {
                        // Most likely cause of this error is a leaked `TupleWriter`
                        return Err(format!(
                            "tuple index {i} (type {ty}) total elements out of sync: {actual_len} vs {expected_len}"
                        ));
                    }
                }

                Ok(())
            }
            LayoutBuilderKind::Map {
                key_val_layouts,
                end_indices,
            } => {
                let DataTypeNode::Map([key_ty, val_ty]) = non_nullable else {
                    return Err(format!("expected type Map(...), got {non_nullable}"));
                };

                let keys_len = key_val_layouts[0].num_values();
                let values_len = key_val_layouts[1].num_values();

                if keys_len != values_len {
                    return Err(format!(
                        "number of keys and values is out of sync: {keys_len} vs {values_len}"
                    ));
                }

                for (i, &end_index) in end_indices.iter().enumerate() {
                    if end_index > keys_len {
                        return Err(format!(
                            "map {i} end index ({end_index}) out of bounds: {keys_len}"
                        ));
                    }
                }

                let last_index = end_indices.last().copied().unwrap_or(0);

                if last_index != keys_len {
                    // Most likely cause of this error is a leaked `MapWriter`
                    return Err(format!(
                        "last map index ({last_index}) out of sync with total elements: {keys_len}"
                    ));
                }

                key_val_layouts[0].validate(key_ty)?;
                key_val_layouts[1].validate(val_ty)?;

                Ok(())
            }
        }
    }

    fn validate_nulls(&self, data_type: &DataTypeNode) -> Result<(), String> {
        match (&self.nulls, data_type) {
            (Some(nulls), DataTypeNode::Nullable(_)) => {
                if nulls.len() != self.num_values() {
                    return Err(format!(
                        "null bitmap length invalid: {}; expected: {}",
                        nulls.len(),
                        self.num_values()
                    ));
                }
            }
            (Some(nulls), _) => {
                return Err(format!(
                    "null bitmap of length {} created for non-nullable type {data_type}",
                    nulls.len()
                ));
            }
            (None, DataTypeNode::Nullable(_)) => {
                return Err(format!("nullable type {data_type} missing null bitmap"));
            }
            _ => (),
        }

        Ok(())
    }

    fn into_layout(self) -> Layout {
        Layout {
            num_values: self.num_values(),
            nulls: self.nulls.map(BytesMut::freeze),
            kind: match self.kind {
                LayoutBuilderKind::Fixed { type_width, data } => LayoutKind::Fixed {
                    type_width,
                    data: data.freeze(),
                },
                LayoutBuilderKind::Variable { end_offsets, data } => LayoutKind::Variable {
                    end_offsets: end_offsets.into(),
                    data: data.freeze(),
                },
                LayoutBuilderKind::Array {
                    end_indices,
                    elem_layout,
                } => LayoutKind::Array {
                    end_indices: end_indices.into(),
                    elem_layout: Box::new(elem_layout.into_layout()),
                },
                LayoutBuilderKind::Tuple { layouts } => LayoutKind::Tuple {
                    layouts: layouts
                        .into_iter()
                        .map(LayoutBuilder::into_layout)
                        .collect(),
                },
                LayoutBuilderKind::Map {
                    key_val_layouts,
                    end_indices,
                } => LayoutKind::Map {
                    key_val_layouts: Box::new(key_val_layouts.map(LayoutBuilder::into_layout)),
                    end_indices: end_indices.into(),
                },
            },
        }
    }
}
impl Debug for LayoutBuilder {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LayoutBuilder")
            .field("kind", &self.kind)
            .field("nulls", &self.nulls.as_deref().map(DebugNullMap))
            .finish()
    }
}

impl Debug for LayoutBuilderKind {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        use LayoutBuilderKind::*;

        match self {
            Fixed { type_width, data } => f
                .debug_struct("Fixed")
                .field(
                    "data",
                    &DebugFixedData {
                        type_width: *type_width,
                        data,
                    },
                )
                .finish(),
            Variable { end_offsets, data } => f
                .debug_struct("Variable")
                .field("data", &DebugVariableData { end_offsets, data })
                .finish(),
            Array {
                elem_layout,
                end_indices,
            } => f
                .debug_struct("Array")
                .field("elem_layout", elem_layout)
                .field("end_indices", end_indices)
                .finish(),
            Tuple { layouts } => {
                let mut tuple = f.debug_tuple("Tuple");

                for layout in layouts {
                    tuple.field(layout);
                }

                tuple.finish()
            }
            Map {
                key_val_layouts,
                end_indices,
            } => f
                .debug_struct("Map")
                .field("keys", &key_val_layouts[0])
                .field("values", &key_val_layouts[1])
                .field("end_indices", end_indices)
                .finish(),
        }
    }
}

/// Erase `LowCardinality` and `SimpleAggregateFunction` from the column type as inserts can be done
/// without them.
///
/// TODO: encode `LowCardinality`
fn erase_wrappers(data_type: DataTypeNode) -> DataTypeNode {
    match data_type {
        DataTypeNode::LowCardinality(inner) | DataTypeNode::SimpleAggregateFunction(_, inner) => {
            erase_wrappers(*inner)
        }
        DataTypeNode::Nullable(mut inner) => {
            *inner = erase_wrappers(*inner);
            DataTypeNode::Nullable(inner)
        }
        DataTypeNode::Array(mut inner) => {
            *inner = erase_wrappers(*inner);
            DataTypeNode::Array(inner)
        }
        DataTypeNode::Tuple(types) => {
            // Converting to a `VeqDeque` is an `O(1)` operation that then lets us
            // iterate through `types` by-value and push them back into the same allocation.
            let mut types = VecDeque::from(types);

            for _ in 0..types.len() {
                let ty = types.pop_front().unwrap();
                types.push_back(erase_wrappers(ty));
            }

            // The vector should be linear again, so this conversion should also be trivial.
            DataTypeNode::Tuple(types.into())
        }
        DataTypeNode::Map([mut key_ty, mut val_ty]) => {
            *key_ty = erase_wrappers(*key_ty);
            *val_ty = erase_wrappers(*val_ty);
            DataTypeNode::Map([key_ty, val_ty])
        }
        other => other,
    }
}

fn is_forbidden_nullable(data_type: &DataTypeNode) -> bool {
    // https://clickhouse.com/docs/reference/data-types/nullable
    matches!(data_type, DataTypeNode::Array(_) | DataTypeNode::Map(_))
}
