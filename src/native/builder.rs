use crate::native::string::MaybeUtf8;
use crate::native::{Block, Column, Layout, LayoutKind, type_fixed_width};
use bytes::BytesMut;
use clickhouse_types::DataTypeNode;
use hashbrown::{HashMap, hash_map};
use std::collections::VecDeque;
use std::mem;
use std::ops::{Index, IndexMut};

#[derive(Default)]
pub struct BlockBuilder {
    column_names: HashMap<MaybeUtf8, usize>,
    columns: Vec<ColumnBuilder>,
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
    #[error("unsupported data type of column `{name} {data_type}`")]
    UnsupportedType {
        name: String,
        data_type: DataTypeNode,
    },
    #[error(
        "block contains columns of mismatched lengths; longest column: `{column_name} {column_type}` with {len} values"
    )]
    MismatchedLengths {
        column_name: String,
        column_type: DataTypeNode,
        len: usize,
    },
    #[error("column `{column_name} {column_type}` contains invalid data: {message}")]
    ColumnDataInvalid {
        column_name: String,
        column_type: DataTypeNode,
        message: String,
    },
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
    pub fn add_column(
        &mut self,
        name: impl Into<String>,
        data_type: DataTypeNode,
    ) -> Result<&mut ColumnBuilder, Box<BlockBuilderError>> {
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
                let col = ColumnBuilder {
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

    pub fn columns(&self) -> &[ColumnBuilder] {
        &self.columns
    }

    pub fn columns_mut(&mut self) -> &mut [ColumnBuilder] {
        &mut self.columns
    }

    pub fn column(&self, name: &str) -> Option<&ColumnBuilder> {
        Some(&self.columns[*self.column_names.get(name)?])
    }

    pub fn column_mut(&mut self, name: &str) -> Option<&mut ColumnBuilder> {
        Some(&mut self.columns[*self.column_names.get(name)?])
    }

    pub fn build(&mut self) -> Result<Block, Box<BlockBuilderError>> {
        let mut num_rows = 0;

        // Check that all the columns have the same length
        if let Some((mut largest_col, columns)) = self.columns.split_first() {
            let mut len_mismatch = false;
            num_rows = largest_col.num_values();

            for col in columns {
                if largest_col.num_values() < col.num_values() {
                    largest_col = col;
                    len_mismatch = true
                }
            }

            if len_mismatch {
                return Err(BlockBuilderError::MismatchedLengths {
                    column_name: largest_col.name.to_string(),
                    column_type: largest_col.data_type.clone(),
                    len: largest_col.num_values(),
                }
                .into());
            }
        }

        // Note: try to perform as much validation as possible before consuming `self`
        for col in &self.columns {
            col.layout
                .validate_nulls(&col.data_type)
                .map_err(|message| BlockBuilderError::ColumnDataInvalid {
                    column_name: col.name.to_string(),
                    column_type: col.data_type.clone(),
                    message,
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

impl Index<&str> for BlockBuilder {
    type Output = ColumnBuilder;

    fn index(&self, name: &str) -> &Self::Output {
        self.column(name)
            .unwrap_or_else(|| panic!("column {name:?} does not exist"))
    }
}

impl IndexMut<&str> for BlockBuilder {
    fn index_mut(&mut self, name: &str) -> &mut Self::Output {
        self.column_mut(name)
            .unwrap_or_else(|| panic!("column {name:?} does not exist"))
    }
}

impl Index<usize> for BlockBuilder {
    type Output = ColumnBuilder;

    fn index(&self, index: usize) -> &Self::Output {
        self.columns
            .get(index)
            .unwrap_or_else(|| panic!("column index {index} out of bounds: {}", self.columns.len()))
    }
}

impl IndexMut<usize> for BlockBuilder {
    fn index_mut(&mut self, index: usize) -> &mut Self::Output {
        let len = self.columns.len();
        self.columns
            .get_mut(index)
            .unwrap_or_else(|| panic!("column index {index} out of bounds: {len}"))
    }
}

pub struct ColumnBuilder {
    name: MaybeUtf8,
    data_type: DataTypeNode,
    layout: LayoutBuilder,
}

impl ColumnBuilder {
    pub fn num_values(&self) -> usize {
        self.layout.num_values()
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
    fn new(column_name: &MaybeUtf8, data_type: &DataTypeNode) -> Result<Self, BlockBuilderError> {
        let (non_nullable, nulls) = if let DataTypeNode::Nullable(inner) = data_type {
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
            _ => Err(BlockBuilderError::UnsupportedType {
                name: column_name.to_string(),
                data_type: data_type.clone(),
            }),
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
            LayoutBuilderKind::Map {
                key_val_layouts, ..
            } => key_val_layouts[0].num_values(),
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
