use crate::error::Error;
use crate::native::string::MaybeUtf8;
use bytes::Bytes;
use clickhouse_types::data_types::{DecimalType, EnumType};
use std::ops::Index;

use hashbrown::HashMap;

pub use array::{ArrayData, ArrayReader};
pub use decode::Decode;

pub use clickhouse_types::DataTypeNode;

pub(crate) mod array;
pub(crate) mod decode;
pub(crate) mod reader;
pub(crate) mod string;

pub struct Block {
    column_names: HashMap<MaybeUtf8, usize>,
    columns: Vec<Column>,
    num_rows: usize,
}

impl Block {
    fn from_columns(columns: Vec<Column>, num_rows: usize) -> Self {
        Self {
            column_names: columns
                .iter()
                .enumerate()
                .map(|(i, column)| (column.name.clone(), i))
                .collect(),
            columns,
            num_rows,
        }
    }

    pub fn num_rows(&self) -> usize {
        self.num_rows
    }

    pub fn columns(&self) -> &[Column] {
        &self.columns
    }

    pub fn column_by_name(&self, name: &str) -> Option<&Column> {
        // Requires `hashbrown` for the `Equivalent` trait
        let idx = *self.column_names.get(name)?;

        Some(&self.columns[idx])
    }
}

impl Index<&str> for Block {
    type Output = Column;

    fn index(&self, name: &str) -> &Self::Output {
        self.column_by_name(name)
            .unwrap_or_else(|| panic!("block does not contain a column with the name {name:?}"))
    }
}

impl Index<usize> for Block {
    type Output = Column;

    fn index(&self, index: usize) -> &Self::Output {
        &self.columns[index]
    }
}

pub struct Column {
    name: MaybeUtf8,
    data_type: DataTypeNode,
    layout: Layout,
}

struct Layout {
    kind: LayoutKind,
    nulls: Option<Bytes>,
    // Equivalent to `num_rows` for top-level values, but may be different for nested values
    num_values: usize,
}

enum LayoutKind {
    /// Fixed layout. Width of each cell depends only on [`DataTypeNode`].
    Fixed {
        type_width: usize,
        data: Bytes,
    },
    /// Variable-length data (namely strings)
    Variable {
        /// Ending offset of each string in `data`.
        ///
        /// The offset of the first string is always `0` unless this is empty.
        end_offsets: Box<[usize]>,
        // Each `Bytes` instance is 4 `usizes` (32 bytes on 64-bit),
        // so we save 24 bytes per string by linearizing the string data and storing offsets,
        // assuming many small strings instead of fewer big ones, which also amortizes allocations
        data: Bytes,
    },
    /// Layout determined by `LowCardinality` metadata.
    LowCardinality(LayoutLowCardinality),
    /// Array data. Element data governed by `elem_layout`.
    Array {
        /// Ending index of each array in `elem_layout`.
        end_indices: Box<[usize]>,
        elem_layout: Box<Layout>,
    },
    Tuple {
        layouts: Box<[Layout]>,
    },
    Map {
        key_val_layouts: Box<[Layout; 2]>,
        end_indices: Box<[usize]>,
    },
}

struct LayoutLowCardinality {
    keys: Box<[usize]>,
    dict: Box<Layout>,
    /// If `true`, `key = 0` should decode as `NULL` instead of the placeholder value given.
    is_nullable: bool,
}

impl Column {
    /// # Panics
    /// If the column name is not valid UTF-8.
    ///
    /// This is unlikely to happen if you have control over the column set returned by the query.
    ///
    /// Use [`Self::name_bytes()`] if panicking is unacceptable.
    pub fn name(&self) -> &str {
        self.name
            .as_str()
            .unwrap_or_else(|| panic!("column name {:?} is not valid UTF-8", self.name))
    }

    pub fn name_bytes(&self) -> &[u8] {
        self.name.as_bytes()
    }

    pub fn iter<'a, T: Decode<'a>>(&'a self) -> Result<ColumnIter<'a, T>, Error> {
        if !T::compatible(self.data_type.remove_low_cardinality()) {
            return Err(Error::Custom(format!(
                "incompatible data type {:?} of column {:?}",
                self.data_type, self.name
            )));
        }

        Ok(ColumnIter {
            column: self,
            iter: ArrayData {
                elem_type: &self.data_type,
                layout: &self.layout,
                indices: 0..self.layout.num_values,
            }
            .into_reader_unchecked(), // we already checked above with a more specific error
        })
    }
}

pub struct ColumnIter<'a, T> {
    column: &'a Column,
    iter: ArrayReader<'a, T>,
}

impl<'a, T: 'a> Iterator for ColumnIter<'a, T>
where
    T: Decode<'a>,
{
    type Item = Result<T, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.iter.size_hint()
    }
}

impl<'a, T> ColumnIter<'a, T> {
    pub fn column(&self) -> &'a Column {
        self.column
    }
}

fn type_fixed_width(data_type: &DataTypeNode) -> Option<usize> {
    match data_type {
        DataTypeNode::Bool => Some(1),
        DataTypeNode::UInt8 => Some(1),
        DataTypeNode::UInt16 => Some(2),
        DataTypeNode::UInt32 => Some(4),
        DataTypeNode::UInt64 => Some(8),
        DataTypeNode::UInt128 => Some(16),
        DataTypeNode::UInt256 => Some(32),
        DataTypeNode::Int8 => Some(1),
        DataTypeNode::Int16 => Some(2),
        DataTypeNode::Int32 => Some(4),
        DataTypeNode::Int64 => Some(8),
        DataTypeNode::Int128 => Some(16),
        DataTypeNode::Int256 => Some(32),
        DataTypeNode::Float32 => Some(4),
        DataTypeNode::Float64 => Some(8),
        DataTypeNode::BFloat16 => Some(2),
        DataTypeNode::Decimal(_, _, type_) => match type_ {
            DecimalType::Decimal32 => Some(4),
            DecimalType::Decimal64 => Some(8),
            DecimalType::Decimal128 => Some(16),
            DecimalType::Decimal256 => Some(32),
        },
        DataTypeNode::String => None,
        DataTypeNode::FixedString(len) => Some(*len),
        DataTypeNode::UUID => Some(16),
        DataTypeNode::Date => Some(2),
        DataTypeNode::Date32 => Some(4),
        DataTypeNode::DateTime(_) => Some(4),
        DataTypeNode::DateTime64(_, _) => Some(8),
        DataTypeNode::Time => Some(4),
        DataTypeNode::Time64(_) => Some(8),
        DataTypeNode::Interval(_) => Some(8),
        DataTypeNode::IPv4 => Some(4),
        DataTypeNode::IPv6 => Some(16),
        // Nullable needs to be handled specially
        DataTypeNode::Nullable(_) => None,
        // Type width determined by metadata that comes before column data.
        DataTypeNode::LowCardinality(_) => None,
        DataTypeNode::Array(_) => None,
        DataTypeNode::Tuple(types) => types.iter().try_fold(0usize, |total, inner| {
            total.checked_add(type_fixed_width(inner)?)
        }),
        DataTypeNode::Enum(type_, _) => match type_ {
            EnumType::Enum8 => Some(1),
            EnumType::Enum16 => Some(2),
        },
        DataTypeNode::Map(_) => None,
        DataTypeNode::AggregateFunction(_, _) => None,
        DataTypeNode::SimpleAggregateFunction(_, inner) => type_fixed_width(inner),
        DataTypeNode::Variant(_) => None,
        DataTypeNode::Dynamic => None,
        DataTypeNode::JSON => None,
        DataTypeNode::JsonWithHint(_) => None,
        DataTypeNode::Point => Some(16), // Tuple(Float64, Float64)
        DataTypeNode::Ring => None,
        DataTypeNode::LineString => None,
        DataTypeNode::MultiLineString => None,
        DataTypeNode::Polygon => None,
        DataTypeNode::MultiPolygon => None,
        _ => None,
    }
}
