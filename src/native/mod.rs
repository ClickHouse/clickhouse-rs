use crate::error::Error;
use crate::native::decode::Decode;
use crate::native::string::MaybeUtf8;
use bytes::Bytes;
use clickhouse_types::DataTypeNode;
use clickhouse_types::data_types::{DecimalType, EnumType};
use std::marker::PhantomData;
use std::ops::Index;
use std::slice;
use std::sync::Arc;

use hashbrown::HashMap;

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
    data: Bytes,
    layout: Layout,
}

enum Layout {
    /// Fixed layout. Width of each cell depends only on [`DataTypeNode`].
    Fixed { type_width: usize },
    /// Layout determined by `LowCardinality` metadata.
    LowCardinality(Arc<LowCardinality>),
    /// Offset of each item in the column data.
    Offsets(Box<[usize]>),
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
            .try_as_str()
            .unwrap_or_else(|| panic!("column name {:?} is not valid UTF-8", self.name))
    }

    pub fn name_bytes(&self) -> &[u8] {
        self.name.as_bytes()
    }

    pub fn iter<'a, T: Decode<'a>>(&'a self) -> Result<ColumnIter<'a, T>, Error> {
        if !T::compatible(&self.data_type) {
            return Err(Error::Custom(format!(
                "incompatible data type {:?} of column {:?}",
                self.data_type, self.name
            )));
        }

        Ok(ColumnIter {
            kind: match self.layout {
                Layout::Fixed { type_width } => IterKind::Fixed(self.data.chunks_exact(type_width)),
                Layout::Offsets(ref offsets) => IterKind::Offsets(offsets.iter()),
                _ => todo!("unsupported layout kind"),
            },
            column: self,
            ty: PhantomData,
        })
    }
}

pub struct ColumnIter<'a, T: 'a> {
    kind: IterKind<'a>,
    column: &'a Column,
    ty: PhantomData<fn() -> T>,
}

enum IterKind<'a> {
    Fixed(slice::ChunksExact<'a, u8>),
    Offsets(slice::Iter<'a, usize>),
}

impl<'a, T: 'a> Iterator for ColumnIter<'a, T>
where
    T: Decode<'a>,
{
    type Item = Result<T, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        match &mut self.kind {
            IterKind::Fixed(iter) => {
                Some(T::decode(&self.column.data_type, iter.next()?).map_err(Error::Other))
            }
            IterKind::Offsets(iter) => {
                let start = *iter.next()?;
                let end = iter
                    .as_slice()
                    .first()
                    .copied()
                    .unwrap_or(self.column.data.len());

                Some(
                    T::decode(&self.column.data_type, &self.column.data[start..end])
                        .map_err(Error::Other),
                )
            }
        }
    }
}

struct LowCardinality {}

fn fixed_width(data_type: &DataTypeNode) -> Option<usize> {
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
        DataTypeNode::IPv6 => Some(8),
        DataTypeNode::Nullable(inner) => fixed_width(inner)
            // Nullable adds one byte per element
            .and_then(|size| size.checked_add(1)),
        // Type width determined by metadata that comes before column data.
        DataTypeNode::LowCardinality(_) => None,
        DataTypeNode::Array(_) => None,
        DataTypeNode::Tuple(types) => types.iter().try_fold(0usize, |total, inner| {
            total.checked_add(fixed_width(inner)?)
        }),
        DataTypeNode::Enum(type_, _) => match type_ {
            EnumType::Enum8 => Some(1),
            EnumType::Enum16 => Some(2),
        },
        DataTypeNode::Map(_) => None,
        DataTypeNode::AggregateFunction(_, _) => None,
        DataTypeNode::SimpleAggregateFunction(_, inner) => fixed_width(inner),
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
