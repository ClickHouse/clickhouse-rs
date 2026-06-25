use crate::native::string::MaybeUtf8;
use bytes::Bytes;
use clickhouse_types::DataTypeNode;
use clickhouse_types::data_types::{DecimalType, EnumType};
use std::collections::HashMap;
use std::sync::Arc;

mod reader;
mod string;

pub struct Block {
    column_names: HashMap<MaybeUtf8, usize>,
    columns: Vec<Column>,
}

impl Block {
    fn from_columns(columns: Vec<Column>) -> Self {
        Self {
            column_names: columns
                .iter()
                .enumerate()
                .map(|(i, column)| (column.name.clone(), i))
                .collect(),
            columns,
        }
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
