use crate::error::BoxedError;
use crate::native::builder::{LayoutBuilder, LayoutBuilderKind};
use bytes::BufMut;
use clickhouse_types::DataTypeNode;

pub trait Encode {
    fn produces() -> DataTypeNode;

    fn encode(&self, writer: &mut ValueWriter<'_>) -> Result<(), BoxedError>;

    fn compatible(&self, column_type: &DataTypeNode) -> bool {
        let produced_type = Self::produces();

        default_compatible(&produced_type, column_type)
    }
}

pub struct ValueWriter<'a> {
    column_type: &'a DataTypeNode,
    layout: &'a mut LayoutBuilder,
}

#[derive(Debug, thiserror::Error)]
pub enum ValueWriteError {
    #[error("attempting to use incorrect writer method for this type")]
    IncorrectMethod,

    #[error("column does not allow nullable values here")]
    UnexpectedNull,

    #[error("expected {expected} bytes, got {actual}")]
    InvalidLength { expected: usize, actual: usize },
}

impl<'a> ValueWriter<'a> {
    pub fn column_type(&self) -> &'a DataTypeNode {
        self.column_type
    }

    pub fn write_fixed(&mut self, bytes: &[u8]) -> Result<(), ValueWriteError> {
        let LayoutBuilderKind::Fixed {
            type_width,
            ref mut data,
        } = self.layout.kind
        else {
            return Err(ValueWriteError::IncorrectMethod);
        };

        if bytes.len() != type_width {
            return Err(ValueWriteError::InvalidLength {
                expected: type_width,
                actual: bytes.len(),
            });
        }

        data.extend_from_slice(bytes);
        self.write_not_null();

        Ok(())
    }

    pub fn write_string(&mut self, string_bytes: &[u8]) -> Result<(), ValueWriteError> {
        let LayoutBuilderKind::Variable { end_offsets, data } = &mut self.layout.kind else {
            return Err(ValueWriteError::IncorrectMethod);
        };

        data.extend_from_slice(string_bytes);
        end_offsets.push(data.len());

        self.write_not_null();

        Ok(())
    }

    pub fn write_null(&mut self) -> Result<(), ValueWriteError> {
        let nulls = self
            .layout
            .nulls
            .as_mut()
            .ok_or(ValueWriteError::UnexpectedNull)?;

        nulls.put_u8(1);

        Ok(())
    }

    fn write_not_null(&mut self) {
        if let Some(nulls) = &mut self.layout.nulls {
            nulls.put_u8(0);
        }
    }
}

impl<T> Encode for Option<T>
where
    T: Encode,
{
    fn produces() -> DataTypeNode {
        DataTypeNode::Nullable(Box::new(T::produces()))
    }

    fn encode(&self, writer: &mut ValueWriter<'_>) -> Result<(), BoxedError> {
        match self {
            Some(inner) => inner.encode(writer),
            None => Ok(writer.write_null()?),
        }
    }

    fn compatible(&self, column_type: &DataTypeNode) -> bool {
        if let Some(inner) = self {
            inner.compatible(column_type)
        } else {
            default_compatible(&T::produces(), column_type)
        }
    }
}

impl<'a, T> Encode for &'a T
where
    T: Encode + ?Sized,
{
    fn produces() -> DataTypeNode {
        T::produces()
    }

    fn encode(&self, writer: &mut ValueWriter<'_>) -> Result<(), BoxedError> {
        (**self).encode(writer)
    }

    fn compatible(&self, column_type: &DataTypeNode) -> bool {
        (**self).compatible(column_type)
    }
}

macro_rules! impl_to_le_bytes {
    ($($dataty:ident: $ty:ident),* $(,)?) => {
        $(
            impl Encode for $ty {
                fn produces() -> DataTypeNode {
                    DataTypeNode::$dataty
                }

                fn encode(&self, writer: &mut ValueWriter<'_>) -> Result<(), BoxedError> {
                    Ok(writer.write_fixed(&self.to_le_bytes())?)
                }
            }
        )*
    };
}

// All scalar primitives are in little-endian
impl_to_le_bytes!(
    // 8-bit ints don't have a concept of "endianness" but they still implement `to_le_bytes()`
    // for the express purpose of being included in macros like this
    Int8: i8,
    Int16: i16,
    Int32: i32,
    Int64: i64,
    Int128: i128,
    UInt8: u8,
    UInt16: u16,
    UInt32: u32,
    UInt64: u64,
    UInt128: u128,
    Float32: f32,
    Float64: f64,
);

impl Encode for str {
    fn produces() -> DataTypeNode {
        DataTypeNode::String
    }

    fn encode(&self, writer: &mut ValueWriter<'_>) -> Result<(), BoxedError> {
        if let DataTypeNode::FixedString(_) = writer.column_type {
            writer.write_fixed(self.as_bytes())?;
        } else {
            writer.write_string(self.as_bytes())?;
        }

        Ok(())
    }

    fn compatible(&self, column_type: &DataTypeNode) -> bool {
        default_compatible(&DataTypeNode::String, column_type)
            || default_compatible(&DataTypeNode::FixedString(self.len()), column_type)
    }
}

impl Encode for String {
    fn produces() -> DataTypeNode {
        <str>::produces()
    }

    fn encode(&self, writer: &mut ValueWriter<'_>) -> Result<(), BoxedError> {
        self.as_str().encode(writer)
    }

    fn compatible(&self, column_type: &DataTypeNode) -> bool {
        self.as_str().compatible(column_type)
    }
}

fn default_compatible(produced_type: &DataTypeNode, column_type: &DataTypeNode) -> bool {
    if produced_type == column_type {
        return true;
    }

    match (produced_type, column_type) {
        // SimpleAggregateFunction has the same wire image as the underlying type
        // and the server should implicitly expand LowCardinality
        (
            DataTypeNode::LowCardinality(left) | DataTypeNode::SimpleAggregateFunction(_, left),
            right,
        ) => default_compatible(left, right),
        // Not-null value can be written to nullable column but not vice versa
        (DataTypeNode::Nullable(left), DataTypeNode::Nullable(right)) => {
            default_compatible(left, right)
        }
        (
            left,
            DataTypeNode::LowCardinality(right)
            | DataTypeNode::SimpleAggregateFunction(_, right)
            | DataTypeNode::Nullable(right),
        ) => default_compatible(left, right),
        _ => false,
    }
}
