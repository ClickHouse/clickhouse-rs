use crate::error::BoxedError;
use crate::native::builder::{LayoutBuilder, LayoutBuilderKind};
use bytes::{BufMut, BytesMut};
use clickhouse_types::DataTypeNode;
use std::marker::PhantomData;
use std::ops::Range;

pub trait Encode {
    fn produces() -> DataTypeNode;

    fn encode(&self, writer: &mut ValueWriter<'_>) -> Result<(), BoxedError>;

    fn compatible(&self, column_type: &DataTypeNode) -> bool {
        let produced_type = Self::produces();

        default_compatible(&produced_type, column_type)
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

pub struct ValueWriter<'a> {
    data_type: &'a DataTypeNode,
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
        self.data_type
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

    pub fn write_array<T>(&mut self) -> Result<ArrayWriter<'_, T>, ValueWriteError> {
        let LayoutBuilderKind::Array {
            end_indices,
            elem_layout,
        } = &mut self.layout.kind
        else {
            return Err(ValueWriteError::IncorrectMethod);
        };

        let DataTypeNode::Array(elem_type) = &self.data_type else {
            return Err(ValueWriteError::IncorrectMethod);
        };

        Ok(ArrayWriter {
            elem_type,
            elem_layout,
            end_indices,
            outer_nulls: self.layout.nulls.as_mut(),
            finished: false,
            _marker: PhantomData,
        })
    }

    pub fn write_tuple(&mut self) -> Result<TupleWriter<'_>, ValueWriteError> {
        let LayoutBuilderKind::Tuple { layouts } = &mut self.layout.kind else {
            return Err(ValueWriteError::IncorrectMethod);
        };

        let DataTypeNode::Tuple(types) = &self.data_type else {
            return Err(ValueWriteError::IncorrectMethod);
        };

        Ok(TupleWriter {
            indices: 0..layouts.len(),
            elem_layouts: layouts,
            elem_types: types,
            finished: false,
        })
    }

    fn write_not_null(&mut self) {
        if let Some(nulls) = &mut self.layout.nulls {
            nulls.put_u8(0);
        }
    }
}

#[must_use = "an empty array is written on-drop if `.finish()` is not called"]
pub struct ArrayWriter<'a, T> {
    elem_type: &'a DataTypeNode,
    elem_layout: &'a mut LayoutBuilder,
    end_indices: &'a mut Vec<usize>,
    outer_nulls: Option<&'a mut BytesMut>,
    finished: bool,
    _marker: PhantomData<fn(T)>,
}

impl<T> ArrayWriter<'_, T> {
    pub fn write(&mut self, value: T) -> Result<&mut Self, BoxedError>
    where
        T: Encode,
    {
        if !value.compatible(self.elem_type) {
            return Err(format!(
                "value type is not compatible with array element type {}",
                self.elem_type
            )
            .into());
        }

        value.encode(&mut ValueWriter {
            data_type: self.elem_type,
            layout: &mut self.elem_layout,
        })?;

        Ok(self)
    }

    pub fn finish(mut self) {
        self.finish_mut()
    }

    fn finish_mut(&mut self) {
        if self.finished {
            return;
        }

        self.end_indices.push(self.elem_layout.num_values());

        if let Some(nulls) = &mut self.outer_nulls {
            nulls.put_u8(0);
        }

        self.finished = true;
    }
}

impl<T> Drop for ArrayWriter<'_, T> {
    fn drop(&mut self) {
        self.finish_mut();
    }
}

pub struct TupleWriter<'a> {
    indices: Range<usize>,
    elem_layouts: &'a mut [LayoutBuilder],
    elem_types: &'a [DataTypeNode],
    finished: bool,
}

impl TupleWriter<'_> {
    pub fn write<T>(&mut self, value: T) -> Result<&mut Self, BoxedError>
    where
        T: Encode,
    {
        let index = self
            .indices
            .next()
            .ok_or("attempting to write to a full tuple")?;

        let data_type = &self.elem_types[index];

        if !value.compatible(data_type) {
            return Err(format!(
                "value type is not compatible with tuple expected type {data_type}"
            )
            .into());
        }

        value.encode(&mut ValueWriter {
            layout: &mut self.elem_layouts[index],
            data_type,
        })?;

        Ok(self)
    }

    pub fn finish(mut self) -> Result<(), &'static str> {
        self.finish_mut()
    }

    fn finish_mut(&mut self) -> Result<(), &'static str> {}
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

impl<T> Encode for &'_ T
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
        if let DataTypeNode::FixedString(_) = writer.data_type {
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

impl<T> Encode for [T]
where
    T: Encode,
{
    fn produces() -> DataTypeNode {
        DataTypeNode::Array(Box::new(T::produces()))
    }

    fn encode(&self, writer: &mut ValueWriter<'_>) -> Result<(), BoxedError> {
        let mut writer = writer.write_array()?;

        for val in self {
            writer.write(val)?;
        }

        writer.finish();

        Ok(())
    }
}

macro_rules! tuple_impl {
    ($var1:ident: $ty1:ident $(, $var:ident: $ty:ident)*) => {
        impl<'a, $ty1 $(, $ty)* > Encode for ($ty1, $($ty),*)
            where
                $ty1: Encode,
                $($ty: Encode,)*
        {
            fn produces() -> DataTypeNode {
                DataTypeNode::Tuple(vec![$ty1::produces() $(, $ty::produces())*])
            }

            fn encode(&self, writer: &mut ValueWriter<'_>) -> Result<(), BoxedError> {

            }
        }

        tuple_impl!($($var: $ty),*);
    };
    () => {}
}

tuple_impl!(
    t1: T1, t2: T2, t3: T3, t4: T4, t5: T5, t6: T6, t7: T7, t8: T8, t9: T9,
    t10: T10, t11: T11, t12: T12, t13: T13, t14: T14, t15: T15, t16: T16
);
