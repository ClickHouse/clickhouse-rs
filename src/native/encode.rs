use crate::error::BoxedError;
use crate::native::builder::{LayoutBuilder, LayoutBuilderKind};
use bytes::{BufMut, BytesMut};
use clickhouse_types::DataTypeNode;
use std::cmp;
use std::collections::{BTreeMap, HashMap};
use std::marker::PhantomData;
use std::net::{Ipv4Addr, Ipv6Addr};

pub trait Encode {
    fn produces() -> DataTypeNode;

    fn encode(&self, writer: &mut ValueWriter<'_>) -> Result<(), BoxedError>;

    fn compatible(column_type: &DataTypeNode) -> bool {
        let produced_type = Self::produces();

        default_compatible(&produced_type, column_type)
    }
}

fn default_compatible(produced_type: &DataTypeNode, column_type: &DataTypeNode) -> bool {
    recursive_compatible(column_type, |column_type| {
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
    })
}

fn recursive_compatible<F: Fn(&DataTypeNode) -> bool>(
    column_type: &DataTypeNode,
    compatible: F,
) -> bool {
    if compatible(column_type) {
        return true;
    }

    match column_type {
        // SimpleAggregateFunction has the same wire image as the underlying type
        // and the server should implicitly expand LowCardinality
        // Not-null value can be written to nullable column but not vice versa
        DataTypeNode::LowCardinality(inner)
        | DataTypeNode::SimpleAggregateFunction(_, inner)
        | DataTypeNode::Nullable(inner) => recursive_compatible(inner, compatible),
        _ => false,
    }
}

pub struct ValueWriter<'a> {
    pub(super) data_type: &'a DataTypeNode,
    pub(super) layout: &'a mut LayoutBuilder,
}

#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
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

        self.layout.push_placeholder();

        Ok(())
    }

    pub fn write_array<T>(&mut self) -> Result<ArrayWriter<'_, T>, ArrayWriteError>
    where
        T: Encode,
    {
        let DataTypeNode::Array(elem_type) = &self.data_type else {
            return Err(ArrayWriteError::NotAnArray {
                data_type: self.data_type.clone(),
            });
        };

        if !T::compatible(elem_type) {
            return Err(ArrayWriteError::IncompatibleType {
                expected_type: (**elem_type).clone(),
            });
        }

        let LayoutBuilderKind::Array {
            end_indices,
            elem_layout,
        } = &mut self.layout.kind
        else {
            // Technically a bug if we reach this point
            unreachable!("BUG: expected LayoutBuilderKind::Array")
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
            index: 0,
            elem_layouts: layouts,
            elem_types: types,
            finished: false,
        })
    }

    pub fn write_map<K, V>(&mut self) -> Result<MapWriter<'_, K, V>, ValueWriteError> {
        let LayoutBuilderKind::Map {
            key_val_layouts,
            end_indices,
        } = &mut self.layout.kind
        else {
            return Err(ValueWriteError::IncorrectMethod);
        };

        let DataTypeNode::Map([key_ty, val_ty]) = &self.data_type else {
            return Err(ValueWriteError::IncorrectMethod);
        };

        let [key_layout, val_layout] = &mut **key_val_layouts;

        Ok(MapWriter {
            key_ty,
            val_ty,
            key_layout,
            val_layout,
            end_indices,
            finished: false,
            _marker: PhantomData,
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

#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ArrayWriteError {
    #[error("attempted to write an array to a non-array column: {data_type}")]
    NotAnArray { data_type: DataTypeNode },

    #[error("value type is not compatible with expected type {expected_type}")]
    IncompatibleType { expected_type: DataTypeNode },

    #[error("error writing value at array index {index}")]
    ValueWriteError {
        index: usize,
        #[source]
        error: BoxedError,
    },
}

impl<T> ArrayWriter<'_, T> {
    pub fn write(&mut self, value: T) -> Result<&mut Self, ArrayWriteError>
    where
        T: Encode,
    {
        value
            .encode(&mut ValueWriter {
                data_type: self.elem_type,
                layout: self.elem_layout,
            })
            .map_err(|error| ArrayWriteError::ValueWriteError {
                index: self.written_len(),
                error,
            })?;

        Ok(self)
    }

    pub fn finish(mut self) {
        self.finish_mut()
    }

    fn written_len(&self) -> usize {
        let last_array_end = self.end_indices.last().copied().unwrap_or(0);
        self.elem_layout.num_values().saturating_sub(last_array_end)
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

#[must_use = "rolls back the written tuple elements on-drop if `.finish()` is not called"]
pub struct TupleWriter<'a> {
    index: usize,
    elem_layouts: &'a mut [LayoutBuilder],
    elem_types: &'a [DataTypeNode],
    finished: bool,
}

#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum TupleWriteError {
    #[error("attempting to write to a full tuple")]
    TupleFull,

    #[error(
        "value type is not compatible with expected type {expected_type} at tuple index {index}"
    )]
    IncompatibleType {
        index: usize,
        expected_type: DataTypeNode,
    },

    #[error("error writing value at tuple index {index}")]
    ValueWriteError {
        index: usize,
        #[source]
        error: BoxedError,
    },
}

#[derive(Debug, thiserror::Error)]
#[error("tuple not fully written; expected {expected_len} values, got {written_len}")]
pub struct IncompleteTupleError {
    expected_len: usize,
    written_len: usize,
}

impl TupleWriter<'_> {
    pub fn write<T>(&mut self, value: T) -> Result<&mut Self, TupleWriteError>
    where
        T: Encode,
    {
        let data_type = self
            .elem_types
            .get(self.index)
            .ok_or(TupleWriteError::TupleFull)?;

        if !T::compatible(data_type) {
            return Err(TupleWriteError::IncompatibleType {
                index: self.index,
                expected_type: data_type.clone(),
            });
        }

        value
            .encode(&mut ValueWriter {
                layout: &mut self.elem_layouts[self.index],
                data_type,
            })
            .map_err(|error| TupleWriteError::ValueWriteError {
                index: self.index,
                error,
            })?;

        // Overflow here is likely to be a bug since
        // `self.elem_types` would have to be `usize::MAX` long
        self.index = self.index.checked_add(1).expect("tuple index overflowed");

        Ok(self)
    }

    pub fn finish(mut self) -> Result<(), IncompleteTupleError> {
        if self.index < self.elem_types.len() {
            return Err(IncompleteTupleError {
                expected_len: self.elem_types.len(),
                written_len: self.index,
            });
        }

        self.finished = true;

        Ok(())
    }

    fn abort_mut(&mut self) {
        if self.finished {
            return;
        }

        let written_len = cmp::min(self.index, self.elem_layouts.len());

        for layout in &mut self.elem_layouts[..written_len] {
            let len = layout.num_values();
            layout.truncate(len.saturating_sub(1));
        }
    }
}

impl Drop for TupleWriter<'_> {
    fn drop(&mut self) {
        self.abort_mut();
    }
}

#[must_use = "rolls back the written map elements on-drop if `.finish()` is not called"]
pub struct MapWriter<'a, K, V> {
    key_ty: &'a DataTypeNode,
    val_ty: &'a DataTypeNode,
    key_layout: &'a mut LayoutBuilder,
    val_layout: &'a mut LayoutBuilder,
    end_indices: &'a mut Vec<usize>,
    finished: bool,
    _marker: PhantomData<fn(K, V)>,
}

impl<K, V> MapWriter<'_, K, V>
where
    K: Encode,
    V: Encode,
{
    pub fn write(&mut self, key: K, value: V) -> Result<&mut Self, MapWriteError> {
        key.encode(&mut ValueWriter {
            layout: self.key_layout,
            data_type: self.key_ty,
        })
        .map_err(|error| MapWriteError::ValueWriteError {
            error,
            index: self.key_layout.num_values(),
        })?;

        value
            .encode(&mut ValueWriter {
                layout: self.val_layout,
                data_type: self.val_ty,
            })
            .map_err(|error| MapWriteError::ValueWriteError {
                error,
                index: self.val_layout.num_values(),
            })?;

        Ok(self)
    }

    pub fn finish(mut self) {
        if self.finished {
            return;
        }

        let end_index = self.key_layout.num_values();
        self.end_indices.push(end_index);

        self.finished = true;
    }
}

impl<K, V> Drop for MapWriter<'_, K, V> {
    fn drop(&mut self) {
        if self.finished {
            return;
        }

        let truncate_len = self.end_indices.last().copied().unwrap_or(0);

        self.key_layout.truncate(truncate_len);
        self.val_layout.truncate(truncate_len);
    }
}

#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum MapWriteError {
    #[error("type is not compatible with expected type {expected_type} at entry index {index}")]
    IncompatibleType {
        index: usize,
        expected_type: DataTypeNode,
    },

    #[error("error writing value at entry index {index}")]
    ValueWriteError {
        index: usize,
        #[source]
        error: BoxedError,
    },
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

    fn compatible(column_type: &DataTypeNode) -> bool {
        // Make sure we forward to `T::compatible()`
        recursive_compatible(column_type, T::compatible)
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

    fn compatible(column_type: &DataTypeNode) -> bool {
        T::compatible(column_type)
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

impl Encode for bool {
    fn produces() -> DataTypeNode {
        DataTypeNode::Bool
    }

    fn encode(&self, writer: &mut ValueWriter<'_>) -> Result<(), BoxedError> {
        writer.write_fixed(&[*self as u8])?;
        Ok(())
    }
}

impl Encode for str {
    fn produces() -> DataTypeNode {
        DataTypeNode::String
    }

    fn encode(&self, writer: &mut ValueWriter<'_>) -> Result<(), BoxedError> {
        if let DataTypeNode::FixedString(fixed_len) = writer.data_type {
            if self.len() != *fixed_len {
                // Give a more informative error
                return Err(format!(
                    "attempting to write a string of length {} to FixedString({fixed_len})",
                    self.len()
                )
                .into());
            }

            writer.write_fixed(self.as_bytes())?;
        } else {
            writer.write_string(self.as_bytes())?;
        }

        Ok(())
    }

    fn compatible(column_type: &DataTypeNode) -> bool {
        recursive_compatible(column_type, |column_type| {
            matches!(
                column_type,
                DataTypeNode::String | DataTypeNode::FixedString(_)
            )
        })
    }
}

impl Encode for String {
    fn produces() -> DataTypeNode {
        str::produces()
    }

    fn encode(&self, writer: &mut ValueWriter<'_>) -> Result<(), BoxedError> {
        self.as_str().encode(writer)
    }

    fn compatible(column_type: &DataTypeNode) -> bool {
        str::compatible(column_type)
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

    fn compatible(column_type: &DataTypeNode) -> bool {
        recursive_compatible(column_type, |column_type| match column_type {
            DataTypeNode::Array(elem_type) => T::compatible(elem_type),
            _ => false,
        })
    }
}

impl<T> Encode for Vec<T>
where
    T: Encode,
{
    fn produces() -> DataTypeNode {
        <[T]>::produces()
    }

    fn encode(&self, writer: &mut ValueWriter<'_>) -> Result<(), BoxedError> {
        self.as_slice().encode(writer)
    }

    fn compatible(column_type: &DataTypeNode) -> bool {
        <[T]>::compatible(column_type)
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
                let mut writer = writer.write_tuple()?;

                let ($var1, $($var),*) = self;

                writer.write($var1)?;
                $(
                    writer.write($var)?;
                )*

                writer.finish()?;
                Ok(())
            }

            fn compatible(column_type: &DataTypeNode) -> bool {
                recursive_compatible(column_type, |column_type| {
                    let DataTypeNode::Tuple(types) = column_type else {
                        return false;
                    };

                    let [$var1, $($var),*] = &types[..] else {
                        return false;
                    };

                    $ty1::compatible($var1)
                        $(&& $ty::compatible($var))*
                })
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

impl Encode for Ipv4Addr {
    fn produces() -> DataTypeNode {
        DataTypeNode::IPv4
    }

    fn encode(&self, writer: &mut ValueWriter<'_>) -> Result<(), BoxedError> {
        writer.write_fixed(&self.to_bits().to_le_bytes())?;
        Ok(())
    }
}

impl Encode for Ipv6Addr {
    fn produces() -> DataTypeNode {
        DataTypeNode::IPv6
    }

    fn encode(&self, writer: &mut ValueWriter<'_>) -> Result<(), BoxedError> {
        writer.write_fixed(&self.octets())?;
        Ok(())
    }
}

impl<K, V, H> Encode for HashMap<K, V, H>
where
    K: Encode,
    V: Encode,
{
    fn produces() -> DataTypeNode {
        DataTypeNode::Map([Box::new(K::produces()), Box::new(V::produces())])
    }

    fn encode(&self, writer: &mut ValueWriter<'_>) -> Result<(), BoxedError> {
        let mut writer = writer.write_map()?;

        for (k, v) in self {
            writer.write(k, v)?;
        }

        writer.finish();

        Ok(())
    }

    fn compatible(column_type: &DataTypeNode) -> bool {
        recursive_compatible(column_type, |column_type| {
            let DataTypeNode::Map([key_ty, val_ty]) = column_type else {
                return false;
            };

            K::compatible(key_ty) && V::compatible(val_ty)
        })
    }
}

impl<K, V> Encode for BTreeMap<K, V>
where
    K: Encode,
    V: Encode,
{
    fn produces() -> DataTypeNode {
        DataTypeNode::Map([Box::new(K::produces()), Box::new(V::produces())])
    }

    fn encode(&self, writer: &mut ValueWriter<'_>) -> Result<(), BoxedError> {
        let mut writer = writer.write_map()?;

        for (k, v) in self {
            writer.write(k, v)?;
        }

        writer.finish();

        Ok(())
    }

    fn compatible(column_type: &DataTypeNode) -> bool {
        recursive_compatible(column_type, |column_type| {
            let DataTypeNode::Map([key_ty, val_ty]) = column_type else {
                return false;
            };

            K::compatible(key_ty) && V::compatible(val_ty)
        })
    }
}

#[cfg(feature = "uuid")]
mod uuid {
    use super::{Encode, ValueWriter};
    use crate::error::BoxedError;
    use clickhouse_types::DataTypeNode;
    use uuid::Uuid;

    impl Encode for Uuid {
        fn produces() -> DataTypeNode {
            DataTypeNode::UUID
        }

        fn encode(&self, writer: &mut ValueWriter<'_>) -> Result<(), BoxedError> {
            // https://clickhouse.com/docs/interfaces/specs/NativeFormat#uuid
            // Wire bytes 0..7 = canonical bytes 0..7 reversed.
            // Wire bytes 8..15 = canonical bytes 8..15 reversed.
            let (lo_bytes, hi_bytes) = self.as_u64_pair();

            let mut bytes = [0u8; 16];
            bytes[..8].copy_from_slice(&lo_bytes.to_le_bytes());
            bytes[8..].copy_from_slice(&hi_bytes.to_le_bytes());

            writer.write_fixed(&bytes)?;

            Ok(())
        }
    }
}
