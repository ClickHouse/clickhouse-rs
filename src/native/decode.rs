use crate::native::array::{ArrayData, TupleIter};
use clickhouse_types::DataTypeNode;
use std::collections::{BTreeMap, HashMap};
use std::error::Error;
use std::hash::{BuildHasher, Hash};

pub struct ValueReader<'a> {
    pub(super) data_type: &'a DataTypeNode,
    pub(super) native_bytes: &'a [u8],
}

impl<'a> ValueReader<'a> {
    pub fn data_type(&self) -> &DataTypeNode {
        self.data_type
    }

    pub fn read_bytes_fixed<const LEN: usize>(&mut self) -> Result<&'a [u8; LEN], ValueReadError> {
        let (ret, rem) =
            self.native_bytes
                .split_first_chunk()
                .ok_or(ValueReadError::InvalidLength {
                    expected: LEN,
                    actual: self.native_bytes.len(),
                })?;

        self.native_bytes = rem;

        Ok(ret)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ValueReadError {
    #[error("expected {expected} bytes, got {actual}")]
    InvalidLength { expected: usize, actual: usize },
}

pub trait Decode<'a>: 'a + Sized {
    fn compatible(data_type: &DataTypeNode) -> bool;

    fn decode(reader: &mut ValueReader<'a>)
    -> Result<Self, Box<dyn Error + Send + Sync + 'static>>;

    fn decode_null(
        data_type: &DataTypeNode,
    ) -> Result<Self, Box<dyn Error + Send + Sync + 'static>> {
        Err(format!("data type {data_type:?} cannot be NULL").into())
    }

    fn decode_array(data: ArrayData<'a>) -> Result<Self, Box<dyn Error + Send + Sync + 'static>> {
        Err(format!("unexpected data type Array({})", data.elem_type).into())
    }

    fn decode_tuple(data: TupleIter<'a>) -> Result<Self, Box<dyn Error + Send + Sync + 'static>> {
        Err(format!("unexpected data type Tuple({:?})", data.types.as_slice()).into())
    }

    fn decode_map(
        key_data: ArrayData<'a>,
        value_data: ArrayData<'a>,
    ) -> Result<Self, Box<dyn Error + Send + Sync + 'static>> {
        Err(format!(
            "unexpected data type Map({}, {})",
            key_data.elem_type, value_data.elem_type
        )
        .into())
    }
}

impl<'a> Decode<'a> for &'a str {
    fn compatible(data_type: &DataTypeNode) -> bool {
        <&[u8] as Decode>::compatible(data_type)
    }

    fn decode(
        reader: &mut ValueReader<'a>,
    ) -> Result<Self, Box<dyn Error + Send + Sync + 'static>> {
        Ok(str::from_utf8(<&[u8] as Decode>::decode(reader)?)?)
    }
}

impl<'a> Decode<'a> for String {
    fn compatible(data_type: &DataTypeNode) -> bool {
        <&str as Decode>::compatible(data_type)
    }

    fn decode(
        reader: &mut ValueReader<'a>,
    ) -> Result<Self, Box<dyn Error + Send + Sync + 'static>> {
        Ok(<&str as Decode>::decode(reader)?.into())
    }
}

impl<'a> Decode<'a> for &'a [u8] {
    fn compatible(data_type: &DataTypeNode) -> bool {
        matches!(
            data_type,
            DataTypeNode::String | DataTypeNode::FixedString(_)
        )
    }

    fn decode(
        reader: &mut ValueReader<'a>,
    ) -> Result<Self, Box<dyn Error + Send + Sync + 'static>> {
        Ok(reader.native_bytes)
    }
}

impl<'a, T: Decode<'a>> Decode<'a> for Option<T> {
    fn compatible(data_type: &DataTypeNode) -> bool {
        if let DataTypeNode::Nullable(inner) = data_type {
            T::compatible(inner)
        } else {
            false
        }
    }

    fn decode(
        reader: &mut ValueReader<'a>,
    ) -> Result<Self, Box<dyn Error + Send + Sync + 'static>> {
        let DataTypeNode::Nullable(inner_type) = reader.data_type else {
            return Err(format!("expected `Nullable(_)`, got {:?}", reader.data_type).into());
        };

        Ok(Some(T::decode(&mut ValueReader {
            data_type: inner_type,
            native_bytes: reader.native_bytes,
        })?))
    }

    fn decode_null(
        _data_type: &DataTypeNode,
    ) -> Result<Self, Box<dyn Error + Send + Sync + 'static>> {
        Ok(None)
    }

    fn decode_array(data: ArrayData<'a>) -> Result<Self, Box<dyn Error + Send + Sync + 'static>> {
        T::decode_array(data).map(Some)
    }

    fn decode_tuple(data: TupleIter<'a>) -> Result<Self, Box<dyn Error + Send + Sync + 'static>> {
        T::decode_tuple(data).map(Some)
    }
}

// FIXME: need to decide if `Vec<u8>` corresponds to `String` or `Array<UInt8>`
impl<'a, T: Decode<'a> + 'a> Decode<'a> for Vec<T> {
    fn compatible(data_type: &DataTypeNode) -> bool {
        if let DataTypeNode::Array(elem_type) = data_type {
            T::compatible(elem_type)
        } else {
            false
        }
    }

    fn decode(
        reader: &mut ValueReader<'a>,
    ) -> Result<Self, Box<dyn Error + Send + Sync + 'static>> {
        Err(format!("expected array type, got {}", reader.data_type).into())
    }

    fn decode_array(data: ArrayData<'a>) -> Result<Self, Box<dyn Error + Send + Sync + 'static>> {
        data.into_reader::<T>()?
            .collect::<Result<Vec<T>, crate::Error>>()
            .map_err(Into::into)
    }
}

macro_rules! tuple_impl {
    ($var1:ident: $ty1:ident $(, $var:ident: $ty:ident)*) => {
        impl<'a, $ty1 $(, $ty)* > Decode<'a> for ($ty1, $($ty),*)
            where
                $ty1: Decode<'a>,
                $($ty: Decode<'a>,)*
        {
            fn compatible(data_type: &DataTypeNode) -> bool {
                let DataTypeNode::Tuple(types) = data_type else {
                    return false;
                };

                // Matches exact length of array
                let [$var1 $(, $var)*] = &types[..] else {
                    return false;
                };

                <$ty1 as Decode>::compatible($var1)
                $(&& <$ty as Decode>::compatible($var))*

            }

            fn decode(
                reader: &mut ValueReader<'a>,
            ) -> Result<Self, Box<dyn Error + Send + Sync + 'static>> {
                Err(format!("expected array type, got {}", reader.data_type).into())
            }

            fn decode_tuple(mut data: TupleIter<'a>) -> Result<Self, Box<dyn Error + Send + Sync + 'static>> {
                Ok((
                    data.decode_next::<$ty1>()?,
                    $(data.decode_next::<$ty>()?),*
                ))
            }
        }

        tuple_impl!($($var: $ty),*);
    };
    () => {}
}

tuple_impl!(t1: T1, t2: T2, t3: T3);

macro_rules! impl_from_le_bytes {
    ($($dataty:ident: $ty:ident),* $(,)?) => {
        $(
            impl<'a> Decode<'a> for $ty {
                fn compatible(data_type: &DataTypeNode) -> bool {
                    matches!(data_type, DataTypeNode::$dataty)
                }

                fn decode(
                    reader: &mut ValueReader<'a>,
                ) -> Result<Self, Box<dyn Error + Send + Sync + 'static>> {
                    Ok($ty::from_le_bytes(*reader.read_bytes_fixed()?))
                }
            }
        )*
    };
}

// All scalar primitives are in little-endian
impl_from_le_bytes!(
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

impl<'a, K, V, S> Decode<'a> for HashMap<K, V, S>
where
    K: Decode<'a> + Hash + Eq,
    V: Decode<'a>,
    S: BuildHasher + Default + 'a,
{
    fn compatible(data_type: &DataTypeNode) -> bool {
        let DataTypeNode::Map([key_ty, val_ty]) = data_type else {
            return false;
        };

        K::compatible(key_ty) && V::compatible(val_ty)
    }

    fn decode(
        reader: &mut ValueReader<'a>,
    ) -> Result<Self, Box<dyn Error + Send + Sync + 'static>> {
        Err(format!("expected map, got {}", reader.data_type).into())
    }

    fn decode_map(
        key_data: ArrayData<'a>,
        value_data: ArrayData<'a>,
    ) -> Result<Self, Box<dyn Error + Send + Sync + 'static>> {
        key_data
            .into_reader::<K>()?
            .zip(value_data.into_reader::<V>()?)
            .map(|(k, v)| Ok((k?, v?)))
            .collect::<Result<Self, Box<dyn Error + Send + Sync + 'static>>>()
    }
}

impl<'a, K, V> Decode<'a> for BTreeMap<K, V>
where
    K: Decode<'a> + Ord + Eq,
    V: Decode<'a>,
{
    fn compatible(data_type: &DataTypeNode) -> bool {
        let DataTypeNode::Map([key_ty, val_ty]) = data_type else {
            return false;
        };

        K::compatible(key_ty) && V::compatible(val_ty)
    }

    fn decode(
        reader: &mut ValueReader<'a>,
    ) -> Result<Self, Box<dyn Error + Send + Sync + 'static>> {
        Err(format!("expected map, got {}", reader.data_type).into())
    }

    fn decode_map(
        key_data: ArrayData<'a>,
        value_data: ArrayData<'a>,
    ) -> Result<Self, Box<dyn Error + Send + Sync + 'static>> {
        key_data
            .into_reader::<K>()?
            .zip(value_data.into_reader::<V>()?)
            .map(|(k, v)| Ok((k?, v?)))
            .collect::<Result<Self, Box<dyn Error + Send + Sync + 'static>>>()
    }
}
