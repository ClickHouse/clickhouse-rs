use crate::native::array::{ArrayData, TupleIter};
use clickhouse_types::DataTypeNode;
use std::collections::{BTreeMap, HashMap};
use std::error::Error;
use std::hash::{BuildHasher, Hash};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

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
    // 8-bit ints don't have a concept of "endianness" but they still implement `from_bytes_le()`
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

impl Decode<'_> for bool {
    fn compatible(data_type: &DataTypeNode) -> bool {
        matches!(data_type, DataTypeNode::Bool)
    }

    fn decode(
        reader: &mut ValueReader<'_>,
    ) -> Result<Self, Box<dyn Error + Send + Sync + 'static>> {
        // https://clickhouse.com/docs/interfaces/specs/NativeFormat#bool
        let [b] = reader.read_bytes_fixed()?;
        Ok(*b != 0)
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

impl Decode<'_> for Ipv4Addr {
    fn compatible(data_type: &DataTypeNode) -> bool {
        matches!(data_type, DataTypeNode::IPv4)
    }

    fn decode(
        reader: &mut ValueReader<'_>,
    ) -> Result<Self, Box<dyn Error + Send + Sync + 'static>> {
        // https://clickhouse.com/docs/interfaces/specs/NativeFormat#ipv4-and-ipv6
        // IPv4 is byte-reversed, so little-endian
        let bytes_le = u32::from_le_bytes(*reader.read_bytes_fixed()?);
        Ok(Ipv4Addr::from(bytes_le.to_be()))
    }
}

impl Decode<'_> for Ipv6Addr {
    fn compatible(data_type: &DataTypeNode) -> bool {
        matches!(data_type, DataTypeNode::IPv6)
    }

    fn decode(
        reader: &mut ValueReader<'_>,
    ) -> Result<Self, Box<dyn Error + Send + Sync + 'static>> {
        // https://clickhouse.com/docs/interfaces/specs/NativeFormat#ipv4-and-ipv6
        // IPv6 uses canonical (big-endian) encoding
        Ok(Ipv6Addr::from(*reader.read_bytes_fixed::<16>()?))
    }
}

impl Decode<'_> for IpAddr {
    fn compatible(data_type: &DataTypeNode) -> bool {
        matches!(data_type, DataTypeNode::IPv4 | DataTypeNode::IPv6)
    }

    fn decode(
        reader: &mut ValueReader<'_>,
    ) -> Result<Self, Box<dyn Error + Send + Sync + 'static>> {
        match reader.data_type {
            DataTypeNode::IPv4 => Ipv4Addr::decode(reader).map(Into::into),
            DataTypeNode::IPv6 => Ipv6Addr::decode(reader).map(Into::into),
            other => Err(format!("expected IP address, got {other}").into()),
        }
    }
}

#[cfg(feature = "uuid")]
mod uuid {
    use super::{Decode, ValueReader};
    use clickhouse_types::DataTypeNode;
    use std::error::Error;
    use uuid::Uuid;

    impl Decode<'_> for Uuid {
        fn compatible(data_type: &DataTypeNode) -> bool {
            matches!(data_type, DataTypeNode::UUID)
        }

        fn decode(
            reader: &mut ValueReader<'_>,
        ) -> Result<Self, Box<dyn Error + Send + Sync + 'static>> {
            // https://clickhouse.com/docs/interfaces/specs/NativeFormat#uuid
            // Wire bytes 0..7 = canonical bytes 0..7 reversed.
            // Wire bytes 8..15 = canonical bytes 8..15 reversed.
            let hi_bytes = u64::from_le_bytes(*reader.read_bytes_fixed()?);
            let low_bytes = u64::from_le_bytes(*reader.read_bytes_fixed()?);

            Ok(Uuid::from_u64_pair(hi_bytes.to_be(), low_bytes.to_be()))
        }
    }
}
