use crate::native::array::ArrayData;
use clickhouse_types::DataTypeNode;
use std::error::Error;

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

pub trait Decode<'a>: Sized {
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
}

impl<'a> Decode<'a> for u64 {
    fn compatible(data_type: &DataTypeNode) -> bool {
        matches!(data_type, DataTypeNode::UInt64)
    }

    fn decode(
        reader: &mut ValueReader<'a>,
    ) -> Result<Self, Box<dyn Error + Send + Sync + 'static>> {
        Ok(u64::from_le_bytes(*reader.read_bytes_fixed()?))
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
