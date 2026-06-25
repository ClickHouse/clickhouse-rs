use crate::native::reader::parse_varuint;
use clickhouse_types::DataTypeNode;
use std::error::Error;
use std::ops::ControlFlow;

pub trait Decode<'a>: Sized {
    fn compatible(data_type: &DataTypeNode) -> bool;

    fn decode(
        data_type: &DataTypeNode,
        native_bytes: &'a [u8],
    ) -> Result<Self, Box<dyn Error + Send + Sync + 'static>>;
}

impl<'a> Decode<'a> for u64 {
    fn compatible(data_type: &DataTypeNode) -> bool {
        matches!(data_type, DataTypeNode::UInt64)
    }

    fn decode(
        _data_type: &DataTypeNode,
        native_bytes: &'a [u8],
    ) -> Result<Self, Box<dyn Error + Send + Sync + 'static>> {
        Ok(u64::from_le_bytes(native_bytes.try_into().map_err(
            |_| format!("expected 8 bytes, got {}", native_bytes.len()),
        )?))
    }
}

impl<'a> Decode<'a> for &'a str {
    fn compatible(data_type: &DataTypeNode) -> bool {
        <&[u8] as Decode>::compatible(data_type)
    }

    fn decode(
        data_type: &DataTypeNode,
        native_bytes: &'a [u8],
    ) -> Result<Self, Box<dyn Error + Send + Sync + 'static>> {
        let s = <&[u8] as Decode>::decode(data_type, native_bytes)?;
        Ok(str::from_utf8(s)?)
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
        data_type: &DataTypeNode,
        mut native_bytes: &'a [u8],
    ) -> Result<Self, Box<dyn Error + Send + Sync + 'static>> {
        let len = if let DataTypeNode::FixedString(len) = *data_type {
            len
        } else {
            let len = match parse_varuint(&mut native_bytes) {
                ControlFlow::Break(Ok(len)) => len,
                ControlFlow::Break(Err(e)) => return Err(e.into()),
                ControlFlow::Continue(_) => {
                    return Err(format!(
                        "missing terminating byte in VarUInt encoding: {native_bytes:?}"
                    )
                    .into());
                }
            };

            usize::try_from(len)
                .map_err(|_| format!("length prefix of String out of range: {len}"))?
        };

        if len != native_bytes.len() {
            return Err(format!(
                "length of {data_type} does not match remaining length of buffer: {len} vs {}",
                native_bytes.len()
            )
            .into());
        }

        Ok(native_bytes)
    }
}
