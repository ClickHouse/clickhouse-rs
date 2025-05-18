pub(crate) use de::deserialize_from;
pub(crate) use de::deserialize_from_and_validate;
pub(crate) use ser::serialize_into;
use std::fmt::Display;

mod de;
mod de_rbwnat;
mod ser;
#[cfg(test)]
mod tests;
mod utils;

/// Which Serde data type (De)serializer used for the given type.
/// Displays into Rust types for convenience in errors reporting.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum SerdeType {
    Bool,
    I8,
    I16,
    I32,
    I64,
    I128,
    U8,
    U16,
    U32,
    U64,
    U128,
    F32,
    F64,
    Char,
    Str,
    String,
    Bytes,
    ByteBuf,
    Option,
    Unit,
    UnitStruct,
    NewtypeStruct,
    Seq,
    Tuple,
    TupleStruct,
    Map,
    Struct,
    Enum,
    Identifier,
    IgnoredAny,
}

impl Default for SerdeType {
    fn default() -> Self {
        SerdeType::Struct
    }
}

impl Display for SerdeType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let type_name = match self {
            SerdeType::Bool => "bool",
            SerdeType::I8 => "i8",
            SerdeType::I16 => "i16",
            SerdeType::I32 => "i32",
            SerdeType::I64 => "i64",
            SerdeType::I128 => "i128",
            SerdeType::U8 => "u8",
            SerdeType::U16 => "u16",
            SerdeType::U32 => "u32",
            SerdeType::U64 => "u64",
            SerdeType::U128 => "u128",
            SerdeType::F32 => "f32",
            SerdeType::F64 => "f64",
            SerdeType::Char => "char",
            SerdeType::Str => "&str",
            SerdeType::String => "String",
            SerdeType::Bytes => "&[u8]",
            SerdeType::ByteBuf => "Vec<u8>",
            SerdeType::Option => "Option<T>",
            SerdeType::Unit => "()",
            SerdeType::UnitStruct => "unit struct",
            SerdeType::NewtypeStruct => "newtype struct",
            SerdeType::Seq => "Vec<T>",
            SerdeType::Tuple => "tuple",
            SerdeType::TupleStruct => "tuple struct",
            SerdeType::Map => "map",
            SerdeType::Struct => "struct",
            SerdeType::Enum => "enum",
            SerdeType::Identifier => "identifier",
            SerdeType::IgnoredAny => "ignored any",
        };
        write!(f, "{}", type_name)
    }
}
