use crate::error::Result;
use clickhouse_rowbinary::data_types::{Column, DataTypeHint, DataTypeNode, DecimalSize, EnumType};
use std::collections::HashMap;
use std::fmt::Display;

pub(crate) trait ValidateDataType: Sized {
    fn validate(
        &mut self,
        serde_type: &'static SerdeType,
        compatible_db_types: &'static [DataTypeHint],
    ) -> Result<Option<InnerDataTypeValidator<'_>>>;
    fn validate_enum8(&mut self, value: i8);
    fn validate_enum16(&mut self, value: i16);
    fn validate_fixed_string(&mut self, len: usize);
}

#[derive(Default)]
pub(crate) struct DataTypeValidator<'cursor> {
    columns: &'cursor [Column],
}

impl<'cursor> DataTypeValidator<'cursor> {
    #[inline(always)]
    pub(crate) fn new(columns: &'cursor [Column]) -> Self {
        Self { columns }
    }
}

#[derive(Debug)]
pub(crate) enum MapValidatorState {
    Key,
    Value,
    Validated,
}

#[derive(Debug)]
pub(crate) enum ArrayValidatorState {
    Pending,
    Validated,
}

#[derive(Debug)]
pub(crate) enum InnerDataTypeValidator<'cursor> {
    Array(&'cursor DataTypeNode, ArrayValidatorState),
    FixedString(usize),
    Map(
        &'cursor DataTypeNode,
        &'cursor DataTypeNode,
        MapValidatorState,
    ),
    Tuple(&'cursor [DataTypeNode]),
    Enum(&'cursor HashMap<i16, String>),
    Variant(&'cursor [DataTypeNode]),
    Nullable(&'cursor DataTypeNode),
}

impl ValidateDataType for () {
    #[inline(always)]
    fn validate(
        &mut self,
        _serde_type: &'static SerdeType,
        _compatible_db_types: &'static [DataTypeHint],
        // _len: usize,
    ) -> Result<Option<InnerDataTypeValidator<'_>>> {
        Ok(None)
    }

    #[inline(always)]
    fn validate_enum8(&mut self, _enum_value: i8) {}

    #[inline(always)]
    fn validate_enum16(&mut self, _enum_value: i16) {}

    #[inline(always)]
    fn validate_fixed_string(&mut self, _len: usize) {}
}

impl<'cursor> ValidateDataType for Option<InnerDataTypeValidator<'cursor>> {
    #[inline]
    fn validate(
        &mut self,
        serde_type: &'static SerdeType,
        compatible_db_types: &'static [DataTypeHint],
        // seq_len: usize,
    ) -> Result<Option<InnerDataTypeValidator<'cursor>>> {
        // println!("Validating inner data type: {:?} against serde type: {} with compatible db types: {:?}",
        //          self, serde_type, compatible_db_types);
        match self {
            None => Ok(None),
            Some(InnerDataTypeValidator::Map(key_type, value_type, state)) => match state {
                MapValidatorState::Key => {
                    let result = validate_impl(key_type, serde_type, compatible_db_types);
                    *state = MapValidatorState::Value;
                    result
                }
                MapValidatorState::Value => {
                    let result = validate_impl(value_type, serde_type, compatible_db_types);
                    *state = MapValidatorState::Validated;
                    result
                }
                MapValidatorState::Validated => Ok(None),
            },
            Some(InnerDataTypeValidator::Array(inner_type, state)) => match state {
                ArrayValidatorState::Pending => {
                    let result = validate_impl(inner_type, serde_type, compatible_db_types);
                    *state = ArrayValidatorState::Validated;
                    result
                }
                // TODO: perhaps we can allow to validate the inner type more than once
                //  avoiding e.g. issues with Array(Nullable(T)) when the first element in NULL
                ArrayValidatorState::Validated => Ok(None),
            },
            Some(InnerDataTypeValidator::Nullable(inner_type)) => {
                validate_impl(inner_type, serde_type, compatible_db_types)
            }
            Some(InnerDataTypeValidator::Tuple(elements_types)) => {
                match elements_types.split_first() {
                    Some((first, rest)) => {
                        *elements_types = rest;
                        validate_impl(first, serde_type, compatible_db_types)
                    }
                    None => panic!(
                        "Struct tries to deserialize {} as a tuple element, but there are no more allowed elements in the database schema",
                        serde_type,
                    )
                }
            }
            Some(InnerDataTypeValidator::FixedString(_len)) => {
                Ok(None) // actually unreachable
            }
            Some(InnerDataTypeValidator::Variant(_possible_types)) => {
                Ok(None) // FIXME: requires comparing DataTypeNode vs TypeHint...
            }
            Some(InnerDataTypeValidator::Enum(_values_map)) => {
                todo!() // TODO - check value correctness in the hashmap
            }
        }
    }

    #[inline(always)]
    fn validate_fixed_string(&mut self, len: usize) {
        if let Some(InnerDataTypeValidator::FixedString(expected_len)) = self {
            if *expected_len != len {
                panic!(
                    "FixedString byte length mismatch: expected {}, got {}",
                    expected_len, len
                );
            }
        }
    }

    #[inline(always)]
    fn validate_enum8(&mut self, value: i8) {
        if let Some(InnerDataTypeValidator::Enum(values_map)) = self {
            if !values_map.contains_key(&(value as i16)) {
                panic!("Enum8 value `{value}` is not present in the database schema");
            }
        }
    }

    #[inline(always)]
    fn validate_enum16(&mut self, enum_value: i16) {
        if let Some(InnerDataTypeValidator::Enum(value)) = self {
            if !value.contains_key(&enum_value) {
                panic!("Enum16 value `{enum_value}` is not present in the database schema");
            }
        }
    }
}

impl Drop for InnerDataTypeValidator<'_> {
    fn drop(&mut self) {
        if let InnerDataTypeValidator::Tuple(elements_types) = self {
            if !elements_types.is_empty() {
                panic!(
                    "Tuple was not fully deserialized, remaining elements: {:?}",
                    elements_types
                );
            }
        }
    }
}

#[inline]
fn validate_impl<'cursor>(
    data_type: &'cursor DataTypeNode,
    serde_type: &'static SerdeType,
    compatible_db_types: &'static [DataTypeHint],
) -> Result<Option<InnerDataTypeValidator<'cursor>>> {
    // println!(
    //     "Validating data type: {:?} against serde type: {} with compatible db types: {:?}",
    //     data_type, serde_type, compatible_db_types
    // );
    // FIXME: multiple branches with similar patterns
    match data_type {
        DataTypeNode::Bool if compatible_db_types.contains(&DataTypeHint::Bool) => Ok(None),

        DataTypeNode::Int8 if compatible_db_types.contains(&DataTypeHint::Int8) => Ok(None),
        DataTypeNode::Int16 if compatible_db_types.contains(&DataTypeHint::Int16) => Ok(None),
        DataTypeNode::Int32
        | DataTypeNode::Date32
        | DataTypeNode::Decimal(_, _, DecimalSize::Int32)
        if compatible_db_types.contains(&DataTypeHint::Int32) =>
            {
                Ok(None)
            }
        DataTypeNode::Int64
        | DataTypeNode::DateTime64(_, _)
        | DataTypeNode::Decimal(_, _, DecimalSize::Int64)
        if compatible_db_types.contains(&DataTypeHint::Int64) =>
            {
                Ok(None)
            }
        DataTypeNode::Int128 | DataTypeNode::Decimal(_, _, DecimalSize::Int128)
        if compatible_db_types.contains(&DataTypeHint::Int128) =>
            {
                Ok(None)
            }

        DataTypeNode::UInt8 if compatible_db_types.contains(&DataTypeHint::UInt8) => Ok(None),
        DataTypeNode::UInt16 | DataTypeNode::Date
        if compatible_db_types.contains(&DataTypeHint::UInt16) =>
            {
                Ok(None)
            }
        DataTypeNode::UInt32 | DataTypeNode::DateTime(_) | DataTypeNode::IPv4
        if compatible_db_types.contains(&DataTypeHint::UInt32) =>
            {
                Ok(None)
            }
        DataTypeNode::UInt64 if compatible_db_types.contains(&DataTypeHint::UInt64) => Ok(None),
        DataTypeNode::UInt128 if compatible_db_types.contains(&DataTypeHint::UInt128) => Ok(None),

        DataTypeNode::Float32 if compatible_db_types.contains(&DataTypeHint::Float32) => Ok(None),
        DataTypeNode::Float64 if compatible_db_types.contains(&DataTypeHint::Float64) => Ok(None),

        // Currently, we allow new JSON type only with `output_format_binary_write_json_as_string`
        DataTypeNode::String | DataTypeNode::JSON
        if compatible_db_types.contains(&DataTypeHint::String) =>
            {
                Ok(None)
            }

        DataTypeNode::FixedString(n)
        if compatible_db_types.contains(&DataTypeHint::FixedString) =>
            {
                Ok(Some(InnerDataTypeValidator::FixedString(*n)))
            }

        // FIXME: IPv4 from ClickHouse ends up reversed.
        //  Ideally, requires a ReversedSeqAccess implementation. Perhaps memoize IPv4 col index?
        // IPv4 = [u8; 4]
        // DataTypeNode::IPv4 if compatible_db_types.contains(&DataTypeHint::IPv4) => Ok(Some(
        //     InnerDataTypeValidator::Array(&DataTypeNode::UInt8, ArrayValidatorState::Pending(4)),
        // )),

        // IPv6 = [u8; 16]
        DataTypeNode::IPv6 if compatible_db_types.contains(&DataTypeHint::Array) => Ok(Some(
            InnerDataTypeValidator::Array(&DataTypeNode::UInt8, ArrayValidatorState::Pending),
        )),

        // UUID = [u64; 2]
        DataTypeNode::UUID => Ok(Some(InnerDataTypeValidator::Tuple(&[
            DataTypeNode::UInt64,
            DataTypeNode::UInt64,
        ]))),

        DataTypeNode::Array(inner_type) if compatible_db_types.contains(&DataTypeHint::Array) => {
            Ok(Some(InnerDataTypeValidator::Array(
                inner_type,
                ArrayValidatorState::Pending,
            )))
        }

        DataTypeNode::Map(key_type, value_type)
        if compatible_db_types.contains(&DataTypeHint::Map) =>
            {
                Ok(Some(InnerDataTypeValidator::Map(
                    key_type,
                    value_type,
                    MapValidatorState::Key,
                )))
            }

        DataTypeNode::Tuple(elements) if compatible_db_types.contains(&DataTypeHint::Tuple) => {
            Ok(Some(InnerDataTypeValidator::Tuple(elements)))
        }

        DataTypeNode::Nullable(inner_type)
        if compatible_db_types.contains(&DataTypeHint::Nullable) =>
            {
                Ok(Some(InnerDataTypeValidator::Nullable(inner_type)))
            }

        // LowCardinality is completely transparent on the client side
        DataTypeNode::LowCardinality(inner_type) => {
            validate_impl(inner_type, serde_type, compatible_db_types)
        }

        DataTypeNode::Enum(EnumType::Enum8, values_map)
        if compatible_db_types.contains(&DataTypeHint::Int8) =>
            {
                Ok(Some(InnerDataTypeValidator::Enum(values_map)))
            }
        DataTypeNode::Enum(EnumType::Enum16, values_map)
        if compatible_db_types.contains(&DataTypeHint::Int16) =>
            {
                Ok(Some(InnerDataTypeValidator::Enum(values_map)))
            }

        DataTypeNode::Variant(possible_types) => {
            Ok(Some(InnerDataTypeValidator::Variant(possible_types)))
        }

        DataTypeNode::AggregateFunction(_, _) => panic!("AggregateFunction is not supported yet"),
        DataTypeNode::Int256 => panic!("Int256 is not supported yet"),
        DataTypeNode::UInt256 => panic!("UInt256 is not supported yet"),
        DataTypeNode::BFloat16 => panic!("BFloat16 is not supported yet"),
        DataTypeNode::Dynamic => panic!("Dynamic is not supported yet"),

        _ => panic!(
            "Database type is {}, but struct field is deserialized as {}, which is compatible only with {:?}",
            data_type, serde_type, compatible_db_types
        ),
    }
}

impl<'cursor> ValidateDataType for DataTypeValidator<'cursor> {
    #[inline]
    fn validate(
        &mut self,
        serde_type: &'static SerdeType,
        compatible_db_types: &'static [DataTypeHint],
    ) -> Result<Option<InnerDataTypeValidator<'cursor>>> {
        match self.columns.split_first() {
            Some((first, rest)) => {
                self.columns = rest;
                validate_impl(&first.data_type, serde_type, compatible_db_types)
            }
            None => panic!("Struct has more fields than columns in the database schema"),
        }
    }

    #[cold]
    #[inline(never)]
    fn validate_enum8(&mut self, _value: i8) {
        unreachable!()
    }

    #[cold]
    #[inline(never)]
    fn validate_enum16(&mut self, _value: i16) {
        unreachable!()
    }

    #[cold]
    #[inline(never)]
    fn validate_fixed_string(&mut self, _len: usize) {
        unreachable!()
    }
}

/// Which Serde data type (De)serializer used for the given type.
/// Displays into Rust types for convenience in errors reporting.
#[derive(Clone, Debug, PartialEq)]
#[allow(dead_code)]
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
