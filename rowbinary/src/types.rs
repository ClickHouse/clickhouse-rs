use crate::error::ParserError;
use std::collections::HashMap;
use std::fmt::{Display, Formatter};

#[derive(Debug, Clone, PartialEq)]
pub struct Column {
    pub name: String,
    pub data_type: DataTypeNode,
    pub type_hints: Vec<DataTypeHint>,
}

impl Column {
    pub fn new(name: String, data_type: DataTypeNode) -> Self {
        let type_hints = data_type.get_type_hints();
        Self {
            name,
            data_type,
            type_hints,
        }
    }
}

impl Display for Column {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.name, self.data_type)
    }
}

#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum DataTypeNode {
    Bool,

    UInt8,
    UInt16,
    UInt32,
    UInt64,
    UInt128,
    UInt256,

    Int8,
    Int16,
    Int32,
    Int64,
    Int128,
    Int256,

    Float32,
    Float64,
    BFloat16,
    Decimal(u8, u8, DecimalSize), // Scale, Precision, 32 | 64 | 128 | 256

    String,
    FixedString(usize),
    UUID,

    Date,
    Date32,
    DateTime(Option<String>),                      // Optional timezone
    DateTime64(DateTimePrecision, Option<String>), // Precision and optional timezone

    IPv4,
    IPv6,

    Nullable(Box<DataTypeNode>),
    LowCardinality(Box<DataTypeNode>),

    Array(Box<DataTypeNode>),
    Tuple(Vec<DataTypeNode>),
    Map(Box<DataTypeNode>, Box<DataTypeNode>),
    Enum(EnumType, HashMap<i16, String>),

    AggregateFunction(String, Vec<DataTypeNode>),

    Variant(Vec<DataTypeNode>),
    Dynamic,
    JSON,
    // TODO: Geo
}

// TODO - should be the same top-levels as DataTypeNode;
//  gen from DataTypeNode via macro maybe?
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum DataTypeHint {
    Bool,

    UInt8,
    UInt16,
    UInt32,
    UInt64,
    UInt128,
    UInt256,

    Int8,
    Int16,
    Int32,
    Int64,
    Int128,
    Int256,

    Float32,
    Float64,
    BFloat16,
    Decimal(DecimalSize),

    String,
    FixedString(usize),
    UUID,

    Date,
    Date32,
    DateTime,
    DateTime64,

    IPv4,
    IPv6,

    Nullable,
    LowCardinality,

    Array,
    Tuple,
    Map,
    Enum,

    AggregateFunction,

    Variant,
    Dynamic,
    JSON,
    // TODO: Geo
}

impl Display for DataTypeHint {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            DataTypeHint::Bool => write!(f, "Bool"),
            DataTypeHint::UInt8 => write!(f, "UInt8"),
            DataTypeHint::UInt16 => write!(f, "UInt16"),
            DataTypeHint::UInt32 => write!(f, "UInt32"),
            DataTypeHint::UInt64 => write!(f, "UInt64"),
            DataTypeHint::UInt128 => write!(f, "UInt128"),
            DataTypeHint::UInt256 => write!(f, "UInt256"),
            DataTypeHint::Int8 => write!(f, "Int8"),
            DataTypeHint::Int16 => write!(f, "Int16"),
            DataTypeHint::Int32 => write!(f, "Int32"),
            DataTypeHint::Int64 => write!(f, "Int64"),
            DataTypeHint::Int128 => write!(f, "Int128"),
            DataTypeHint::Int256 => write!(f, "Int256"),
            DataTypeHint::Float32 => write!(f, "Float32"),
            DataTypeHint::Float64 => write!(f, "Float64"),
            DataTypeHint::BFloat16 => write!(f, "BFloat16"),
            DataTypeHint::Decimal(size) => write!(f, "Decimal{}", size),
            DataTypeHint::String => write!(f, "String"),
            DataTypeHint::FixedString(size) => write!(f, "FixedString({})", size),
            DataTypeHint::UUID => write!(f, "UUID"),
            DataTypeHint::Date => write!(f, "Date"),
            DataTypeHint::Date32 => write!(f, "Date32"),
            DataTypeHint::DateTime => write!(f, "DateTime"),
            DataTypeHint::DateTime64 => write!(f, "DateTime64"),
            DataTypeHint::IPv4 => write!(f, "IPv4"),
            DataTypeHint::IPv6 => write!(f, "IPv6"),
            DataTypeHint::Nullable => write!(f, "Nullable"),
            DataTypeHint::LowCardinality => write!(f, "LowCardinality"),
            DataTypeHint::Array => {
                write!(f, "Array")
            }
            DataTypeHint::Tuple => {
                write!(f, "Tuple")
            }
            DataTypeHint::Map => {
                write!(f, "Map")
            }
            DataTypeHint::Enum => {
                write!(f, "Enum")
            }
            DataTypeHint::AggregateFunction => {
                write!(f, "AggregateFunction")
            }
            DataTypeHint::Variant => {
                write!(f, "Variant")
            }
            DataTypeHint::Dynamic => {
                write!(f, "Dynamic")
            }
            DataTypeHint::JSON => {
                write!(f, "JSON")
            }
        }
    }
}

impl Into<String> for DataTypeHint {
    fn into(self) -> String {
        self.to_string()
    }
}

macro_rules! data_type_is {
    ($method:ident, $pattern:pat) => {
        #[inline]
        pub fn $method(&self) -> Result<(), ParserError> {
            match self {
                $pattern => Ok(()),
                _ => Err(ParserError::TypeParsingError(format!(
                    "Expected {}, got {}",
                    stringify!($pattern),
                    self
                ))),
            }
        }
    };
}

impl DataTypeNode {
    pub fn new(name: &str) -> Result<Self, ParserError> {
        match name {
            "UInt8" => Ok(Self::UInt8),
            "UInt16" => Ok(Self::UInt16),
            "UInt32" => Ok(Self::UInt32),
            "UInt64" => Ok(Self::UInt64),
            "UInt128" => Ok(Self::UInt128),
            "UInt256" => Ok(Self::UInt256),
            "Int8" => Ok(Self::Int8),
            "Int16" => Ok(Self::Int16),
            "Int32" => Ok(Self::Int32),
            "Int64" => Ok(Self::Int64),
            "Int128" => Ok(Self::Int128),
            "Int256" => Ok(Self::Int256),
            "Float32" => Ok(Self::Float32),
            "Float64" => Ok(Self::Float64),
            "BFloat16" => Ok(Self::BFloat16),
            "String" => Ok(Self::String),
            "UUID" => Ok(Self::UUID),
            "Date" => Ok(Self::Date),
            "Date32" => Ok(Self::Date32),
            "IPv4" => Ok(Self::IPv4),
            "IPv6" => Ok(Self::IPv6),
            "Bool" => Ok(Self::Bool),
            "Dynamic" => Ok(Self::Dynamic),
            "JSON" => Ok(Self::JSON),

            str if str.starts_with("Decimal") => parse_decimal(str),
            str if str.starts_with("DateTime64") => parse_datetime64(str),
            str if str.starts_with("DateTime") => parse_datetime(str),

            str if str.starts_with("Nullable") => parse_nullable(str),
            str if str.starts_with("LowCardinality") => parse_low_cardinality(str),
            str if str.starts_with("FixedString") => parse_fixed_string(str),

            str if str.starts_with("Array") => parse_array(str),
            str if str.starts_with("Enum") => parse_enum(str),
            str if str.starts_with("Map") => parse_map(str),
            str if str.starts_with("Tuple") => parse_tuple(str),
            str if str.starts_with("Variant") => parse_variant(str),

            // ...
            str => Err(ParserError::TypeParsingError(format!(
                "Unknown data type: {}",
                str
            ))),
        }
    }

    pub fn get_type_hints_internal(&self, hints: &mut Vec<DataTypeHint>) {
        match self {
            DataTypeNode::Bool => hints.push(DataTypeHint::Bool),
            DataTypeNode::UInt8 => hints.push(DataTypeHint::UInt8),
            DataTypeNode::UInt16 => hints.push(DataTypeHint::UInt16),
            DataTypeNode::UInt32 => hints.push(DataTypeHint::UInt32),
            DataTypeNode::UInt64 => hints.push(DataTypeHint::UInt64),
            DataTypeNode::UInt128 => hints.push(DataTypeHint::UInt128),
            DataTypeNode::UInt256 => hints.push(DataTypeHint::UInt256),
            DataTypeNode::Int8 => hints.push(DataTypeHint::Int8),
            DataTypeNode::Int16 => hints.push(DataTypeHint::Int16),
            DataTypeNode::Int32 => hints.push(DataTypeHint::Int32),
            DataTypeNode::Int64 => hints.push(DataTypeHint::Int64),
            DataTypeNode::Int128 => hints.push(DataTypeHint::Int128),
            DataTypeNode::Int256 => hints.push(DataTypeHint::Int256),
            DataTypeNode::Float32 => hints.push(DataTypeHint::Float32),
            DataTypeNode::Float64 => hints.push(DataTypeHint::Float64),
            DataTypeNode::BFloat16 => hints.push(DataTypeHint::BFloat16),
            DataTypeNode::Decimal(_, _, size) => {
                hints.push(DataTypeHint::Decimal(size.clone()));
            }
            DataTypeNode::String => hints.push(DataTypeHint::String),
            DataTypeNode::FixedString(size) => hints.push(DataTypeHint::FixedString(*size)),
            DataTypeNode::UUID => hints.push(DataTypeHint::UUID),
            DataTypeNode::Date => hints.push(DataTypeHint::Date),
            DataTypeNode::Date32 => hints.push(DataTypeHint::Date32),
            DataTypeNode::DateTime(_) => hints.push(DataTypeHint::DateTime),
            DataTypeNode::DateTime64(_, _) => hints.push(DataTypeHint::DateTime64),
            DataTypeNode::IPv4 => hints.push(DataTypeHint::IPv4),
            DataTypeNode::IPv6 => hints.push(DataTypeHint::IPv6),
            DataTypeNode::Nullable(inner) => {
                hints.push(DataTypeHint::Nullable);
                inner.get_type_hints_internal(hints);
            }
            DataTypeNode::LowCardinality(inner) => {
                hints.push(DataTypeHint::LowCardinality);
                inner.get_type_hints_internal(hints);
            }
            DataTypeNode::Array(inner) => {
                hints.push(DataTypeHint::Array);
                inner.get_type_hints_internal(hints);
            }
            DataTypeNode::Tuple(elements) => {
                hints.push(DataTypeHint::Tuple);
                for element in elements {
                    element.get_type_hints_internal(hints);
                }
            }
            DataTypeNode::Map(key, value) => {
                hints.push(DataTypeHint::Map);
                key.get_type_hints_internal(hints);
                value.get_type_hints_internal(hints);
            }
            DataTypeNode::Enum(_, _) => hints.push(DataTypeHint::Enum),
            DataTypeNode::AggregateFunction(_, args) => {
                hints.push(DataTypeHint::AggregateFunction);
                for arg in args {
                    arg.get_type_hints_internal(hints);
                }
            }
            DataTypeNode::Variant(types) => {
                hints.push(DataTypeHint::Variant);
                for ty in types {
                    ty.get_type_hints_internal(hints);
                }
            }
            DataTypeNode::Dynamic => hints.push(DataTypeHint::Dynamic),
            DataTypeNode::JSON => hints.push(DataTypeHint::JSON),
        }
    }

    pub fn get_type_hints(&self) -> Vec<DataTypeHint> {
        let capacity = match self {
            DataTypeNode::Tuple(elements) | DataTypeNode::Variant(elements) => elements.len() + 1,
            DataTypeNode::Map(_, _) => 3,
            DataTypeNode::Nullable(_)
            | DataTypeNode::LowCardinality(_)
            | DataTypeNode::Array(_) => 2,
            _ => 1,
        };
        let mut vec = Vec::with_capacity(capacity);
        self.get_type_hints_internal(&mut vec);
        vec
    }

    data_type_is!(is_bool, DataTypeNode::Bool);
    data_type_is!(is_uint8, DataTypeNode::UInt8);
    data_type_is!(is_uint16, DataTypeNode::UInt16);
    data_type_is!(is_uint32, DataTypeNode::UInt32);
    data_type_is!(is_uint64, DataTypeNode::UInt64);
    data_type_is!(is_uint128, DataTypeNode::UInt128);
    data_type_is!(is_uint256, DataTypeNode::UInt256);
    data_type_is!(is_int8, DataTypeNode::Int8);
    data_type_is!(is_int16, DataTypeNode::Int16);
    data_type_is!(is_int32, DataTypeNode::Int32);
    data_type_is!(is_int64, DataTypeNode::Int64);
    data_type_is!(is_int128, DataTypeNode::Int128);
    data_type_is!(is_int256, DataTypeNode::Int256);
    data_type_is!(is_float32, DataTypeNode::Float32);
    data_type_is!(is_float64, DataTypeNode::Float64);
    data_type_is!(is_bfloat16, DataTypeNode::BFloat16);
    data_type_is!(is_string, DataTypeNode::String);
    data_type_is!(is_uuid, DataTypeNode::UUID);
    data_type_is!(is_date, DataTypeNode::Date);
    data_type_is!(is_date32, DataTypeNode::Date32);
    data_type_is!(is_datetime, DataTypeNode::DateTime(_));
    data_type_is!(is_datetime64, DataTypeNode::DateTime64(_, _));
    data_type_is!(is_ipv4, DataTypeNode::IPv4);
    data_type_is!(is_ipv6, DataTypeNode::IPv6);
    data_type_is!(is_nullable, DataTypeNode::Nullable(_));
    data_type_is!(is_array, DataTypeNode::Array(_));
    data_type_is!(is_tuple, DataTypeNode::Tuple(_));
    data_type_is!(is_map, DataTypeNode::Map(_, _));
    data_type_is!(is_low_cardinality, DataTypeNode::LowCardinality(_));
    data_type_is!(is_decimal, DataTypeNode::Decimal(_, _, _));
    data_type_is!(is_enum, DataTypeNode::Enum(_, _));
    data_type_is!(is_aggregate_function, DataTypeNode::AggregateFunction(_, _));
    data_type_is!(is_fixed_string, DataTypeNode::FixedString(_));
    data_type_is!(is_variant, DataTypeNode::Variant(_));
    data_type_is!(is_dynamic, DataTypeNode::Dynamic);
    data_type_is!(is_json, DataTypeNode::JSON);
}

impl Into<String> for DataTypeNode {
    fn into(self) -> String {
        self.to_string()
    }
}

impl Display for DataTypeNode {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        use DataTypeNode::*;
        let str = match self {
            UInt8 => "UInt8".to_string(),
            UInt16 => "UInt16".to_string(),
            UInt32 => "UInt32".to_string(),
            UInt64 => "UInt64".to_string(),
            UInt128 => "UInt128".to_string(),
            UInt256 => "UInt256".to_string(),
            Int8 => "Int8".to_string(),
            Int16 => "Int16".to_string(),
            Int32 => "Int32".to_string(),
            Int64 => "Int64".to_string(),
            Int128 => "Int128".to_string(),
            Int256 => "Int256".to_string(),
            Float32 => "Float32".to_string(),
            Float64 => "Float64".to_string(),
            BFloat16 => "BFloat16".to_string(),
            Decimal(precision, scale, _) => {
                format!("Decimal({}, {})", precision, scale)
            }
            String => "String".to_string(),
            UUID => "UUID".to_string(),
            Date => "Date".to_string(),
            Date32 => "Date32".to_string(),
            DateTime(None) => "DateTime".to_string(),
            DateTime(Some(tz)) => format!("DateTime('{}')", tz),
            DateTime64(precision, None) => format!("DateTime64({})", precision),
            DateTime64(precision, Some(tz)) => format!("DateTime64({}, '{}')", precision, tz),
            IPv4 => "IPv4".to_string(),
            IPv6 => "IPv6".to_string(),
            Bool => "Bool".to_string(),
            Nullable(inner) => format!("Nullable({})", inner.to_string()),
            Array(inner) => format!("Array({})", inner.to_string()),
            Tuple(elements) => {
                let elements_str = data_types_to_string(elements);
                format!("Tuple({})", elements_str)
            }
            Map(key, value) => {
                format!("Map({}, {})", key.to_string(), value.to_string())
            }
            LowCardinality(inner) => {
                format!("LowCardinality({})", inner.to_string())
            }
            Enum(enum_type, values) => {
                let mut values_vec = values.iter().collect::<Vec<_>>();
                values_vec.sort_by(|(i1, _), (i2, _)| (*i1).cmp(*i2));
                let values_str = values_vec
                    .iter()
                    .map(|(index, name)| format!("'{}' = {}", name, index))
                    .collect::<Vec<_>>()
                    .join(", ");
                format!("{}({})", enum_type, values_str)
            }
            AggregateFunction(func_name, args) => {
                let args_str = data_types_to_string(args);
                format!("AggregateFunction({}, {})", func_name, args_str)
            }
            FixedString(size) => {
                format!("FixedString({})", size)
            }
            Variant(types) => {
                let types_str = data_types_to_string(types);
                format!("Variant({})", types_str)
            }
            JSON => "JSON".to_string(),
            Dynamic => "Dynamic".to_string(),
        };
        write!(f, "{}", str)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum EnumType {
    Enum8,
    Enum16,
}

impl Display for EnumType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EnumType::Enum8 => write!(f, "Enum8"),
            EnumType::Enum16 => write!(f, "Enum16"),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum DateTimePrecision {
    Precision0,
    Precision1,
    Precision2,
    Precision3,
    Precision4,
    Precision5,
    Precision6,
    Precision7,
    Precision8,
    Precision9,
}

impl DateTimePrecision {
    pub(crate) fn new(char: char) -> Result<DateTimePrecision, ParserError> {
        match char {
            '0' => Ok(DateTimePrecision::Precision0),
            '1' => Ok(DateTimePrecision::Precision1),
            '2' => Ok(DateTimePrecision::Precision2),
            '3' => Ok(DateTimePrecision::Precision3),
            '4' => Ok(DateTimePrecision::Precision4),
            '5' => Ok(DateTimePrecision::Precision5),
            '6' => Ok(DateTimePrecision::Precision6),
            '7' => Ok(DateTimePrecision::Precision7),
            '8' => Ok(DateTimePrecision::Precision8),
            '9' => Ok(DateTimePrecision::Precision9),
            _ => Err(ParserError::TypeParsingError(format!(
                "Invalid DateTime64 precision, expected to be within [0, 9] interval, got {}",
                char
            ))),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum DecimalSize {
    Int32,
    Int64,
    Int128,
    Int256,
}

impl Display for DecimalSize {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            DecimalSize::Int32 => write!(f, "32"),
            DecimalSize::Int64 => write!(f, "64"),
            DecimalSize::Int128 => write!(f, "128"),
            DecimalSize::Int256 => write!(f, "256"),
        }
    }
}

impl DecimalSize {
    pub(crate) fn new(precision: u8) -> Result<Self, ParserError> {
        if precision <= 9 {
            Ok(DecimalSize::Int32)
        } else if precision <= 18 {
            Ok(DecimalSize::Int64)
        } else if precision <= 38 {
            Ok(DecimalSize::Int128)
        } else if precision <= 76 {
            Ok(DecimalSize::Int256)
        } else {
            return Err(ParserError::TypeParsingError(format!(
                "Invalid Decimal precision: {}",
                precision
            )));
        }
    }
}

impl Display for DateTimePrecision {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DateTimePrecision::Precision0 => write!(f, "0"),
            DateTimePrecision::Precision1 => write!(f, "1"),
            DateTimePrecision::Precision2 => write!(f, "2"),
            DateTimePrecision::Precision3 => write!(f, "3"),
            DateTimePrecision::Precision4 => write!(f, "4"),
            DateTimePrecision::Precision5 => write!(f, "5"),
            DateTimePrecision::Precision6 => write!(f, "6"),
            DateTimePrecision::Precision7 => write!(f, "7"),
            DateTimePrecision::Precision8 => write!(f, "8"),
            DateTimePrecision::Precision9 => write!(f, "9"),
        }
    }
}

fn data_types_to_string(elements: &[DataTypeNode]) -> String {
    elements
        .iter()
        .map(|a| a.to_string())
        .collect::<Vec<_>>()
        .join(", ")
}

fn parse_fixed_string(input: &str) -> Result<DataTypeNode, ParserError> {
    if input.len() >= 14 {
        let size_str = &input[12..input.len() - 1];
        let size = size_str.parse::<usize>().map_err(|err| {
                ParserError::TypeParsingError(format!(
                    "Invalid FixedString size, expected a valid number. Underlying error: {}, input: {}, size_str: {}",
                    err, input, size_str
                ))
            })?;
        if size == 0 {
            return Err(ParserError::TypeParsingError(format!(
                "Invalid FixedString size, expected a positive number, got zero. Input: {}",
                input
            )));
        }
        return Ok(DataTypeNode::FixedString(size));
    }
    Err(ParserError::TypeParsingError(format!(
        "Invalid FixedString format, expected FixedString(N), got {}",
        input
    )))
}

fn parse_array(input: &str) -> Result<DataTypeNode, ParserError> {
    if input.len() >= 8 {
        let inner_type_str = &input[6..input.len() - 1];
        let inner_type = DataTypeNode::new(inner_type_str)?;
        return Ok(DataTypeNode::Array(Box::new(inner_type)));
    }
    Err(ParserError::TypeParsingError(format!(
        "Invalid Array format, expected Array(InnerType), got {}",
        input
    )))
}

fn parse_enum(input: &str) -> Result<DataTypeNode, ParserError> {
    if input.len() >= 9 {
        let (enum_type, prefix_len) = if input.starts_with("Enum8") {
            (EnumType::Enum8, 6)
        } else if input.starts_with("Enum16") {
            (EnumType::Enum16, 7)
        } else {
            return Err(ParserError::TypeParsingError(format!(
                "Invalid Enum type, expected Enum8 or Enum16, got {}",
                input
            )));
        };
        let enum_values_map_str = &input[prefix_len..input.len() - 1];
        let enum_values_map = parse_enum_values_map(enum_values_map_str)?;
        return Ok(DataTypeNode::Enum(enum_type, enum_values_map));
    }
    Err(ParserError::TypeParsingError(format!(
        "Invalid Enum format, expected Enum8('name' = value), got {}",
        input
    )))
}

fn parse_datetime(input: &str) -> Result<DataTypeNode, ParserError> {
    if input == "DateTime" {
        return Ok(DataTypeNode::DateTime(None));
    }
    if input.len() >= 12 {
        let timezone = (&input[10..input.len() - 2]).to_string();
        return Ok(DataTypeNode::DateTime(Some(timezone)));
    }
    Err(ParserError::TypeParsingError(format!(
        "Invalid DateTime format, expected DateTime('timezone'), got {}",
        input
    )))
}

fn parse_decimal(input: &str) -> Result<DataTypeNode, ParserError> {
    if input.len() >= 10 {
        let precision_and_scale_str = (&input[8..input.len() - 1]).split(", ").collect::<Vec<_>>();
        if precision_and_scale_str.len() != 2 {
            return Err(ParserError::TypeParsingError(format!(
                "Invalid Decimal format, expected Decimal(P, S), got {}",
                input
            )));
        }
        let parsed = precision_and_scale_str
            .iter()
            .map(|s| s.parse::<u8>())
            .collect::<Result<Vec<_>, _>>()
            .map_err(|err| {
                ParserError::TypeParsingError(format!(
                    "Invalid Decimal format, expected Decimal(P, S), got {}. Underlying error: {}",
                    input, err
                ))
            })?;
        let precision = parsed[0];
        let scale = parsed[1];
        if scale < 1 || precision < 1 {
            return Err(ParserError::TypeParsingError(format!(
                "Invalid Decimal format, expected Decimal(P, S) with P > 0 and S > 0, got {}",
                input
            )));
        }
        if precision < scale {
            return Err(ParserError::TypeParsingError(format!(
                "Invalid Decimal format, expected Decimal(P, S) with P >= S, got {}",
                input
            )));
        }
        let size = DecimalSize::new(parsed[0])?;
        return Ok(DataTypeNode::Decimal(precision, scale, size));
    }
    Err(ParserError::TypeParsingError(format!(
        "Invalid Decimal format, expected Decimal(P), got {}",
        input
    )))
}

fn parse_datetime64(input: &str) -> Result<DataTypeNode, ParserError> {
    if input.len() >= 13 {
        let mut chars = (&input[11..input.len() - 1]).chars();
        let precision_char = chars.next().ok_or(ParserError::TypeParsingError(format!(
            "Invalid DateTime64 precision, expected a positive number. Input: {}",
            input
        )))?;
        let precision = DateTimePrecision::new(precision_char)?;
        let maybe_tz = match chars.as_str() {
            str if str.len() > 2 => Some((&str[3..str.len() - 1]).to_string()),
            _ => None,
        };
        return Ok(DataTypeNode::DateTime64(precision, maybe_tz));
    }
    Err(ParserError::TypeParsingError(format!(
        "Invalid DateTime format, expected DateTime('timezone'), got {}",
        input
    )))
}

fn parse_low_cardinality(input: &str) -> Result<DataTypeNode, ParserError> {
    if input.len() >= 16 {
        let inner_type_str = &input[15..input.len() - 1];
        let inner_type = DataTypeNode::new(inner_type_str)?;
        return Ok(DataTypeNode::LowCardinality(Box::new(inner_type)));
    }
    Err(ParserError::TypeParsingError(format!(
        "Invalid LowCardinality format, expected LowCardinality(InnerType), got {}",
        input
    )))
}

fn parse_nullable(input: &str) -> Result<DataTypeNode, ParserError> {
    if input.len() >= 10 {
        let inner_type_str = &input[9..input.len() - 1];
        let inner_type = DataTypeNode::new(inner_type_str)?;
        return Ok(DataTypeNode::Nullable(Box::new(inner_type)));
    }
    Err(ParserError::TypeParsingError(format!(
        "Invalid Nullable format, expected Nullable(InnerType), got {}",
        input
    )))
}

fn parse_map(input: &str) -> Result<DataTypeNode, ParserError> {
    if input.len() >= 5 {
        let inner_types_str = &input[4..input.len() - 1];
        let inner_types = parse_inner_types(inner_types_str)?;
        if inner_types.len() != 2 {
            return Err(ParserError::TypeParsingError(format!(
                "Expected two inner elements in a Map from input {}",
                input
            )));
        }
        return Ok(DataTypeNode::Map(
            Box::new(inner_types[0].clone()),
            Box::new(inner_types[1].clone()),
        ));
    }
    Err(ParserError::TypeParsingError(format!(
        "Invalid Map format, expected Map(KeyType, ValueType), got {}",
        input
    )))
}

fn parse_tuple(input: &str) -> Result<DataTypeNode, ParserError> {
    if input.len() > 7 {
        let inner_types_str = &input[6..input.len() - 1];
        let inner_types = parse_inner_types(inner_types_str)?;
        if inner_types.is_empty() {
            return Err(ParserError::TypeParsingError(format!(
                "Expected at least one inner element in a Tuple from input {}",
                input
            )));
        }
        return Ok(DataTypeNode::Tuple(inner_types));
    }
    Err(ParserError::TypeParsingError(format!(
        "Invalid Tuple format, expected Tuple(Type1, Type2, ...), got {}",
        input
    )))
}

fn parse_variant(input: &str) -> Result<DataTypeNode, ParserError> {
    if input.len() >= 9 {
        let inner_types_str = &input[8..input.len() - 1];
        let inner_types = parse_inner_types(inner_types_str)?;
        return Ok(DataTypeNode::Variant(inner_types));
    }
    Err(ParserError::TypeParsingError(format!(
        "Invalid Variant format, expected Variant(Type1, Type2, ...), got {}",
        input
    )))
}

/// Considers the element type parsed once we reach a comma outside of parens AND after an unescaped tick.
/// The most complicated cases are values names in the self-defined Enum types:
/// ```
///  let input1 = "Tuple(Enum8('f\'()' = 1))`";  // the result is  `f\'()`
///  let input2 = "Tuple(Enum8('(' = 1))";       // the result is  `(`
/// ```
fn parse_inner_types(input: &str) -> Result<Vec<DataTypeNode>, ParserError> {
    let mut inner_types: Vec<DataTypeNode> = Vec::new();

    let input_bytes = input.as_bytes();

    let mut open_parens = 0;
    let mut quote_open = false;
    let mut char_escaped = false;
    let mut last_element_index = 0;

    let mut i = 0;
    while i < input_bytes.len() {
        if char_escaped {
            char_escaped = false;
        } else if input_bytes[i] == b'\\' {
            char_escaped = true;
        } else if input_bytes[i] == b'\'' {
            quote_open = !quote_open; // unescaped quote
        } else {
            if !quote_open {
                if input_bytes[i] == b'(' {
                    open_parens += 1;
                } else if input_bytes[i] == b')' {
                    open_parens -= 1;
                } else if input_bytes[i] == b',' {
                    if open_parens == 0 {
                        let data_type_str =
                            String::from_utf8(input_bytes[last_element_index..i].to_vec())
                                .map_err(|_| {
                                    ParserError::TypeParsingError(format!(
                                "Invalid UTF-8 sequence in input for the inner data type: {}",
                                &input[last_element_index..]
                            ))
                                })?;
                        let data_type = DataTypeNode::new(&data_type_str)?;
                        inner_types.push(data_type);
                        // Skip ', ' (comma and space)
                        if i + 2 <= input_bytes.len() && input_bytes[i + 1] == b' ' {
                            i += 2;
                        } else {
                            i += 1;
                        }
                        last_element_index = i;
                        continue; // Skip the normal increment at the end of the loop
                    }
                }
            }
        }
        i += 1;
    }

    // Push the remaining part of the type if it seems to be valid (at least all parentheses are closed)
    if open_parens == 0 && last_element_index < input_bytes.len() {
        let data_type_str =
            String::from_utf8(input_bytes[last_element_index..].to_vec()).map_err(|_| {
                ParserError::TypeParsingError(format!(
                    "Invalid UTF-8 sequence in input for the inner data type: {}",
                    &input[last_element_index..]
                ))
            })?;
        let data_type = DataTypeNode::new(&data_type_str)?;
        inner_types.push(data_type);
    }

    Ok(inner_types)
}

fn parse_enum_values_map(input: &str) -> Result<HashMap<i16, String>, ParserError> {
    let mut names: Vec<String> = Vec::new();
    let mut indices: Vec<i16> = Vec::new();
    let mut parsing_name = true; // false when parsing the index
    let mut char_escaped = false; // we should ignore escaped ticks
    let mut start_index = 1; // Skip the first '

    let mut i = 1;
    let input_bytes = input.as_bytes();
    while i < input_bytes.len() {
        if parsing_name {
            if char_escaped {
                char_escaped = false;
            } else {
                if input_bytes[i] == b'\\' {
                    char_escaped = true;
                } else if input_bytes[i] == b'\'' {
                    // non-escaped closing tick - push the name
                    let name_bytes = &input_bytes[start_index..i];
                    let name = String::from_utf8(name_bytes.to_vec()).map_err(|_| {
                        ParserError::TypeParsingError(format!(
                            "Invalid UTF-8 sequence in input for the enum name: {}",
                            &input[start_index..i]
                        ))
                    })?;
                    names.push(name);

                    // Skip ` = ` and the first digit, as it will always have at least one
                    if i + 4 >= input_bytes.len() {
                        return Err(ParserError::TypeParsingError(format!(
                            "Invalid Enum format - expected ` = ` after name, input: {}",
                            input,
                        )));
                    }
                    i += 4;
                    start_index = i;
                    parsing_name = false;
                }
            }
        }
        // Parsing the index, skipping next iterations until the first non-digit one
        else if input_bytes[i] < b'0' || input_bytes[i] > b'9' {
            let index = String::from_utf8(input_bytes[start_index..i].to_vec())
                .map_err(|_| {
                    ParserError::TypeParsingError(format!(
                        "Invalid UTF-8 sequence in input for the enum index: {}",
                        &input[start_index..i]
                    ))
                })?
                .parse::<i16>()
                .map_err(|_| {
                    ParserError::TypeParsingError(format!(
                        "Invalid Enum index, expected a valid number. Input: {}",
                        input
                    ))
                })?;
            indices.push(index);

            // the char at this index should be comma
            // Skip `, '`, but not the first char - ClickHouse allows something like Enum8('foo' = 0, '' = 42)
            if i + 2 >= input_bytes.len() {
                break; // At the end of the enum, no more entries
            }
            i += 2;
            start_index = i + 1;
            parsing_name = true;
            char_escaped = false;
        }

        i += 1;
    }

    let index = String::from_utf8(input_bytes[start_index..i].to_vec())
        .map_err(|_| {
            ParserError::TypeParsingError(format!(
                "Invalid UTF-8 sequence in input for the enum index: {}",
                &input[start_index..i]
            ))
        })?
        .parse::<i16>()
        .map_err(|_| {
            ParserError::TypeParsingError(format!(
                "Invalid Enum index, expected a valid number. Input: {}",
                input
            ))
        })?;
    indices.push(index);

    if names.len() != indices.len() {
        return Err(ParserError::TypeParsingError(format!(
                "Invalid Enum format - expected the same number of names and indices, got names: {}, indices: {}",
                names.join(", "),
                indices.iter().map(|index| index.to_string()).collect::<Vec<String>>().join(", "),
            )));
    }

    Ok(indices
        .into_iter()
        .zip(names)
        .collect::<HashMap<i16, String>>())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_data_type_new_simple() {
        assert_eq!(DataTypeNode::new("UInt8").unwrap(), DataTypeNode::UInt8);
        assert_eq!(DataTypeNode::new("UInt16").unwrap(), DataTypeNode::UInt16);
        assert_eq!(DataTypeNode::new("UInt32").unwrap(), DataTypeNode::UInt32);
        assert_eq!(DataTypeNode::new("UInt64").unwrap(), DataTypeNode::UInt64);
        assert_eq!(DataTypeNode::new("UInt128").unwrap(), DataTypeNode::UInt128);
        assert_eq!(DataTypeNode::new("UInt256").unwrap(), DataTypeNode::UInt256);
        assert_eq!(DataTypeNode::new("Int8").unwrap(), DataTypeNode::Int8);
        assert_eq!(DataTypeNode::new("Int16").unwrap(), DataTypeNode::Int16);
        assert_eq!(DataTypeNode::new("Int32").unwrap(), DataTypeNode::Int32);
        assert_eq!(DataTypeNode::new("Int64").unwrap(), DataTypeNode::Int64);
        assert_eq!(DataTypeNode::new("Int128").unwrap(), DataTypeNode::Int128);
        assert_eq!(DataTypeNode::new("Int256").unwrap(), DataTypeNode::Int256);
        assert_eq!(DataTypeNode::new("Float32").unwrap(), DataTypeNode::Float32);
        assert_eq!(DataTypeNode::new("Float64").unwrap(), DataTypeNode::Float64);
        assert_eq!(
            DataTypeNode::new("BFloat16").unwrap(),
            DataTypeNode::BFloat16
        );
        assert_eq!(DataTypeNode::new("String").unwrap(), DataTypeNode::String);
        assert_eq!(DataTypeNode::new("UUID").unwrap(), DataTypeNode::UUID);
        assert_eq!(DataTypeNode::new("Date").unwrap(), DataTypeNode::Date);
        assert_eq!(DataTypeNode::new("Date32").unwrap(), DataTypeNode::Date32);
        assert_eq!(DataTypeNode::new("IPv4").unwrap(), DataTypeNode::IPv4);
        assert_eq!(DataTypeNode::new("IPv6").unwrap(), DataTypeNode::IPv6);
        assert_eq!(DataTypeNode::new("Bool").unwrap(), DataTypeNode::Bool);
        assert_eq!(DataTypeNode::new("Dynamic").unwrap(), DataTypeNode::Dynamic);
        assert_eq!(DataTypeNode::new("JSON").unwrap(), DataTypeNode::JSON);
        assert!(DataType::new("SomeUnknownType").is_err(),);
    }

    #[test]
    fn test_data_type_new_fixed_string() {
        assert_eq!(
            DataTypeNode::new("FixedString(1)").unwrap(),
            DataTypeNode::FixedString(1)
        );
        assert_eq!(
            DataTypeNode::new("FixedString(16)").unwrap(),
            DataTypeNode::FixedString(16)
        );
        assert_eq!(
            DataTypeNode::new("FixedString(255)").unwrap(),
            DataTypeNode::FixedString(255)
        );
        assert_eq!(
            DataTypeNode::new("FixedString(65535)").unwrap(),
            DataTypeNode::FixedString(65_535)
        );
        assert!(DataTypeNode::new("FixedString()").is_err());
        assert!(DataTypeNode::new("FixedString(0)").is_err());
        assert!(DataTypeNode::new("FixedString(-1)").is_err());
        assert!(DataTypeNode::new("FixedString(abc)").is_err());
    }

    #[test]
    fn test_data_type_new_array() {
        assert_eq!(
            DataTypeNode::new("Array(UInt8)").unwrap(),
            DataTypeNode::Array(Box::new(DataTypeNode::UInt8))
        );
        assert_eq!(
            DataTypeNode::new("Array(String)").unwrap(),
            DataTypeNode::Array(Box::new(DataTypeNode::String))
        );
        assert_eq!(
            DataTypeNode::new("Array(FixedString(16))").unwrap(),
            DataTypeNode::Array(Box::new(DataTypeNode::FixedString(16)))
        );
        assert_eq!(
            DataTypeNode::new("Array(Nullable(Int32))").unwrap(),
            DataTypeNode::Array(Box::new(DataTypeNode::Nullable(Box::new(
                DataTypeNode::Int32
            ))))
        );
        assert!(DataTypeNode::new("Array()").is_err());
        assert!(DataTypeNode::new("Array(abc)").is_err());
    }

    #[test]
    fn test_data_type_new_decimal() {
        assert_eq!(
            DataTypeNode::new("Decimal(7, 2)").unwrap(),
            DataTypeNode::Decimal(7, 2, DecimalSize::Int32)
        );
        assert_eq!(
            DataTypeNode::new("Decimal(12, 4)").unwrap(),
            DataTypeNode::Decimal(12, 4, DecimalSize::Int64)
        );
        assert_eq!(
            DataTypeNode::new("Decimal(27, 6)").unwrap(),
            DataTypeNode::Decimal(27, 6, DecimalSize::Int128)
        );
        assert_eq!(
            DataTypeNode::new("Decimal(42, 8)").unwrap(),
            DataTypeNode::Decimal(42, 8, DecimalSize::Int256)
        );
        assert!(DataTypeNode::new("Decimal").is_err());
        assert!(DataTypeNode::new("Decimal(").is_err());
        assert!(DataTypeNode::new("Decimal()").is_err());
        assert!(DataTypeNode::new("Decimal(1)").is_err());
        assert!(DataTypeNode::new("Decimal(1,)").is_err());
        assert!(DataTypeNode::new("Decimal(1, )").is_err());
        assert!(DataTypeNode::new("Decimal(0, 0)").is_err()); // Precision must be > 0
        assert!(DataTypeNode::new("Decimal(x, 0)").is_err()); // Non-numeric precision
        assert!(DataTypeNode::new("Decimal(', ')").is_err());
        assert!(DataTypeNode::new("Decimal(77, 1)").is_err()); // Max precision is 76
        assert!(DataTypeNode::new("Decimal(1, 2)").is_err()); // Scale must be less than precision
        assert!(DataTypeNode::new("Decimal(1, x)").is_err()); // Non-numeric scale
        assert!(DataTypeNode::new("Decimal(42, ,)").is_err());
        assert!(DataTypeNode::new("Decimal(42, ')").is_err());
        assert!(DataTypeNode::new("Decimal(foobar)").is_err());
    }

    #[test]
    fn test_data_type_new_datetime() {
        assert_eq!(
            DataTypeNode::new("DateTime").unwrap(),
            DataTypeNode::DateTime(None)
        );
        assert_eq!(
            DataTypeNode::new("DateTime('UTC')").unwrap(),
            DataTypeNode::DateTime(Some("UTC".to_string()))
        );
        assert_eq!(
            DataTypeNode::new("DateTime('America/New_York')").unwrap(),
            DataTypeNode::DateTime(Some("America/New_York".to_string()))
        );
        assert!(DataTypeNode::new("DateTime()").is_err());
    }

    #[test]
    fn test_data_type_new_datetime64() {
        assert_eq!(
            DataTypeNode::new("DateTime64(0)").unwrap(),
            DataTypeNode::DateTime64(DateTimePrecision::Precision0, None)
        );
        assert_eq!(
            DataTypeNode::new("DateTime64(1)").unwrap(),
            DataTypeNode::DateTime64(DateTimePrecision::Precision1, None)
        );
        assert_eq!(
            DataTypeNode::new("DateTime64(2)").unwrap(),
            DataTypeNode::DateTime64(DateTimePrecision::Precision2, None)
        );
        assert_eq!(
            DataTypeNode::new("DateTime64(3)").unwrap(),
            DataTypeNode::DateTime64(DateTimePrecision::Precision3, None)
        );
        assert_eq!(
            DataTypeNode::new("DateTime64(4)").unwrap(),
            DataTypeNode::DateTime64(DateTimePrecision::Precision4, None)
        );
        assert_eq!(
            DataTypeNode::new("DateTime64(5)").unwrap(),
            DataTypeNode::DateTime64(DateTimePrecision::Precision5, None)
        );
        assert_eq!(
            DataTypeNode::new("DateTime64(6)").unwrap(),
            DataTypeNode::DateTime64(DateTimePrecision::Precision6, None)
        );
        assert_eq!(
            DataTypeNode::new("DateTime64(7)").unwrap(),
            DataTypeNode::DateTime64(DateTimePrecision::Precision7, None)
        );
        assert_eq!(
            DataTypeNode::new("DateTime64(8)").unwrap(),
            DataTypeNode::DateTime64(DateTimePrecision::Precision8, None)
        );
        assert_eq!(
            DataTypeNode::new("DateTime64(9)").unwrap(),
            DataTypeNode::DateTime64(DateTimePrecision::Precision9, None)
        );
        assert_eq!(
            DataTypeNode::new("DateTime64(0, 'UTC')").unwrap(),
            DataTypeNode::DateTime64(DateTimePrecision::Precision0, Some("UTC".to_string()))
        );
        assert_eq!(
            DataTypeNode::new("DateTime64(3, 'America/New_York')").unwrap(),
            DataTypeNode::DateTime64(
                DateTimePrecision::Precision3,
                Some("America/New_York".to_string())
            )
        );
        assert_eq!(
            DataTypeNode::new("DateTime64(6, 'America/New_York')").unwrap(),
            DataTypeNode::DateTime64(
                DateTimePrecision::Precision6,
                Some("America/New_York".to_string())
            )
        );
        assert_eq!(
            DataTypeNode::new("DateTime64(9, 'Europe/Amsterdam')").unwrap(),
            DataTypeNode::DateTime64(
                DateTimePrecision::Precision9,
                Some("Europe/Amsterdam".to_string())
            )
        );
        assert!(DataTypeNode::new("DateTime64()").is_err());
    }

    #[test]
    fn test_data_type_new_low_cardinality() {
        assert_eq!(
            DataTypeNode::new("LowCardinality(UInt8)").unwrap(),
            DataTypeNode::LowCardinality(Box::new(DataTypeNode::UInt8))
        );
        assert_eq!(
            DataTypeNode::new("LowCardinality(String)").unwrap(),
            DataTypeNode::LowCardinality(Box::new(DataTypeNode::String))
        );
        assert_eq!(
            DataTypeNode::new("LowCardinality(Array(Int32))").unwrap(),
            DataTypeNode::LowCardinality(Box::new(DataTypeNode::Array(Box::new(
                DataTypeNode::Int32
            ))))
        );
        assert!(DataTypeNode::new("LowCardinality()").is_err());
    }

    #[test]
    fn test_data_type_new_nullable() {
        assert_eq!(
            DataTypeNode::new("Nullable(UInt8)").unwrap(),
            DataTypeNode::Nullable(Box::new(DataTypeNode::UInt8))
        );
        assert_eq!(
            DataTypeNode::new("Nullable(String)").unwrap(),
            DataTypeNode::Nullable(Box::new(DataTypeNode::String))
        );
        assert!(DataTypeNode::new("Nullable()").is_err());
    }

    #[test]
    fn test_data_type_new_map() {
        assert_eq!(
            DataTypeNode::new("Map(UInt8, String)").unwrap(),
            DataTypeNode::Map(
                Box::new(DataTypeNode::UInt8),
                Box::new(DataTypeNode::String)
            )
        );
        assert_eq!(
            DataTypeNode::new("Map(String, Int32)").unwrap(),
            DataTypeNode::Map(
                Box::new(DataTypeNode::String),
                Box::new(DataTypeNode::Int32)
            )
        );
        assert_eq!(
            DataTypeNode::new("Map(String, Map(Int32, Array(Nullable(String))))").unwrap(),
            DataTypeNode::Map(
                Box::new(DataTypeNode::String),
                Box::new(DataTypeNode::Map(
                    Box::new(DataTypeNode::Int32),
                    Box::new(DataTypeNode::Array(Box::new(DataTypeNode::Nullable(
                        Box::new(DataTypeNode::String)
                    ))))
                ))
            )
        );
        assert!(DataTypeNode::new("Map()").is_err());
    }

    #[test]
    fn test_data_type_new_variant() {
        assert_eq!(
            DataTypeNode::new("Variant(UInt8, String)").unwrap(),
            DataTypeNode::Variant(vec![DataTypeNode::UInt8, DataTypeNode::String])
        );
        assert_eq!(
            DataTypeNode::new("Variant(String, Int32)").unwrap(),
            DataTypeNode::Variant(vec![DataTypeNode::String, DataTypeNode::Int32])
        );
        assert_eq!(
            DataTypeNode::new("Variant(Int32, Array(Nullable(String)), Map(Int32, String))")
                .unwrap(),
            DataTypeNode::Variant(vec![
                DataTypeNode::Int32,
                DataTypeNode::Array(Box::new(DataTypeNode::Nullable(Box::new(
                    DataTypeNode::String
                )))),
                DataTypeNode::Map(
                    Box::new(DataTypeNode::Int32),
                    Box::new(DataTypeNode::String)
                )
            ])
        );
        assert!(DataTypeNode::new("Variant").is_err());
    }

    #[test]
    fn test_data_type_new_tuple() {
        assert_eq!(
            DataTypeNode::new("Tuple(UInt8, String)").unwrap(),
            DataTypeNode::Tuple(vec![DataTypeNode::UInt8, DataTypeNode::String])
        );
        assert_eq!(
            DataTypeNode::new("Tuple(String, Int32)").unwrap(),
            DataTypeNode::Tuple(vec![DataTypeNode::String, DataTypeNode::Int32])
        );
        assert_eq!(
            DataTypeNode::new(
                "Tuple(Int32, Array(Nullable(String)), Map(Int32, Tuple(String, Array(UInt8))))"
            )
            .unwrap(),
            DataTypeNode::Tuple(vec![
                DataTypeNode::Int32,
                DataTypeNode::Array(Box::new(DataTypeNode::Nullable(Box::new(
                    DataTypeNode::String
                )))),
                DataTypeNode::Map(
                    Box::new(DataTypeNode::Int32),
                    Box::new(DataTypeNode::Tuple(vec![
                        DataTypeNode::String,
                        DataTypeNode::Array(Box::new(DataTypeNode::UInt8))
                    ]))
                )
            ])
        );
        assert!(DataTypeNode::new("Tuple").is_err());
    }

    #[test]
    fn test_data_type_new_enum() {
        assert_eq!(
            DataTypeNode::new("Enum8('A' = -42)").unwrap(),
            DataTypeNode::Enum(EnumType::Enum8, HashMap::from([(-42, "A".to_string())]))
        );
        assert_eq!(
            DataTypeNode::new("Enum16('A' = -144)").unwrap(),
            DataTypeNode::Enum(EnumType::Enum16, HashMap::from([(-144, "A".to_string())]))
        );

        assert_eq!(
            DataTypeNode::new("Enum8('A' = 1, 'B' = 2)").unwrap(),
            DataTypeNode::Enum(
                EnumType::Enum8,
                HashMap::from([(1, "A".to_string()), (2, "B".to_string())])
            )
        );
        assert_eq!(
            DataTypeNode::new("Enum16('A' = 1, 'B' = 2)").unwrap(),
            DataTypeNode::Enum(
                EnumType::Enum16,
                HashMap::from([(1, "A".to_string()), (2, "B".to_string())])
            )
        );
        assert_eq!(
            DataTypeNode::new(
                "Enum8('f\\'' = 1, 'x =' = 2, 'b\\'\\'' = 3, '\\'c=4=' = 42, '4' = 100)"
            )
            .unwrap(),
            DataTypeNode::Enum(
                EnumType::Enum8,
                HashMap::from([
                    (1, "f\\'".to_string()),
                    (2, "x =".to_string()),
                    (3, "b\\'\\'".to_string()),
                    (42, "\\'c=4=".to_string()),
                    (100, "4".to_string())
                ])
            )
        );
        assert_eq!(
            DataTypeNode::new("Enum8('foo' = 0, '' = 42)").unwrap(),
            DataTypeNode::Enum(
                EnumType::Enum8,
                HashMap::from([(0, "foo".to_string()), (42, "".to_string())])
            )
        );

        assert!(DataTypeNode::new("Enum()").is_err());
        assert!(DataTypeNode::new("Enum8()").is_err());
        assert!(DataTypeNode::new("Enum16()").is_err());
    }

    #[test]
    fn test_data_type_to_string_simple() {
        // Simple types
        assert_eq!(DataTypeNode::UInt8.to_string(), "UInt8");
        assert_eq!(DataTypeNode::UInt16.to_string(), "UInt16");
        assert_eq!(DataTypeNode::UInt32.to_string(), "UInt32");
        assert_eq!(DataTypeNode::UInt64.to_string(), "UInt64");
        assert_eq!(DataTypeNode::UInt128.to_string(), "UInt128");
        assert_eq!(DataTypeNode::UInt256.to_string(), "UInt256");
        assert_eq!(DataTypeNode::Int8.to_string(), "Int8");
        assert_eq!(DataTypeNode::Int16.to_string(), "Int16");
        assert_eq!(DataTypeNode::Int32.to_string(), "Int32");
        assert_eq!(DataTypeNode::Int64.to_string(), "Int64");
        assert_eq!(DataTypeNode::Int128.to_string(), "Int128");
        assert_eq!(DataTypeNode::Int256.to_string(), "Int256");
        assert_eq!(DataTypeNode::Float32.to_string(), "Float32");
        assert_eq!(DataTypeNode::Float64.to_string(), "Float64");
        assert_eq!(DataTypeNode::BFloat16.to_string(), "BFloat16");
        assert_eq!(DataTypeNode::UUID.to_string(), "UUID");
        assert_eq!(DataTypeNode::Date.to_string(), "Date");
        assert_eq!(DataTypeNode::Date32.to_string(), "Date32");
        assert_eq!(DataTypeNode::IPv4.to_string(), "IPv4");
        assert_eq!(DataTypeNode::IPv6.to_string(), "IPv6");
        assert_eq!(DataTypeNode::Bool.to_string(), "Bool");
        assert_eq!(DataTypeNode::Dynamic.to_string(), "Dynamic");
        assert_eq!(DataTypeNode::JSON.to_string(), "JSON");
        assert_eq!(DataTypeNode::String.to_string(), "String");
    }

    #[test]
    fn test_data_types_to_string_complex() {
        assert_eq!(DataTypeNode::DateTime(None).to_string(), "DateTime");
        assert_eq!(
            DataTypeNode::DateTime(Some("UTC".to_string())).to_string(),
            "DateTime('UTC')"
        );
        assert_eq!(
            DataTypeNode::DateTime(Some("America/New_York".to_string())).to_string(),
            "DateTime('America/New_York')"
        );

        assert_eq!(
            DataTypeNode::Nullable(Box::new(DataTypeNode::UInt64)).to_string(),
            "Nullable(UInt64)"
        );
        assert_eq!(
            DataTypeNode::Array(Box::new(DataTypeNode::String)).to_string(),
            "Array(String)"
        );
        assert_eq!(
            DataTypeNode::Array(Box::new(DataTypeNode::Nullable(Box::new(
                DataTypeNode::String
            ))))
            .to_string(),
            "Array(Nullable(String))"
        );
        assert_eq!(
            DataTypeNode::Tuple(vec![
                DataTypeNode::String,
                DataTypeNode::UInt32,
                DataTypeNode::Float64
            ])
            .to_string(),
            "Tuple(String, UInt32, Float64)"
        );
        assert_eq!(
            DataTypeNode::Map(
                Box::new(DataTypeNode::String),
                Box::new(DataTypeNode::UInt32)
            )
            .to_string(),
            "Map(String, UInt32)"
        );
        assert_eq!(
            DataTypeNode::Decimal(10, 2, DecimalSize::Int32).to_string(),
            "Decimal(10, 2)"
        );
        assert_eq!(
            DataTypeNode::Enum(
                EnumType::Enum8,
                HashMap::from([(1, "A".to_string()), (2, "B".to_string())]),
            )
            .to_string(),
            "Enum8('A' = 1, 'B' = 2)"
        );
        assert_eq!(
            DataTypeNode::AggregateFunction("sum".to_string(), vec![DataTypeNode::UInt64])
                .to_string(),
            "AggregateFunction(sum, UInt64)"
        );
        assert_eq!(DataTypeNode::FixedString(16).to_string(), "FixedString(16)");
        assert_eq!(
            DataTypeNode::Variant(vec![DataTypeNode::UInt8, DataTypeNode::Bool]).to_string(),
            "Variant(UInt8, Bool)"
        );
        assert_eq!(
            DataTypeNode::DateTime64(DateTimePrecision::Precision3, Some("UTC".to_string()))
                .to_string(),
            "DateTime64(3, 'UTC')"
        );
    }
}
