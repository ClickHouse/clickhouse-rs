use crate::error::ColumnsParserError;
use std::collections::HashMap;
use std::fmt::Display;

#[derive(Debug, Clone, PartialEq)]
pub struct ColumnSpec {
    name: String,
    data_type: DataType,
}

#[derive(Debug, Clone, PartialEq)]
pub enum DataType {
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
    String,
    UUID,
    Date,
    Date32,
    DateTime(Option<String>),                      // Optional timezone
    DateTime64(DateTimePrecision, Option<String>), // Precision and optional timezone
    IPv4,
    IPv6,

    Nullable(Box<DataType>),
    Array(Box<DataType>),
    Tuple(Vec<DataType>),
    Map(Box<DataType>, Box<DataType>),
    LowCardinality(Box<DataType>),
    Decimal(u8, u8, DecimalSize),
    Enum(EnumType, HashMap<i16, String>),
    AggregateFunction(String, Vec<DataType>),
    FixedString(usize),

    Variant(Vec<DataType>),
    Dynamic,
    JSON,
    // TODO: Nested, Geo
}

impl DataType {
    pub fn new(name: &str) -> Result<Self, ColumnsParserError> {
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
            str => Err(ColumnsParserError::TypeParsingError(format!(
                "Unknown data type: {}",
                str
            ))),
        }
    }
}

impl Into<String> for DataType {
    fn into(self) -> String {
        self.to_string()
    }
}

impl Display for DataType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use DataType::*;
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
            Decimal(precision, scale, _) => {
                format!("Decimal({}, {})", precision, scale)
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
    pub(crate) fn new(char: char) -> Result<DateTimePrecision, ColumnsParserError> {
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
            _ => Err(ColumnsParserError::TypeParsingError(format!(
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

impl DecimalSize {
    pub(crate) fn new(precision: u8) -> Result<Self, ColumnsParserError> {
        if precision <= 9 {
            Ok(DecimalSize::Int32)
        } else if precision <= 18 {
            Ok(DecimalSize::Int64)
        } else if precision <= 38 {
            Ok(DecimalSize::Int128)
        } else if precision <= 76 {
            Ok(DecimalSize::Int256)
        } else {
            return Err(ColumnsParserError::TypeParsingError(format!(
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

fn data_types_to_string(elements: &[DataType]) -> String {
    elements
        .iter()
        .map(|a| a.to_string())
        .collect::<Vec<_>>()
        .join(", ")
}

fn parse_fixed_string(input: &str) -> Result<DataType, ColumnsParserError> {
    if input.len() >= 14 {
        let size_str = &input[12..input.len() - 1];
        let size = size_str.parse::<usize>().map_err(|err| {
            ColumnsParserError::TypeParsingError(format!(
                "Invalid FixedString size, expected a valid number. Underlying error: {}, input: {}, size_str: {}",
                err, input, size_str
            ))
        })?;
        if size == 0 {
            return Err(ColumnsParserError::TypeParsingError(format!(
                "Invalid FixedString size, expected a positive number, got zero. Input: {}",
                input
            )));
        }
        return Ok(DataType::FixedString(size));
    }
    Err(ColumnsParserError::TypeParsingError(format!(
        "Invalid FixedString format, expected FixedString(N), got {}",
        input
    )))
}

fn parse_array(input: &str) -> Result<DataType, ColumnsParserError> {
    if input.len() >= 8 {
        let inner_type_str = &input[6..input.len() - 1];
        let inner_type = DataType::new(inner_type_str)?;
        return Ok(DataType::Array(Box::new(inner_type)));
    }
    Err(ColumnsParserError::TypeParsingError(format!(
        "Invalid Array format, expected Array(InnerType), got {}",
        input
    )))
}

fn parse_enum(input: &str) -> Result<DataType, ColumnsParserError> {
    if input.len() >= 9 {
        let (enum_type, prefix_len) = if input.starts_with("Enum8") {
            (EnumType::Enum8, 6)
        } else if input.starts_with("Enum16") {
            (EnumType::Enum16, 7)
        } else {
            return Err(ColumnsParserError::TypeParsingError(format!(
                "Invalid Enum type, expected Enum8 or Enum16, got {}",
                input
            )));
        };
        let enum_values_map_str = &input[prefix_len..input.len() - 1];
        let enum_values_map = parse_enum_values_map(enum_values_map_str)?;
        return Ok(DataType::Enum(enum_type, enum_values_map));
    }
    Err(ColumnsParserError::TypeParsingError(format!(
        "Invalid Enum format, expected Enum8('name' = value), got {}",
        input
    )))
}

fn parse_datetime(input: &str) -> Result<DataType, ColumnsParserError> {
    if input == "DateTime" {
        return Ok(DataType::DateTime(None));
    }
    if input.len() >= 12 {
        let timezone = (&input[10..input.len() - 2]).to_string();
        return Ok(DataType::DateTime(Some(timezone)));
    }
    Err(ColumnsParserError::TypeParsingError(format!(
        "Invalid DateTime format, expected DateTime('timezone'), got {}",
        input
    )))
}

fn parse_decimal(input: &str) -> Result<DataType, ColumnsParserError> {
    if input.len() >= 10 {
        let precision_and_scale_str = (&input[8..input.len() - 1]).split(", ").collect::<Vec<_>>();
        if precision_and_scale_str.len() != 2 {
            return Err(ColumnsParserError::TypeParsingError(format!(
                "Invalid Decimal format, expected Decimal(P, S), got {}",
                input
            )));
        }
        let parsed = precision_and_scale_str
            .iter()
            .map(|s| s.parse::<u8>())
            .collect::<Result<Vec<_>, _>>()
            .map_err(|err| {
                ColumnsParserError::TypeParsingError(format!(
                    "Invalid Decimal format, expected Decimal(P, S), got {}. Underlying error: {}",
                    input, err
                ))
            })?;
        let precision = parsed[0];
        let scale = parsed[1];
        if scale < 1 || precision < 1 {
            return Err(ColumnsParserError::TypeParsingError(format!(
                "Invalid Decimal format, expected Decimal(P, S) with P > 0 and S > 0, got {}",
                input
            )));
        }
        if precision < scale {
            return Err(ColumnsParserError::TypeParsingError(format!(
                "Invalid Decimal format, expected Decimal(P, S) with P >= S, got {}",
                input
            )));
        }
        let size = DecimalSize::new(parsed[0])?;
        return Ok(DataType::Decimal(precision, scale, size));
    }
    Err(ColumnsParserError::TypeParsingError(format!(
        "Invalid Decimal format, expected Decimal(P), got {}",
        input
    )))
}

fn parse_datetime64(input: &str) -> Result<DataType, ColumnsParserError> {
    if input.len() >= 13 {
        let mut chars = (&input[11..input.len() - 1]).chars();
        let precision_char = chars
            .next()
            .ok_or(ColumnsParserError::TypeParsingError(format!(
                "Invalid DateTime64 precision, expected a positive number. Input: {}",
                input
            )))?;
        let precision = DateTimePrecision::new(precision_char)?;
        let maybe_tz = match chars.as_str() {
            str if str.len() > 2 => Some((&str[3..str.len() - 1]).to_string()),
            _ => None,
        };
        return Ok(DataType::DateTime64(precision, maybe_tz));
    }
    Err(ColumnsParserError::TypeParsingError(format!(
        "Invalid DateTime format, expected DateTime('timezone'), got {}",
        input
    )))
}

fn parse_low_cardinality(input: &str) -> Result<DataType, ColumnsParserError> {
    if input.len() >= 16 {
        let inner_type_str = &input[15..input.len() - 1];
        let inner_type = DataType::new(inner_type_str)?;
        return Ok(DataType::LowCardinality(Box::new(inner_type)));
    }
    Err(ColumnsParserError::TypeParsingError(format!(
        "Invalid LowCardinality format, expected LowCardinality(InnerType), got {}",
        input
    )))
}

fn parse_nullable(input: &str) -> Result<DataType, ColumnsParserError> {
    if input.len() >= 10 {
        let inner_type_str = &input[9..input.len() - 1];
        let inner_type = DataType::new(inner_type_str)?;
        return Ok(DataType::Nullable(Box::new(inner_type)));
    }
    Err(ColumnsParserError::TypeParsingError(format!(
        "Invalid Nullable format, expected Nullable(InnerType), got {}",
        input
    )))
}

fn parse_map(input: &str) -> Result<DataType, ColumnsParserError> {
    if input.len() >= 5 {
        let inner_types_str = &input[4..input.len() - 1];
        let inner_types = parse_inner_types(inner_types_str)?;
        if inner_types.len() != 2 {
            return Err(ColumnsParserError::TypeParsingError(format!(
                "Expected two inner elements in a Map from input {}",
                input
            )));
        }
        return Ok(DataType::Map(
            Box::new(inner_types[0].clone()),
            Box::new(inner_types[1].clone()),
        ));
    }
    Err(ColumnsParserError::TypeParsingError(format!(
        "Invalid Map format, expected Map(KeyType, ValueType), got {}",
        input
    )))
}

fn parse_tuple(input: &str) -> Result<DataType, ColumnsParserError> {
    if input.len() > 7 {
        let inner_types_str = &input[6..input.len() - 1];
        let inner_types = parse_inner_types(inner_types_str)?;
        if inner_types.is_empty() {
            return Err(ColumnsParserError::TypeParsingError(format!(
                "Expected at least one inner element in a Tuple from input {}",
                input
            )));
        }
        return Ok(DataType::Tuple(inner_types));
    }
    Err(ColumnsParserError::TypeParsingError(format!(
        "Invalid Tuple format, expected Tuple(Type1, Type2, ...), got {}",
        input
    )))
}

fn parse_variant(input: &str) -> Result<DataType, ColumnsParserError> {
    if input.len() >= 9 {
        let inner_types_str = &input[8..input.len() - 1];
        let inner_types = parse_inner_types(inner_types_str)?;
        return Ok(DataType::Variant(inner_types));
    }
    Err(ColumnsParserError::TypeParsingError(format!(
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
fn parse_inner_types(input: &str) -> Result<Vec<DataType>, ColumnsParserError> {
    let mut inner_types: Vec<DataType> = Vec::new();

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
                                    ColumnsParserError::TypeParsingError(format!(
                                "Invalid UTF-8 sequence in input for the inner data type: {}",
                                &input[last_element_index..]
                            ))
                                })?;
                        let data_type = DataType::new(&data_type_str)?;
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
                ColumnsParserError::TypeParsingError(format!(
                    "Invalid UTF-8 sequence in input for the inner data type: {}",
                    &input[last_element_index..]
                ))
            })?;
        let data_type = DataType::new(&data_type_str)?;
        inner_types.push(data_type);
    }

    Ok(inner_types)
}

fn parse_enum_values_map(input: &str) -> Result<HashMap<i16, String>, ColumnsParserError> {
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
                        ColumnsParserError::TypeParsingError(format!(
                            "Invalid UTF-8 sequence in input for the enum name: {}",
                            &input[start_index..i]
                        ))
                    })?;
                    names.push(name);

                    // Skip ` = ` and the first digit, as it will always have at least one
                    if i + 4 >= input_bytes.len() {
                        return Err(ColumnsParserError::TypeParsingError(format!(
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
                    ColumnsParserError::TypeParsingError(format!(
                        "Invalid UTF-8 sequence in input for the enum index: {}",
                        &input[start_index..i]
                    ))
                })?
                .parse::<i16>()
                .map_err(|_| {
                    ColumnsParserError::TypeParsingError(format!(
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
            ColumnsParserError::TypeParsingError(format!(
                "Invalid UTF-8 sequence in input for the enum index: {}",
                &input[start_index..i]
            ))
        })?
        .parse::<i16>()
        .map_err(|_| {
            ColumnsParserError::TypeParsingError(format!(
                "Invalid Enum index, expected a valid number. Input: {}",
                input
            ))
        })?;
    indices.push(index);

    if names.len() != indices.len() {
        return Err(ColumnsParserError::TypeParsingError(format!(
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
        assert_eq!(DataType::new("UInt8").unwrap(), DataType::UInt8);
        assert_eq!(DataType::new("UInt16").unwrap(), DataType::UInt16);
        assert_eq!(DataType::new("UInt32").unwrap(), DataType::UInt32);
        assert_eq!(DataType::new("UInt64").unwrap(), DataType::UInt64);
        assert_eq!(DataType::new("UInt128").unwrap(), DataType::UInt128);
        assert_eq!(DataType::new("UInt256").unwrap(), DataType::UInt256);
        assert_eq!(DataType::new("Int8").unwrap(), DataType::Int8);
        assert_eq!(DataType::new("Int16").unwrap(), DataType::Int16);
        assert_eq!(DataType::new("Int32").unwrap(), DataType::Int32);
        assert_eq!(DataType::new("Int64").unwrap(), DataType::Int64);
        assert_eq!(DataType::new("Int128").unwrap(), DataType::Int128);
        assert_eq!(DataType::new("Int256").unwrap(), DataType::Int256);
        assert_eq!(DataType::new("Float32").unwrap(), DataType::Float32);
        assert_eq!(DataType::new("Float64").unwrap(), DataType::Float64);
        assert_eq!(DataType::new("BFloat16").unwrap(), DataType::BFloat16);
        assert_eq!(DataType::new("String").unwrap(), DataType::String);
        assert_eq!(DataType::new("UUID").unwrap(), DataType::UUID);
        assert_eq!(DataType::new("Date").unwrap(), DataType::Date);
        assert_eq!(DataType::new("Date32").unwrap(), DataType::Date32);
        assert_eq!(DataType::new("IPv4").unwrap(), DataType::IPv4);
        assert_eq!(DataType::new("IPv6").unwrap(), DataType::IPv6);
        assert_eq!(DataType::new("Bool").unwrap(), DataType::Bool);
        assert_eq!(DataType::new("Dynamic").unwrap(), DataType::Dynamic);
        assert_eq!(DataType::new("JSON").unwrap(), DataType::JSON);
        assert!(DataType::new("SomeUnknownType").is_err(),);
    }

    #[test]
    fn test_data_type_new_fixed_string() {
        assert_eq!(
            DataType::new("FixedString(1)").unwrap(),
            DataType::FixedString(1)
        );
        assert_eq!(
            DataType::new("FixedString(16)").unwrap(),
            DataType::FixedString(16)
        );
        assert_eq!(
            DataType::new("FixedString(255)").unwrap(),
            DataType::FixedString(255)
        );
        assert_eq!(
            DataType::new("FixedString(65535)").unwrap(),
            DataType::FixedString(65_535)
        );
        assert!(DataType::new("FixedString()").is_err());
        assert!(DataType::new("FixedString(0)").is_err());
        assert!(DataType::new("FixedString(-1)").is_err());
        assert!(DataType::new("FixedString(abc)").is_err());
    }

    #[test]
    fn test_data_type_new_array() {
        assert_eq!(
            DataType::new("Array(UInt8)").unwrap(),
            DataType::Array(Box::new(DataType::UInt8))
        );
        assert_eq!(
            DataType::new("Array(String)").unwrap(),
            DataType::Array(Box::new(DataType::String))
        );
        assert_eq!(
            DataType::new("Array(FixedString(16))").unwrap(),
            DataType::Array(Box::new(DataType::FixedString(16)))
        );
        assert_eq!(
            DataType::new("Array(Nullable(Int32))").unwrap(),
            DataType::Array(Box::new(DataType::Nullable(Box::new(DataType::Int32))))
        );
        assert!(DataType::new("Array()").is_err());
        assert!(DataType::new("Array(abc)").is_err());
    }

    #[test]
    fn test_data_type_new_decimal() {
        assert_eq!(
            DataType::new("Decimal(7, 2)").unwrap(),
            DataType::Decimal(7, 2, DecimalSize::Int32)
        );
        assert_eq!(
            DataType::new("Decimal(12, 4)").unwrap(),
            DataType::Decimal(12, 4, DecimalSize::Int64)
        );
        assert_eq!(
            DataType::new("Decimal(27, 6)").unwrap(),
            DataType::Decimal(27, 6, DecimalSize::Int128)
        );
        assert_eq!(
            DataType::new("Decimal(42, 8)").unwrap(),
            DataType::Decimal(42, 8, DecimalSize::Int256)
        );
        assert!(DataType::new("Decimal").is_err());
        assert!(DataType::new("Decimal(").is_err());
        assert!(DataType::new("Decimal()").is_err());
        assert!(DataType::new("Decimal(1)").is_err());
        assert!(DataType::new("Decimal(1,)").is_err());
        assert!(DataType::new("Decimal(1, )").is_err());
        assert!(DataType::new("Decimal(0, 0)").is_err()); // Precision must be > 0
        assert!(DataType::new("Decimal(x, 0)").is_err()); // Non-numeric precision
        assert!(DataType::new("Decimal(', ')").is_err());
        assert!(DataType::new("Decimal(77, 1)").is_err()); // Max precision is 76
        assert!(DataType::new("Decimal(1, 2)").is_err()); // Scale must be less than precision
        assert!(DataType::new("Decimal(1, x)").is_err()); // Non-numeric scale
        assert!(DataType::new("Decimal(42, ,)").is_err());
        assert!(DataType::new("Decimal(42, ')").is_err());
        assert!(DataType::new("Decimal(foobar)").is_err());
    }

    #[test]
    fn test_data_type_new_datetime() {
        assert_eq!(DataType::new("DateTime").unwrap(), DataType::DateTime(None));
        assert_eq!(
            DataType::new("DateTime('UTC')").unwrap(),
            DataType::DateTime(Some("UTC".to_string()))
        );
        assert_eq!(
            DataType::new("DateTime('America/New_York')").unwrap(),
            DataType::DateTime(Some("America/New_York".to_string()))
        );
        assert!(DataType::new("DateTime()").is_err());
    }

    #[test]
    fn test_data_type_new_datetime64() {
        assert_eq!(
            DataType::new("DateTime64(0)").unwrap(),
            DataType::DateTime64(DateTimePrecision::Precision0, None)
        );
        assert_eq!(
            DataType::new("DateTime64(1)").unwrap(),
            DataType::DateTime64(DateTimePrecision::Precision1, None)
        );
        assert_eq!(
            DataType::new("DateTime64(2)").unwrap(),
            DataType::DateTime64(DateTimePrecision::Precision2, None)
        );
        assert_eq!(
            DataType::new("DateTime64(3)").unwrap(),
            DataType::DateTime64(DateTimePrecision::Precision3, None)
        );
        assert_eq!(
            DataType::new("DateTime64(4)").unwrap(),
            DataType::DateTime64(DateTimePrecision::Precision4, None)
        );
        assert_eq!(
            DataType::new("DateTime64(5)").unwrap(),
            DataType::DateTime64(DateTimePrecision::Precision5, None)
        );
        assert_eq!(
            DataType::new("DateTime64(6)").unwrap(),
            DataType::DateTime64(DateTimePrecision::Precision6, None)
        );
        assert_eq!(
            DataType::new("DateTime64(7)").unwrap(),
            DataType::DateTime64(DateTimePrecision::Precision7, None)
        );
        assert_eq!(
            DataType::new("DateTime64(8)").unwrap(),
            DataType::DateTime64(DateTimePrecision::Precision8, None)
        );
        assert_eq!(
            DataType::new("DateTime64(9)").unwrap(),
            DataType::DateTime64(DateTimePrecision::Precision9, None)
        );
        assert_eq!(
            DataType::new("DateTime64(0, 'UTC')").unwrap(),
            DataType::DateTime64(DateTimePrecision::Precision0, Some("UTC".to_string()))
        );
        assert_eq!(
            DataType::new("DateTime64(3, 'America/New_York')").unwrap(),
            DataType::DateTime64(
                DateTimePrecision::Precision3,
                Some("America/New_York".to_string())
            )
        );
        assert_eq!(
            DataType::new("DateTime64(6, 'America/New_York')").unwrap(),
            DataType::DateTime64(
                DateTimePrecision::Precision6,
                Some("America/New_York".to_string())
            )
        );
        assert_eq!(
            DataType::new("DateTime64(9, 'Europe/Amsterdam')").unwrap(),
            DataType::DateTime64(
                DateTimePrecision::Precision9,
                Some("Europe/Amsterdam".to_string())
            )
        );
        assert!(DataType::new("DateTime64()").is_err());
    }

    #[test]
    fn test_data_type_new_low_cardinality() {
        assert_eq!(
            DataType::new("LowCardinality(UInt8)").unwrap(),
            DataType::LowCardinality(Box::new(DataType::UInt8))
        );
        assert_eq!(
            DataType::new("LowCardinality(String)").unwrap(),
            DataType::LowCardinality(Box::new(DataType::String))
        );
        assert_eq!(
            DataType::new("LowCardinality(Array(Int32))").unwrap(),
            DataType::LowCardinality(Box::new(DataType::Array(Box::new(DataType::Int32))))
        );
        assert!(DataType::new("LowCardinality()").is_err());
    }

    #[test]
    fn test_data_type_new_nullable() {
        assert_eq!(
            DataType::new("Nullable(UInt8)").unwrap(),
            DataType::Nullable(Box::new(DataType::UInt8))
        );
        assert_eq!(
            DataType::new("Nullable(String)").unwrap(),
            DataType::Nullable(Box::new(DataType::String))
        );
        assert!(DataType::new("Nullable()").is_err());
    }

    #[test]
    fn test_data_type_new_map() {
        assert_eq!(
            DataType::new("Map(UInt8, String)").unwrap(),
            DataType::Map(Box::new(DataType::UInt8), Box::new(DataType::String))
        );
        assert_eq!(
            DataType::new("Map(String, Int32)").unwrap(),
            DataType::Map(Box::new(DataType::String), Box::new(DataType::Int32))
        );
        assert_eq!(
            DataType::new("Map(String, Map(Int32, Array(Nullable(String))))").unwrap(),
            DataType::Map(
                Box::new(DataType::String),
                Box::new(DataType::Map(
                    Box::new(DataType::Int32),
                    Box::new(DataType::Array(Box::new(DataType::Nullable(Box::new(
                        DataType::String
                    )))))
                ))
            )
        );
        assert!(DataType::new("Map()").is_err());
    }

    #[test]
    fn test_data_type_new_variant() {
        assert_eq!(
            DataType::new("Variant(UInt8, String)").unwrap(),
            DataType::Variant(vec![DataType::UInt8, DataType::String])
        );
        assert_eq!(
            DataType::new("Variant(String, Int32)").unwrap(),
            DataType::Variant(vec![DataType::String, DataType::Int32])
        );
        assert_eq!(
            DataType::new("Variant(Int32, Array(Nullable(String)), Map(Int32, String))").unwrap(),
            DataType::Variant(vec![
                DataType::Int32,
                DataType::Array(Box::new(DataType::Nullable(Box::new(DataType::String)))),
                DataType::Map(Box::new(DataType::Int32), Box::new(DataType::String))
            ])
        );
        assert!(DataType::new("Variant").is_err());
    }

    #[test]
    fn test_data_type_new_tuple() {
        assert_eq!(
            DataType::new("Tuple(UInt8, String)").unwrap(),
            DataType::Tuple(vec![DataType::UInt8, DataType::String])
        );
        assert_eq!(
            DataType::new("Tuple(String, Int32)").unwrap(),
            DataType::Tuple(vec![DataType::String, DataType::Int32])
        );
        assert_eq!(
            DataType::new(
                "Tuple(Int32, Array(Nullable(String)), Map(Int32, Tuple(String, Array(UInt8))))"
            )
            .unwrap(),
            DataType::Tuple(vec![
                DataType::Int32,
                DataType::Array(Box::new(DataType::Nullable(Box::new(DataType::String)))),
                DataType::Map(
                    Box::new(DataType::Int32),
                    Box::new(DataType::Tuple(vec![
                        DataType::String,
                        DataType::Array(Box::new(DataType::UInt8))
                    ]))
                )
            ])
        );
        assert!(DataType::new("Tuple").is_err());
    }

    #[test]
    fn test_data_type_new_enum() {
        assert_eq!(
            DataType::new("Enum8('A' = -42)").unwrap(),
            DataType::Enum(EnumType::Enum8, HashMap::from([(-42, "A".to_string())]))
        );
        assert_eq!(
            DataType::new("Enum16('A' = -144)").unwrap(),
            DataType::Enum(EnumType::Enum16, HashMap::from([(-144, "A".to_string())]))
        );

        assert_eq!(
            DataType::new("Enum8('A' = 1, 'B' = 2)").unwrap(),
            DataType::Enum(
                EnumType::Enum8,
                HashMap::from([(1, "A".to_string()), (2, "B".to_string())])
            )
        );
        assert_eq!(
            DataType::new("Enum16('A' = 1, 'B' = 2)").unwrap(),
            DataType::Enum(
                EnumType::Enum16,
                HashMap::from([(1, "A".to_string()), (2, "B".to_string())])
            )
        );
        assert_eq!(
            DataType::new("Enum8('f\\'' = 1, 'x =' = 2, 'b\\'\\'' = 3, '\\'c=4=' = 42, '4' = 100)")
                .unwrap(),
            DataType::Enum(
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
            DataType::new("Enum8('foo' = 0, '' = 42)").unwrap(),
            DataType::Enum(
                EnumType::Enum8,
                HashMap::from([(0, "foo".to_string()), (42, "".to_string())])
            )
        );

        assert!(DataType::new("Enum()").is_err());
        assert!(DataType::new("Enum8()").is_err());
        assert!(DataType::new("Enum16()").is_err());
    }

    #[test]
    fn test_data_type_to_string_simple() {
        // Simple types
        assert_eq!(DataType::UInt8.to_string(), "UInt8");
        assert_eq!(DataType::UInt16.to_string(), "UInt16");
        assert_eq!(DataType::UInt32.to_string(), "UInt32");
        assert_eq!(DataType::UInt64.to_string(), "UInt64");
        assert_eq!(DataType::UInt128.to_string(), "UInt128");
        assert_eq!(DataType::UInt256.to_string(), "UInt256");
        assert_eq!(DataType::Int8.to_string(), "Int8");
        assert_eq!(DataType::Int16.to_string(), "Int16");
        assert_eq!(DataType::Int32.to_string(), "Int32");
        assert_eq!(DataType::Int64.to_string(), "Int64");
        assert_eq!(DataType::Int128.to_string(), "Int128");
        assert_eq!(DataType::Int256.to_string(), "Int256");
        assert_eq!(DataType::Float32.to_string(), "Float32");
        assert_eq!(DataType::Float64.to_string(), "Float64");
        assert_eq!(DataType::BFloat16.to_string(), "BFloat16");
        assert_eq!(DataType::UUID.to_string(), "UUID");
        assert_eq!(DataType::Date.to_string(), "Date");
        assert_eq!(DataType::Date32.to_string(), "Date32");
        assert_eq!(DataType::IPv4.to_string(), "IPv4");
        assert_eq!(DataType::IPv6.to_string(), "IPv6");
        assert_eq!(DataType::Bool.to_string(), "Bool");
        assert_eq!(DataType::Dynamic.to_string(), "Dynamic");
        assert_eq!(DataType::JSON.to_string(), "JSON");
        assert_eq!(DataType::String.to_string(), "String");
    }

    #[test]
    fn test_data_types_to_string_complex() {
        assert_eq!(DataType::DateTime(None).to_string(), "DateTime");
        assert_eq!(
            DataType::DateTime(Some("UTC".to_string())).to_string(),
            "DateTime('UTC')"
        );
        assert_eq!(
            DataType::DateTime(Some("America/New_York".to_string())).to_string(),
            "DateTime('America/New_York')"
        );

        assert_eq!(
            DataType::Nullable(Box::new(DataType::UInt64)).to_string(),
            "Nullable(UInt64)"
        );
        assert_eq!(
            DataType::Array(Box::new(DataType::String)).to_string(),
            "Array(String)"
        );
        assert_eq!(
            DataType::Array(Box::new(DataType::Nullable(Box::new(DataType::String)))).to_string(),
            "Array(Nullable(String))"
        );
        assert_eq!(
            DataType::Tuple(vec![DataType::String, DataType::UInt32, DataType::Float64])
                .to_string(),
            "Tuple(String, UInt32, Float64)"
        );
        assert_eq!(
            DataType::Map(Box::new(DataType::String), Box::new(DataType::UInt32)).to_string(),
            "Map(String, UInt32)"
        );
        assert_eq!(
            DataType::Decimal(10, 2, DecimalSize::Int32).to_string(),
            "Decimal(10, 2)"
        );
        assert_eq!(
            DataType::Enum(
                EnumType::Enum8,
                HashMap::from([(1, "A".to_string()), (2, "B".to_string())]),
            )
            .to_string(),
            "Enum8('A' = 1, 'B' = 2)"
        );
        assert_eq!(
            DataType::AggregateFunction("sum".to_string(), vec![DataType::UInt64]).to_string(),
            "AggregateFunction(sum, UInt64)"
        );
        assert_eq!(DataType::FixedString(16).to_string(), "FixedString(16)");
        assert_eq!(
            DataType::Variant(vec![DataType::UInt8, DataType::Bool]).to_string(),
            "Variant(UInt8, Bool)"
        );
        assert_eq!(
            DataType::DateTime64(DateTimePrecision::Precision3, Some("UTC".to_string()))
                .to_string(),
            "DateTime64(3, 'UTC')"
        );
    }
}
