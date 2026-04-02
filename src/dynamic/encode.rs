//! Runtime RowBinary encoder for `serde_json::Value`.
//!
//! Converts a JSON map to RowBinary bytes using a [`DynamicSchema`].
//! This is the bridge between dynamic schemas (`Map<String, Value>`)
//! and the efficient binary wire format that ClickHouse expects.
//!
//! **Performance:** avoids the JSON text overhead of JSONEachRow.
//! ClickHouse receives pre-columnarised binary -- zero server-side parsing.
//!
//! # Hot path optimisations (ported from dfe-loader)
//!
//! This encoder runs once per column per row. Three optimisations reduce
//! overhead at scale:
//!
//! - **`Cow<str>` in `value_to_str()`:** the common case (Value::String)
//!   borrows the existing string directly -- zero allocation. Only non-string
//!   types (numbers, bools) allocate a temporary for conversion. At 100k
//!   rows x 20 string columns, that is 2M allocations avoided per batch.
//!
//! - **`TypeTag` enum dispatch:** `encode_typed()` matches on a pre-computed
//!   integer discriminant (`pt.tag`) instead of `pt.base.as_str()`. String
//!   comparison is O(n) per character; enum match is a single jump table.
//!   The tag is resolved once at schema-fetch time, reused on every row.
//!
//! - **`FxHashMap` for schema lookups:** column_index, schema caches, and
//!   metadata caches use `rustc_hash::FxHashMap` (non-cryptographic, 2-3x
//!   faster than std HashMap for string keys). These are internal maps with
//!   application-controlled keys -- no need for DoS-resistant hashing.
//!
//! Note: `serde_json::Value` in the public API is deliberate. sonic-rs (SIMD
//! JSON) accelerates text-to-Value parsing, which happens in the *caller*
//! (e.g. dfe-loader). This encoder only reads already-parsed Values and
//! writes binary -- the Value type itself is not the bottleneck.
//!
//! # Encoding rules
//!
//! - Columns are written in schema order
//! - Missing columns with server-side defaults are skipped
//! - Missing columns without defaults get a type-appropriate zero value
//! - Nullable columns: `0x01` for NULL, `0x00` + value for non-NULL
//! - Strings: varint length prefix + UTF-8 bytes
//! - Integers: little-endian fixed-width
//! - UUID: two little-endian u64 (high, low)

use std::borrow::Cow;

use serde_json::{Map, Value};

use super::error::DynamicError;
use super::schema::{ColumnDef, DynamicSchema};

/// Encode a JSON row map to RowBinary bytes according to the schema.
///
/// Columns are written in schema order. Missing columns with server-side
/// defaults are omitted (the INSERT column list excludes them). Missing
/// columns WITHOUT defaults get a type-appropriate zero value.
pub fn encode_dynamic_row(
    row: &Map<String, Value>,
    _schema: &DynamicSchema,
    columns_to_send: &[&ColumnDef],
) -> Result<Vec<u8>, DynamicError> {
    let mut buf = Vec::with_capacity(256);

    for col in columns_to_send {
        let value = row.get(&col.name).unwrap_or(&Value::Null);
        encode_value(value, col, &mut buf)?;
    }

    Ok(buf)
}

/// Determine which columns to include in the INSERT column list.
///
/// Includes columns that are present in the row OR that have no default
/// (must send something). Columns with defaults that aren't in the row
/// are omitted -- ClickHouse fills them server-side.
pub fn columns_to_send<'a>(
    row: &Map<String, Value>,
    schema: &'a DynamicSchema,
) -> Vec<&'a ColumnDef> {
    schema
        .columns
        .iter()
        .filter(|col| row.contains_key(&col.name) || !col.has_default)
        .collect()
}

// ---------------------------------------------------------------------------
// Core encoding
// ---------------------------------------------------------------------------

fn encode_value(value: &Value, col: &ColumnDef, buf: &mut Vec<u8>) -> Result<(), DynamicError> {
    let pt = &col.parsed_type;

    // Handle Nullable wrapper.
    // JSON columns skip the Nullable prefix — ClickHouse RowBinary uses a
    // different encoding for Nullable(JSON) where NULL is represented within
    // the JSON binary format itself, not with a 0x00/0x01 prefix byte.
    if pt.nullable && pt.tag != super::parsed_type::TypeTag::JSON {
        if value.is_null() {
            buf.push(1); // is_null = true
            return Ok(());
        }
        buf.push(0); // is_null = false
    } else if value.is_null() {
        // Non-nullable column with null value -- write type default
        write_default(pt, buf);
        return Ok(());
    }

    encode_typed(value, pt, &col.name, buf)
}

fn encode_typed(
    value: &Value,
    pt: &super::parsed_type::ParsedType,
    col_name: &str,
    buf: &mut Vec<u8>,
) -> Result<(), DynamicError> {
    use super::parsed_type::TypeTag;

    // Dispatch on pre-computed TypeTag -- integer comparison, not string.
    match pt.tag {
        TypeTag::String => {
            let s = value_to_str(value);
            write_string(s.as_bytes(), buf);
        }
        TypeTag::FixedString => {
            let s = value_to_str(value);
            let n = pt.fixed_size.unwrap_or(1);
            let bytes = s.as_bytes();
            if bytes.len() <= n {
                buf.extend_from_slice(bytes);
                buf.resize(buf.len() + (n - bytes.len()), 0);
            } else {
                buf.extend_from_slice(&bytes[..n]);
            }
        }
        TypeTag::UInt8 | TypeTag::Bool => {
            buf.push(as_u64(value, col_name)? as u8);
        }
        TypeTag::UInt16 => {
            buf.extend_from_slice(&(as_u64(value, col_name)? as u16).to_le_bytes());
        }
        TypeTag::UInt32 | TypeTag::DateTime => {
            buf.extend_from_slice(&(as_u64(value, col_name)? as u32).to_le_bytes());
        }
        TypeTag::UInt64 => {
            buf.extend_from_slice(&as_u64(value, col_name)?.to_le_bytes());
        }
        TypeTag::Int8 | TypeTag::Enum8 => {
            buf.extend_from_slice(&(as_i64(value, col_name)? as i8).to_le_bytes());
        }
        TypeTag::Int16 | TypeTag::Enum16 => {
            buf.extend_from_slice(&(as_i64(value, col_name)? as i16).to_le_bytes());
        }
        TypeTag::Date => {
            // Date is UInt16 (days since 1970-01-01), not signed.
            buf.extend_from_slice(&(as_u64(value, col_name)? as u16).to_le_bytes());
        }
        TypeTag::Int32 | TypeTag::Date32 | TypeTag::Decimal32 => {
            buf.extend_from_slice(&(as_i64(value, col_name)? as i32).to_le_bytes());
        }
        TypeTag::Int64 | TypeTag::DateTime64 | TypeTag::Decimal64 => {
            buf.extend_from_slice(&as_i64(value, col_name)?.to_le_bytes());
        }
        TypeTag::Float32 => {
            buf.extend_from_slice(&(as_f64(value, col_name)? as f32).to_le_bytes());
        }
        TypeTag::Float64 => {
            buf.extend_from_slice(&as_f64(value, col_name)?.to_le_bytes());
        }
        TypeTag::UUID => encode_uuid(value, col_name, buf)?,
        TypeTag::IPv4 => encode_ipv4(value, col_name, buf)?,
        TypeTag::IPv6 => encode_ipv6(value, col_name, buf)?,
        TypeTag::Array => {
            let elem = pt
                .array_element
                .as_ref()
                .ok_or_else(|| enc_err(col_name, "Array without element type"))?;
            encode_array(value, elem, col_name, buf)?;
        }
        TypeTag::Map => {
            let (kt, vt) = pt
                .map_types
                .as_ref()
                .ok_or_else(|| enc_err(col_name, "Map without key/value types"))?;
            encode_map(value, kt, vt, col_name, buf)?;
        }
        TypeTag::JSON => {
            // JSON type -- send as length-prefixed JSON string.
            // For Nullable(JSON), NULL values send an empty string (ClickHouse
            // interprets a zero-length RowBinary string as NULL for JSON columns).
            if value.is_null() {
                write_string(b"", buf);
            } else {
                let json_str = value.to_string();
                write_string(json_str.as_bytes(), buf);
            }
        }
        // 128/256-bit types, Point, Tuple: encode as string (forward-compat).
        // These are rarely used in dynamic insert paths.
        _ => {
            let s = value_to_str(value);
            write_string(s.as_bytes(), buf);
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Wire format helpers
// ---------------------------------------------------------------------------

fn write_string(bytes: &[u8], buf: &mut Vec<u8>) {
    write_varint(bytes.len() as u64, buf);
    buf.extend_from_slice(bytes);
}

fn write_varint(mut value: u64, buf: &mut Vec<u8>) {
    loop {
        let byte = (value & 0x7F) as u8;
        value >>= 7;
        if value == 0 {
            buf.push(byte);
            break;
        }
        buf.push(byte | 0x80);
    }
}

fn write_default(pt: &super::parsed_type::ParsedType, buf: &mut Vec<u8>) {
    if let Some(size) = pt.fixed_byte_size() {
        buf.extend(std::iter::repeat_n(0u8, size));
    } else {
        // Variable-length: empty string / empty array / empty map
        write_varint(0, buf);
    }
}

// ---------------------------------------------------------------------------
// Value coercion helpers
// ---------------------------------------------------------------------------

/// Borrow the string directly when possible (the common case), only
/// allocate for non-string types that need conversion.
fn value_to_str(value: &Value) -> Cow<'_, str> {
    match value {
        Value::String(s) => Cow::Borrowed(s.as_str()),
        Value::Number(n) => Cow::Owned(n.to_string()),
        Value::Bool(b) => Cow::Borrowed(if *b { "true" } else { "false" }),
        Value::Null => Cow::Borrowed(""),
        other => Cow::Owned(other.to_string()),
    }
}

fn as_u64(value: &Value, col: &str) -> Result<u64, DynamicError> {
    match value {
        Value::Number(n) => n
            .as_u64()
            .or_else(|| n.as_i64().map(|v| v as u64))
            .or_else(|| n.as_f64().map(|v| v as u64))
            .ok_or_else(|| enc_err(col, "not a valid unsigned integer")),
        Value::Bool(b) => Ok(u64::from(*b)),
        Value::String(s) => s
            .parse::<u64>()
            .map_err(|_| enc_err(col, "string not parseable as u64")),
        _ => Err(enc_err(col, "expected number")),
    }
}

fn as_i64(value: &Value, col: &str) -> Result<i64, DynamicError> {
    match value {
        Value::Number(n) => n
            .as_i64()
            .or_else(|| n.as_u64().map(|v| v as i64))
            .or_else(|| n.as_f64().map(|v| v as i64))
            .ok_or_else(|| enc_err(col, "not a valid integer")),
        Value::Bool(b) => Ok(i64::from(*b)),
        Value::String(s) => s
            .parse::<i64>()
            .map_err(|_| enc_err(col, "string not parseable as i64")),
        _ => Err(enc_err(col, "expected number")),
    }
}

fn as_f64(value: &Value, col: &str) -> Result<f64, DynamicError> {
    match value {
        Value::Number(n) => n.as_f64().ok_or_else(|| enc_err(col, "not a valid float")),
        Value::String(s) => s
            .parse::<f64>()
            .map_err(|_| enc_err(col, "string not parseable as f64")),
        _ => Err(enc_err(col, "expected number")),
    }
}

// ---------------------------------------------------------------------------
// Complex type encoders
// ---------------------------------------------------------------------------

fn encode_uuid(value: &Value, col: &str, buf: &mut Vec<u8>) -> Result<(), DynamicError> {
    let s = value_to_str(value);
    let hex: String = s.chars().filter(|c| c.is_ascii_hexdigit()).collect();
    if hex.len() != 32 {
        return Err(enc_err(col, "invalid UUID length"));
    }
    // ClickHouse RowBinary UUID: two LE u64 (high word first, then low)
    let high = u64::from_str_radix(&hex[..16], 16).map_err(|_| enc_err(col, "invalid UUID hex"))?;
    let low = u64::from_str_radix(&hex[16..], 16).map_err(|_| enc_err(col, "invalid UUID hex"))?;
    buf.extend_from_slice(&high.to_le_bytes());
    buf.extend_from_slice(&low.to_le_bytes());
    Ok(())
}

fn encode_ipv4(value: &Value, col: &str, buf: &mut Vec<u8>) -> Result<(), DynamicError> {
    let s = value_to_str(value);
    let addr: std::net::Ipv4Addr = s.parse().map_err(|_| enc_err(col, "invalid IPv4"))?;
    // ClickHouse stores IPv4 as UInt32 little-endian
    buf.extend_from_slice(&u32::from(addr).to_le_bytes());
    Ok(())
}

fn encode_ipv6(value: &Value, col: &str, buf: &mut Vec<u8>) -> Result<(), DynamicError> {
    let s = value_to_str(value);
    let addr: std::net::Ipv6Addr = s.parse().map_err(|_| enc_err(col, "invalid IPv6"))?;
    buf.extend_from_slice(&addr.octets());
    Ok(())
}

fn encode_array(
    value: &Value,
    elem_type: &super::parsed_type::ParsedType,
    col_name: &str,
    buf: &mut Vec<u8>,
) -> Result<(), DynamicError> {
    let arr = match value {
        Value::Array(a) => a,
        _ => return Err(enc_err(col_name, "expected array")),
    };
    write_varint(arr.len() as u64, buf);
    let dummy_col = ColumnDef {
        name: col_name.to_string(),
        raw_type: String::new(),
        parsed_type: elem_type.clone(),
        default_kind: String::new(),
        has_default: false,
    };
    for item in arr {
        encode_value(item, &dummy_col, buf)?;
    }
    Ok(())
}

fn encode_map(
    value: &Value,
    key_type: &super::parsed_type::ParsedType,
    val_type: &super::parsed_type::ParsedType,
    col_name: &str,
    buf: &mut Vec<u8>,
) -> Result<(), DynamicError> {
    let obj = match value {
        Value::Object(m) => m,
        _ => return Err(enc_err(col_name, "expected object for Map")),
    };
    write_varint(obj.len() as u64, buf);
    let val_col = ColumnDef {
        name: format!("{col_name}.value"),
        raw_type: String::new(),
        parsed_type: val_type.clone(),
        default_kind: String::new(),
        has_default: false,
    };
    for (k, v) in obj {
        // Map keys are always String in ClickHouse. Write directly
        // instead of cloning into a Value::String wrapper.
        write_string(k.as_bytes(), buf);
        encode_value(v, &val_col, buf)?;
    }
    Ok(())
}

fn enc_err(col: &str, msg: &str) -> DynamicError {
    DynamicError::EncodingError {
        column: col.to_string(),
        message: msg.to_string(),
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::super::parsed_type::ParsedType;
    use super::super::schema::DynamicSchema;
    use super::*;

    fn col(name: &str, type_str: &str) -> ColumnDef {
        ColumnDef {
            name: name.to_string(),
            raw_type: type_str.to_string(),
            parsed_type: ParsedType::parse(type_str),
            default_kind: String::new(),
            has_default: false,
        }
    }

    fn col_default(name: &str, type_str: &str) -> ColumnDef {
        ColumnDef {
            name: name.to_string(),
            raw_type: type_str.to_string(),
            parsed_type: ParsedType::parse(type_str),
            default_kind: "DEFAULT".to_string(),
            has_default: true,
        }
    }

    #[test]
    fn test_encode_string() {
        let schema = DynamicSchema::from_columns("t", vec![col("name", "String")]);
        let cols = columns_to_send(
            &serde_json::json!({"name": "hello"}).as_object().unwrap(),
            &schema,
        );
        let row = serde_json::json!({"name": "hello"});
        let bytes = encode_dynamic_row(row.as_object().unwrap(), &schema, &cols).unwrap();
        // varint(5) + "hello"
        assert_eq!(bytes, vec![5, b'h', b'e', b'l', b'l', b'o']);
    }

    #[test]
    fn test_encode_uint32() {
        let schema = DynamicSchema::from_columns("t", vec![col("id", "UInt32")]);
        let row = serde_json::json!({"id": 42});
        let cols = columns_to_send(row.as_object().unwrap(), &schema);
        let bytes = encode_dynamic_row(row.as_object().unwrap(), &schema, &cols).unwrap();
        assert_eq!(bytes, 42u32.to_le_bytes().to_vec());
    }

    #[test]
    fn test_encode_int64() {
        let schema = DynamicSchema::from_columns("t", vec![col("val", "Int64")]);
        let row = serde_json::json!({"val": -100});
        let cols = columns_to_send(row.as_object().unwrap(), &schema);
        let bytes = encode_dynamic_row(row.as_object().unwrap(), &schema, &cols).unwrap();
        assert_eq!(bytes, (-100i64).to_le_bytes().to_vec());
    }

    #[test]
    fn test_encode_float64() {
        let schema = DynamicSchema::from_columns("t", vec![col("f", "Float64")]);
        let row = serde_json::json!({"f": 3.14});
        let cols = columns_to_send(row.as_object().unwrap(), &schema);
        let bytes = encode_dynamic_row(row.as_object().unwrap(), &schema, &cols).unwrap();
        assert_eq!(bytes, 3.14f64.to_le_bytes().to_vec());
    }

    #[test]
    fn test_encode_bool() {
        let schema = DynamicSchema::from_columns("t", vec![col("b", "Bool")]);
        let row = serde_json::json!({"b": true});
        let cols = columns_to_send(row.as_object().unwrap(), &schema);
        let bytes = encode_dynamic_row(row.as_object().unwrap(), &schema, &cols).unwrap();
        assert_eq!(bytes, vec![1]);
    }

    #[test]
    fn test_encode_nullable_null() {
        let schema = DynamicSchema::from_columns("t", vec![col("n", "Nullable(String)")]);
        let row = serde_json::json!({"n": null});
        let cols = columns_to_send(row.as_object().unwrap(), &schema);
        let bytes = encode_dynamic_row(row.as_object().unwrap(), &schema, &cols).unwrap();
        // 0x01 = is_null
        assert_eq!(bytes, vec![1]);
    }

    #[test]
    fn test_encode_nullable_non_null() {
        let schema = DynamicSchema::from_columns("t", vec![col("n", "Nullable(String)")]);
        let row = serde_json::json!({"n": "hi"});
        let cols = columns_to_send(row.as_object().unwrap(), &schema);
        let bytes = encode_dynamic_row(row.as_object().unwrap(), &schema, &cols).unwrap();
        // 0x00 = not_null, varint(2), "hi"
        assert_eq!(bytes, vec![0, 2, b'h', b'i']);
    }

    #[test]
    fn test_encode_missing_column_with_default_skipped() {
        let schema = DynamicSchema::from_columns(
            "t",
            vec![col("id", "UInt32"), col_default("ts", "DateTime64(3)")],
        );
        let row = serde_json::json!({"id": 1});
        let cols = columns_to_send(row.as_object().unwrap(), &schema);
        // Only id should be in the column list (ts has default and is absent)
        assert_eq!(cols.len(), 1);
        assert_eq!(cols[0].name, "id");
        let bytes = encode_dynamic_row(row.as_object().unwrap(), &schema, &cols).unwrap();
        assert_eq!(bytes, 1u32.to_le_bytes().to_vec());
    }

    #[test]
    fn test_encode_missing_non_nullable_gets_zero() {
        let schema = DynamicSchema::from_columns("t", vec![col("x", "UInt32")]);
        let row = serde_json::json!({});
        let cols = columns_to_send(row.as_object().unwrap(), &schema);
        let bytes = encode_dynamic_row(row.as_object().unwrap(), &schema, &cols).unwrap();
        assert_eq!(bytes, 0u32.to_le_bytes().to_vec());
    }

    #[test]
    fn test_encode_array() {
        let schema = DynamicSchema::from_columns("t", vec![col("a", "Array(UInt32)")]);
        let row = serde_json::json!({"a": [1, 2, 3]});
        let cols = columns_to_send(row.as_object().unwrap(), &schema);
        let bytes = encode_dynamic_row(row.as_object().unwrap(), &schema, &cols).unwrap();
        let mut expected = vec![3u8]; // varint(3)
        expected.extend_from_slice(&1u32.to_le_bytes());
        expected.extend_from_slice(&2u32.to_le_bytes());
        expected.extend_from_slice(&3u32.to_le_bytes());
        assert_eq!(bytes, expected);
    }

    #[test]
    fn test_encode_multi_column() {
        let schema =
            DynamicSchema::from_columns("t", vec![col("id", "UInt32"), col("name", "String")]);
        let row = serde_json::json!({"id": 42, "name": "test"});
        let cols = columns_to_send(row.as_object().unwrap(), &schema);
        let bytes = encode_dynamic_row(row.as_object().unwrap(), &schema, &cols).unwrap();
        let mut expected = Vec::new();
        expected.extend_from_slice(&42u32.to_le_bytes());
        expected.extend_from_slice(&[4, b't', b'e', b's', b't']); // varint(4) + "test"
        assert_eq!(bytes, expected);
    }

    #[test]
    fn test_encode_fixed_string() {
        let schema = DynamicSchema::from_columns("t", vec![col("f", "FixedString(4)")]);
        let row = serde_json::json!({"f": "ab"});
        let cols = columns_to_send(row.as_object().unwrap(), &schema);
        let bytes = encode_dynamic_row(row.as_object().unwrap(), &schema, &cols).unwrap();
        assert_eq!(bytes, vec![b'a', b'b', 0, 0]); // padded with zeros
    }

    #[test]
    fn test_encode_string_from_number() {
        // Numbers should coerce to string
        let schema = DynamicSchema::from_columns("t", vec![col("s", "String")]);
        let row = serde_json::json!({"s": 42});
        let cols = columns_to_send(row.as_object().unwrap(), &schema);
        let bytes = encode_dynamic_row(row.as_object().unwrap(), &schema, &cols).unwrap();
        assert_eq!(bytes, vec![2, b'4', b'2']);
    }

    #[test]
    fn test_varint_encoding() {
        let mut buf = Vec::new();
        write_varint(0, &mut buf);
        assert_eq!(buf, vec![0]);

        buf.clear();
        write_varint(127, &mut buf);
        assert_eq!(buf, vec![127]);

        buf.clear();
        write_varint(128, &mut buf);
        assert_eq!(buf, vec![0x80, 0x01]);

        buf.clear();
        write_varint(300, &mut buf);
        assert_eq!(buf, vec![0xAC, 0x02]);
    }
}
