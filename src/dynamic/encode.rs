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
    encode_dynamic_row_with_raw(row, _schema, columns_to_send, &[])
}

/// Encode a JSON row map to RowBinary, with raw byte passthrough for named columns.
///
/// `raw_columns` provides pre-encoded bytes for specific columns. When a column
/// name appears in `raw_columns`, its raw bytes are written directly as a
/// length-prefixed string (varint + bytes) instead of going through the normal
/// `Value` encoding path. This enables zero-copy for columns like `_json` where
/// the caller already has the raw bytes and wants to avoid `Value::String`
/// wrapping + `to_string()` re-serialisation.
///
/// For Nullable raw columns, the non-null marker (`0x00`) is written before
/// the raw bytes. NULL raw columns are not supported — omit from `raw_columns`
/// and let the normal encoding path handle it.
pub fn encode_dynamic_row_with_raw(
    row: &Map<String, Value>,
    _schema: &DynamicSchema,
    columns_to_send: &[&ColumnDef],
    raw_columns: &[(&str, &[u8])],
) -> Result<Vec<u8>, DynamicError> {
    let mut buf = Vec::with_capacity(256);

    for col in columns_to_send {
        // Check if this column has raw bytes provided.
        if let Some((_, raw_bytes)) = raw_columns.iter().find(|(name, _)| *name == col.name) {
            // Raw passthrough: write Nullable prefix if needed, then raw bytes
            // as a length-prefixed string (same wire format as JSON/String).
            if col.parsed_type.nullable {
                buf.push(0); // not null
            }
            write_string(raw_bytes, &mut buf);
            continue;
        }

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

    // Handle Nullable wrapper
    if pt.nullable {
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
        TypeTag::Int64 | TypeTag::Decimal64 => {
            buf.extend_from_slice(&as_i64(value, col_name)?.to_le_bytes());
        }
        TypeTag::DateTime64 => {
            let precision = pt.precision.unwrap_or(3);
            let epoch = datetime64_to_epoch(value, precision, col_name)?;
            buf.extend_from_slice(&epoch.to_le_bytes());
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
            // When the value is already a String (e.g. raw JSON payload stored
            // as Value::String by the caller), write the content directly.
            // value.to_string() would double-quote and escape it, producing
            // "\"{ ... }\"" which ClickHouse rejects.
            //
            // Caller responsibility: if sending Value::String to a JSON column,
            // the string content must be valid JSON. Non-JSON strings (e.g.
            // "hello") will be accepted here but rejected by ClickHouse.
            let json_bytes = match value {
                Value::String(s) => Cow::Borrowed(s.as_bytes()),
                _ => Cow::Owned(value.to_string().into_bytes()),
            };
            write_string(&json_bytes, buf);
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
// DateTime64 string -> epoch conversion
// ---------------------------------------------------------------------------

/// Convert a JSON value to a DateTime64 epoch value scaled to the given precision.
///
/// Accepts:
/// - Numeric values (already epoch-scaled, passed through)
/// - Numeric strings (parsed as i64)
/// - Datetime strings: `"YYYY-MM-DD HH:MM:SS"`, `"YYYY-MM-DD HH:MM:SS.fff"`,
///   ISO 8601 with `T` separator, optional `Z` suffix.
///   Non-zero timezone offsets (e.g. `+05:30`) are rejected -- all values
///   must be UTC. ClickHouse DateTime64 stores UTC epoch values; silently
///   dropping offsets would produce wrong timestamps.
fn datetime64_to_epoch(value: &Value, precision: u8, col: &str) -> Result<i64, DynamicError> {
    match value {
        Value::Number(_) => as_i64(value, col),
        Value::String(s) => {
            // Fast path: numeric string (already-scaled epoch value)
            if let Ok(n) = s.parse::<i64>() {
                return Ok(n);
            }
            // Parse datetime string -> (unix_seconds, fractional_nanoseconds)
            let (secs, frac_nanos) = parse_datetime_str(s).map_err(|msg| enc_err(col, &msg))?;
            // Scale to target precision
            let multiplier = 10i64.pow(precision as u32);
            let base = secs
                .checked_mul(multiplier)
                .ok_or_else(|| enc_err(col, "DateTime64 epoch overflow"))?;
            let frac_scaled =
                (frac_nanos as i64) / 10i64.pow(9u32.saturating_sub(precision as u32));
            Ok(base + frac_scaled)
        }
        _ => Err(enc_err(col, "expected number or datetime string")),
    }
}

/// Lightweight datetime string parser (no chrono dependency).
///
/// Handles: `YYYY-MM-DD HH:MM:SS[.fff...]`, ISO 8601 `T` separator,
/// optional `Z` suffix (UTC). Non-zero timezone offsets are rejected.
///
/// Returns `(unix_seconds, fractional_nanoseconds)` or an error message.
fn parse_datetime_str(s: &str) -> Result<(i64, u32), String> {
    let bytes = s.as_bytes();
    let len = bytes.len();

    // Find the separator between date and time: space or 'T'
    // Minimum valid: "YYYY-MM-DD HH:MM:SS" = 19 chars
    if len < 19 {
        return Err("invalid DateTime64 string".into());
    }
    let sep = bytes[10];
    if sep != b' ' && sep != b'T' {
        return Err("invalid DateTime64 string".into());
    }

    // Work out where the datetime core ends (before fractional / suffix)
    // After the 19-char core we may have: nothing, '.fff...', 'Z', '+HH:MM', or combos
    let mut pos = 19; // past "YYYY-MM-DD_HH:MM:SS"

    // Parse optional fractional seconds
    let frac_nanos = if pos < len && bytes[pos] == b'.' {
        pos += 1; // skip dot
        let frac_start = pos;
        while pos < len && bytes[pos].is_ascii_digit() {
            pos += 1;
        }
        let frac_digits = pos - frac_start;
        if frac_digits == 0 {
            return Err("invalid DateTime64 string".into());
        }
        // Parse up to 9 digits to avoid u32 overflow. Truncate extra precision.
        let clamped = if frac_digits > 9 { 9 } else { frac_digits };
        let frac_slice = &s[frac_start..frac_start + clamped];
        let mut frac: u32 = frac_slice
            .parse()
            .map_err(|_| "invalid fractional seconds".to_string())?;
        if clamped < 9 {
            frac *= 10u32.pow(9 - clamped as u32);
        }
        frac
    } else {
        0u32
    };

    // Handle trailing timezone: 'Z', '+HH:MM', '-HH:MM'
    if pos < len {
        if bytes[pos] == b'Z' {
            pos += 1;
        } else if (bytes[pos] == b'+' || bytes[pos] == b'-') && pos + 6 <= len {
            // Check format: +HH:MM or -HH:MM
            let sign = bytes[pos];
            let tz_slice = &s[pos + 1..pos + 6];
            if tz_slice.len() == 5
                && tz_slice.as_bytes()[2] == b':'
                && tz_slice[..2].bytes().all(|b| b.is_ascii_digit())
                && tz_slice[3..].bytes().all(|b| b.is_ascii_digit())
            {
                // Reject non-zero offsets -- silent drop is data corruption
                if sign == b'-' || &tz_slice[..2] != "00" || &tz_slice[3..] != "00" {
                    return Err(format!(
                        "non-UTC timezone offset '{}{}' not supported; convert to UTC first",
                        sign as char, tz_slice
                    ));
                }
                pos += 6;
            }
        }
    }

    // Reject trailing garbage
    if pos != len {
        return Err("invalid DateTime64 string".into());
    }

    // Parse date and time components from the first 19 bytes
    let year: i32 = s[0..4].parse().map_err(|_| "invalid year".to_string())?;
    let month: u32 = s[5..7].parse().map_err(|_| "invalid month".to_string())?;
    let day: u32 = s[8..10].parse().map_err(|_| "invalid day".to_string())?;
    let hour: u32 = s[11..13].parse().map_err(|_| "invalid hour".to_string())?;
    let min: u32 = s[14..16]
        .parse()
        .map_err(|_| "invalid minute".to_string())?;
    let sec: u32 = s[17..19]
        .parse()
        .map_err(|_| "invalid second".to_string())?;

    if month < 1 || month > 12 || day < 1 || day > 31 || hour > 23 || min > 59 || sec > 59 {
        return Err("invalid DateTime64 string".into());
    }

    // Civil date to Unix timestamp (Howard Hinnant algorithm)
    let days = civil_days_from_epoch(year, month, day);
    let secs = days as i64 * 86400 + hour as i64 * 3600 + min as i64 * 60 + sec as i64;

    Ok((secs, frac_nanos))
}

/// Convert a civil date to days since 1970-01-01 (Howard Hinnant algorithm).
fn civil_days_from_epoch(year: i32, month: u32, day: u32) -> i64 {
    let y = if month <= 2 { year - 1 } else { year } as i64;
    let m = if month <= 2 { month + 9 } else { month - 3 } as i64;
    let era = y.div_euclid(400);
    let yoe = y.rem_euclid(400);
    let doy = (153 * m + 2) / 5 + day as i64 - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146097 + doe - 719468
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
    fn test_encode_json() {
        // JSON type encodes as length-prefixed JSON string (same wire format as String).
        // ClickHouse accepts this when input_format_binary_read_json_as_string=1.
        let schema = DynamicSchema::from_columns("t", vec![col("data", "JSON")]);
        let row = serde_json::json!({"data": {"key": "value", "num": 42}});
        let cols = columns_to_send(row.as_object().unwrap(), &schema);
        let bytes = encode_dynamic_row(row.as_object().unwrap(), &schema, &cols).unwrap();
        // Should be varint(len) + JSON string bytes
        let json_str = r#"{"key":"value","num":42}"#;
        let mut expected = Vec::new();
        write_varint(json_str.len() as u64, &mut expected);
        expected.extend_from_slice(json_str.as_bytes());
        assert_eq!(bytes, expected);
    }

    #[test]
    fn test_encode_nullable_json_non_null() {
        // Nullable(JSON) with a value: 0x00 (not null) + length-prefixed JSON string.
        // This is the exact scenario that caused the 0x73 error in production.
        let schema = DynamicSchema::from_columns("t", vec![col("tags", "Nullable(JSON)")]);
        let row = serde_json::json!({"tags": {"env": "prod"}});
        let cols = columns_to_send(row.as_object().unwrap(), &schema);
        let bytes = encode_dynamic_row(row.as_object().unwrap(), &schema, &cols).unwrap();
        let json_str = r#"{"env":"prod"}"#;
        let mut expected = vec![0u8]; // not null
        write_varint(json_str.len() as u64, &mut expected);
        expected.extend_from_slice(json_str.as_bytes());
        assert_eq!(bytes, expected);
    }

    #[test]
    fn test_encode_nullable_json_null() {
        // Nullable(JSON) with null: just 0x01 (is null).
        let schema = DynamicSchema::from_columns("t", vec![col("tags", "Nullable(JSON)")]);
        let row = serde_json::json!({"tags": null});
        let cols = columns_to_send(row.as_object().unwrap(), &schema);
        let bytes = encode_dynamic_row(row.as_object().unwrap(), &schema, &cols).unwrap();
        assert_eq!(bytes, vec![1]); // is_null = true
    }

    #[test]
    fn test_encode_json_empty_object() {
        let schema = DynamicSchema::from_columns("t", vec![col("data", "JSON")]);
        let row = serde_json::json!({"data": {}});
        let cols = columns_to_send(row.as_object().unwrap(), &schema);
        let bytes = encode_dynamic_row(row.as_object().unwrap(), &schema, &cols).unwrap();
        // "{}" = 2 bytes
        assert_eq!(bytes, vec![2, b'{', b'}']);
    }

    #[test]
    fn test_encode_json_string_value() {
        // Value::String writes content directly -- no double-quoting.
        // Caller is responsible for ensuring the string is valid JSON
        // when targeting a JSON column.
        let schema = DynamicSchema::from_columns("t", vec![col("data", "JSON")]);
        let row = serde_json::json!({"data": "just a string"});
        let cols = columns_to_send(row.as_object().unwrap(), &schema);
        let bytes = encode_dynamic_row(row.as_object().unwrap(), &schema, &cols).unwrap();
        let content = "just a string";
        let mut expected = Vec::new();
        write_varint(content.len() as u64, &mut expected);
        expected.extend_from_slice(content.as_bytes());
        assert_eq!(bytes, expected);
    }

    #[test]
    fn test_encode_json_string_containing_json_object() {
        // Simulates dfe-loader's _json column: raw Kafka payload stored as
        // Value::String containing a JSON object string.
        let schema = DynamicSchema::from_columns("t", vec![col("data", "JSON")]);
        let json_payload = r#"{"event":"login","user_id":"usr-123"}"#;
        let row = serde_json::json!({"data": json_payload});
        let cols = columns_to_send(row.as_object().unwrap(), &schema);
        let bytes = encode_dynamic_row(row.as_object().unwrap(), &schema, &cols).unwrap();
        let mut expected = Vec::new();
        write_varint(json_payload.len() as u64, &mut expected);
        expected.extend_from_slice(json_payload.as_bytes());
        assert_eq!(bytes, expected);
    }

    #[test]
    fn test_encode_json_number_value() {
        // Non-string values go through value.to_string() -- verify numbers work.
        let schema = DynamicSchema::from_columns("t", vec![col("data", "JSON")]);
        let row = serde_json::json!({"data": 42});
        let cols = columns_to_send(row.as_object().unwrap(), &schema);
        let bytes = encode_dynamic_row(row.as_object().unwrap(), &schema, &cols).unwrap();
        let mut expected = Vec::new();
        write_varint(2, &mut expected); // "42" = 2 bytes
        expected.extend_from_slice(b"42");
        assert_eq!(bytes, expected);
    }

    #[test]
    fn test_encode_json_array_value() {
        // JSON column receiving a JSON array (not Value::String).
        let schema = DynamicSchema::from_columns("t", vec![col("data", "JSON")]);
        let row = serde_json::json!({"data": [1, 2, 3]});
        let cols = columns_to_send(row.as_object().unwrap(), &schema);
        let bytes = encode_dynamic_row(row.as_object().unwrap(), &schema, &cols).unwrap();
        let json_str = "[1,2,3]";
        let mut expected = Vec::new();
        write_varint(json_str.len() as u64, &mut expected);
        expected.extend_from_slice(json_str.as_bytes());
        assert_eq!(bytes, expected);
    }

    #[test]
    fn test_encode_raw_json_passthrough() {
        // Raw bytes written directly for a JSON column — zero-copy path.
        // The row map does NOT contain _json; raw bytes are passed separately.
        let schema = DynamicSchema::from_columns(
            "t",
            vec![col("id", "UInt32"), col("_json", "Nullable(JSON)")],
        );
        let row = serde_json::json!({"id": 1});
        let cols = columns_to_send(row.as_object().unwrap(), &schema);
        // _json has no default and is not in the row, so columns_to_send includes it.
        assert!(cols.iter().any(|c| c.name == "_json"));

        let raw_payload = br#"{"event":"login","user":"alice"}"#;
        let bytes = encode_dynamic_row_with_raw(
            row.as_object().unwrap(),
            &schema,
            &cols,
            &[("_json", raw_payload.as_slice())],
        )
        .unwrap();

        let mut expected = Vec::new();
        // id: UInt32 LE
        expected.extend_from_slice(&1u32.to_le_bytes());
        // _json: Nullable(JSON) — 0x00 (not null) + varint(len) + raw bytes
        expected.push(0); // not null
        write_varint(raw_payload.len() as u64, &mut expected);
        expected.extend_from_slice(raw_payload);
        assert_eq!(bytes, expected);
    }

    #[test]
    fn test_encode_raw_passthrough_matches_value_path() {
        // Verify that raw passthrough produces identical wire format to the
        // normal Value::String encoding path for JSON columns.
        let schema = DynamicSchema::from_columns("t", vec![col("data", "JSON")]);
        let raw_payload = br#"{"key":"value","num":42}"#;

        // Normal path: Value::String with JSON content
        let row_normal = serde_json::json!({"data": std::str::from_utf8(raw_payload).unwrap()});
        let cols = columns_to_send(row_normal.as_object().unwrap(), &schema);
        let bytes_normal =
            encode_dynamic_row(row_normal.as_object().unwrap(), &schema, &cols).unwrap();

        // Raw passthrough: empty row + raw bytes
        let row_raw = serde_json::json!({});
        let cols_raw = columns_to_send(row_raw.as_object().unwrap(), &schema);
        let bytes_raw = encode_dynamic_row_with_raw(
            row_raw.as_object().unwrap(),
            &schema,
            &cols_raw,
            &[("data", raw_payload.as_slice())],
        )
        .unwrap();

        assert_eq!(
            bytes_normal, bytes_raw,
            "raw passthrough must match Value::String encoding"
        );
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

    // -----------------------------------------------------------------------
    // DateTime64 encoding tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_encode_datetime64_from_string() {
        // The exact scenario that caused the production error:
        // transformer outputs "YYYY-MM-DD HH:MM:SS.mmm" string for DateTime64(3)
        let schema = DynamicSchema::from_columns("t", vec![col("ts", "DateTime64(3)")]);
        let row = serde_json::json!({"ts": "2026-04-07 07:03:00.095"});
        let cols = columns_to_send(row.as_object().unwrap(), &schema);
        let bytes = encode_dynamic_row(row.as_object().unwrap(), &schema, &cols).unwrap();
        // 2026-04-07 07:03:00.095 UTC = 1,775,545,380 seconds * 1000 + 95 = 1775545380095 ms
        let expected_ms: i64 = 1_775_545_380_095;
        assert_eq!(bytes, expected_ms.to_le_bytes().to_vec());
    }

    #[test]
    fn test_encode_datetime64_from_number() {
        // Numeric epoch value (already scaled to precision) passes through
        let schema = DynamicSchema::from_columns("t", vec![col("ts", "DateTime64(3)")]);
        let row = serde_json::json!({"ts": 1_775_545_380_095_i64});
        let cols = columns_to_send(row.as_object().unwrap(), &schema);
        let bytes = encode_dynamic_row(row.as_object().unwrap(), &schema, &cols).unwrap();
        assert_eq!(bytes, 1_775_545_380_095_i64.to_le_bytes().to_vec());
    }

    #[test]
    fn test_encode_datetime64_from_numeric_string() {
        // Numeric string (epoch ms) passes through as i64
        let schema = DynamicSchema::from_columns("t", vec![col("ts", "DateTime64(3)")]);
        let row = serde_json::json!({"ts": "1775545380095"});
        let cols = columns_to_send(row.as_object().unwrap(), &schema);
        let bytes = encode_dynamic_row(row.as_object().unwrap(), &schema, &cols).unwrap();
        assert_eq!(bytes, 1_775_545_380_095_i64.to_le_bytes().to_vec());
    }

    #[test]
    fn test_encode_datetime64_iso8601_with_t() {
        // ISO 8601 with T separator and Z suffix
        let schema = DynamicSchema::from_columns("t", vec![col("ts", "DateTime64(3)")]);
        let row = serde_json::json!({"ts": "2026-04-07T07:03:00.095Z"});
        let cols = columns_to_send(row.as_object().unwrap(), &schema);
        let bytes = encode_dynamic_row(row.as_object().unwrap(), &schema, &cols).unwrap();
        assert_eq!(bytes, 1_775_545_380_095_i64.to_le_bytes().to_vec());
    }

    #[test]
    fn test_encode_datetime64_no_fractional() {
        // No fractional seconds -- should produce .000
        let schema = DynamicSchema::from_columns("t", vec![col("ts", "DateTime64(3)")]);
        let row = serde_json::json!({"ts": "2026-04-07 07:03:00"});
        let cols = columns_to_send(row.as_object().unwrap(), &schema);
        let bytes = encode_dynamic_row(row.as_object().unwrap(), &schema, &cols).unwrap();
        assert_eq!(bytes, 1_775_545_380_000_i64.to_le_bytes().to_vec());
    }

    #[test]
    fn test_encode_datetime64_precision_6() {
        // DateTime64(6) -- microseconds
        let schema = DynamicSchema::from_columns("t", vec![col("ts", "DateTime64(6)")]);
        let row = serde_json::json!({"ts": "2026-04-07 07:03:00.095123"});
        let cols = columns_to_send(row.as_object().unwrap(), &schema);
        let bytes = encode_dynamic_row(row.as_object().unwrap(), &schema, &cols).unwrap();
        let expected_us: i64 = 1_775_545_380_095_123;
        assert_eq!(bytes, expected_us.to_le_bytes().to_vec());
    }

    #[test]
    fn test_encode_datetime64_precision_9() {
        // DateTime64(9) -- nanoseconds
        let schema = DynamicSchema::from_columns("t", vec![col("ts", "DateTime64(9)")]);
        let row = serde_json::json!({"ts": "2026-04-07 07:03:00.095123456"});
        let cols = columns_to_send(row.as_object().unwrap(), &schema);
        let bytes = encode_dynamic_row(row.as_object().unwrap(), &schema, &cols).unwrap();
        let expected_ns: i64 = 1_775_545_380_095_123_456;
        assert_eq!(bytes, expected_ns.to_le_bytes().to_vec());
    }

    #[test]
    fn test_encode_datetime64_precision_0() {
        // DateTime64(0) -- whole seconds only
        let schema = DynamicSchema::from_columns("t", vec![col("ts", "DateTime64(0)")]);
        let row = serde_json::json!({"ts": "2026-04-07 07:03:00.999"});
        let cols = columns_to_send(row.as_object().unwrap(), &schema);
        let bytes = encode_dynamic_row(row.as_object().unwrap(), &schema, &cols).unwrap();
        // precision 0: just seconds, fractional part truncated
        assert_eq!(bytes, 1_775_545_380_i64.to_le_bytes().to_vec());
    }

    #[test]
    fn test_encode_nullable_datetime64_null() {
        let schema = DynamicSchema::from_columns("t", vec![col("ts", "Nullable(DateTime64(3))")]);
        let row = serde_json::json!({"ts": null});
        let cols = columns_to_send(row.as_object().unwrap(), &schema);
        let bytes = encode_dynamic_row(row.as_object().unwrap(), &schema, &cols).unwrap();
        assert_eq!(bytes, vec![1]); // is_null
    }

    #[test]
    fn test_encode_nullable_datetime64_string() {
        let schema = DynamicSchema::from_columns("t", vec![col("ts", "Nullable(DateTime64(3))")]);
        let row = serde_json::json!({"ts": "2026-04-07 07:03:00.095"});
        let cols = columns_to_send(row.as_object().unwrap(), &schema);
        let bytes = encode_dynamic_row(row.as_object().unwrap(), &schema, &cols).unwrap();
        let mut expected = vec![0u8]; // not_null
        expected.extend_from_slice(&1_775_545_380_095_i64.to_le_bytes());
        assert_eq!(bytes, expected);
    }

    // -----------------------------------------------------------------------
    // DateTime64 parser unit tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_parse_datetime_str_basic() {
        let (secs, frac) = parse_datetime_str("2026-04-07 07:03:00.095").unwrap();
        assert_eq!(secs, 1_775_545_380);
        assert_eq!(frac, 95_000_000); // 0.095s = 95,000,000 ns
    }

    #[test]
    fn test_parse_datetime_str_epoch() {
        // Unix epoch
        let (secs, frac) = parse_datetime_str("1970-01-01 00:00:00").unwrap();
        assert_eq!(secs, 0);
        assert_eq!(frac, 0);
    }

    #[test]
    fn test_parse_datetime_str_iso8601() {
        let (secs, _) = parse_datetime_str("2026-04-07T07:03:00Z").unwrap();
        assert_eq!(secs, 1_775_545_380);
    }

    #[test]
    fn test_parse_datetime_str_utc_offset_zero() {
        // +00:00 is accepted (it's UTC)
        let (secs, _) = parse_datetime_str("2026-04-07 07:03:00+00:00").unwrap();
        assert_eq!(secs, 1_775_545_380);
    }

    #[test]
    fn test_parse_datetime_str_rejects_nonzero_positive_offset() {
        let err = parse_datetime_str("2026-04-07 07:03:00+05:30").unwrap_err();
        assert!(err.contains("non-UTC timezone offset"), "got: {err}");
    }

    #[test]
    fn test_parse_datetime_str_rejects_nonzero_negative_offset() {
        let err = parse_datetime_str("2026-04-07 07:03:00-05:00").unwrap_err();
        assert!(err.contains("non-UTC timezone offset"), "got: {err}");
    }

    #[test]
    fn test_parse_datetime_str_high_precision_fractional() {
        // 12 fractional digits -- truncated to 9 (nanoseconds)
        let (secs, frac) = parse_datetime_str("2026-04-07 07:03:00.123456789012").unwrap();
        assert_eq!(secs, 1_775_545_380);
        assert_eq!(frac, 123_456_789); // truncated, not overflowed
    }

    #[test]
    fn test_parse_datetime_str_single_fractional_digit() {
        // "0.1" = 100ms = 100,000,000 ns
        let (_, frac) = parse_datetime_str("2026-04-07 07:03:00.1").unwrap();
        assert_eq!(frac, 100_000_000);
    }

    #[test]
    fn test_parse_datetime_str_rejects_garbage() {
        assert!(parse_datetime_str("not a date").is_err());
        assert!(parse_datetime_str("2026-04-07").is_err()); // date only, no time
        assert!(parse_datetime_str("").is_err());
        assert!(parse_datetime_str("2026-04-07 07:03:00JUNK").is_err());
    }

    #[test]
    fn test_parse_datetime_str_rejects_invalid_components() {
        assert!(parse_datetime_str("2026-13-07 07:03:00").is_err()); // month 13
        assert!(parse_datetime_str("2026-00-07 07:03:00").is_err()); // month 0
        assert!(parse_datetime_str("2026-04-00 07:03:00").is_err()); // day 0
        assert!(parse_datetime_str("2026-04-07 25:03:00").is_err()); // hour 25
        assert!(parse_datetime_str("2026-04-07 07:60:00").is_err()); // min 60
        assert!(parse_datetime_str("2026-04-07 07:03:60").is_err()); // sec 60
    }

    #[test]
    fn test_datetime64_epoch_overflow_returns_error() {
        // i64::MAX as seconds * 1000 would overflow
        let schema = DynamicSchema::from_columns("t", vec![col("ts", "DateTime64(3)")]);
        // Year 9999 * 1000 should still fit in i64, just verifying no panic.
        let row = serde_json::json!({"ts": "9999-12-31 23:59:59"});
        let cols = columns_to_send(row.as_object().unwrap(), &schema);
        // Year 9999 * 1000 should still fit in i64, so this should succeed.
        // The overflow guard is for extreme values close to i64::MAX seconds.
        let result = encode_dynamic_row(row.as_object().unwrap(), &schema, &cols);
        assert!(result.is_ok()); // 9999 is fine, just verifying no panic

        // But a direct call with absurd seconds would overflow:
        let bad = datetime64_to_epoch(
            &serde_json::json!("9999-12-31 23:59:59"),
            18, // precision 18 = 10^18 multiplier, guaranteed overflow
            "ts",
        );
        assert!(bad.is_err());
        let msg = format!("{}", bad.unwrap_err());
        assert!(msg.contains("overflow"), "got: {msg}");
    }

    #[test]
    fn test_civil_days_epoch() {
        assert_eq!(civil_days_from_epoch(1970, 1, 1), 0);
    }

    #[test]
    fn test_civil_days_known_date() {
        // 2026-04-07 = day 20,550 from epoch
        assert_eq!(civil_days_from_epoch(2026, 4, 7), 20_550);
    }

    #[test]
    fn test_civil_days_pre_epoch() {
        // 1969-12-31 = day -1
        assert_eq!(civil_days_from_epoch(1969, 12, 31), -1);
    }

    #[test]
    fn test_civil_days_leap_year() {
        // 2000-02-29 exists (leap year)
        // 2000-03-01 should be one day later
        let feb29 = civil_days_from_epoch(2000, 2, 29);
        let mar01 = civil_days_from_epoch(2000, 3, 1);
        assert_eq!(mar01 - feb29, 1);
    }
}
