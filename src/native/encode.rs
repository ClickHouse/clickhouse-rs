//! Columnar block encoder for native INSERT.
//!
//! Transposes row-oriented RowBinary data (one `Vec<u8>` per row) into the
//! native columnar wire format used by ClickHouse data blocks.
//!
//! # Supported types for INSERT
//!
//! All scalar fixed-size types, String, FixedString(N), Nullable(T),
//! Array(T), Map(K, V), Tuple(T1..Tn), and nested combinations thereof.
//! LowCardinality is stripped to its inner type (ClickHouse accepts plain values).
//! Variant, Dynamic, and JSON are not yet supported.

use crate::error::{Error, Result};
use crate::native::columns::ColumnType;
use crate::native::io::ClickHouseBytesWrite;
use crate::native::protocol::DBMS_MIN_PROTOCOL_VERSION_WITH_CUSTOM_SERIALIZATION;

/// Column schema entry for a native INSERT block.
#[derive(Debug, Clone)]
pub(crate) struct ColumnSchema {
    /// Column name as declared to the server.
    pub(crate) name: String,
    /// Type name string sent on the wire (LowCardinality stripped).
    pub(crate) type_name: String,
    /// Parsed column type used for encoding decisions.
    pub(crate) col_type: ColumnType,
}

impl ColumnSchema {
    /// Build a `ColumnSchema` list from server-provided `(name, type_name)` pairs.
    ///
    /// LowCardinality wrappers are stripped — the inner type is sent on wire,
    /// which ClickHouse accepts transparently.
    pub(crate) fn from_headers(headers: &[(String, String)]) -> Result<Vec<Self>> {
        headers
            .iter()
            .map(|(name, type_name)| {
                let col_type =
                    ColumnType::parse(type_name).ok_or_else(|| {
                        Error::BadResponse(format!(
                            "native INSERT: unsupported column type '{type_name}' \
                             for column '{name}'"
                        ))
                    })?;
                // Strip LowCardinality: send inner type bytes, CH handles encoding
                let (effective_type, effective_name) =
                    strip_low_cardinality(col_type, type_name);
                Ok(ColumnSchema {
                    name: name.clone(),
                    type_name: effective_name,
                    col_type: effective_type,
                })
            })
            .collect()
    }
}

/// Recursively strip `LowCardinality(...)` returning the inner `(ColumnType, type_name)`.
fn strip_low_cardinality(col_type: ColumnType, type_name: &str) -> (ColumnType, String) {
    match col_type {
        ColumnType::LowCardinality(inner) => {
            let inner_name = extract_inner(type_name, "LowCardinality");
            strip_low_cardinality(*inner, inner_name)
        }
        other => (other, type_name.to_string()),
    }
}

fn extract_inner<'a>(s: &'a str, wrapper: &str) -> &'a str {
    let prefix = format!("{wrapper}(");
    if let Some(rest) = s.strip_prefix(prefix.as_str()) {
        if let Some(inner) = rest.strip_suffix(')') {
            return inner;
        }
    }
    s
}

/// Encode buffered RowBinary rows into native columnar block column bytes.
///
/// Returns a flat byte buffer containing, for each column in order:
/// - `string(column_name)`
/// - `string(column_type_name)`
/// - optional custom-serialization flag byte (0x00) for newer servers
/// - column data (native columnar encoding, recursively for Array/Map/Tuple)
///
/// This output is written directly after the block header
/// (`num_columns` + `num_rows`) in a Data packet.
///
/// # Errors
///
/// Returns `Error::BadResponse` if any row's RowBinary data is truncated or
/// contains an unsupported type for INSERT.
pub(crate) fn encode_columns(
    rows: &[Vec<u8>],
    columns: &[ColumnSchema],
    revision: u64,
) -> Result<Vec<u8>> {
    let has_custom_ser = revision >= DBMS_MIN_PROTOCOL_VERSION_WITH_CUSTOM_SERIALIZATION;
    if columns.is_empty() {
        return Ok(Vec::new());
    }

    // Pass 1 — extract per-column raw RowBinary value bytes (one per row).
    let n = rows.len();
    let mut per_col: Vec<Vec<Vec<u8>>> = vec![Vec::with_capacity(n); columns.len()];
    for row in rows {
        let mut pos = 0;
        for (ci, col) in columns.iter().enumerate() {
            let start = pos;
            rb_advance(row, &mut pos, &col.col_type)?;
            per_col[ci].push(row[start..pos].to_vec());
        }
    }

    // Pass 2 — emit header + native-encoded data for each column.
    let mut out = Vec::new();
    for (ci, col) in columns.iter().enumerate() {
        out.put_string(col.name.as_bytes());
        out.put_string(col.type_name.as_bytes());
        // Newer servers expect a custom-serialization flag byte (0 = normal) per column.
        if has_custom_ser {
            out.push(0u8);
        }
        write_col_values(&per_col[ci], &col.col_type, &mut out)?;
    }

    Ok(out)
}

/// Recursively write native columnar data for `values` (one `Vec<u8>` per row,
/// containing raw RowBinary bytes for a single value).
fn write_col_values(values: &[Vec<u8>], col_type: &ColumnType, out: &mut Vec<u8>) -> Result<()> {
    // Fixed-size scalars, String, and FixedString: RowBinary bytes == native bytes.
    if col_type.fixed_size().is_some()
        || matches!(
            col_type,
            ColumnType::String | ColumnType::FixedString(_) | ColumnType::Json
        )
    {
        for v in values {
            out.extend_from_slice(v);
        }
        return Ok(());
    }

    match col_type {
        ColumnType::Nullable(inner) => {
            // Native: u8[n] null flags, then inner_type[n] values (zero for nulls).
            let mut inner_vals: Vec<Vec<u8>> = Vec::with_capacity(values.len());
            for v in values {
                if v.is_empty() {
                    return Err(rb_truncated());
                }
                let flag = v[0]; // RowBinary: 0 = has value, 1 = null
                out.push(flag);
                if flag == 0 {
                    inner_vals.push(v[1..].to_vec()); // value bytes follow flag
                } else {
                    let mut def = Vec::new();
                    rb_write_default(&mut def, inner); // zero bytes for null slot
                    inner_vals.push(def);
                }
            }
            write_col_values(&inner_vals, inner, out)?;
        }

        ColumnType::LowCardinality(inner) => {
            // Stripped at schema level — just encode as inner type.
            write_col_values(values, inner, out)?;
        }

        ColumnType::Array(inner) => {
            // Native: u64[n] cumulative offsets, then all elements as a sub-column.
            let mut cum: u64 = 0;
            let mut offsets: Vec<u64> = Vec::with_capacity(values.len());
            let mut all_elems: Vec<Vec<u8>> = Vec::new();

            for v in values {
                let mut pos = 0;
                let (count, hdr) = rb_read_varuint(v, pos)?;
                pos += hdr;
                for _ in 0..count {
                    let start = pos;
                    rb_advance(v, &mut pos, inner)?;
                    all_elems.push(v[start..pos].to_vec());
                }
                cum += count;
                offsets.push(cum);
            }

            for off in &offsets {
                out.extend_from_slice(&off.to_le_bytes());
            }
            write_col_values(&all_elems, inner, out)?;
        }

        ColumnType::Map(key_type, val_type) => {
            // Native: u64[n] cumulative offsets, then key sub-column, then value sub-column.
            let mut cum: u64 = 0;
            let mut offsets: Vec<u64> = Vec::with_capacity(values.len());
            let mut all_keys: Vec<Vec<u8>> = Vec::new();
            let mut all_vals: Vec<Vec<u8>> = Vec::new();

            for v in values {
                let mut pos = 0;
                let (count, hdr) = rb_read_varuint(v, pos)?;
                pos += hdr;
                for _ in 0..count {
                    let ks = pos;
                    rb_advance(v, &mut pos, key_type)?;
                    all_keys.push(v[ks..pos].to_vec());
                    let vs = pos;
                    rb_advance(v, &mut pos, val_type)?;
                    all_vals.push(v[vs..pos].to_vec());
                }
                cum += count;
                offsets.push(cum);
            }

            for off in &offsets {
                out.extend_from_slice(&off.to_le_bytes());
            }
            write_col_values(&all_keys, key_type, out)?;
            write_col_values(&all_vals, val_type, out)?;
        }

        ColumnType::Tuple(fields) => {
            // Native: each field is a separate sub-column in definition order.
            let mut field_vals: Vec<Vec<Vec<u8>>> =
                vec![Vec::with_capacity(values.len()); fields.len()];
            for v in values {
                let mut pos = 0;
                for (fi, field_type) in fields.iter().enumerate() {
                    let start = pos;
                    rb_advance(v, &mut pos, field_type)?;
                    field_vals[fi].push(v[start..pos].to_vec());
                }
            }
            for (fi, field_type) in fields.iter().enumerate() {
                write_col_values(&field_vals[fi], field_type, out)?;
            }
        }

        unsupported => {
            return Err(Error::BadResponse(format!(
                "native INSERT: column type {unsupported:?} is not supported for INSERT"
            )));
        }
    }

    Ok(())
}

/// Advance `pos` past one RowBinary-encoded value of `col_type`.
///
/// RowBinary and native wire formats are identical for all scalar types.
/// Only `Nullable` differs: RowBinary has a per-row flag followed by the
/// value (or nothing for null), while native packs flags and values separately.
fn rb_advance(data: &[u8], pos: &mut usize, col_type: &ColumnType) -> Result<()> {
    // Fixed-size types: same byte count in RowBinary and native.
    if let Some(size) = col_type.fixed_size() {
        if *pos + size > data.len() {
            return Err(rb_truncated());
        }
        *pos += size;
        return Ok(());
    }

    match col_type {
        ColumnType::String | ColumnType::Json => {
            let (len, hdr) = rb_read_varuint(data, *pos)?;
            let end = *pos + hdr + len as usize;
            if end > data.len() {
                return Err(rb_truncated());
            }
            *pos = end;
        }
        ColumnType::FixedString(n) => {
            if *pos + n > data.len() {
                return Err(rb_truncated());
            }
            *pos += n;
        }
        ColumnType::Nullable(inner) => {
            if *pos >= data.len() {
                return Err(rb_truncated());
            }
            let flag = data[*pos];
            *pos += 1;
            if flag == 0 {
                rb_advance(data, pos, inner)?;
            }
        }
        ColumnType::LowCardinality(inner) => {
            // RowBinary serialises LowCardinality transparently as the inner type.
            rb_advance(data, pos, inner)?;
        }
        ColumnType::Array(inner) => {
            let (count, hdr) = rb_read_varuint(data, *pos)?;
            *pos += hdr;
            for _ in 0..count {
                rb_advance(data, pos, inner)?;
            }
        }
        ColumnType::Tuple(fields) => {
            for field in fields {
                rb_advance(data, pos, field)?;
            }
        }
        ColumnType::Map(key_type, val_type) => {
            let (count, hdr) = rb_read_varuint(data, *pos)?;
            *pos += hdr;
            for _ in 0..count {
                rb_advance(data, pos, key_type)?;
                rb_advance(data, pos, val_type)?;
            }
        }
        unsupported => {
            return Err(Error::BadResponse(format!(
                "native INSERT: column type {unsupported:?} is not supported for INSERT"
            )));
        }
    }
    Ok(())
}

/// Write the default (zero) native encoding for `col_type`.
///
/// Used to fill the value slot for NULL rows in a Nullable column —
/// the native protocol requires value bytes even when the null flag is set.
fn rb_write_default(out: &mut Vec<u8>, col_type: &ColumnType) {
    if let Some(size) = col_type.fixed_size() {
        out.extend(std::iter::repeat_n(0u8, size));
        return;
    }
    match col_type {
        ColumnType::String | ColumnType::Json => {
            out.put_var_uint(0); // empty string: single 0x00 varuint
        }
        ColumnType::FixedString(n) => {
            out.extend(std::iter::repeat_n(0u8, *n));
        }
        ColumnType::LowCardinality(inner) => {
            rb_write_default(out, inner);
        }
        _ => {
            // Best-effort: empty string for unknown variable-length types
            out.put_var_uint(0);
        }
    }
}

/// Read a varuint from `data` starting at `pos`, returning `(value, bytes_consumed)`.
fn rb_read_varuint(data: &[u8], pos: usize) -> Result<(u64, usize)> {
    let mut out = 0u64;
    let mut shift = 0u32;
    let mut i = pos;
    loop {
        if i >= data.len() {
            return Err(rb_truncated());
        }
        let b = data[i];
        i += 1;
        out |= u64::from(b & 0x7F) << shift;
        shift += 7;
        if b & 0x80 == 0 {
            break;
        }
        if shift >= 64 {
            return Err(Error::BadResponse(
                "native INSERT: varuint overflow in RowBinary".to_string(),
            ));
        }
    }
    Ok((out, i - pos))
}

fn rb_truncated() -> Error {
    Error::BadResponse(
        "native INSERT: RowBinary row data is truncated; \
         does the row struct match the table schema?"
            .to_string(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn u8_col() -> ColumnSchema {
        ColumnSchema {
            name: "n".to_string(),
            type_name: "UInt8".to_string(),
            col_type: ColumnType::UInt8,
        }
    }

    fn str_col() -> ColumnSchema {
        ColumnSchema {
            name: "s".to_string(),
            type_name: "String".to_string(),
            col_type: ColumnType::String,
        }
    }

    fn nullable_u8_col() -> ColumnSchema {
        ColumnSchema {
            name: "n".to_string(),
            type_name: "Nullable(UInt8)".to_string(),
            col_type: ColumnType::Nullable(Box::new(ColumnType::UInt8)),
        }
    }

    #[test]
    fn test_encode_single_u8_column() {
        // Two rows: UInt8 values 1 and 2
        let rows = vec![vec![1u8], vec![2u8]];
        let cols = vec![u8_col()];
        let out = encode_columns(&rows, &cols, 0).unwrap();

        // string("n") = varuint(1) + "n"
        // string("UInt8") = varuint(5) + "UInt8"
        // data = [1, 2]
        let expected_name = b"\x01n";
        let expected_type = b"\x05UInt8";
        let expected_data = b"\x01\x02";
        assert!(out.starts_with(expected_name));
        let after_name = &out[expected_name.len()..];
        assert!(after_name.starts_with(expected_type));
        let after_type = &after_name[expected_type.len()..];
        assert_eq!(after_type, expected_data);
    }

    #[test]
    fn test_encode_string_column() {
        // One row: String "hi"
        let mut row = Vec::new();
        row.push(0x02u8); // varuint(2)
        row.extend_from_slice(b"hi");
        let rows = vec![row.clone()];
        let cols = vec![str_col()];
        let out = encode_columns(&rows, &cols, 0).unwrap();
        // After header: the string bytes from RowBinary are passed through unchanged
        let after_hdr = out[b"\x01s\x06String".len()..].to_vec();
        assert_eq!(after_hdr, row);
    }

    #[test]
    fn test_encode_nullable_u8_not_null() {
        // One row: Nullable(UInt8) = Some(42)
        // RowBinary: [0x00 (not null), 42]
        let rows = vec![vec![0x00u8, 42u8]];
        let cols = vec![nullable_u8_col()];
        let out = encode_columns(&rows, &cols, 0).unwrap();
        // After header: [0x00 (null flag)] then [42 (value)]
        let hdr_len = b"\x01n\x10Nullable(UInt8)".len();
        let data = &out[hdr_len..];
        assert_eq!(data, &[0x00u8, 42u8]); // flag then value
    }

    #[test]
    fn test_encode_nullable_u8_null() {
        // One row: Nullable(UInt8) = None
        // RowBinary: [0x01 (null)]
        let rows = vec![vec![0x01u8]];
        let cols = vec![nullable_u8_col()];
        let out = encode_columns(&rows, &cols, 0).unwrap();
        // After header: [0x01 (null flag)] then [0x00 (zero default value)]
        let hdr_len = b"\x01n\x10Nullable(UInt8)".len();
        let data = &out[hdr_len..];
        assert_eq!(data, &[0x01u8, 0x00u8]);
    }
}
