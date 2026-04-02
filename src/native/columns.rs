//! Native binary column type system and data reader.
//!
//! Reads ClickHouse native binary column data and re-serializes it as
//! RowBinary so the existing `rowbinary::deserialize_row` machinery can consume it.
//!
//! RowBinary and native binary formats are identical for scalar types — the
//! only difference is layout (columnar vs row-oriented). Nullable is the only
//! type that differs structurally.

use tokio::io::AsyncReadExt;

use crate::error::{Error, Result};
use crate::native::io::ClickHouseRead;

/// Supported ClickHouse column types for native transport.
#[derive(Debug, Clone)]
pub(crate) enum ColumnType {
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    Int8,
    Int16,
    Int32,
    Int64,
    Int128,
    UInt128,
    Int256,
    UInt256,
    Float32,
    Float64,
    /// BFloat16 — 16-bit brain float, 2 bytes on wire.
    BFloat16,
    /// Decimal32/64/128/256 — wire format identical to Int32/64/128/256 (raw LE bytes).
    Decimal32,
    Decimal64,
    Decimal128,
    Decimal256,
    String,
    FixedString(usize),
    Uuid,
    /// IPv4 — stored as 4-byte little-endian UInt32.
    IPv4,
    /// IPv6 — stored as 16 bytes.
    IPv6,
    Date,
    Date32,
    DateTime,
    DateTime64,
    /// Time — stored as UInt32 (seconds since midnight).
    Time,
    /// Time64 — stored as Int64 (ticks since midnight at given precision).
    Time64,
    Nullable(Box<ColumnType>),
    LowCardinality(Box<ColumnType>),
    /// Enum8/Enum16 — wire-compatible with UInt8/UInt16 respectively.
    Enum8,
    Enum16,
    /// SimpleAggregateFunction(func, T) — wire-compatible with inner type T.
    SimpleAggregateFunction(Box<ColumnType>),
    /// Array(T) — n cumulative u64 offsets, then all values packed as T column.
    Array(Box<ColumnType>),
    /// Tuple(T1, T2, ...) — each field stored as a separate columnar block.
    Tuple(Vec<ColumnType>),
    /// Map(K, V) — n cumulative u64 offsets, then K column, then V column.
    Map(Box<ColumnType>, Box<ColumnType>),
    /// JSON (legacy Object('json')) — wire format is a length-prefixed String.
    Json,
    /// Point — pair of Float64 (16 bytes), ClickHouse geo type.
    Point,
    /// Variant(T1, T2, ...) — discriminated union (ClickHouse 24.x+).
    /// Wire prefix: u64 version (=0).
    /// Wire data: u8[n] discriminators (255=NULL, 0..k-1 = type index in definition order),
    ///            then per-variant sub-columns in definition order.
    Variant(Vec<ColumnType>),
    /// New JSON type (ClickHouse 24.x+). Complex path-based columnar format.
    /// Wire prefix: u64 JSON version (1=string, 2=object-v2, 3=object-v3).
    /// Wire data (v2): per-path Dynamic v2 headers + discriminators + values + n×u64 shared data.
    NewJson,
    /// Standalone Dynamic type (ClickHouse 24.x+).
    /// Wire prefix: u64 version (1=deprecated, 2=intermediate, 3=flat).
    /// Wire data: discriminators + per-type column data.
    Dynamic,
}

impl ColumnType {
    /// Parse a ClickHouse type name into `ColumnType`.
    ///
    /// Returns `None` for unsupported types.
    pub(crate) fn parse(type_str: &str) -> Option<Self> {
        let type_str = type_str.trim();

        if let Some(inner) = strip_outer(type_str, "Nullable") {
            return Self::parse(inner).map(|t| Self::Nullable(Box::new(t)));
        }

        if let Some(inner) = strip_outer(type_str, "LowCardinality") {
            return Self::parse(inner).map(|t| Self::LowCardinality(Box::new(t)));
        }

        if let Some(n_str) = strip_outer(type_str, "FixedString") {
            return n_str.parse::<usize>().ok().map(Self::FixedString);
        }

        if type_str.starts_with("DateTime64(") {
            return Some(Self::DateTime64);
        }
        if type_str.starts_with("DateTime(") {
            return Some(Self::DateTime);
        }
        if type_str.starts_with("Time64(") {
            return Some(Self::Time64);
        }

        // Decimal variants — scale is not needed for wire reading (raw LE bytes).
        if type_str.starts_with("Decimal32(") {
            return Some(Self::Decimal32);
        }
        if type_str.starts_with("Decimal64(") {
            return Some(Self::Decimal64);
        }
        if type_str.starts_with("Decimal128(") {
            return Some(Self::Decimal128);
        }
        if type_str.starts_with("Decimal256(") {
            return Some(Self::Decimal256);
        }
        // Generic Decimal(precision, scale) — map to Decimal32/64/128/256 by precision.
        if let Some(args_str) = strip_outer(type_str, "Decimal") {
            let args = split_type_args(args_str);
            if args.len() == 2 {
                if let Ok(precision) = args[0].trim().parse::<usize>() {
                    return Some(if precision <= 9 {
                        Self::Decimal32
                    } else if precision <= 18 {
                        Self::Decimal64
                    } else if precision <= 38 {
                        Self::Decimal128
                    } else {
                        Self::Decimal256
                    });
                }
            }
            return None;
        }

        // Enum8(...) / Enum16(...) — wire format = UInt8/UInt16
        if type_str.starts_with("Enum8(") {
            return Some(Self::Enum8);
        }
        if type_str.starts_with("Enum16(") {
            return Some(Self::Enum16);
        }

        // Array(T)
        if let Some(inner_str) = strip_outer(type_str, "Array") {
            return Self::parse(inner_str).map(|t| Self::Array(Box::new(t)));
        }

        // Tuple(T1, T2, ...)
        if let Some(args_str) = strip_outer(type_str, "Tuple") {
            let arg_strings = split_type_args(args_str);
            let fields: Vec<ColumnType> =
                arg_strings.iter().filter_map(|s| Self::parse(s)).collect();
            // Only accept if all fields parsed successfully.
            if !fields.is_empty() && fields.len() == arg_strings.len() {
                return Some(Self::Tuple(fields));
            }
            return None;
        }

        // Map(K, V)
        if let Some(args_str) = strip_outer(type_str, "Map") {
            let args = split_type_args(args_str);
            if args.len() == 2 {
                let k = Self::parse(args[0])?;
                let v = Self::parse(args[1])?;
                return Some(Self::Map(Box::new(k), Box::new(v)));
            }
            return None;
        }

        // SimpleAggregateFunction(func, T) — strip wrapper, read as T
        if type_str.starts_with("SimpleAggregateFunction(") {
            if let Some(rest) = type_str.strip_prefix("SimpleAggregateFunction(") {
                // Find first ", " at depth 0 to split function name from type
                if let Some(comma_pos) = find_first_comma_at_depth0(rest) {
                    let inner_str = rest[comma_pos + 1..].trim();
                    let inner_str = inner_str.strip_suffix(')').unwrap_or(inner_str);
                    if let Some(inner) = Self::parse(inner_str) {
                        return Some(Self::SimpleAggregateFunction(Box::new(inner)));
                    }
                }
            }
            return None;
        }

        // Variant(T1, T2, ...) — discriminated union
        if let Some(args_str) = strip_outer(type_str, "Variant") {
            let arg_strings = split_type_args(args_str);
            let fields: Vec<ColumnType> =
                arg_strings.iter().filter_map(|s| Self::parse(s)).collect();
            if !fields.is_empty() && fields.len() == arg_strings.len() {
                return Some(Self::Variant(fields));
            }
            return None;
        }

        // Dynamic(N) — with optional max_types param
        if type_str.starts_with("Dynamic(") {
            return Some(Self::Dynamic);
        }

        match type_str {
            // Bool is an alias for UInt8 (true=1, false=0) on the wire.
            "Bool" => Some(Self::UInt8),
            "UInt8" => Some(Self::UInt8),
            "UInt16" => Some(Self::UInt16),
            "UInt32" => Some(Self::UInt32),
            "UInt64" => Some(Self::UInt64),
            "Int8" => Some(Self::Int8),
            "Int16" => Some(Self::Int16),
            "Int32" => Some(Self::Int32),
            "Int64" => Some(Self::Int64),
            "Int128" => Some(Self::Int128),
            "UInt128" => Some(Self::UInt128),
            "Int256" => Some(Self::Int256),
            "UInt256" => Some(Self::UInt256),
            "Float32" => Some(Self::Float32),
            "Float64" => Some(Self::Float64),
            "BFloat16" => Some(Self::BFloat16),
            "String" => Some(Self::String),
            "UUID" => Some(Self::Uuid),
            "IPv4" => Some(Self::IPv4),
            "IPv6" => Some(Self::IPv6),
            "Date" => Some(Self::Date),
            "Date32" => Some(Self::Date32),
            "DateTime" => Some(Self::DateTime),
            "Time" => Some(Self::Time),
            // New JSON type (ClickHouse 24.x+) — path-based columnar format.
            "JSON" => Some(Self::NewJson),
            "Dynamic" => Some(Self::Dynamic),
            // Legacy Object('json') — stored as a plain String on the wire.
            "Object('json')" => Some(Self::Json),
            // Geo types
            "Point" => Some(Self::Point),
            _ => None,
        }
    }

    /// Fixed wire-format size in bytes; `None` for variable-length types.
    pub(crate) fn fixed_size(&self) -> Option<usize> {
        match self {
            Self::UInt8 | Self::Int8 | Self::Enum8 => Some(1),
            Self::BFloat16 | Self::UInt16 | Self::Int16 | Self::Date | Self::Enum16 => Some(2),
            Self::UInt32
            | Self::Int32
            | Self::Float32
            | Self::DateTime
            | Self::Date32
            | Self::Decimal32
            | Self::IPv4
            | Self::Time => Some(4),
            Self::UInt64
            | Self::Int64
            | Self::Float64
            | Self::DateTime64
            | Self::Decimal64
            | Self::Time64 => Some(8),
            Self::Int128 | Self::UInt128 | Self::Uuid | Self::IPv6 | Self::Decimal128 => Some(16),
            Self::Int256 | Self::UInt256 | Self::Decimal256 => Some(32),
            Self::FixedString(n) => Some(*n),
            Self::String
            | Self::Json
            | Self::Nullable(_)
            | Self::LowCardinality(_)
            | Self::SimpleAggregateFunction(_)
            | Self::Array(_)
            | Self::Tuple(_)
            | Self::Map(_, _)
            | Self::Variant(_)
            | Self::NewJson
            | Self::Dynamic
            // Point is Tuple(Float64, Float64) in columnar format — not a flat 16-byte blob.
            | Self::Point => None,
        }
    }
}

// Strip "TypeName(" prefix and ")" suffix, returning the contents.
fn strip_outer<'a>(s: &'a str, name: &str) -> Option<&'a str> {
    let prefix = format!("{name}(");
    s.strip_prefix(prefix.as_str())?.strip_suffix(')')
}

/// Split a comma-separated type argument list respecting parentheses depth.
///
/// `"String, UInt64"` → `["String", "UInt64"]`
/// `"Array(String), UInt64"` → `["Array(String)", "UInt64"]`
fn split_type_args(s: &str) -> Vec<&str> {
    let mut result = Vec::new();
    let mut depth = 0usize;
    let mut start = 0;
    for (i, c) in s.char_indices() {
        match c {
            '(' => depth += 1,
            ')' => depth = depth.saturating_sub(1),
            ',' if depth == 0 => {
                result.push(s[start..i].trim());
                start = i + 1;
            }
            _ => {}
        }
    }
    let tail = s[start..].trim();
    if !tail.is_empty() {
        result.push(tail);
    }
    result
}

/// Find the byte offset of the first ',' at parentheses depth 0.
fn find_first_comma_at_depth0(s: &str) -> Option<usize> {
    let mut depth = 0usize;
    for (i, c) in s.char_indices() {
        match c {
            '(' => depth += 1,
            ')' => depth = depth.saturating_sub(1),
            ',' if depth == 0 => return Some(i),
            _ => {}
        }
    }
    None
}

/// Per-row RowBinary bytes for a single column's values.
///
/// Each element is the RowBinary-encoded bytes for that row's field value.
pub(crate) type ColumnData = Vec<Vec<u8>>;

/// Read all `num_rows` values for `col_type` from the native binary stream.
///
/// Returns per-row RowBinary bytes ready for concatenation with other column data.
///
/// Uses `Box::pin` internally for the recursive async cases (Nullable, LowCardinality).
pub(crate) fn read_column<'a, R: ClickHouseRead + 'a>(
    reader: &'a mut R,
    col_type: &'a ColumnType,
    num_rows: u64,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<ColumnData>> + Send + 'a>> {
    Box::pin(async move {
        let n = num_rows as usize;

        match col_type {
            ColumnType::UInt8
            | ColumnType::Int8
            | ColumnType::Enum8
            | ColumnType::BFloat16
            | ColumnType::UInt16
            | ColumnType::Int16
            | ColumnType::Enum16
            | ColumnType::UInt32
            | ColumnType::Int32
            | ColumnType::Float32
            | ColumnType::Date
            | ColumnType::Date32
            | ColumnType::DateTime
            | ColumnType::Decimal32
            | ColumnType::IPv4
            | ColumnType::Time
            | ColumnType::UInt64
            | ColumnType::Int64
            | ColumnType::Float64
            | ColumnType::DateTime64
            | ColumnType::Decimal64
            | ColumnType::Time64
            | ColumnType::Uuid
            | ColumnType::Int128
            | ColumnType::UInt128
            | ColumnType::IPv6
            | ColumnType::Decimal128
            | ColumnType::Int256
            | ColumnType::UInt256
            | ColumnType::Decimal256 => {
                let size = col_type.fixed_size().expect("size is known for fixed type");
                read_fixed_column(reader, n, size).await
            }

            // Point = Tuple(Float64, Float64) in the native columnar format:
            // all N x-values come first, then all N y-values.
            // Transpose here so each row becomes the 16 raw bytes [f64(x) || f64(y)].
            ColumnType::Point => {
                let x_col = read_fixed_column(reader, n, 8).await?;
                let y_col = read_fixed_column(reader, n, 8).await?;
                Ok(x_col
                    .into_iter()
                    .zip(y_col)
                    .map(|(mut x, y)| { x.extend_from_slice(&y); x })
                    .collect())
            }

            ColumnType::String | ColumnType::Json => read_string_column(reader, n).await,
            ColumnType::FixedString(size) => read_fixed_string_column(reader, n, *size).await,
            ColumnType::Nullable(inner) => read_nullable_column(reader, n, inner).await,
            ColumnType::LowCardinality(inner) => {
                read_low_cardinality_column(reader, n, inner).await
            }
            ColumnType::SimpleAggregateFunction(inner) => {
                read_column(reader, inner, num_rows).await
            }
            ColumnType::Array(inner) => read_array_column(reader, n, inner).await,
            ColumnType::Tuple(fields) => read_tuple_column(reader, n, fields).await,
            ColumnType::Map(key_type, val_type) => {
                read_map_column(reader, n, key_type, val_type).await
            }
            ColumnType::Variant(variant_types) => {
                read_variant_column(reader, n, variant_types).await
            }
            ColumnType::NewJson => read_json_column(reader, n).await,
            ColumnType::Dynamic => read_dynamic_column(reader, n).await,
        }
    })
}

async fn read_fixed_column<R: ClickHouseRead>(
    reader: &mut R,
    n: usize,
    size: usize,
) -> Result<ColumnData> {
    let mut result = Vec::with_capacity(n);
    for _ in 0..n {
        let mut buf = vec![0u8; size];
        reader.read_exact(&mut buf).await?;
        result.push(buf);
    }
    Ok(result)
}

async fn read_fixed_string_column<R: ClickHouseRead>(
    reader: &mut R,
    n: usize,
    size: usize,
) -> Result<ColumnData> {
    let mut result = Vec::with_capacity(n);
    for _ in 0..n {
        let mut buf = vec![0u8; size];
        reader.read_exact(&mut buf).await?;
        // Re-encode as RowBinary String: varint(len) + bytes
        let mut row = Vec::with_capacity(size + 9);
        write_var_uint(size as u64, &mut row);
        row.extend_from_slice(&buf);
        result.push(row);
    }
    Ok(result)
}

async fn read_string_column<R: ClickHouseRead>(reader: &mut R, n: usize) -> Result<ColumnData> {
    let mut result = Vec::with_capacity(n);
    for _ in 0..n {
        // read_string() returns raw bytes (varint length already consumed)
        let s = reader.read_string().await?;
        // Re-encode as RowBinary: varint(len) + bytes
        let mut row = Vec::with_capacity(s.len() + 9);
        write_var_uint(s.len() as u64, &mut row);
        row.extend_from_slice(&s);
        result.push(row);
    }
    Ok(result)
}

async fn read_nullable_column<R: ClickHouseRead>(
    reader: &mut R,
    n: usize,
    inner: &ColumnType,
) -> Result<ColumnData> {
    // Native: N null-flags (1 byte each: 1=null, 0=has-value) then N values
    let mut null_flags = vec![0u8; n];
    reader.read_exact(&mut null_flags).await?;

    // All N values are always present (native sends placeholder for nulls too)
    let inner_data = read_column(reader, inner, n as u64).await?;

    let mut result = Vec::with_capacity(n);
    for (flag, value) in null_flags.into_iter().zip(inner_data.into_iter()) {
        if flag != 0 {
            // NULL — RowBinary: 1 byte = 1
            result.push(vec![1u8]);
        } else {
            // Not null — RowBinary: 0 byte then value
            let mut row = Vec::with_capacity(1 + value.len());
            row.push(0u8);
            row.extend_from_slice(&value);
            result.push(row);
        }
    }
    Ok(result)
}

/// LowCardinality column reader.
///
/// Wire format uses fixed uint64 (little-endian) for sizes, not varint.
///
/// ```text
/// u64      state_and_type
///            bits 0-1: index size (0=U8, 1=U16, 2=U32, 3=U64)
///            bit  8:   has global dictionary
///            bit  9:   has additional keys (new rows not in global dict)
/// if bit 8 set:
///   u64      global_dict_size
///   global_dict_size × inner_type values
/// if bit 9 set:
///   u64      additional_keys_size
///   additional_keys_size × inner_type values
/// u64      num_indices   (must equal num_rows)
/// num_indices × index_bytes  (indices into combined dict)
/// ```
async fn read_low_cardinality_column<R: ClickHouseRead>(
    reader: &mut R,
    n: usize,
    inner: &ColumnType,
) -> Result<ColumnData> {
    use tokio::io::AsyncReadExt as _;

    // Wire format starts with a serialization version u64 (= 1).
    let _version = reader.read_u64_le().await?;

    let state = reader.read_u64_le().await?;
    let index_type = (state & 0x03) as u8;
    // Bit 8: NEED_GLOBAL_DICTIONARY — server sends a shared global dict
    let has_global_dict = (state & 0x100) != 0;
    // Bit 9: HAS_ADDITIONAL_KEYS — server sends per-block additional keys
    let has_additional_keys = (state & 0x200) != 0;

    // Indices 0.. reference additional_keys first, then global_dict.
    // Build combined dict in that order.
    let mut additional: ColumnData = Vec::new();
    let mut global: ColumnData = Vec::new();

    if has_global_dict {
        let sz = reader.read_u64_le().await?;
        global = read_column(reader, inner, sz).await?;
    }

    if has_additional_keys {
        let sz = reader.read_u64_le().await?;
        additional = read_column(reader, inner, sz).await?;
    }

    // Combined dict: additional_keys first (indices 0..additional.len()),
    // then global_dict (indices additional.len()..).
    // For the common case (only additional_keys, no global dict), dict = additional.
    let mut dict: ColumnData = additional;
    dict.extend(global);

    // If neither flag is set the entire dict is sent as a single section
    // (older / simpler LowCardinality without shared dictionaries).
    if !has_global_dict && !has_additional_keys {
        let sz = reader.read_u64_le().await?;
        dict.extend(read_column(reader, inner, sz).await?);
    }

    let num_indices = reader.read_u64_le().await?;
    if num_indices != n as u64 {
        return Err(Error::BadResponse(format!(
            "native protocol: LowCardinality index count {num_indices} != row count {n}"
        )));
    }

    let index_bytes = match index_type {
        0 => 1usize,
        1 => 2,
        2 => 4,
        3 => 8,
        other => {
            return Err(Error::BadResponse(format!(
                "native protocol: unknown LowCardinality index type {other}"
            )));
        }
    };

    let dict_size = dict.len();
    let mut result = Vec::with_capacity(n);
    for _ in 0..n {
        let idx = read_index(reader, index_bytes).await? as usize;
        let value = dict.get(idx).ok_or_else(|| {
            Error::BadResponse(format!(
                "native protocol: LowCardinality index {idx} out of range (dict size {dict_size})"
            ))
        })?;
        result.push(value.clone());
    }
    Ok(result)
}

/// Array(T) column reader.
///
/// Native wire format:
/// ```text
/// n × u64   cumulative end-offsets (last value = total element count)
/// total_elements × T   values packed as a regular T column
/// ```
/// Output RowBinary per row: varuint(count) + count × T_rowbinary
async fn read_array_column<R: ClickHouseRead>(
    reader: &mut R,
    n: usize,
    inner: &ColumnType,
) -> Result<ColumnData> {
    use tokio::io::AsyncReadExt as _;

    let mut offsets = Vec::with_capacity(n);
    for _ in 0..n {
        offsets.push(reader.read_u64_le().await?);
    }

    let total = offsets.last().copied().unwrap_or(0);
    let all_values = read_column(reader, inner, total).await?;

    let mut result = Vec::with_capacity(n);
    let mut prev = 0usize;
    for &end in &offsets {
        let end = end as usize;
        let count = end - prev;
        let mut row = Vec::new();
        write_var_uint(count as u64, &mut row);
        for v in &all_values[prev..end] {
            row.extend_from_slice(v);
        }
        result.push(row);
        prev = end;
    }
    Ok(result)
}

/// Tuple(T1, T2, ...) column reader.
///
/// Native wire format: each field is its own complete columnar block in field order.
/// Output RowBinary per row: T1_bytes + T2_bytes + ... (simple concatenation).
async fn read_tuple_column<R: ClickHouseRead>(
    reader: &mut R,
    n: usize,
    fields: &[ColumnType],
) -> Result<ColumnData> {
    let mut rows = vec![Vec::new(); n];
    for field_type in fields {
        let field_data = read_column(reader, field_type, n as u64).await?;
        for (row, cell) in rows.iter_mut().zip(field_data.into_iter()) {
            row.extend_from_slice(&cell);
        }
    }
    Ok(rows)
}

/// Map(K, V) column reader.
///
/// Native wire format:
/// ```text
/// n × u64   cumulative end-offsets
/// total_entries × K   key column
/// total_entries × V   value column
/// ```
/// Output RowBinary per row: varuint(count) + count × (K_bytes + V_bytes)
async fn read_map_column<R: ClickHouseRead>(
    reader: &mut R,
    n: usize,
    key_type: &ColumnType,
    val_type: &ColumnType,
) -> Result<ColumnData> {
    use tokio::io::AsyncReadExt as _;

    let mut offsets = Vec::with_capacity(n);
    for _ in 0..n {
        offsets.push(reader.read_u64_le().await?);
    }

    let total = offsets.last().copied().unwrap_or(0);
    let keys = read_column(reader, key_type, total).await?;
    let vals = read_column(reader, val_type, total).await?;

    let mut result = Vec::with_capacity(n);
    let mut prev = 0usize;
    for &end in &offsets {
        let end = end as usize;
        let count = end - prev;
        let mut row = Vec::new();
        write_var_uint(count as u64, &mut row);
        for i in prev..end {
            row.extend_from_slice(&keys[i]);
            row.extend_from_slice(&vals[i]);
        }
        result.push(row);
        prev = end;
    }
    Ok(result)
}

/// Variant(T1, T2, ...) column reader.
///
/// Wire format:
/// ```text
/// u64      version (= 0)
/// n × u8   discriminators  (255 = NULL, 0..k-1 = type index in definition order)
/// for each variant type Ti in order:
///   [rows where discriminator == i, in original row order]
/// ```
/// Output: per-row JSON string encoded as RowBinary String.
async fn read_variant_column<R: ClickHouseRead>(
    reader: &mut R,
    n: usize,
    variant_types: &[ColumnType],
) -> Result<ColumnData> {
    use tokio::io::AsyncReadExt as _;

    // Wire prefix: u64 version = 0
    let _version = reader.read_u64_le().await?;

    let mut discriminators = vec![0u8; n];
    reader.read_exact(&mut discriminators).await?;

    let k = variant_types.len();
    let mut type_counts = vec![0u64; k];
    for &d in &discriminators {
        if (d as usize) < k {
            type_counts[d as usize] += 1;
        }
    }

    let mut type_values: Vec<ColumnData> = Vec::with_capacity(k);
    for (i, col_type) in variant_types.iter().enumerate() {
        type_values.push(read_column(reader, col_type, type_counts[i]).await?);
    }

    let mut type_cursors = vec![0usize; k];
    let mut result = Vec::with_capacity(n);
    for &d in &discriminators {
        let json_bytes: Vec<u8> = if d == 255 || (d as usize) >= k {
            b"null".to_vec()
        } else {
            let idx = d as usize;
            let cursor = type_cursors[idx];
            type_cursors[idx] += 1;
            rowbinary_to_json(&type_values[idx][cursor], &variant_types[idx])
        };
        let mut row = Vec::with_capacity(json_bytes.len() + 9);
        write_var_uint(json_bytes.len() as u64, &mut row);
        row.extend_from_slice(&json_bytes);
        result.push(row);
    }
    Ok(result)
}

/// New JSON column (ClickHouse 24.x+) reader.
///
/// Dispatches based on the wire serialization version:
/// - `1`: each row is a plain JSON string (String column format)
/// - `2`: path-based object format with Dynamic v1/v2 sub-columns + shared data
/// - `3`: path-based object format with Dynamic v3 sub-columns (no shared data)
async fn read_json_column<R: ClickHouseRead>(reader: &mut R, n: usize) -> Result<ColumnData> {
    use tokio::io::AsyncReadExt as _;

    let version = reader.read_u64_le().await?;
    match version {
        1 => read_string_column(reader, n).await,
        2 => read_json_object_v2_column(reader, n).await,
        3 => read_json_object_v3_column(reader, n).await,
        _ => Err(Error::BadResponse(format!(
            "native protocol: unsupported JSON serialization version: {version}"
        ))),
    }
}

/// JSON v2 object column reader.
///
/// Wire format (after the u64 version=2 already consumed):
/// ```text
/// varuint   numDynamicPaths
/// String[]  pathNames (sorted alphabetically)
/// for each path:
///   u64     dynVersion (1 or 2)
///   [if dynVersion==1: varuint maxTypes]
///   varuint numTypes (server types, excluding SharedVariant)
///   String[] typeNames
///   u64     variantVersion (= 0)
/// for each path:
///   u8[n]   discriminators (index in sorted(typeNames+"SharedVariant"), 255=NULL)
///   for each type in sorted order: column data
/// n × u64   shared data (discard)
/// ```
async fn read_json_object_v2_column<R: ClickHouseRead>(
    reader: &mut R,
    n: usize,
) -> Result<ColumnData> {
    use tokio::io::AsyncReadExt as _;

    let num_paths = reader.read_var_uint().await? as usize;
    let mut path_names: Vec<String> = Vec::with_capacity(num_paths);
    for _ in 0..num_paths {
        path_names.push(reader.read_utf8_string().await?);
    }

    // Read Dynamic v1/v2 header for each path.
    // sorted_types[p] = Vec<(type_name, col_type)> in sorted order (SharedVariant included).
    let mut path_sorted_types: Vec<Vec<(String, ColumnType)>> = Vec::with_capacity(num_paths);
    for path_name in &path_names {
        let dyn_version = reader.read_u64_le().await?;
        if dyn_version == 1 {
            // v1 has an extra maxTypes field before numTypes
            let _max_types = reader.read_var_uint().await?;
        } else if dyn_version != 2 {
            return Err(Error::BadResponse(format!(
                "native protocol: unexpected Dynamic version {dyn_version} in JSON v2 path \"{path_name}\""
            )));
        }

        let num_types = reader.read_var_uint().await? as usize;
        let mut type_names: Vec<String> = Vec::with_capacity(num_types + 1);
        for _ in 0..num_types {
            type_names.push(reader.read_utf8_string().await?);
        }
        // SharedVariant is implicit — add and sort to get the discriminator indices.
        type_names.push("SharedVariant".to_string());
        type_names.sort();

        let _variant_version = reader.read_u64_le().await?;

        let types: Vec<(String, ColumnType)> = type_names
            .into_iter()
            .map(|name| {
                let ct = if name == "SharedVariant" {
                    ColumnType::String
                } else {
                    ColumnType::parse(&name).unwrap_or(ColumnType::String)
                };
                (name, ct)
            })
            .collect();

        path_sorted_types.push(types);
    }

    // Read data for each path: discriminators then per-type column values.
    let mut path_discriminators: Vec<Vec<u8>> = Vec::with_capacity(num_paths);
    let mut path_values: Vec<Vec<ColumnData>> = Vec::with_capacity(num_paths);

    for types in &path_sorted_types {
        let k = types.len();

        let mut discriminators = vec![0u8; n];
        reader.read_exact(&mut discriminators).await?;

        let mut type_counts = vec![0u64; k];
        for &d in &discriminators {
            if (d as usize) < k {
                type_counts[d as usize] += 1;
            }
        }

        let mut col_values: Vec<ColumnData> = Vec::with_capacity(k);
        for (i, (_, col_type)) in types.iter().enumerate() {
            col_values.push(read_column(reader, col_type, type_counts[i]).await?);
        }

        path_discriminators.push(discriminators);
        path_values.push(col_values);
    }

    // Discard shared data: n × u64 (one u64 per row, unused by us).
    for _ in 0..n {
        let _ = reader.read_u64_le().await?;
    }

    // Build per-row JSON objects by reassembling path values.
    let mut path_cursors: Vec<Vec<usize>> = path_sorted_types
        .iter()
        .map(|types| vec![0usize; types.len()])
        .collect();

    let mut result = Vec::with_capacity(n);
    for row_i in 0..n {
        let mut json = b"{".to_vec();
        let mut first = true;

        for (path_idx, path_name) in path_names.iter().enumerate() {
            let disc = path_discriminators[path_idx][row_i] as usize;
            let k = path_sorted_types[path_idx].len();

            if disc == 255 || disc >= k {
                // Absent / NULL — omit key from output.
                continue;
            }

            let cursor = path_cursors[path_idx][disc];
            path_cursors[path_idx][disc] += 1;

            let (type_name, col_type) = &path_sorted_types[path_idx][disc];
            if type_name == "SharedVariant" {
                // SharedVariant stores overflow values in an opaque binary format; skip.
                continue;
            }

            if !first {
                json.push(b',');
            }
            first = false;

            json.extend_from_slice(&json_quote_bytes(path_name.as_bytes()));
            json.push(b':');

            let cell = &path_values[path_idx][disc][cursor];
            json.extend_from_slice(&rowbinary_to_json(cell, col_type));
        }

        json.push(b'}');

        let mut row = Vec::with_capacity(json.len() + 9);
        write_var_uint(json.len() as u64, &mut row);
        row.extend_from_slice(&json);
        result.push(row);
    }

    Ok(result)
}

/// JSON v3 object column reader (new flat format, ClickHouse 25.6+).
///
/// Wire format (after the u64 version=3 already consumed):
/// ```text
/// varuint   numDynamicPaths
/// String[]  pathNames
/// for each path:
///   varuint numTypes
///   String[] typeNames
/// for each path:
///   discriminators (u8/u16/u32/u64 depending on numTypes+1)
///   for each type in order: column data
/// ```
/// No shared data section in v3.
async fn read_json_object_v3_column<R: ClickHouseRead>(
    reader: &mut R,
    n: usize,
) -> Result<ColumnData> {
    let num_paths = reader.read_var_uint().await? as usize;
    let mut path_names: Vec<String> = Vec::with_capacity(num_paths);
    for _ in 0..num_paths {
        path_names.push(reader.read_utf8_string().await?);
    }

    let mut path_col_types: Vec<Vec<ColumnType>> = Vec::with_capacity(num_paths);
    let mut path_total_types: Vec<usize> = Vec::with_capacity(num_paths);

    for _ in 0..num_paths {
        let num_types = reader.read_var_uint().await? as usize;
        let mut col_types: Vec<ColumnType> = Vec::with_capacity(num_types);
        for _ in 0..num_types {
            let name = reader.read_utf8_string().await?;
            col_types.push(ColumnType::parse(&name).unwrap_or(ColumnType::String));
        }
        path_total_types.push(num_types);
        path_col_types.push(col_types);
    }

    let mut path_discriminators: Vec<Vec<usize>> = Vec::with_capacity(num_paths);
    let mut path_values: Vec<Vec<ColumnData>> = Vec::with_capacity(num_paths);

    for (p, col_types) in path_col_types.iter().enumerate() {
        let total_types = path_total_types[p];
        // NULL discriminator = total_types; discriminator range = [0, total_types].
        let disc_size = if total_types <= 254 { 1usize }
                        else if total_types <= 65535 { 2 }
                        else if total_types <= u32::MAX as usize { 4 }
                        else { 8 };

        let mut discriminators: Vec<usize> = Vec::with_capacity(n);
        for _ in 0..n {
            discriminators.push(read_index(reader, disc_size).await? as usize);
        }

        let mut type_counts = vec![0u64; total_types];
        for &d in &discriminators {
            if d < total_types {
                type_counts[d] += 1;
            }
        }

        let mut col_values: Vec<ColumnData> = Vec::with_capacity(total_types);
        for (i, col_type) in col_types.iter().enumerate() {
            col_values.push(read_column(reader, col_type, type_counts[i]).await?);
        }

        path_discriminators.push(discriminators);
        path_values.push(col_values);
    }

    let mut path_cursors: Vec<Vec<usize>> = path_col_types
        .iter()
        .map(|types| vec![0usize; types.len()])
        .collect();

    let mut result = Vec::with_capacity(n);
    for row_i in 0..n {
        let mut json = b"{".to_vec();
        let mut first = true;

        for (path_idx, path_name) in path_names.iter().enumerate() {
            let disc = path_discriminators[path_idx][row_i];
            let total_types = path_total_types[path_idx];

            if disc == total_types || disc > total_types {
                // NULL — omit key.
                continue;
            }

            let cursor = path_cursors[path_idx][disc];
            path_cursors[path_idx][disc] += 1;

            if !first {
                json.push(b',');
            }
            first = false;

            json.extend_from_slice(&json_quote_bytes(path_name.as_bytes()));
            json.push(b':');

            let cell = &path_values[path_idx][disc][cursor];
            let col_type = &path_col_types[path_idx][disc];
            json.extend_from_slice(&rowbinary_to_json(cell, col_type));
        }

        json.push(b'}');

        let mut row = Vec::with_capacity(json.len() + 9);
        write_var_uint(json.len() as u64, &mut row);
        row.extend_from_slice(&json);
        result.push(row);
    }

    Ok(result)
}

/// Standalone Dynamic column (ClickHouse 24.x+) reader.
///
/// Dispatches based on the wire serialization version prefix:
/// - `1`: deprecated format (maxTypes + totalTypes + sorted types + SharedVariant + variantVersion)
/// - `2`: intermediate format (totalTypes + sorted types + SharedVariant + variantVersion)
/// - `3`: flat format (totalTypes + types, NULL = totalTypes, no SharedVariant)
async fn read_dynamic_column<R: ClickHouseRead>(reader: &mut R, n: usize) -> Result<ColumnData> {
    use tokio::io::AsyncReadExt as _;

    let version = reader.read_u64_le().await?;
    match version {
        1 => read_dynamic_v1v2_column(reader, n, true).await,
        2 => read_dynamic_v1v2_column(reader, n, false).await,
        3 => read_dynamic_v3_column(reader, n).await,
        _ => Err(Error::BadResponse(format!(
            "native protocol: unsupported Dynamic serialization version: {version}"
        ))),
    }
}

/// Dynamic v1/v2 column reader.
///
/// v1 has an extra `maxTypes` varuint before `totalTypes`; v2 does not.
/// Both add "SharedVariant" to the type list and sort alphabetically.
/// NULL discriminator = 255.
async fn read_dynamic_v1v2_column<R: ClickHouseRead>(
    reader: &mut R,
    n: usize,
    has_max_types: bool,
) -> Result<ColumnData> {
    use tokio::io::AsyncReadExt as _;

    if has_max_types {
        let _max_types = reader.read_var_uint().await?;
    }

    let total_types = reader.read_var_uint().await? as usize;
    let mut type_names: Vec<String> = Vec::with_capacity(total_types + 1);
    for _ in 0..total_types {
        type_names.push(reader.read_utf8_string().await?);
    }
    type_names.push("SharedVariant".to_string());
    type_names.sort();

    let _variant_version = reader.read_u64_le().await?;

    let col_types: Vec<ColumnType> = type_names
        .iter()
        .map(|name| {
            if name == "SharedVariant" {
                ColumnType::String
            } else {
                ColumnType::parse(name).unwrap_or(ColumnType::String)
            }
        })
        .collect();

    let k = col_types.len();
    let mut discriminators = vec![0u8; n];
    reader.read_exact(&mut discriminators).await?;

    let mut type_counts = vec![0u64; k];
    for &d in &discriminators {
        if (d as usize) < k {
            type_counts[d as usize] += 1;
        }
    }

    let mut type_values: Vec<ColumnData> = Vec::with_capacity(k);
    for (i, col_type) in col_types.iter().enumerate() {
        type_values.push(read_column(reader, col_type, type_counts[i]).await?);
    }

    let mut type_cursors = vec![0usize; k];
    let mut result = Vec::with_capacity(n);
    for &d in &discriminators {
        let json_bytes: Vec<u8> = if d == 255 || (d as usize) >= k {
            b"null".to_vec()
        } else {
            let idx = d as usize;
            let cursor = type_cursors[idx];
            type_cursors[idx] += 1;
            if type_names[idx] == "SharedVariant" {
                b"null".to_vec()
            } else {
                rowbinary_to_json(&type_values[idx][cursor], &col_types[idx])
            }
        };
        let mut row = Vec::with_capacity(json_bytes.len() + 9);
        write_var_uint(json_bytes.len() as u64, &mut row);
        row.extend_from_slice(&json_bytes);
        result.push(row);
    }
    Ok(result)
}

/// Dynamic v3 column reader (new flat format, ClickHouse 25.6+).
///
/// No SharedVariant; NULL discriminator = totalTypes.
/// Discriminator width scales with totalTypes: u8/u16/u32/u64.
async fn read_dynamic_v3_column<R: ClickHouseRead>(reader: &mut R, n: usize) -> Result<ColumnData> {
    let total_types = reader.read_var_uint().await? as usize;
    let mut type_names: Vec<String> = Vec::with_capacity(total_types);
    let mut col_types: Vec<ColumnType> = Vec::with_capacity(total_types);
    for _ in 0..total_types {
        let name = reader.read_utf8_string().await?;
        col_types.push(ColumnType::parse(&name).unwrap_or(ColumnType::String));
        type_names.push(name);
    }

    let disc_size = if total_types <= 254 { 1usize }
                    else if total_types <= 65535 { 2 }
                    else if total_types <= u32::MAX as usize { 4 }
                    else { 8 };
    let null_disc = total_types;

    let mut discriminators: Vec<usize> = Vec::with_capacity(n);
    for _ in 0..n {
        discriminators.push(read_index(reader, disc_size).await? as usize);
    }

    let mut type_counts = vec![0u64; total_types];
    for &d in &discriminators {
        if d < total_types {
            type_counts[d] += 1;
        }
    }

    let mut type_values: Vec<ColumnData> = Vec::with_capacity(total_types);
    for (i, col_type) in col_types.iter().enumerate() {
        type_values.push(read_column(reader, col_type, type_counts[i]).await?);
    }

    let mut type_cursors = vec![0usize; total_types];
    let mut result = Vec::with_capacity(n);
    for &d in &discriminators {
        let json_bytes: Vec<u8> = if d == null_disc || d > total_types {
            b"null".to_vec()
        } else {
            let cursor = type_cursors[d];
            type_cursors[d] += 1;
            rowbinary_to_json(&type_values[d][cursor], &col_types[d])
        };
        let mut row = Vec::with_capacity(json_bytes.len() + 9);
        write_var_uint(json_bytes.len() as u64, &mut row);
        row.extend_from_slice(&json_bytes);
        result.push(row);
    }
    Ok(result)
}

/// Convert a RowBinary-encoded value for `col_type` into JSON bytes.
///
/// Returns `b"null"` on any parse error rather than propagating — callers should
/// treat this as a best-effort JSON representation for use in Dynamic/Variant columns.
fn rowbinary_to_json(bytes: &[u8], col_type: &ColumnType) -> Vec<u8> {
    match rowbinary_to_json_inner(bytes, col_type) {
        Ok((json, _)) => json,
        Err(()) => b"null".to_vec(),
    }
}

/// Inner parser: returns `(json_bytes, bytes_consumed)` or `Err(())` on underflow.
#[allow(clippy::too_many_lines)]
fn rowbinary_to_json_inner(bytes: &[u8], col_type: &ColumnType) -> Result<(Vec<u8>, usize), ()> {
    macro_rules! fixed {
        ($n:expr, $t:ty, $fmt:expr) => {{
            if bytes.len() < $n { return Err(()); }
            let v = <$t>::from_le_bytes(bytes[..$n].try_into().unwrap());
            (format!($fmt, v).into_bytes(), $n)
        }};
    }

    Ok(match col_type {
        ColumnType::UInt8 => fixed!(1, u8, "{}"),
        ColumnType::UInt16 => fixed!(2, u16, "{}"),
        ColumnType::UInt32 | ColumnType::IPv4 | ColumnType::Time => fixed!(4, u32, "{}"),
        ColumnType::UInt64 => fixed!(8, u64, "{}"),
        ColumnType::Int8 => {
            if bytes.is_empty() { return Err(()); }
            ((bytes[0] as i8).to_string().into_bytes(), 1)
        }
        ColumnType::Int16 => fixed!(2, i16, "{}"),
        ColumnType::Int32 | ColumnType::Decimal32 => fixed!(4, i32, "{}"),
        ColumnType::Date32 => fixed!(4, i32, "{}"),
        ColumnType::Int64 | ColumnType::Time64 | ColumnType::Decimal64 => fixed!(8, i64, "{}"),
        ColumnType::Int128 | ColumnType::Decimal128 => {
            if bytes.len() < 16 { return Err(()); }
            let v = i128::from_le_bytes(bytes[..16].try_into().unwrap());
            (v.to_string().into_bytes(), 16)
        }
        ColumnType::UInt128 => {
            if bytes.len() < 16 { return Err(()); }
            let v = u128::from_le_bytes(bytes[..16].try_into().unwrap());
            (v.to_string().into_bytes(), 16)
        }
        ColumnType::Int256 | ColumnType::UInt256 | ColumnType::Decimal256 => {
            // 32-byte big integer — emit as hex string for safety
            if bytes.len() < 32 { return Err(()); }
            let hex: String = bytes[..32].iter().rev().map(|b| format!("{b:02x}")).collect();
            (format!("\"{hex}\"").into_bytes(), 32)
        }
        ColumnType::Float32 => {
            if bytes.len() < 4 { return Err(()); }
            let v = f32::from_le_bytes(bytes[..4].try_into().unwrap());
            (format_float_json(v as f64).into_bytes(), 4)
        }
        ColumnType::Float64 => {
            if bytes.len() < 8 { return Err(()); }
            let v = f64::from_le_bytes(bytes[..8].try_into().unwrap());
            (format_float_json(v).into_bytes(), 8)
        }
        ColumnType::BFloat16 => {
            // BFloat16 is u16 mantissa — convert via f32
            if bytes.len() < 2 { return Err(()); }
            let raw = u16::from_le_bytes([bytes[0], bytes[1]]);
            let v = f32::from_bits((raw as u32) << 16);
            (format_float_json(v as f64).into_bytes(), 2)
        }
        ColumnType::Date => {
            if bytes.len() < 2 { return Err(()); }
            let days = u16::from_le_bytes([bytes[0], bytes[1]]) as u32;
            (format!("\"{days}\"").into_bytes(), 2)
        }
        ColumnType::DateTime | ColumnType::DateTime64 => {
            let size = col_type.fixed_size().unwrap_or(4);
            if bytes.len() < size { return Err(()); }
            let v: u64 = match size {
                4 => u32::from_le_bytes(bytes[..4].try_into().unwrap()) as u64,
                8 => u64::from_le_bytes(bytes[..8].try_into().unwrap()),
                _ => return Err(()),
            };
            (format!("{v}").into_bytes(), size)
        }
        ColumnType::Uuid => {
            if bytes.len() < 16 { return Err(()); }
            // UUID is stored as two u64s in big-endian byte order within ClickHouse
            let hi = u64::from_be_bytes(bytes[..8].try_into().unwrap());
            let lo = u64::from_be_bytes(bytes[8..16].try_into().unwrap());
            let s = format!(
                "\"{:08x}-{:04x}-{:04x}-{:04x}-{:012x}\"",
                (hi >> 32) as u32,
                (hi >> 16) as u16,
                hi as u16,
                (lo >> 48) as u16,
                lo & 0x0000_ffff_ffff_ffff
            );
            (s.into_bytes(), 16)
        }
        ColumnType::IPv6 => {
            if bytes.len() < 16 { return Err(()); }
            let hex: String = bytes[..16].chunks(2).map(|c| format!("{:02x}{:02x}", c[0], c[1])).collect::<Vec<_>>().join(":");
            (format!("\"[{hex}]\"").into_bytes(), 16)
        }
        ColumnType::Point => {
            // 2 × f64 LE
            if bytes.len() < 16 { return Err(()); }
            let x = f64::from_le_bytes(bytes[..8].try_into().unwrap());
            let y = f64::from_le_bytes(bytes[8..16].try_into().unwrap());
            (format!("[{},{}]", format_float_json(x), format_float_json(y)).into_bytes(), 16)
        }
        ColumnType::Enum8 => {
            if bytes.is_empty() { return Err(()); }
            ((bytes[0] as i8).to_string().into_bytes(), 1)
        }
        ColumnType::Enum16 => fixed!(2, i16, "{}"),
        // String types: RowBinary format = varuint(len) + bytes
        ColumnType::String
        | ColumnType::FixedString(_)
        | ColumnType::Json => {
            let (len, hdr) = read_var_uint_from_slice(bytes).ok_or(())?;
            let len = len as usize;
            let end = hdr + len;
            if bytes.len() < end { return Err(()); }
            (json_quote_bytes(&bytes[hdr..end]), end)
        }
        ColumnType::Nullable(inner) => {
            if bytes.is_empty() { return Err(()); }
            if bytes[0] != 0 {
                (b"null".to_vec(), 1)
            } else {
                let (json, consumed) = rowbinary_to_json_inner(&bytes[1..], inner)?;
                (json, 1 + consumed)
            }
        }
        ColumnType::LowCardinality(inner) => {
            // After LowCardinality expansion, individual cells are the inner type's bytes
            rowbinary_to_json_inner(bytes, inner)?
        }
        ColumnType::SimpleAggregateFunction(inner) => {
            rowbinary_to_json_inner(bytes, inner)?
        }
        ColumnType::Array(inner) => {
            let (count, hdr) = read_var_uint_from_slice(bytes).ok_or(())?;
            let mut pos = hdr;
            let mut json = b"[".to_vec();
            for i in 0..count {
                if i > 0 { json.push(b','); }
                let (elem, consumed) = rowbinary_to_json_inner(&bytes[pos..], inner)?;
                json.extend_from_slice(&elem);
                pos += consumed;
            }
            json.push(b']');
            (json, pos)
        }
        ColumnType::Tuple(fields) => {
            let mut pos = 0;
            let mut json = b"[".to_vec();
            for (i, field_type) in fields.iter().enumerate() {
                if i > 0 { json.push(b','); }
                let (elem, consumed) = rowbinary_to_json_inner(&bytes[pos..], field_type)?;
                json.extend_from_slice(&elem);
                pos += consumed;
            }
            json.push(b']');
            (json, pos)
        }
        ColumnType::Map(key_type, val_type) => {
            let (count, hdr) = read_var_uint_from_slice(bytes).ok_or(())?;
            let mut pos = hdr;
            let mut json = b"{".to_vec();
            for i in 0..count {
                if i > 0 { json.push(b','); }
                let (k, kc) = rowbinary_to_json_inner(&bytes[pos..], key_type)?;
                pos += kc;
                json.extend_from_slice(&k);
                json.push(b':');
                let (v, vc) = rowbinary_to_json_inner(&bytes[pos..], val_type)?;
                pos += vc;
                json.extend_from_slice(&v);
            }
            json.push(b'}');
            (json, pos)
        }
        // Dynamic/Variant/NewJson cells are already JSON strings (varuint + bytes)
        ColumnType::Dynamic | ColumnType::NewJson | ColumnType::Variant(_) => {
            let (len, hdr) = read_var_uint_from_slice(bytes).ok_or(())?;
            let len = len as usize;
            let end = hdr + len;
            if bytes.len() < end { return Err(()); }
            (bytes[hdr..end].to_vec(), end)
        }
    })
}

/// Format a float for JSON: avoids NaN/Infinity (not valid JSON), uses finite repr.
fn format_float_json(v: f64) -> String {
    if v.is_nan() || v.is_infinite() {
        "null".to_string()
    } else {
        // Use Rust's default float formatting (no trailing zeros)
        format!("{v}")
    }
}

/// JSON-quote raw bytes as a UTF-8 string (or escaped if not valid UTF-8).
fn json_quote_bytes(bytes: &[u8]) -> Vec<u8> {
    let mut out = vec![b'"'];
    for &b in bytes {
        match b {
            b'"' => { out.push(b'\\'); out.push(b'"'); }
            b'\\' => { out.push(b'\\'); out.push(b'\\'); }
            b'\n' => { out.push(b'\\'); out.push(b'n'); }
            b'\r' => { out.push(b'\\'); out.push(b'r'); }
            b'\t' => { out.push(b'\\'); out.push(b't'); }
            0x00..=0x1f => {
                // Control character — escape as \uXXXX
                out.extend_from_slice(format!("\\u{b:04x}").as_bytes());
            }
            _ => out.push(b),
        }
    }
    out.push(b'"');
    out
}

/// Read a varuint (LEB128) from a byte slice. Returns `(value, bytes_consumed)`.
fn read_var_uint_from_slice(bytes: &[u8]) -> Option<(u64, usize)> {
    let mut value = 0u64;
    let mut shift = 0u32;
    for (i, &b) in bytes.iter().enumerate() {
        value |= ((b & 0x7f) as u64) << shift;
        if b & 0x80 == 0 {
            return Some((value, i + 1));
        }
        shift += 7;
        if shift >= 63 { return None; } // overflow guard
    }
    None // ran out of bytes
}

async fn read_index<R: ClickHouseRead>(reader: &mut R, bytes: usize) -> Result<u64> {
    Ok(match bytes {
        1 => u64::from(reader.read_u8().await?),
        2 => u64::from(reader.read_u16_le().await?),
        4 => u64::from(reader.read_u32_le().await?),
        8 => reader.read_u64_le().await?,
        _ => unreachable!(),
    })
}

/// Write a LEB128 varint (ClickHouse 63-bit variant) into a buffer.
pub(crate) fn write_var_uint(mut value: u64, buf: &mut Vec<u8>) {
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

/// Transpose columnar data into row-oriented RowBinary bytes.
///
/// `column_data` contains one `ColumnData` per column.
/// Returns one `Vec<u8>` per row, suitable for `rowbinary::deserialize_row()`.
pub(crate) fn transpose_to_rowbinary(
    column_data: Vec<ColumnData>,
    num_rows: u64,
) -> Vec<Vec<u8>> {
    let n = num_rows as usize;
    let mut rows = vec![Vec::new(); n];
    for col in column_data {
        for (row_idx, cell) in col.into_iter().enumerate().take(n) {
            rows[row_idx].extend_from_slice(&cell);
        }
    }
    rows
}
