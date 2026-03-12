# Type Coverage

This document lists all ClickHouse column types and their support status across
both the HTTP and native TCP transports.

## Scalars

| ClickHouse Type | Rust Type | Wire Size | HTTP | Native SELECT | Native INSERT |
|---|---|---|---|---|---|
| UInt8 | `u8` | 1 | Yes | Yes | Yes |
| UInt16 | `u16` | 2 | Yes | Yes | Yes |
| UInt32 | `u32` | 4 | Yes | Yes | Yes |
| UInt64 | `u64` | 8 | Yes | Yes | Yes |
| UInt128 | `u128` | 16 | Yes | Yes | Yes |
| UInt256 | `clickhouse::types::UInt256` (`[u8; 32]`) | 32 | Yes | Yes | Yes |
| Int8 | `i8` | 1 | Yes | Yes | Yes |
| Int16 | `i16` | 2 | Yes | Yes | Yes |
| Int32 | `i32` | 4 | Yes | Yes | Yes |
| Int64 | `i64` | 8 | Yes | Yes | Yes |
| Int128 | `i128` | 16 | Yes | Yes | Yes |
| Int256 | `clickhouse::types::Int256` (`[u8; 32]`) | 32 | Yes | Yes | Yes |
| Float32 | `f32` | 4 | Yes | Yes | Yes |
| Float64 | `f64` | 8 | Yes | Yes | Yes |
| BFloat16 | `u16` (raw bits) | 2 | No | Yes | Yes |
| Boolean | `bool` | 1 | Yes | Yes | Yes |
| Decimal32(S) | `i32` | 4 | Yes | Yes | Yes |
| Decimal64(S) | `i64` | 8 | Yes | Yes | Yes |
| Decimal128(S) | `i128` | 16 | Yes | Yes | Yes |
| Decimal256(S) | `[u8; 32]` | 32 | Yes | Yes | Yes |
| Date | `u16` (days since epoch) | 2 | Yes | Yes | Yes |
| Date32 | `i32` (days since epoch) | 4 | Yes | Yes | Yes |
| DateTime | `u32` (seconds since epoch) | 4 | Yes | Yes | Yes |
| DateTime64(P) | `i64` (scaled since epoch) | 8 | Yes | Yes | Yes |
| Time | `i32` (seconds since midnight) | 4 | No | Yes | Yes |
| Time64(P) | `i64` (scaled since midnight) | 8 | No | Yes | Yes |
| UUID | `uuid::Uuid` (with `serde::uuid`) | 16 | Yes | Yes | Yes |
| IPv4 | `Ipv4Addr` (with `serde::ipv4`) | 4 | Yes | Yes | Yes |
| IPv6 | `Ipv6Addr` | 16 | Yes | Yes | Yes |
| Enum8 | `#[repr(i8)]` enum | 1 | Yes | Yes | Yes |
| Enum16 | `#[repr(i16)]` enum | 2 | Yes | Yes | Yes |
| Point | `(f64, f64)` | 16 | Yes | Yes | Yes |

### Notes on scalars

- **Decimal**: scale is part of the type string but ignored on the wire. Map to
  the corresponding integer type or use [fixnum](https://docs.rs/fixnum).
- **DateTime/DateTime64**: timezone and precision are in the type string but do
  not affect wire encoding. Use `serde::time` or `serde::chrono` helpers for
  ergonomic date/time types.
- **BFloat16, Time, Time64**: only available on the native transport. These are
  newer ClickHouse types not yet supported by the HTTP RowBinary path.

## String types

| ClickHouse Type | Rust Type | HTTP | Native SELECT | Native INSERT |
|---|---|---|---|---|
| String | `String`, `&str`, `Vec<u8>`, `&[u8]` | Yes | Yes | Yes |
| FixedString(N) | `[u8; N]` | Yes | Yes | Yes |

### FixedString encoding

- **HTTP (RowBinary)**: `varuint(N)` + N bytes
- **Native wire**: N raw bytes (no length prefix)
- **Native INSERT**: strips the varuint prefix from RowBinary before sending

## Composite types

| ClickHouse Type | Rust Type | HTTP | Native SELECT | Native INSERT |
|---|---|---|---|---|
| Nullable(T) | `Option<T>` | Yes | Yes | Yes |
| LowCardinality(T) | same as T | Yes | Yes | Yes |
| Array(T) | `Vec<T>`, `&[T]` | Yes | Yes | Yes |
| Tuple(T1, ..., Tn) | `(T1, ..., Tn)` | Yes | Yes | Yes |
| Map(K, V) | `HashMap<K, V>`, `Vec<(K, V)>` | Yes | Yes | Yes |
| Nested(col1 T1, ...) | multiple `Vec<T>` with `#[serde(rename)]` | Yes | Yes | No |
| SimpleAggregateFunction(f, T) | same as T | Yes | Yes | No |

### LowCardinality

LowCardinality wraps String, FixedString, or Nullable variants of these. On
the wire it uses a dictionary encoding:

- **SELECT**: dictionary + index array decoded transparently
- **INSERT**: values are dictionary-encoded automatically, including
  `LowCardinality(Nullable(T))` where index 0 is the null sentinel

See [Wire Format](wire-format.md) for encoding details.

## Modern types (ClickHouse 24.x+)

| ClickHouse Type | Rust Type | HTTP | Native SELECT | Native INSERT |
|---|---|---|---|---|
| Variant(T1, ..., Tn) | `enum` or `String` (JSON) | Yes | Yes | No |
| Dynamic | `String` (JSON) | No | Yes | No |
| JSON (new, 24.10+) | `String` (JSON) | Yes | Yes | No |
| Object('json') (legacy) | `String` | Yes | Yes | No |

### Variant output

On the native transport, Variant cells are read from their per-discriminator
sub-columns and emitted as JSON-encoded strings. On HTTP, Variant maps to a
Rust enum with variants in alphabetical order matching the ClickHouse type
definition.

### Dynamic / JSON output

The new JSON type (ClickHouse 24.10+) uses the Dynamic wire format internally.
On the native transport, Dynamic/JSON cells are decoded from discriminator +
per-type sub-columns and emitted as JSON strings. Users map these to `String`
or deserialize with `serde_json::Value`.

## Geo types

| ClickHouse Type | Rust Type | HTTP | Native SELECT | Native INSERT |
|---|---|---|---|---|
| Point | `(f64, f64)` | Yes | Yes | Yes |
| Ring | `Vec<(f64, f64)>` | Yes | No | No |
| Polygon | `Vec<Vec<(f64, f64)>>` | Yes | No | No |
| MultiPolygon | `Vec<Vec<Vec<(f64, f64)>>>` | Yes | No | No |
| LineString | `Vec<(f64, f64)>` | Yes | No | No |
| MultiLineString | `Vec<Vec<(f64, f64)>>` | Yes | No | No |

Ring, Polygon, MultiPolygon, LineString, and MultiLineString are composed of
Arrays of Points. They work on HTTP via the standard Array machinery but are
not yet implemented as named types on the native transport.

## Not yet supported

| ClickHouse Type | Notes |
|---|---|
| AggregateFunction(...) | Opaque binary blob; complex intermediate state |
| Sparse serialization | Per-column `custom_ser = 1` flag; returns error on native transport |
