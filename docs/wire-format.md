# Wire Format Reference

Internal documentation for the native TCP protocol wire encoding of complex
column types. This is intended for contributors and anyone debugging protocol
issues.

For the authoritative source, see the ClickHouse C++ code in
`src/DataTypes/Serializations/`.

## Native protocol overview

Data is exchanged in **blocks** — each block contains N rows across all columns.
Within a block, data is columnar: all N values for column 1, then all N values
for column 2, etc.

```text
Block header:
  varuint  block_info.field1 (0)
  u8       block_info.is_overflows (0)
  varuint  block_info.field2 (0)
  i32      block_info.bucket_num (-1)
  varuint  0 (end of block info)
  varuint  num_columns
  varuint  num_rows

Per column (repeated num_columns times):
  String   column_name
  String   column_type
  [u8]     custom_serialization flag (if revision >= 54454)
  [bytes]  column data (num_rows values)
```

### Custom serialization flag

ClickHouse servers with revision >= 54454 (`DBMS_MIN_PROTOCOL_VERSION_WITH_CUSTOM_SERIALIZATION`)
send a `u8` flag after each column's type string:
- `0x00` = normal serialization
- `0x01` = sparse serialization (offsets + values)

The INSERT encoder must also write this flag. Omitting it causes the server to
misinterpret the first data byte as the flag, leading to hangs or corrupt data.

## LowCardinality

Wire format (verified working for both SELECT and INSERT):

```text
u64     version = 1              ← serialization version prefix

Per-block:
  u64     flags
            bits 0-1: index type (0=U8, 1=U16, 2=U32, 3=U64)
            bit 8:    NEED_GLOBAL_DICTIONARY (0x100)
            bit 9:    HAS_ADDITIONAL_KEYS    (0x200)

  [if NEED_GLOBAL_DICTIONARY]
    u64   global_dict_size
    global_dict_size × T values

  [if HAS_ADDITIONAL_KEYS]
    u64   additional_keys_size
    additional_keys_size × T values

  [if neither flag]
    u64   dict_size
    dict_size × T values

  u64     num_indices (= num_rows)
  num_indices × index_type bytes
```

### Index space

Indices reference the combined dictionary:
- Indices `0..additional_keys_size` → additional keys
- Indices `additional_keys_size..` → global dictionary

In practice, ClickHouse almost always uses `HAS_ADDITIONAL_KEYS` without a
global dictionary, so the additional keys *are* the entire dictionary.

### LowCardinality(Nullable(T))

When the inner type is `Nullable(T)`:
- The **dictionary type is `T`** (not `Nullable(T)`) — no null flags in the dict
- Index 0 is a **null sentinel** — the value at dict position 0 is the default
  value of T (e.g. empty string), but any row with index 0 should be treated as
  NULL
- For SELECT: index 0 → emit RowBinary null (`0x01`); other indices → emit
  `0x00` (not-null) + T value bytes
- For INSERT: null inputs → index 0; `Some(v)` → extract T bytes (strip null
  flag) and dictionary-encode normally

### INSERT encoding

The INSERT encoder builds the dictionary by collecting unique values:

1. Collect all unique T-values (for Nullable: strip the `0x00`/`0x01` null flag)
2. Assign index 0 as the null sentinel (if Nullable)
3. Choose index type based on dictionary size (U8 if ≤256, U16 if ≤65536, etc.)
4. Write: version=1, flags with `HAS_ADDITIONAL_KEYS`, dict values, indices

## Array(T)

```text
u64[num_rows]   cumulative offsets (last offset = total elements)
T[total]        element values as a sub-column
```

The offsets are cumulative — the i-th array contains elements from
`offsets[i-1]` (or 0 for i=0) to `offsets[i]`.

Arrays of arrays (e.g. `Array(Array(String))`) nest recursively: the outer
offsets point into the inner offset array.

## Map(K, V)

Maps are encoded as arrays of key-value pairs:

```text
u64[num_rows]   cumulative offsets (same as Array)
K[total]        key sub-column
V[total]        value sub-column
```

## Tuple(T1, ..., Tn)

Each element is a separate sub-column in definition order:

```text
T1[num_rows]    first element values
T2[num_rows]    second element values
...
Tn[num_rows]    nth element values
```

## Nullable(T)

```text
u8[num_rows]    null flags (1 = null, 0 = not null)
T[num_rows]     values (null slots contain default/zero T values)
```

All N values are always present on the wire — null rows have zero-initialized
values that are ignored by the reader.

## Variant(T1, ..., Tn)

```text
u64             version = 0       ← different from LowCardinality!
u8[num_rows]    discriminators (255 = NULL)
T1[count1]      values for discriminator 0
T2[count2]      values for discriminator 1
...
Tn[countn]      values for discriminator n-1
```

Types are always in the order specified in the Variant definition (which
ClickHouse sorts alphabetically). The count for each type is derived by
counting its discriminator value in the discriminator array.

## Dynamic

```text
u64             version = 1
varuint         num_prefix_types (usually 0)
String[]        prefix type names (if num_prefix_types > 0)

Per-block:
  varuint       num_types
  String[]      type_names
  u8[num_rows]  discriminators (255 = NULL)
  for each type in order:
    T[count]    values for that discriminator
```

The new JSON type (ClickHouse 24.10+) uses this same wire format. Legacy
`Object('json')` is a plain String on the wire.

## FixedString(N)

- **Native wire**: N raw bytes, no length prefix
- **RowBinary**: `varuint(N)` + N bytes (length-prefixed)

The native transport reader emits FixedString as RowBinary (prepends varuint
length) for compatibility with the serde deserializer. The INSERT encoder
strips the varuint prefix before sending.

## String

```text
varuint(len)    length prefix
u8[len]         UTF-8 bytes
```

Identical encoding in both native wire format and RowBinary.

## Compression (LZ4)

When compression is enabled, data blocks are wrapped:

```text
u8      checksum[16]    (CityHash128 of the rest)
u8      method          (0x82 = LZ4)
u32     compressed_size (including this 9-byte header)
u32     uncompressed_size
u8[]    LZ4-compressed payload
```

**Important**: `Log` and `ProfileEvents` packets from the server are always
sent uncompressed, even when compression is negotiated. The reader detects
these packet types and reads them without decompression.
