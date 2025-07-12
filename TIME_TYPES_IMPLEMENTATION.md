# Time and Time64 Types Support in clickhouse-rs

This document describes the implementation of Time and Time64 types support in clickhouse-rs, based on the ClickHouse core implementation and following the patterns established in clickhouse-go.

## Overview

Time and Time64 are ClickHouse data types for representing time values without date components. They were introduced in ClickHouse PR #81217 and are now supported in clickhouse-rs.

## Data Types

### Time
- **ClickHouse Type**: `Time` or `Time('timezone')`
- **Internal Storage**: `Int32` (seconds since midnight)
- **Range**: 00:00:00 to 23:59:59
- **Timezone Support**: Optional timezone parameter

### Time64
- **ClickHouse Type**: `Time64(precision)` or `Time64(precision, 'timezone')`
- **Internal Storage**: `Int64` with configurable precision
- **Precision Options**: 0-9 (seconds to nanoseconds)
- **Range**: 00:00:00 to 23:59:59 with sub-second precision
- **Timezone Support**: Optional timezone parameter

## Implementation Details

### Data Type Parsing

The implementation adds support for parsing Time and Time64 types in `types/src/data_types.rs`:

```rust
pub enum DataTypeNode {
    // ... existing types ...
    
    /// Optional timezone
    Time(Option<String>),
    /// Precision and optional timezone
    Time64(DateTimePrecision, Option<String>),
}
```

### Parsing Functions

Two new parsing functions were added:

- `parse_time()`: Handles `Time` and `Time('timezone')` formats
- `parse_time64()`: Handles `Time64(precision)` and `Time64(precision, 'timezone')` formats

### Serialization Support

The implementation provides serialization support for both the `time` and `chrono` crates:

#### Time Crate Support

```rust
use clickhouse_rs::serde::time::time;
use time::Time;

#[derive(Serialize, Deserialize)]
struct Example {
    #[serde(with = "clickhouse_rs::serde::time::time")]
    time_field: Time,
    
    #[serde(with = "clickhouse_rs::serde::time::time::option")]
    time_optional: Option<Time>,
}
```

#### Chrono Crate Support

```rust
use clickhouse_rs::serde::chrono::time;
use chrono::NaiveTime;

#[derive(Serialize, Deserialize)]
struct Example {
    #[serde(with = "clickhouse_rs::serde::chrono::time")]
    time_field: NaiveTime,
    
    #[serde(with = "clickhouse_rs::serde::chrono::time::option")]
    time_optional: Option<NaiveTime>,
}
```

### Time64 Precision Support

For Time64 types, different precision levels are supported:

```rust
// Seconds precision
#[serde(with = "clickhouse_rs::serde::time::time64::secs")]
time64_seconds: Time,

// Milliseconds precision
#[serde(with = "clickhouse_rs::serde::time::time64::millis")]
time64_millis: Time,

// Microseconds precision
#[serde(with = "clickhouse_rs::serde::time::time64::micros")]
time64_micros: Time,

// Nanoseconds precision
#[serde(with = "clickhouse_rs::serde::time::time64::nanos")]
time64_nanos: Time,
```

## Usage Examples

### Creating Tables

```sql
CREATE TABLE time_example (
    time_field Time,
    time_optional Nullable(Time),
    time64_seconds Time64(0),
    time64_millis Time64(3),
    time64_micros Time64(6),
    time64_nanos Time64(9)
) ENGINE = MergeTree()
ORDER BY time_field
```

### Rust Code Example

```rust
use clickhouse_rs::{Client, Pool};
use serde::{Deserialize, Serialize};
use time::Time;

#[derive(Debug, Serialize, Deserialize)]
struct TimeExample {
    #[serde(with = "clickhouse_rs::serde::time::time")]
    time_field: Time,
    
    #[serde(with = "clickhouse_rs::serde::time::time64::millis")]
    time64_millis: Time,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let pool = Pool::new("tcp://localhost:9000")?;
    let client = pool.get_handle()?;
    
    // Insert data
    let time_example = TimeExample {
        time_field: Time::from_hms(12, 34, 56).unwrap(),
        time64_millis: Time::from_hms_milli(4, 5, 6, 123).unwrap(),
    };
    
    client.insert("time_example", vec![time_example]).await?;
    
    // Query data
    let rows = client.query("SELECT * FROM time_example").await?;
    for row in rows {
        let time_example: TimeExample = row.try_into()?;
        println!("{:?}", time_example);
    }
    
    Ok(())
}
```

## Testing

The implementation includes comprehensive tests in `types/src/data_types.rs`:

- `test_data_type_new_time()`: Tests Time type parsing
- `test_data_type_new_time64()`: Tests Time64 type parsing with various precisions

## Comparison with clickhouse-go

While clickhouse-go doesn't yet have explicit Time/Time64 support, the clickhouse-rs implementation follows the same patterns used for DateTime and DateTime64 types, ensuring consistency across the ecosystem.

## Future Enhancements

1. **clickhouse-go Integration**: The clickhouse-go team may want to add similar Time/Time64 support following the patterns established in clickhouse-rs.

2. **Additional Features**: 
   - Timezone-aware operations
   - Time arithmetic operations
   - Integration with more time libraries

## References

- ClickHouse PR #81217: Time and Time64 implementation
- ClickHouse Documentation: [Time](https://clickhouse.com/docs/en/sql-reference/data-types/time) and [Time64](https://clickhouse.com/docs/en/sql-reference/data-types/time64) types
- clickhouse-rs: [GitHub Repository](https://github.com/ClickHouse/clickhouse-rs) 