# clickhouse-rs

Official pure Rust typed client for ClickHouse DB.

[![Crates.io][crates-badge]][crates-url]
[![Documentation][docs-badge]][docs-url]
[![License][license-badge]][license-url]
[![Build Status][actions-badge]][actions-url]

[crates-badge]: https://img.shields.io/crates/v/clickhouse.svg
[crates-url]: https://crates.io/crates/clickhouse
[docs-badge]: https://docs.rs/clickhouse/badge.svg
[docs-url]: https://docs.rs/clickhouse
[license-badge]: https://img.shields.io/badge/license-MIT_OR_Apache--2.0-blue.svg
[license-url]: https://github.com/ClickHouse/clickhouse-rs/blob/main/LICENSE-MIT
[actions-badge]: https://github.com/ClickHouse/clickhouse-rs/actions/workflows/ci.yml/badge.svg
[actions-url]: https://github.com/ClickHouse/clickhouse-rs/actions/workflows/ci.yml

* Uses `serde` for encoding/decoding rows.
* Supports `serde` attributes: `skip_serializing`, `skip_deserializing`, `rename`.
* Uses `RowBinary` encoding over HTTP transport.
    * There are plans to switch to `Native` over TCP.
* Supports TLS (see `native-tls` and `rustls-tls` features below).
* Supports compression and decompression (LZ4 and LZ4HC).
* Provides API for selecting.
* Provides API for inserting.
* Provides API for infinite transactional (see below) inserting.
* Provides API for watching live views.
* Provides mocks for unit testing.

Note: [ch2rs](https://github.com/ClickHouse/ch2rs) is useful to generate a row type from ClickHouse.

## Usage

To use the crate, add this to your `Cargo.toml`:
```toml
[dependencies]
clickhouse = "0.13.1"

[dev-dependencies]
clickhouse = { version = "0.13.1", features = ["test-util"] }
```

<details>
<summary>

### Note about ClickHouse prior to v22.6

</summary>

CH server older than v22.6 (2022-06-16) handles `RowBinary` [incorrectly](https://github.com/ClickHouse/ClickHouse/issues/37420) in some rare cases. Use 0.11 and enable `wa-37420` feature to solve this problem. Don't use it for newer versions.

</details>
<details>
<summary>

### Create a client

</summary>

```rust,ignore
use clickhouse::Client;

let client = Client::default()
    .with_url("http://localhost:8123")
    .with_user("name")
    .with_password("123")
    .with_database("test");
```

* Reuse created clients or clone them in order to reuse a connection pool.

</details>
<details>
<summary>

### Select rows

</summary>

```rust,ignore
use serde::Deserialize;
use clickhouse::Row;

#[derive(Row, Deserialize)]
struct MyRow<'a> {
    no: u32,
    name: &'a str,
}

let mut cursor = client
    .query("SELECT ?fields FROM some WHERE no BETWEEN ? AND ?")
    .bind(500)
    .bind(504)
    .fetch::<MyRow<'_>>()?;

while let Some(row) = cursor.next().await? { .. }
```

* Placeholder `?fields` is replaced with `no, name` (fields of `Row`).
* Placeholder `?` is replaced with values in following `bind()` calls.
* Convenient `fetch_one::<Row>()` and `fetch_all::<Row>()` can be used to get a first row or all rows correspondingly.
* `sql::Identifier` can be used to bind table names.

Note that cursors can return an error even after producing some rows. To avoid this, use `client.with_option("wait_end_of_query", "1")` in order to enable buffering on the server-side. [More details](https://clickhouse.com/docs/en/interfaces/http/#response-buffering). The `buffer_size` option can be useful too.

</details>
<details>
<summary>

### Insert a batch

</summary>

```rust,ignore
use serde::Serialize;
use clickhouse::Row;

#[derive(Row, Serialize)]
struct MyRow {
    no: u32,
    name: String,
}

let mut insert = client.insert("some")?;
insert.write(&MyRow { no: 0, name: "foo".into() }).await?;
insert.write(&MyRow { no: 1, name: "bar".into() }).await?;
insert.end().await?;
```

* If `end()` isn't called, the `INSERT` is aborted.
* Rows are being sent progressively to spread network load.
* ClickHouse inserts batches atomically only if all rows fit in the same partition and their number is less [`max_insert_block_size`](https://clickhouse.tech/docs/en/operations/settings/settings/#settings-max_insert_block_size).

</details>
<details>
<summary>

### Infinite inserting

</summary>

Requires the `inserter` feature.

```rust,ignore
let mut inserter = client.inserter("some")?
    .with_timeouts(Some(Duration::from_secs(5)), Some(Duration::from_secs(20)))
    .with_max_bytes(50_000_000)
    .with_max_rows(750_000)
    .with_period(Some(Duration::from_secs(15)));

inserter.write(&MyRow { no: 0, name: "foo".into() })?;
inserter.write(&MyRow { no: 1, name: "bar".into() })?;
let stats = inserter.commit().await?;
if stats.rows > 0 {
    println!(
        "{} bytes, {} rows, {} transactions have been inserted",
        stats.bytes, stats.rows, stats.transactions,
    );
}
```

* `Inserter` ends an active insert in `commit()` if thresholds (`max_bytes`, `max_rows`, `period`) are reached.
* The interval between ending active `INSERT`s can be biased by using `with_period_bias` to avoid load spikes by parallel inserters.
* `Inserter::time_left()` can be used to detect when the current period ends. Call `Inserter::commit()` again to check limits if your stream emits items rarely.
* Time thresholds implemented by using [quanta](https://docs.rs/quanta) crate to speed the inserter up. Not used if `test-util` is enabled (thus, time can be managed by `tokio::time::advance()` in custom tests).
* All rows between `commit()` calls are inserted in the same `INSERT` statement.
* Do not forget to flush if you want to terminate inserting:
```rust,ignore
inserter.end().await?;
```

</details>
<details>
<summary>

### Perform DDL

</summary>

```rust,ignore
client.query("DROP TABLE IF EXISTS some").execute().await?;
```

</details>
<details>
<summary>

### Live views

</summary>

Requires the `watch` feature.

```rust,ignore
let mut cursor = client
    .watch("SELECT max(no), argMax(name, no) FROM some")
    .fetch::<Row<'_>>()?;

let (version, row) = cursor.next().await?.unwrap();
println!("live view updated: version={}, row={:?}", version, row);

// Use `only_events()` to iterate over versions only.
let mut cursor = client.watch("some_live_view").limit(20).only_events().fetch()?;
println!("live view updated: version={:?}", cursor.next().await?);
```

* Use [carefully](https://github.com/ClickHouse/ClickHouse/issues/28309#issuecomment-908666042).
* This code uses or creates if not exists a temporary live view named `lv_{sha1(query)}` to reuse the same live view by parallel watchers.
* You can specify a name instead of a query.
* This API uses `JSONEachRowWithProgress` under the hood because of [the issue](https://github.com/ClickHouse/ClickHouse/issues/22996).
* Only struct rows can be used. Avoid `fetch::<u64>()` and other without specified names.

</details>

See [examples](https://github.com/ClickHouse/clickhouse-rs/tree/main/examples).

## Feature Flags
* `lz4` (enabled by default) — enables `Compression::Lz4`. If enabled, `Compression::Lz4` is used by default for all queries except for `WATCH`.
* `inserter` — enables `client.inserter()`.
* `test-util` — adds mocks. See [the example](https://github.com/ClickHouse/clickhouse-rs/tree/main/examples/mock.rs). Use it only in `dev-dependencies`.
* `watch` — enables `client.watch` functionality. See the corresponding section for details.
* `uuid` — adds `serde::uuid` to work with [uuid](https://docs.rs/uuid) crate.
* `time` — adds `serde::time` to work with [time](https://docs.rs/time) crate.

### TLS
By default, TLS is disabled and one or more following features must be enabled to use HTTPS urls:
* `native-tls` — uses [native-tls], utilizing dynamic linking (e.g. against OpenSSL).
* `rustls-tls` — enables `rustls-tls-aws-lc` and `rustls-tls-webpki-roots` features.
* `rustls-tls-aws-lc` — uses [rustls] with the `aws-lc` cryptography implementation.
* `rustls-tls-ring` — uses [rustls] with the `ring` cryptography implementation.
* `rustls-tls-webpki-roots` — uses [rustls] with certificates provided by the [webpki-roots] crate.
* `rustls-tls-native-roots` — uses [rustls] with certificates provided by the [rustls-native-certs] crate.

If multiple features are enabled, the following priority is applied:
* `native-tls` > `rustls-tls-aws-lc` > `rustls-tls-ring`
* `rustls-tls-native-roots` > `rustls-tls-webpki-roots`

How to choose between all these features? Here are some considerations:
* A good starting point is `rustls-tls`, e.g. if you use ClickHouse Cloud.
* To be more environment-agnostic, prefer `rustls-tls` over `native-tls`.
* Enable `rustls-tls-native-roots` or `native-tls` if you want to use self-signed certificates.

[native-tls]: https://docs.rs/native-tls
[rustls]: https://docs.rs/rustls
[webpki-roots]: https://docs.rs/webpki-roots
[rustls-native-certs]: https://docs.rs/rustls-native-certs

## Data Types
* `(U)Int(8|16|32|64|128)` maps to/from corresponding `(u|i)(8|16|32|64|128)` types or newtypes around them.
* `(U)Int256` aren't supported directly, but there is [a workaround for it](https://github.com/ClickHouse/clickhouse-rs/issues/48).
* `Float(32|64)` maps to/from corresponding `f(32|64)` or newtypes around them.
* `Decimal(32|64|128)` maps to/from corresponding `i(32|64|128)` or newtypes around them. It's more convenient to use [fixnum](https://github.com/loyd/fixnum) or another implementation of signed fixed-point numbers.
* `Boolean` maps to/from `bool` or newtypes around it.
* `String` maps to/from any string or bytes types, e.g. `&str`, `&[u8]`, `String`, `Vec<u8>` or [`SmartString`](https://docs.rs/smartstring/latest/smartstring/struct.SmartString.html). Newtypes are also supported. To store bytes, consider using [serde_bytes](https://docs.rs/serde_bytes/latest/serde_bytes/), because it's more efficient.
    <details>
    <summary>Example</summary>

    ```rust,ignore
    #[derive(Row, Debug, Serialize, Deserialize)]
    struct MyRow<'a> {
        str: &'a str,
        string: String,
        #[serde(with = "serde_bytes")]
        bytes: Vec<u8>,
        #[serde(with = "serde_bytes")]
        byte_slice: &'a [u8],
    }
    ```
    </details>
* `FixedString(N)` is supported as an array of bytes, e.g. `[u8; N]`.
    <details>
    <summary>Example</summary>
  
    ```rust,ignore
    #[derive(Row, Debug, Serialize, Deserialize)]
    struct MyRow {
        fixed_str: [u8; 16], // FixedString(16)
    }
    ```
    </details>
* `Enum(8|16)` are supported using [serde_repr](https://docs.rs/serde_repr/latest/serde_repr/).
    <details>
    <summary>Example</summary>

    ```rust,ignore
    use serde_repr::{Deserialize_repr, Serialize_repr};

    #[derive(Row, Serialize, Deserialize)]
    struct MyRow {
        level: Level,
    }

    #[derive(Debug, Serialize_repr, Deserialize_repr)]
    #[repr(u8)]
    enum Level {
        Debug = 1,
        Info = 2,
        Warn = 3,
        Error = 4,
    }
    ```
    </details>
* `UUID` maps to/from [`uuid::Uuid`](https://docs.rs/uuid/latest/uuid/struct.Uuid.html) by using `serde::uuid`. Requires the `uuid` feature.
    <details>
    <summary>Example</summary>

    ```rust,ignore
    #[derive(Row, Serialize, Deserialize)]
    struct MyRow {
        #[serde(with = "clickhouse::serde::uuid")]
        uuid: uuid::Uuid,
    }
    ```
    </details>
* `IPv6` maps to/from [`std::net::Ipv6Addr`](https://doc.rust-lang.org/stable/std/net/struct.Ipv6Addr.html).
* `IPv4` maps to/from [`std::net::Ipv4Addr`](https://doc.rust-lang.org/stable/std/net/struct.Ipv4Addr.html) by using `serde::ipv4`.
    <details>
    <summary>Example</summary>

    ```rust,ignore
    #[derive(Row, Serialize, Deserialize)]
    struct MyRow {
        #[serde(with = "clickhouse::serde::ipv4")]
        ipv4: std::net::Ipv4Addr,
    }
    ```
    </details>
* `Date` maps to/from `u16` or a newtype around it and represents a number of days elapsed since `1970-01-01`. Also, [`time::Date`](https://docs.rs/time/latest/time/struct.Date.html) is supported by using `serde::time::date`, that requires the `time` feature.
    <details>
    <summary>Example</summary>

    ```rust,ignore
    #[derive(Row, Serialize, Deserialize)]
    struct MyRow {
        days: u16,
        #[serde(with = "clickhouse::serde::time::date")]
        date: Date,
    }
    ```
    </details>
* `Date32` maps to/from `i32` or a newtype around it and represents a number of days elapsed since `1970-01-01`. Also, [`time::Date`](https://docs.rs/time/latest/time/struct.Date.html) is supported by using `serde::time::date32`, that requires the `time` feature.
    <details>
    <summary>Example</summary>

    ```rust,ignore
    #[derive(Row, Serialize, Deserialize)]
    struct MyRow {
        days: i32,
        #[serde(with = "clickhouse::serde::time::date32")]
        date: Date,
    }
    ```
    </details>
* `DateTime` maps to/from `u32` or a newtype around it and represents a number of seconds elapsed since UNIX epoch. Also, [`time::OffsetDateTime`](https://docs.rs/time/latest/time/struct.OffsetDateTime.html) is supported by using `serde::time::datetime`, that requires the `time` feature.
    <details>
    <summary>Example</summary>

    ```rust,ignore
    #[derive(Row, Serialize, Deserialize)]
    struct MyRow {
        ts: u32,
        #[serde(with = "clickhouse::serde::time::datetime")]
        dt: OffsetDateTime,
    }
    ```
    </details>
* `DateTime64(_)` maps to/from `i32` or a newtype around it and represents a time elapsed since UNIX epoch. Also, [`time::OffsetDateTime`](https://docs.rs/time/latest/time/struct.OffsetDateTime.html) is supported by using `serde::time::datetime64::*`, that requires the `time` feature.
    <details>
    <summary>Example</summary>

    ```rust,ignore
    #[derive(Row, Serialize, Deserialize)]
    struct MyRow {
        ts: i64, // elapsed s/us/ms/ns depending on `DateTime64(X)`
        #[serde(with = "clickhouse::serde::time::datetime64::secs")]
        dt64s: OffsetDateTime,  // `DateTime64(0)`
        #[serde(with = "clickhouse::serde::time::datetime64::millis")]
        dt64ms: OffsetDateTime, // `DateTime64(3)`
        #[serde(with = "clickhouse::serde::time::datetime64::micros")]
        dt64us: OffsetDateTime, // `DateTime64(6)`
        #[serde(with = "clickhouse::serde::time::datetime64::nanos")]
        dt64ns: OffsetDateTime, // `DateTime64(9)`
    }
    ```
    </details>
* `Tuple(A, B, ...)` maps to/from `(A, B, ...)` or a newtype around it.
* `Array(_)` maps to/from any slice, e.g. `Vec<_>`, `&[_]`. Newtypes are also supported.
* `Map(K, V)` behaves like `Array((K, V))`.
* `LowCardinality(_)` is supported seamlessly.
* `Nullable(_)` maps to/from `Option<_>`. For `clickhouse::serde::*` helpers add `::option`.
    <details>
    <summary>Example</summary>

    ```rust,ignore
    #[derive(Row, Serialize, Deserialize)]
    struct MyRow {
        #[serde(with = "clickhouse::serde::ipv4::option")]
        ipv4_opt: Option<Ipv4Addr>,
    }
    ```
    </details>
* `Nested` is supported by providing multiple arrays with renaming.
    <details>
    <summary>Example</summary>

    ```rust,ignore
    // CREATE TABLE test(items Nested(name String, count UInt32))
    #[derive(Row, Serialize, Deserialize)]
    struct MyRow {
        #[serde(rename = "items.name")]
        items_name: Vec<String>,
        #[serde(rename = "items.count")]
        items_count: Vec<u32>,
    }
    ```
    </details>
* `Geo` types are supported. `Point` behaves like a tuple `(f64, f64)`, and the rest of the types are just slices of points. 
    <details>
    <summary>Example</summary>

    ```rust,ignore
    type Point = (f64, f64);
    type Ring = Vec<Point>;
    type Polygon = Vec<Ring>;
    type MultiPolygon = Vec<Polygon>;
    type LineString = Vec<Point>;
    type MultiLineString = Vec<LineString>;
  
    #[derive(Row, Serialize, Deserialize)]
    struct MyRow {
        point: Point,
        ring: Ring,
        polygon: Polygon,
        multi_polygon: MultiPolygon,
        line_string: LineString,
        multi_line_string: MultiLineString,
    }
    ```
    </details>
* `Variant` data type is supported as a Rust enum. As the inner Variant types are _always_ sorted alphabetically, Rust enum variants should be defined in the _exactly_ same order as it is in the data type; their names are irrelevant, only the order of the types matters. This following example has a column defined as `Variant(Array(UInt16), Bool, Date, String, UInt32)`:
    <details>
    <summary>Example</summary>
    
    ```rust,ignore
    #[derive(Serialize, Deserialize)]
    enum MyRowVariant {
        Array(Vec<i16>),
        Boolean(bool),
        #[serde(with = "clickhouse::serde::time::date")]
        Date(time::Date),
        String(String),
        UInt32(u32),
    }
    
    #[derive(Row, Serialize, Deserialize)]
    struct MyRow {
        id: u64,
        var: MyRowVariant,
    }
    ```
    </details>
* `Dynamic` data type is not supported for now.

See also the additional examples:

* [Simpler ClickHouse data types](examples/data_types_derive_simple.rs)
* [Container-like ClickHouse data types](examples/data_types_derive_containers.rs)
* [Variant data type](examples/data_types_variant.rs)

## Mocking
The crate provides utils for mocking CH server and testing DDL, `SELECT`, `INSERT` and `WATCH` queries.

The functionality can be enabled with the `test-util` feature. Use it **only** in dev-dependencies.

See [the example](https://github.com/ClickHouse/clickhouse-rs/tree/main/examples/mock.rs).
