# clickhouse.rs

A typed client for ClickHouse.

[![Crates.io][crates-badge]][crates-url]
[![Documentation][docs-badge]][docs-url]
[![MIT licensed][mit-badge]][mit-url]
[![Build Status][actions-badge]][actions-url]

[crates-badge]: https://img.shields.io/crates/v/clickhouse.svg
[crates-url]: https://crates.io/crates/clickhouse
[docs-badge]: https://docs.rs/clickhouse/badge.svg
[docs-url]: https://docs.rs/clickhouse
[mit-badge]: https://img.shields.io/badge/license-MIT-blue.svg
[mit-url]: https://github.com/loyd/clickhouse.rs/blob/master/LICENSE
[actions-badge]: https://github.com/loyd/clickhouse.rs/workflows/CI/badge.svg
[actions-url]: https://github.com/loyd/clickhouse.rs/actions?query=workflow%3ACI+branch%3Amaster

## Features
* Uses `serde` for encoding/decoding rows.
* Uses `RowBinary` encoding.
* Provides API for selecting.
* Provides API for infinite transactional (see below) inserting.
* Provides API for watching live views.
* Decompression (lz4, gzip, zlib and brotli). lz4 by default.

## Usage
To use the crate, add this to your `Cargo.toml`:
```toml
[dependencies]
clickhouse = "0.8"

[dev-dependencies]
clickhouse = { version = "0.8", features = ["test-util"] }
```

## Examples
See [more examples](examples/).

### Create `Client`
```rust
use clickhouse::Client;

let client = Client::default()
    .with_url("http://localhost:8123")
    .with_user("name")
    .with_password("123")
    .with_database("test");
```

* Reuse created clients or clone them in order to reuse a connection pool.

### Select rows
```rust
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
* [soa-derive](https://github.com/lumol-org/soa-derive) is useful for `Nested` types.

### Insert a batch
```rust
let mut insert = client.insert("some")?;
insert.write(&Row { no: 0, name: "foo" }).await?;
insert.write(&Row { no: 1, name: "bar" }).await?;
insert.end().await?;
```

* If `end()` isn't called the insertion will be aborted.
* Rows are being sent progressively to spread network load.
* ClickHouse inserts batches atomically only if all rows fit in the same partition and their number is less [`max_insert_block_size`](https://clickhouse.tech/docs/en/operations/settings/settings/#settings-max_insert_block_size).
* [ch2rs](https://github.com/loyd/ch2rs) is useful to generate a row type from ClickHouse.

### Infinite inserting
```rust
let mut inserter = client.inserter("some")?
    .with_max_entries(150_000) // `250_000` by default
    .with_max_duration(Duration::from_secs(15)); // `10s` by default

inserter.write(&Row { no: 0, name: "foo" }).await?;
inserter.write(&Row { no: 1, name: "bar" }).await?;
let stats = inserter.commit().await?;
if stats.entries > 0 {
    println!("{} entries ({} transactions) have been inserted",
        stats.entries, stats.transactions);
}
```

* `Inserter` ends an active insert in `commit()` if thresholds (`max_entries`, `max_duration`) are reached.
* The interval between ending active inserts is biased (±10% of `max_duration`) to avoid load spikes by parallel inserters.
* All rows between `commit()` calls are inserted in the same `INSERT` statement.
* Do not forget to flush if you want yo terminate inserting:
```rust
inserter.end().await?;
```

### Perform DDL
```rust
client.query("DROP TABLE IF EXISTS some").execute().await?;
```

### Live views
```rust
let mut cursor = client
    .watch("SELECT max(no), argMax(name, no) FROM some")
    .fetch::<Row<'_>>()?;

let (version, row) = cursor.next().await?.unwrap();
println!("live view updated: version={}, row={:?}", version, row);

// Use `only_events()` to iterate over versions only.
let mut cursor = client.watch("some_live_view").limit(20).only_events().fetch()?;
println!("live view updated: version={:?}", cursor.next().await?);
```

* This code uses or creates if not exists a temporary live view named `lv_{sha1(query)}` to reuse the same live view by parallel watchers.
* You can specify a name instead of a query.

### Mocking
The crate provides utils for mocking CH server, both for SELECTs and INSERTs.

The functionality can be enabled with the `test-util` feature. Use it only in dev-dependencies.

See [the example](examples/mock.rs).
