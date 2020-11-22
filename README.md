# clickhouse.rs

[![Build status](https://travis-ci.org/loyd/clickhouse.rs.svg)](https://travis-ci.org/loyd/clickhouse.rs)
[![Crate info](https://img.shields.io/crates/v/clickhouse.svg)](https://crates.io/crates/clickhouse)
[![Documentation](https://docs.rs/clickhouse/badge.svg)](https://docs.rs/clickhouse)

A typed client for ClickHouse.

## Features
* Uses `serde` for encoding/decoding rows.
* Uses `RowBinary` encoding.
* Provides API for selecting.
* Provides API for infinite transactional (see below) inserting.
* Provides API for watching live views.
* Decompression (lz4, gzip, zlib and brotli). lz4 by default.

## Examples
See [more examples](examples/usage.rs).

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
use clickhouse::Reflection;

#[derive(Reflection, Deserialize)]
struct Row<'a> {
    no: u32,
    name: &'a str,
}

let mut cursor = client
    .query("SELECT ?fields FROM some WHERE no BETWEEN ? AND ?")
    .bind(500)
    .bind(504)
    .fetch::<Row<'_>>()?;

while let Some(row) = cursor.next().await? { .. }
```

* Placeholder `?fields` is replaced with `no, name` (fields of `Row`).
* Placeholder `?` is replaced with values in following `bind()` calls.

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
