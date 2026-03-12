# Migration Guide

## HTTP vs Native: choosing a transport

Both transports use the same `Row` derive macro and serde machinery. The
primary differences are in connection model and feature coverage.

### Feature comparison

| Feature | HTTP (`Client`) | Native (`NativeClient`) |
|---|---|---|
| Transport | HTTP/1.1 (port 8123) | TCP (port 9000) |
| Compression | LZ4 stream-level | LZ4 block-level |
| Connection pooling | HTTP keep-alive (hyper) | Deadpool bounded pool |
| Load balancer support | Yes (stateless) | No (stateful TCP) |
| TLS | `native-tls` / `rustls-tls` features | Not yet implemented |
| SELECT | `fetch`, `fetch_one`, `fetch_all` | `fetch`, `fetch_one`, `fetch_optional`, `fetch_all` |
| Single INSERT | `Insert<T>` | `NativeInsert<T>` |
| Multi-batch INSERT | `Inserter<T>` | `NativeInserter<T>` |
| Concurrent INSERT | `AsyncInserter<T>` | `AsyncNativeInserter<T>` |
| Batch wrapper | `TableBatcher<T>` | (use `AsyncNativeInserter` directly) |
| Query bind parameters | `?` placeholders, `?fields` | Not yet implemented |
| Mocking (`test-util`) | Yes | Not yet implemented |
| Validation | `RowBinaryWithNamesAndTypes` | Schema cache (TTL-based) |
| `serde::uuid`, `serde::ipv4`, etc. | Yes | Yes (via RowBinary bridge) |

### When to use HTTP

- Behind a load balancer or HTTP proxy
- Need TLS (not yet available on native)
- Need query bind parameters (`?` placeholders)
- Need mock testing (`test-util` feature)
- General-purpose use

### When to use Native

- Direct connection to ClickHouse (co-located, same network)
- High-throughput INSERT pipelines (less overhead per block)
- Need types only available on native (BFloat16, Time, Time64)
- Need connection pooling with health checks
- Want the same protocol as `clickhouse-client` and the Go client

## Switching from HTTP to Native

### Client creation

```rust
// HTTP
use clickhouse::Client;
let client = Client::default()
    .with_url("http://localhost:8123")
    .with_database("default");

// Native
use clickhouse::native::NativeClient;
let client = NativeClient::default()
    .with_addr("localhost:9000")
    .with_database("default");
```

### Row types — no changes needed

```rust
use clickhouse::Row;
use serde::{Serialize, Deserialize};

#[derive(Row, Serialize, Deserialize)]
struct MyRow {
    id: u64,
    name: String,
}
```

The same `#[derive(Row)]` struct works with both transports.

### SELECT

```rust
// HTTP
let mut cursor = client.query("SELECT ?fields FROM t")
    .fetch::<MyRow>()?;

// Native — no ?fields support yet, list columns explicitly
let mut cursor = client.query("SELECT id, name FROM t")
    .fetch::<MyRow>()?;

// Both use the same cursor API
while let Some(row) = cursor.next().await? {
    // ...
}
```

### INSERT

```rust
// HTTP
let mut insert = client.insert::<MyRow>("t").await?;
insert.write(&row).await?;
insert.end().await?;

// Native — note: no .await on insert creation
let mut insert = client.insert::<MyRow>("t");
insert.write(&row).await?;
insert.end().await?;
```

The key difference: `client.insert()` is `async` on HTTP (opens connection
immediately) but synchronous on native (connection is lazy, opened on first
`write`).

### DDL

```rust
// HTTP
client.query("CREATE TABLE ...").execute().await?;

// Native — identical
client.query("CREATE TABLE ...").execute().await?;
```

## Differences from upstream clickhouse-rs

This fork (HyperI) adds the following on top of upstream v0.14.2:

| Feature | Upstream | HyperI Fork |
|---|---|---|
| Native TCP transport | Planned | Implemented (`feature = "native-transport"`) |
| Connection pooling | N/A (HTTP) | Deadpool-based for native |
| LowCardinality INSERT | N/A | Full dictionary encoding |
| AsyncInserter | N/A | MPSC-based concurrent inserter |
| TableBatcher | N/A | Go-style batch wrapper |
| BFloat16, Time, Time64 | No | Yes (native only) |
| Variant/Dynamic/JSON (native) | No | Yes (SELECT, emitted as JSON strings) |
| Schema cache | N/A | TTL-based, per-client |
| LZ4 for INSERT blocks | N/A | Yes (native) |

### Branch structure

```text
main (upstream v0.14.2)
  └── feature/native-transport    ← native SELECT + INSERT + infrastructure
       └── feature/connection-pooling  ← deadpool pool, cursor drain, health checks
            └── feature/lc-insert      ← LowCardinality INSERT + LC(Nullable) fix
                 └── feature/async-inserter  ← AsyncInserter, TableBatcher
```

Branches are designed to be merged in order. `feature/native-transport` is the
base PR; each subsequent branch stacks cleanly on top.
