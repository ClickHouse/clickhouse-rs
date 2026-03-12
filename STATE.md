# clickhouse-rs — HyperI Fork State

This document records the provenance, attribution, and current state of the
HyperI fork of [`ClickHouse/clickhouse-rs`][upstream].

[upstream]: https://github.com/ClickHouse/clickhouse-rs

---

## Fork Identity

**Maintainer:** HYPERI PTY LIMITED
**Organisation:** [hyperi-io](https://github.com/hyperi-io)
**Base upstream:** `ClickHouse/clickhouse-rs` v0.14.2
**Upstream tracking branch:** `origin/main`

This fork extends the official Rust client with features developed internally
at HyperI and in the DFE (Data Feed Engine) project.  Selected improvements
are candidates for upstreaming to `ClickHouse/clickhouse-rs`.

---

## Branch Strategy

| Branch | Base | Purpose | PR target |
|---|---|---|---|
| `hyperi/native-transport` | `main` | Native TCP protocol (SELECT + INSERT) | Upstream `main` |
| `hyperi/connection-pooling` | `native-transport` | Deadpool pool, cursor drain, health checks | `native-transport` |
| `hyperi/lc-insert` | `connection-pooling` | LowCardinality INSERT + LC(Nullable) fix | `connection-pooling` |
| `hyperi/async-inserter` | `lc-insert` | AsyncInserter, AsyncNativeInserter, TableBatcher, docs | `lc-insert` |
| `hyperi/batching` | `main` | HTTP TableBatcher (independent, from main) | Upstream `main` |

All branches carry the attributions below.

---

## Attribution

### HyperI — Organisational mark (all branches)

All work in this fork is produced by or under direction of HYPERI PTY LIMITED.
Commits on `hyperi/*` branches not credited to upstream contributors are
HyperI work.

### Native TCP protocol — ported from HyperI `clickhouse-arrow` fork

The native protocol implementation (`src/native/`) was ported from the HyperI
fork of [`clickhouse-arrow`][ch-arrow] (`/projects/clickhouse-arrow`).
`clickhouse-arrow` is the most complete HyperI-maintained Rust native-protocol
client and served as the primary reference for:

- All column type wire formats (see `src/native/columns.rs`)
- Variant, Dynamic, and JSON type handling
- LowCardinality wire format
- INSERT column encoding (`src/native/encode.rs`)

[ch-arrow]: https://github.com/hyperi-io/clickhouse-arrow

### Complete type support — migrated from HyperI `clickhouse-arrow` fork

Comprehensive ClickHouse type coverage including:

- **Scalar types:** BFloat16, Decimal32/64/128/256, Time, Time64, IPv4, IPv6,
  Enum8/Enum16, UUID, Date, Date32, DateTime, DateTime64, Point
- **Composite types:** Array, Tuple, Map, Nullable, LowCardinality,
  SimpleAggregateFunction
- **Modern types (24.x):** Variant, Dynamic, JSON (output as JSON strings)
- **Geo types:** Point, Ring, Polygon, MultiPolygon, LineString, MultiLineString

Reference: `clickhouse-arrow/src/types/` in the HyperI fork.

### Sparse serialization support — migrated from HyperI `clickhouse-arrow` fork

Per-column sparse (custom) serialization flag handling:
`src/native/sparse.rs` — offset reading for `custom_ser = 1` columns.
Currently returns an error for sparse columns; full support is a future item.

### Terminology alignment — ClickHouse Go client

Public API method names and configuration parameter names were deliberately
aligned with the [ClickHouse Go client][go-client] (`github.com/ClickHouse/clickhouse-go`),
which is the most mature native-protocol client and the de-facto reference
implementation.  Specific alignments:

| This crate | Go client | Notes |
|---|---|---|
| `TableBatcher::append()` | `Batch.Append()` | Add a row to the buffer |
| `TableBatcher::flush()` | `Batch.Flush()` | Force-flush without closing |
| `TableBatcher::send()` | `Batch.Send()` | Final flush + close |
| `BatchConfig::max_bytes` default 10 MiB | `MaxCompressionBuffer` 10 MiB | Matches async_insert_max_data_size |

[go-client]: https://github.com/ClickHouse/clickhouse-go

### Batching and accumulation — ported from DFE Loader

The per-table batch accumulation design (`src/batcher.rs`, `feature = "batcher"`)
was designed based on patterns in the HyperI DFE Loader project
(`/projects/dfe-loader/src/buffer/`).  Specifically:

- Per-table `HashMap<table, buffer>` pattern from `BufferManager`
- Three-threshold flush (rows / bytes / period) — DFE had rows + period;
  **bytes threshold was present in DFE config but not wired in** — fixed here
- `BatchConfig` defaults derived from DFE's `flush_rows: 20_000`,
  `flush_age_secs: 5`, updated to align with ClickHouse recommendations
- Parts-fragmentation research informed the `max_rows = 100_000` default

The `TableBatcher<T>` supersedes `BufferManager` for typed-row use cases.
DFE Loader may migrate to `TableBatcher` once it adopts typed row structs,
reducing its internal buffer management code.

### Insert optimisations — ported from DFE Loader

LZ4 compression for INSERT blocks was added to the native transport based on
patterns observed in DFE Loader's HTTP client.  The critical discovery that
ClickHouse sends `Log` and `ProfileEvents` data blocks **uncompressed** even
when `write_compression = 1` was identified during DFE integration testing.
See `src/native/reader.rs` — `Log | ProfileEvents` arm.

### LLM-assisted comment and documentation improvements (all branches)

Several source files in this fork contain comments and documentation that were
originally written quickly ("hacky") by Derek and have been improved with
LLM assistance (Claude Sonnet 4.6).  Affected areas:

- `src/native/` — module-level doc comments, inline protocol explanations
- `src/batcher.rs` — full API documentation
- `CLAUDE.md` — implementation state tracking
- This file

No logic was changed by LLM-assisted comment passes; only clarity and
completeness of documentation was improved.

---

## Upstream Sync Status

| Upstream version | Last synced | Notes |
|---|---|---|
| v0.14.2 | Branch base | `hyperi/native-transport` branched from this tag |

To sync with upstream: `git fetch origin && git rebase origin/main` on
`hyperi/native-transport`, then cascade to downstream branches.

---

## Known Gaps vs Upstream PR Readiness

See `CLAUDE.md` for the detailed implementation state table.  Items not yet
ready for upstreaming:

- Sparse (custom) serialization columns — returns error, not yet handled
- INSERT for Variant/Dynamic/JSON types
- AggregateFunction columns — low priority, opaque binary
- Connection pooling for the native transport
- Query cancellation / per-query settings on native transport
