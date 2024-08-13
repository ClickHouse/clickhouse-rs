# ClickHouse Rust client examples

## Overview

We aim to cover various scenarios of client usage with these examples. You should be able to run any of these examples, see [How to run](#how-to-run) section below.

If something is missing, or you found a mistake in one of these examples, please open an issue or a pull request.

### General usage

- [usage.rs](usage.rs) - creating tables, executing other DDLs, inserting the data, and selecting it back. Additionally, it covers the client-side batching via the `inserter` feature, as well as `WATCH` queries. Optional cargo features: `inserter`, `watch`.
- [mock.rs](mock.rs) - writing tests with `mock` feature. Cargo features: requires `test-util`.
- [async_insert.rs](async_insert.rs) - using the server-side batching via the [asynchronous inserts](https://clickhouse.com/docs/en/optimize/asynchronous-inserts) ClickHouse feature
- [clickhouse_cloud.rs](clickhouse_cloud.rs) - using the client with ClickHouse Cloud, highlighting a few relevant settings (`wait_end_of_query`, `select_sequential_consistency`). Cargo features: requires `rustls-tls`; the code also works with `native-tls`.
- [clickhouse_settings.rs](clickhouse_settings.rs) - applying various ClickHouse settings on the query level

### Data types

- [data_types_derive_simple.rs](data_types_derive_simple.rs) - deriving simpler ClickHouse data types in a struct. Required cargo features: `time`, `uuid`.

### Special cases

- [custom_http_client.rs](custom_http_client.rs) - using a custom Hyper client, tweaking its connection pool settings
- [custom_http_headers.rs](custom_http_headers.rs) - setting additional HTTP headers to the client, or overriding the generated ones
- [query_id.rs](query_id.rs) - setting a specific `query_id` on the query level
- [session_id.rs](session_id.rs) - using the client in the session context with temporary tables

## How to run

### Prerequisites

* An [up-to-date Rust installation](https://www.rust-lang.org/tools/install)
* ClickHouse server (see below)

### Running the examples

The examples will require a running ClickHouse server on your machine. 

You could [install it directly](https://clickhouse.com/docs/en/install), or run it via Docker:

```sh
docker run -d -p 8123:8123 -p 9000:9000 --name chrs-clickhouse-server --ulimit nofile=262144:262144 clickhouse/clickhouse-server
```

Then, you should be able to run a particular example via the command-line with:

```sh
cargo run --package clickhouse --example async_insert
```

If a particular example requires a cargo feature, you could run it as follows:

```sh
cargo run --package clickhouse --example usage --features inserter watch
```

Additionally, the individual examples should be runnable via the IDE such as CLion or RustRover.