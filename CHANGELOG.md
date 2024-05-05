# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

<!-- next-header -->

## [Unreleased] - ReleaseDate
### Added
- derive: support `serde::skip_deserializing` ([#83]).
- insert: apply options set on the client ([#90]).
- inserter: can be limited by size, see `Inserter::with_max_bytes()`.
- inserter: `Inserter::pending()` to get stats about still being inserted data.
- inserter: `Inserter::force_commit()` to commit and insert immediately.

### Changed
- **BREAKING** inserter: move under the `inserter` feature.
- **BREAKING** inserter: there is no default limits anymore.
- **BREAKING** inserter: `Inserter::write` is synchronous now.
- **BREAKING** inserter: rename `entries` to `rows`.
- **BREAKING** drop the `wa-37420` feature.
- **BREAKING** remove deprecated items.
- inserter: improve performance of time measurements by using `quanta`.
- inserter: improve performance if the time limit isn't used.
- derive: move to syn v2.

### Fixed
- watch: support a new syntax.

[#83]: https://github.com/loyd/clickhouse.rs/pull/83
[#90]: https://github.com/loyd/clickhouse.rs/pull/90

## [0.11.6] - 2023-09-27
### Fixed
- client: accept HTTPs urls if `tls` feature is enabled ([#58]).

[#58]: https://github.com/loyd/clickhouse.rs/issues/56

## [0.11.5] - 2023-06-12
### Changed
- inserter: start new insert only when the first row is provided ([#68], [#70]).

[#70]: https://github.com/loyd/clickhouse.rs/pull/70
[#68]: https://github.com/loyd/clickhouse.rs/pull/68

## [0.11.4] - 2023-05-14
### Added
- query: `Query::fetch_optional()`.

### Changed
- query: increase performance up to 40%.

## [0.11.3] - 2023-02-19
### Added
- client: support HTTPS ([#54]).

### Changed
- query: improve throughput (~8%).

### Fixed
- cursor: handle errors sent at the end of a response ([#56]).

[#56]: https://github.com/loyd/clickhouse.rs/issues/56
[#54]: https://github.com/loyd/clickhouse.rs/pull/54

## [0.11.2] - 2023-01-03
### Added
- insert: `with_timeouts` to manage timeouts.
- inserter: `with_timeouts` and `set_timeouts` to manage timeouts.

### Changed
- insert: improve throughput (~30%).
- inserter: set a default value of `max_entries` to 500_000.

## [0.11.1] - 2022-11-25
### Added
- ipv4: `serde::ipv4` for ser/de the `IPv4` type to/from `Ipv4Addr`. Note that `IPv6` requires no annotations.
- time: `serde::time::datetime(64)` for ser/de the [`time::OffsetDateTime`] type to/from `DateTime` and `DateTime64`.
- time: `serde::time::date(32)` for ser/de the [`time::Date`] type to/from `Date` and `Date32`.
- serde: add `::option` variants to support `Option<_>`.

### Changed
- uuid: move to the `serde` submodule.

[`time::OffsetDateTime`]: https://docs.rs/time/latest/time/struct.OffsetDateTime.html
[`time::Date`]: https://docs.rs/time/latest/time/struct.Date.html

## [0.11.0] - 2022-11-10
### Added
- compression: implement Lz4/Lz4Hc compression modes for `INSERT`s ([#39]).
- insert: the `wa-37420` feature to avoid [ClickHouse#37420].
- inserter: new method `Inserter::time_left()`.
- uuid: the `uuid` feature and a corresponding module to ser/de [`uuid::Uuid`] ([#26]).

### Changed
- **BREAKING** decompression: HTTP compression (gzip, zlib and brotli) isn't available anymore, only Lz4.
- inserter: skip timer ticks if `INSERT` is too long ([#20]).

[#39]: https://github.com/loyd/clickhouse.rs/issues/39
[#26]: https://github.com/loyd/clickhouse.rs/issues/26
[#20]: https://github.com/loyd/clickhouse.rs/issues/20
[ClickHouse#37420]: https://github.com/ClickHouse/ClickHouse/issues/37420
[`uuid::Uuid`]: https://docs.rs/uuid/latest/uuid/struct.Uuid.html

## [0.10.0] - 2022-01-18
### Added
- client: `Client::with_http_client` to use custom `hyper::Client`, e.g. for https ([#27]).

### Changed
- watch: run `WATCH` queries with `max_execution_time=0`.
- bind: implement `Bind` for all `Serialize` instances ([#33]).

### Fixed
- Implement `Primitive` for `f64` ([#31]).

[#33]: https://github.com/loyd/clickhouse.rs/issues/33
[#31]: https://github.com/loyd/clickhouse.rs/issues/31
[#27]: https://github.com/loyd/clickhouse.rs/pull/27

## [0.9.3] - 2021-12-21
### Added
- Implement `Primitive` for `f64` and `f32` ([#29]).

### Fixed
- Reset quantities on errors to support reusing `Inserter` after errors ([#30]).

[#30]: https://github.com/loyd/clickhouse.rs/pull/30
[#29]: https://github.com/loyd/clickhouse.rs/issues/29

## [0.9.2] - 2021-11-01
### Changed
- HTTP Keep-alive timeout is restricted to 2s explicitly.

### Fixed
- watch: make a cursor cancellation safe.

## [0.9.1] - 2021-10-25
### Added
- mock: add `record_ddl` handler to test DDL queries.
- mock: add `watch` and `watch_only_events` handlers to test WATCH queries.

## [0.9.0] - 2021-10-25
### Fixed
- query: support borrowed long strings ([#22]).
- query: read the whole response of DDL queries.

### Changed
- **BREAKING**: watch: require the `watch` feature.
- **BREAKING**: watch: only struct rows are allowed because JSON requires names.
- query: queries with invalid URLs fail with `Error::InvalidParams`.
- watch: use `JSONEachRowWithProgress` because of [ClickHouse#22996] ([#23]).

[#23]: https://github.com/loyd/clickhouse.rs/issues/23
[#22]: https://github.com/loyd/clickhouse.rs/issues/22
[ClickHouse#22996]: https://github.com/ClickHouse/ClickHouse/issues/22996

## [0.8.1] - 2021-08-26
### Fixed
- Support `?` inside bound arguments ([#18]).
- Use the `POST` method if a query is bigger than 8KiB ([#19]).

[#19]: https://github.com/loyd/clickhouse.rs/issues/19
[#18]: https://github.com/loyd/clickhouse.rs/issues/18

## [0.8.0] - 2021-07-28
### Fixed
- `RowBinarySerializer::is_human_readable()` returns `false`.

## [0.7.2] - 2021-05-07
### Added
- `Watch::refresh()` to specify `REFRESH` clause.

### Fixed
- `derive(Row)`: handle raw identifiers.

## [0.7.1] - 2021-06-29
### Fixed
- Get rid of "socket is not connected" errors.

### Changed
- Set TCP keepalive to 60 seconds.

## [0.7.0] - 2021-05-31
### Changed
- Replace `reflection::Reflection` with `clickhouse::Row`. It's enough to implement `Row` for top-level `struct`s only.

### Added
- `#[derive(Row)]`

## [0.6.8] - 2021-05-28
### Fixed
- docs: enable the `doc_cfg` feature.

## [0.6.7] - 2021-05-28
### Fixed
- docs: show features on docs.rs.
- Now `test-util` implies `hyper/server`.

## [0.6.6] - 2021-05-28
### Added
- `test` module (available with the `test-util` feature).
- `#[must_use]` for `Query`, `Watch`, `Insert` and `Inserter`.

## [0.6.5] - 2021-05-24
### Added
- `&String` values binding to SQL queries.

## [0.6.4] - 2021-05-14
### Fixed
- Depend explicitly on `tokio/macros`.

## [0.6.3] - 2021-05-11
### Added
- Support for `bool` values storage ([#9]).
- `array`s' binding to SQL queries — useful at `IN` operators, etc ([#9]).
- `String` values binding to SQL queries ([#9]).
- `Query::fetch_all()`
- `sql::Identifier`

### Changed
- Expose `query::Bind` ([#11]).
- Deprecate `Compression::encoding()`.

[#11]: https://github.com/loyd/clickhouse.rs/pull/9
[#9]: https://github.com/loyd/clickhouse.rs/pull/9

## [0.6.2] - 2021-04-12
### Fixed
- watch: bind fileds of the type param.

## [0.6.1] - 2021-04-09
### Fixed
- compression: decompress error messages ([#7]).

[#7]: https://github.com/loyd/clickhouse.rs/pull/7

## [0.6.0] - 2021-03-24
### Changed
- Use tokio v1, hyper v0.14, bytes v1.

## [0.5.1] - 2020-11-22
### Added
- decompression: lz4.

## [0.5.0] - 2020-11-19
### Added
- decompression: gzip, zlib and brotli.

## [0.4.0] - 2020-11-17
### Added
- `Query::fetch_one()`, `Watch::fetch_one()`.
- `Query::fetch()` as a replacement for `Query::rows()`.
- `Watch::fetch()` as a replacement for `Watch::rows()`.
- `Watch::only_events().fetch()` as a replacement for `Watch::events()`.

### Changed
- `Error` is `StdError + Send + Sync + 'static` now.

## [0.3.0] - 2020-10-28
### Added
- Expose cursors (`query::RowCursor`, `watch::{RowCursor, EventCursor}`).

## [0.2.0] - 2020-10-14
### Added
- `Client::inserter()` for infinite inserting into tables.
- `Client::watch()` for `LIVE VIEW` related queries.

### Changed
- Renamed `Query::fetch()` to `Query::rows()`.
- Use `GET` requests for `SELECT` statements.

## [0.1.0] - 2020-10-14
### Added
- Support basic types.
- `Client::insert()` for inserting into tables.
- `Client::query()` for selecting from tables and DDL statements.

<!-- next-url -->
[Unreleased]: https://github.com/loyd/clickhouse.rs/compare/v0.11.6...HEAD
[0.11.6]: https://github.com/loyd/clickhouse.rs/compare/v0.11.5...v0.11.6
[0.11.5]: https://github.com/loyd/clickhouse.rs/compare/v0.11.4...v0.11.5
[0.11.4]: https://github.com/loyd/clickhouse.rs/compare/v0.11.3...v0.11.4
[0.11.3]: https://github.com/loyd/clickhouse.rs/compare/v0.11.2...v0.11.3
[0.11.2]: https://github.com/loyd/clickhouse.rs/compare/v0.11.1...v0.11.2
[0.11.1]: https://github.com/loyd/clickhouse.rs/compare/v0.11.0...v0.11.1
[0.11.0]: https://github.com/loyd/clickhouse.rs/compare/v0.10.0...v0.11.0
[0.10.0]: https://github.com/loyd/clickhouse.rs/compare/v0.9.3...v0.10.0
[0.9.3]: https://github.com/loyd/clickhouse.rs/compare/v0.9.2...v0.9.3
[0.9.2]: https://github.com/loyd/clickhouse.rs/compare/v0.9.1...v0.9.2
[0.9.1]: https://github.com/loyd/clickhouse.rs/compare/v0.9.0...v0.9.1
[0.9.0]: https://github.com/loyd/clickhouse.rs/compare/v0.8.1...v0.9.0
[0.8.1]: https://github.com/loyd/clickhouse.rs/compare/v0.8.0...v0.8.1
[0.8.0]: https://github.com/loyd/clickhouse.rs/compare/v0.7.2...v0.8.0
[0.7.2]: https://github.com/loyd/clickhouse.rs/compare/v0.7.1...v0.7.2
[0.7.1]: https://github.com/loyd/clickhouse.rs/compare/v0.7.0...v0.7.1
[0.7.0]: https://github.com/loyd/clickhouse.rs/compare/v0.6.8...v0.7.0
[0.6.8]: https://github.com/loyd/clickhouse.rs/compare/v0.6.7...v0.6.8
[0.6.7]: https://github.com/loyd/clickhouse.rs/compare/v0.6.6...v0.6.7
[0.6.6]: https://github.com/loyd/clickhouse.rs/compare/v0.6.5...v0.6.6
[0.6.5]: https://github.com/loyd/clickhouse.rs/compare/v0.6.4...v0.6.5
[0.6.4]: https://github.com/loyd/clickhouse.rs/compare/v0.6.3...v0.6.4
[0.6.3]: https://github.com/loyd/clickhouse.rs/compare/v0.6.2...v0.6.3
[0.6.2]: https://github.com/loyd/clickhouse.rs/compare/v0.6.1...v0.6.2
[0.6.1]: https://github.com/loyd/clickhouse.rs/compare/v0.6.0...v0.6.1
[0.6.0]: https://github.com/loyd/clickhouse.rs/compare/v0.5.1...v0.6.0
[0.5.1]: https://github.com/loyd/clickhouse.rs/compare/v0.5.0...v0.5.1
[0.5.0]: https://github.com/loyd/clickhouse.rs/compare/v0.4.0...v0.5.0
[0.4.0]: https://github.com/loyd/clickhouse.rs/compare/v0.3.0...v0.4.0
[0.3.0]: https://github.com/loyd/clickhouse.rs/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/loyd/clickhouse.rs/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/loyd/clickhouse.rs/releases/tag/v0.1.0
