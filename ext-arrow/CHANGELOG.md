# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

<!-- next-header -->

## Unreleased - ReleaseDate

## 0.2.0 - 2026-09-17

### Breaking

* Increased `arrow-*` crate version to `59.0.0`.

### Fixed

* Changed how product info is added internally so it doesn't end up in the user agent string ahead of any user-added
  product info. ([#484])

[#484]: https://github.com/ClickHouse/clickhouse-rs/pull/484

## 0.1.0 - 2026-06-01

Initial release.

* Added `ArrowClientExt`, implemented for `clickhouse::Client` and providing `insert_arrow()` and `insert_arrow_with()`
* Added `ArrowQueryExt`, implemented for `clickhouse::query::Query` and providing `fetch_arrow()`
