# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

<!-- next-header -->

## [Unreleased] - ReleaseDate

## [0.1.0] - 2026-06-01

Initial release.

* Added `ArrowClientExt`, implemented for `clickhouse::Client` and providing `insert_arrow()`
* Added `ArrowQueryExt`, implemented for `clickhouse::query::Query` and providing `fetch_arrow()`
